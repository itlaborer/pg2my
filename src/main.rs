//! 数据库迁移工具：支持 PostgreSQL / MySQL 之间的结构与数据迁移。
//!
//! 方向由 `--source` / `--target` 的连接串自动判断，目前已覆盖：
//! pg→pg、mysql→mysql、pg→mysql、mysql→pg。

mod binlog_cdc;
mod cdc;
mod checkpoint;
mod ddl;
mod model;
mod my_source;
mod my_target;
mod pg_direct;
mod pg_source;
mod pg_target;
mod sink;
mod source;
mod type_map;
mod value;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::checkpoint::Checkpoint;
use crate::model::{
    ColumnKind, CopyStat, Dialect, Endpoint, RowResume, Table, apply_env_password,
};
use crate::sink::Sink;
use crate::source::Source;

#[derive(Parser, Debug)]
#[command(
    name = "pg2mysql",
    version,
    about = "PostgreSQL / MySQL 之间的结构与数据迁移：pg→pg、mysql→mysql、pg→mysql、mysql→pg"
)]
struct Cli {
    /// 源连接串：postgres://... 或 mysql://...
    #[arg(long, value_name = "URL")]
    source: String,

    /// 目标连接串：postgres://... 或 mysql://...
    #[arg(long, value_name = "URL")]
    target: String,

    /// 源命名空间（PG schema / MySQL 库名）。PG 默认 public，MySQL 取 URL 里的库名
    #[arg(long)]
    schema: Option<String>,

    /// 目标命名空间，默认与源相同
    #[arg(long)]
    db: Option<String>,

    /// 只迁这些表，逗号分隔；不填则整库
    #[arg(long)]
    tables: Option<String>,

    /// 排除这些表，逗号分隔，优先于 --tables
    #[arg(long)]
    exclude_tables: Option<String>,

    /// 每批写入的行数
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,

    /// 并行搬运的表数，每个任务独立建连接
    #[arg(long, default_value_t = 1)]
    parallel: usize,

    /// 单表按整数主键范围拆成 N 片并发搬运（总连接数约为 parallel×shards×2）
    #[arg(long, default_value_t = 1)]
    shards: usize,

    /// 写入前清空目标表
    #[arg(long)]
    truncate: bool,

    /// PG 目标用 COPY 批量装载（更快；upsert 场景自动退回 INSERT）
    #[arg(long)]
    copy: bool,

    /// 只打印目标方言的 DDL，不连目标库
    #[arg(long)]
    ddl_only: bool,

    /// 数据过滤条件，原始 SQL，附加到每张表的源查询（如 "created_at > '2024-01-01'"）
    #[arg(long = "where", value_name = "SQL")]
    where_clause: Option<String>,

    /// 只打印将要执行的 DDL/DML 计划，不连接目标库、不写数据
    #[arg(long)]
    dry_run: bool,

    /// 迁移完成后比对每张表的行数
    #[arg(long)]
    verify: bool,

    /// 断点文件路径，记录已完成表与增量水位
    #[arg(long)]
    checkpoint: Option<PathBuf>,

    /// 跳过断点文件里已完成的表
    #[arg(long)]
    resume: bool,

    /// 全量迁移后持续增量同步（按水位列轮询，不捕获删除）
    #[arg(long)]
    cdc: bool,

    /// 增量同步方式：poll=水位轮询（默认），binlog=MySQL binlog（需 ROW 格式与复制权限）
    #[arg(long, default_value = "poll", value_parser = ["poll", "binlog"])]
    cdc_mode: String,

    /// 增量同步的水位列，默认自动找 updated_at 等，找不到用主键
    #[arg(long)]
    cdc_column: Option<String>,

    /// 增量同步轮询间隔（秒）
    #[arg(long, default_value_t = 5)]
    cdc_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.parallel == 0 {
        bail!("--parallel 至少为 1");
    }
    if cli.resume && cli.checkpoint.is_none() {
        bail!("--resume 需要配合 --checkpoint <路径> 使用");
    }
    let use_binlog = cli.cdc && cli.cdc_mode == "binlog";

    let src_dialect = Dialect::detect(&cli.source)?;
    let dst_dialect = Dialect::detect(&cli.target)?;
    let src_ns = match &cli.schema {
        Some(s) => s.clone(),
        None => match src_dialect {
            Dialect::Postgres => "public".to_string(),
            Dialect::Mysql => mysql_db(&cli.source)?,
        },
    };
    let dst_ns = match &cli.db {
        Some(s) => s.clone(),
        None => match dst_dialect {
            // MySQL 目标优先用连接串里的库名
            Dialect::Mysql => mysql_db(&cli.target).unwrap_or_else(|_| src_ns.clone()),
            Dialect::Postgres => src_ns.clone(),
        },
    };

    let src_ep = Endpoint {
        dialect: src_dialect,
        url: apply_env_password(&cli.source, src_dialect),
        namespace: src_ns.clone(),
    };
    let dst_ep = Endpoint {
        dialect: dst_dialect,
        url: apply_env_password(&cli.target, dst_dialect),
        namespace: dst_ns.clone(),
    };

    let filter = cli.tables.as_ref().map(|s| {
        s.split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
    });

    println!(
        "{} ({}) → {} ({})",
        src_dialect.label(),
        src_ns,
        dst_dialect.label(),
        dst_ns
    );

    let mut src = Source::connect(&src_ep).await?;
    let mut tables = src.introspect(filter.as_deref()).await?;
    drop(src);

    // 排除表优先于 --tables
    if let Some(list) = &cli.exclude_tables {
        let excluded: Vec<&str> = list.split(',').map(|t| t.trim()).filter(|t| !t.is_empty()).collect();
        let before = tables.len();
        tables.retain(|t| !excluded.contains(&t.name.as_str()));
        if tables.len() != before {
            println!("按 --exclude-tables 排除 {} 张表", before - tables.len());
        }
    }

    if tables.is_empty() {
        println!("源命名空间 {} 下没有找到可迁移的表", src_ns);
        return Ok(());
    }
    println!("共 {} 张表", tables.len());

    // MySQL 没有 PG 的导出快照能力，各并行任务各自开一致性事务，跨表时点可能不同
    if src_dialect == Dialect::Mysql && cli.parallel > 1 && tables.len() > 1 && !use_binlog {
        eprintln!(
            "  警告：MySQL 源不支持跨表一致性快照，--parallel {} 时各表读取时点可能不同。",
            cli.parallel
        );
        eprintln!("        需要严格一致请用 --parallel 1，或改用 --cdc --cdc-mode binlog 在全量后追平。");
    }
    if src_dialect == Dialect::Mysql && cli.shards > 1 {
        eprintln!("  警告：MySQL 源按主键分片时，各片各自开启一致性快照，跨片时点可能不同。");
    }

    if cli.dry_run {
        print_ddl(src_dialect, dst_dialect, &dst_ns, &tables);
        println!("-- 数据搬运计划（每批 {} 行，并行 {}）：", cli.batch_size, cli.parallel);
        for table in &tables {
            let mut line = format!("--   {}", table.name);
            if let Some(w) = &cli.where_clause {
                line.push_str(&format!("  [WHERE {w}]"));
            }
            if cli.shards > 1 && shard_column(table).is_some() {
                line.push_str(&format!("  [主键分片 {}]", cli.shards));
            }
            println!("{line}");
        }
        return Ok(());
    }

    if cli.ddl_only {
        print_ddl(src_dialect, dst_dialect, &dst_ns, &tables);
        return Ok(());
    }

    let mut checkpoint = match &cli.checkpoint {
        Some(path) if cli.resume => Checkpoint::load(path)?,
        _ => Checkpoint::default(),
    };
    // --truncate 会把目标表清空，之前记录的“已完成”和行级断点都不再可信
    if cli.truncate {
        checkpoint.completed.clear();
        checkpoint.progresses.clear();
        // 目标被清空后必须重新全搬，binlog 位点也重新取
        checkpoint.binlog = None;
    }
    // 轮询水位也必须在全量之前记录：若等全量结束再取 max，
    // 全量期间发生的更新会落在该 max 之前，增量阶段用 > 比较就再也扫不到。
    if cli.cdc && !use_binlog {
        let opts = cdc::CdcOptions {
            interval_secs: cli.cdc_interval,
            column: cli.cdc_column.clone(),
            batch_size: cli.batch_size,
        };
        cdc::prime_watermarks(&src_ep, &tables, &opts, &mut checkpoint, cli.checkpoint.as_deref())
            .await?;
    }
    let checkpoint = Arc::new(Mutex::new(checkpoint));

    // binlog 增量同步：在开始搬数据之前记下位点，之后才能补上迁移期间的变更
    if use_binlog {
        binlog_cdc::ensure_supported(&src_ep)?;
        if checkpoint.lock().unwrap().binlog.is_none() {
            let pos = binlog_cdc::current_position(&src_ep.url).await?;
            println!("binlog 起点：{}:{}", pos.file, pos.pos);
            let mut cp = checkpoint.lock().unwrap();
            cp.binlog = Some(pos);
            if let Some(path) = &cli.checkpoint {
                cp.save(path)?;
            }
        }
    }

    // 1. 建表（含枚举类型）
    let mut target = Sink::connect(&dst_ep).await?;
    for table in &tables {
        if dst_dialect == Dialect::Mysql && !table.generated_columns.is_empty() {
            for g in &table.generated_columns {
                eprintln!(
                    "  提示 {}.{} 是生成列，表达式为 PG 方言，不会在 MySQL 目标重建",
                    table.name, g.name
                );
            }
        }
        for sql in ddl::create_statements(src_dialect, dst_dialect, &dst_ns, table) {
            target
                .exec(&sql)
                .await
                .with_context(|| format!("建表 {} 失败", table.name))?;
        }
    }
    // 结构漂移：目标表已存在时 CREATE TABLE IF NOT EXISTS 不会补新列/新索引
    for table in &tables {
        let existing = target
            .existing_columns(table)
            .await
            .with_context(|| format!("读取目标 {} 结构失败", table.name))?;
        let missing: Vec<String> = table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .filter(|name| !existing.contains(name))
            .collect();
        if !missing.is_empty() {
            println!("  补列 {:<32} {}", table.name, missing.join(", "));
            for sql in ddl::add_column_statements(src_dialect, dst_dialect, &dst_ns, table, &missing) {
                target
                    .exec(&sql)
                    .await
                    .with_context(|| format!("补列 {} 失败", table.name))?;
            }
        }
        // MySQL 的索引内联在建表语句里，旧表需要单独补
        if dst_dialect == Dialect::Mysql {
            let existing_idx = target
                .existing_indexes(table)
                .await
                .with_context(|| format!("读取目标 {} 索引失败", table.name))?;
            let missing_idx: Vec<String> = table
                .indexes
                .iter()
                .map(|i| i.name.clone())
                .filter(|name| !existing_idx.contains(name))
                .collect();
            if !missing_idx.is_empty() {
                println!("  补索引 {:<32} {}", table.name, missing_idx.join(", "));
                for sql in ddl::add_index_statements(&dst_ns, table, &missing_idx) {
                    target
                        .exec(&sql)
                        .await
                        .with_context(|| format!("补索引 {} 失败", table.name))?;
                }
            }
        }
    }
    // 先摘掉目标表已有外键，否则 --truncate 会被引用关系挡住；搬完会重新加上
    if cli.truncate {
        target.drop_foreign_keys(&tables).await?;
    }
    drop(target);

    // 2. 并行搬运数据
    // 源库一致性快照：PG 导出后各并行任务共享同一快照；MySQL 由每个任务自己开一致性事务
    let (_snapshot_guard, snapshot) = if src_dialect == Dialect::Postgres {
        let (guard, id) = pg_source::export_snapshot(&src_ep.url).await?;
        (Some(guard), Some(id))
    } else {
        (None, None)
    };

    let started = Instant::now();
    let semaphore = Arc::new(Semaphore::new(cli.parallel));
    let mut tasks = JoinSet::new();
    let mut skipped = 0usize;
    let copy_mode = cli.copy;

    for table in &tables {
        if checkpoint.lock().unwrap().is_done(&table.name) {
            skipped += 1;
            continue;
        }
        let permit = semaphore.clone().acquire_owned().await?;
        let src_ep = src_ep.clone();
        let dst_ep = dst_ep.clone();
        let batch_size = cli.batch_size;
        let shards = cli.shards;
        let truncate = cli.truncate;
        let snapshot = snapshot.clone();
        let where_clause = cli.where_clause.clone();
        let table = table.clone();
        // 行级断点：续搬时已有进度，必须用 upsert 吸收可能重复的少量行
        let resume = build_resume(&checkpoint, &cli, &table);
        let upsert = resume.as_ref().is_some_and(|r| r.after.is_some());
        tasks.spawn(async move {
            let _permit = permit;
            let t0 = Instant::now();
            let result = async {
                let mut src = Source::connect_snapshot(&src_ep, snapshot.as_deref()).await?;
                let mut sink = Sink::connect(&dst_ep).await?;
                if truncate {
                    sink.truncate(&table).await?;
                }
                // 单表分片：整数单列主键、非续搬、非 upsert 时按主键范围拆成多个并发任务
                if shards > 1 && !upsert && resume.is_none() {
                    if let Some(pk) = shard_column(&table) {
                        let conds = shard_conditions(
                            &mut src,
                            src_ep.dialect,
                            &table,
                            pk,
                            shards,
                            where_clause.as_deref(),
                        )
                        .await?;
                        if !conds.is_empty() {
                            // 先释放外层连接，避免与各分片连接叠加
                            drop(sink);
                            let mut futs = Vec::with_capacity(conds.len());
                            for cond in conds {
                                let src_ep = src_ep.clone();
                                let dst_ep = dst_ep.clone();
                                let snapshot = snapshot.clone();
                                let table = table.clone();
                                futs.push(async move {
                                    let mut s =
                                        Source::connect_snapshot(&src_ep, snapshot.as_deref()).await?;
                                    let mut k = Sink::connect(&dst_ep).await?;
                                    s.copy(
                                        &table,
                                        dst_ep.dialect,
                                        batch_size,
                                        None,
                                        &mut k,
                                        false,
                                        None,
                                        copy_mode,
                                        None,
                                        Some(&cond),
                                    )
                                    .await
                                });
                            }
                            let stats = futures_util::future::try_join_all(futs).await?;
                            let rows = stats.iter().map(|s| s.rows).sum();
                            return Ok(CopyStat { rows, last_key: None });
                        }
                    }
                }
                let copy = copy_mode;
                // pg→pg 且无过滤/续搬时，COPY 字节流直通，省掉逐行文本转换
                let direct = copy
                    && src_ep.dialect == Dialect::Postgres
                    && dst_ep.dialect == Dialect::Postgres
                    && !upsert
                    && where_clause.is_none();
                if direct {
                    let rows = src.copy_direct(&table, &mut sink).await?;
                    Ok(CopyStat { rows, last_key: None })
                } else {
                    src.copy(
                        &table,
                        dst_ep.dialect,
                        batch_size,
                        None,
                        &mut sink,
                        upsert,
                        None,
                        copy,
                        resume.as_ref(),
                        where_clause.as_deref(),
                    )
                    .await
                }
            }
            .await;
            (table.name, result, t0.elapsed().as_secs_f64())
        });
    }

    if skipped > 0 {
        println!("断点续传：跳过已完成 {} 张表", skipped);
    }

    let mut failed: Vec<String> = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((name, Ok(stat), secs)) => {
                println!("  ok  {:<32} {:>10} 行  {:.1}s", name, stat.rows, secs);
                let mut cp = checkpoint.lock().unwrap();
                cp.mark_done(&name, stat.rows);
                if let Some(path) = &cli.checkpoint {
                    cp.save(path)?;
                }
            }
            Ok((name, Err(e), _)) => {
                println!("  err {:<32} {e:#}", name);
                failed.push(name);
            }
            Err(e) => eprintln!("任务异常退出: {e}"),
        }
    }

    // 3. 数据搬完后补索引 / 注释 / 外键 / 序列
    let mut target = Sink::connect(&dst_ep).await?;
    let mut warnings = 0usize;
    // 先摘已有外键再重建，保证重复执行不报“约束已存在”
    target.drop_foreign_keys(&tables).await?;
    for table in &tables {
        if failed.contains(&table.name) {
            continue;
        }
        let statements = ddl::index_statements(dst_dialect, &dst_ns, table)
            .into_iter()
            .chain(ddl::comment_statements(dst_dialect, &dst_ns, table))
            .chain(ddl::foreign_key_statements(dst_dialect, &dst_ns, table))
            .chain(ddl::sequence_statements(dst_dialect, &dst_ns, table));
        for sql in statements {
            if let Err(e) = target.exec(&sql).await {
                eprintln!("  warn {}: {e}", table.name);
                warnings += 1;
            }
        }
    }

    // 4. 行数校验
    if cli.verify {
        let mut src = Source::connect(&src_ep).await?;
        let mut target = Sink::connect(&dst_ep).await?;
        println!("\n行数校验：");
        for table in &tables {
            if failed.contains(&table.name) {
                continue;
            }
            let a = src.count(table).await?;
            let b = target.count(table).await?;
            let mark = if a == b { "ok " } else { "BAD" };
            println!("  {mark} {:<32} src={:<12} dst={}", table.name, a, b);
        }
    }

    println!("\n全量迁移完成，耗时 {:.1}s", started.elapsed().as_secs_f64());
    if warnings > 0 {
        println!("有 {warnings} 条补充 DDL 未成功（见上方 warn）");
    }
    if !failed.is_empty() {
        bail!("{} 张表迁移失败: {}", failed.len(), failed.join(", "));
    }

    // 5. 增量同步
    if cli.cdc {
        let mut cp = checkpoint.lock().unwrap().clone();
        if use_binlog {
            binlog_cdc::run(
                &src_ep,
                &dst_ep,
                &tables,
                &mut cp,
                cli.checkpoint.as_deref(),
                std::process::id().max(1),
            )
            .await?;
        } else {
            let opts = cdc::CdcOptions {
                interval_secs: cli.cdc_interval,
                column: cli.cdc_column.clone(),
                batch_size: cli.batch_size,
            };
            cdc::run(
                &src_ep,
                &dst_ep,
                &tables,
                &opts,
                &mut cp,
                cli.checkpoint.as_deref(),
            )
            .await?;
        }
    }
    Ok(())
}

/// 构造行级断点：仅 `--resume` 且单列主键时启用，进度最多每秒落盘一次。
fn build_resume(checkpoint: &Arc<Mutex<Checkpoint>>, cli: &Cli, table: &Table) -> Option<RowResume> {
    if !cli.resume || table.primary_key.len() != 1 {
        return None;
    }
    let column = table.primary_key[0].clone();
    if !table.columns.iter().any(|c| c.name == column) {
        return None;
    }
    let after = checkpoint.lock().ok()?.progress(&table.name).cloned();
    if let Some(after) = &after {
        println!("  续搬 {:<32} 主键 {} > {}", table.name, column, after);
    }
    let path = cli.checkpoint.clone();
    let shared = checkpoint.clone();
    let name = table.name.clone();
    let last_save = Arc::new(Mutex::new(
        Instant::now() - std::time::Duration::from_secs(5),
    ));
    Some(RowResume {
        column,
        after,
        on_progress: Some(Arc::new(move |key: &str| {
            let Ok(mut t) = last_save.lock() else {
                return;
            };
            if t.elapsed() < std::time::Duration::from_secs(1) {
                return;
            }
            *t = Instant::now();
            drop(t);
            if let Ok(mut cp) = shared.lock() {
                cp.set_progress(&name, key);
                if let Some(p) = &path {
                    let _ = cp.save(p);
                }
            }
        })),
    })
}

fn print_ddl(src: Dialect, target: Dialect, ns: &str, tables: &[Table]) {
    for table in tables {
        println!("-- {}.{}", table.schema, table.name);
        for sql in ddl::create_statements(src, target, ns, table)
            .into_iter()
            .chain(ddl::index_statements(target, ns, table))
            .chain(ddl::comment_statements(target, ns, table))
            .chain(ddl::foreign_key_statements(target, ns, table))
        {
            println!("{sql};\n");
        }
    }
}

fn mysql_db(url: &str) -> Result<String> {
    let opts = mysql_async::Opts::from_url(url).context("MySQL 连接串格式不对")?;
    opts.db_name()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("MySQL 源连接串里没有库名，请用 --schema 指定"))
}

/// 只有“单列、整数”主键才能按范围无重叠地切分。
fn shard_column(table: &Table) -> Option<&str> {
    if table.primary_key.len() != 1 {
        return None;
    }
    let name = table.primary_key[0].as_str();
    let col = table.columns.iter().find(|c| c.name == name)?;
    matches!(col.kind, ColumnKind::SmallInt | ColumnKind::Int | ColumnKind::BigInt)
        .then_some(name)
}

/// 把整数主键区间均分成 n 段，返回每段可直接追加到源查询的过滤条件。
async fn shard_conditions(
    src: &mut Source,
    dialect: Dialect,
    table: &Table,
    pk: &str,
    shards: usize,
    extra: Option<&str>,
) -> Result<Vec<String>> {
    let Some((min, max)) = src.pk_bounds(table, extra).await? else {
        // 空表（或全被 where 过滤）：退回单任务搬运
        return Ok(Vec::new());
    };
    // 用 i128 防溢出：i64 全区间做减法会越界
    let span = max as i128 - min as i128 + 1;
    let n = (shards as i128).min(span).max(1);
    let chunk = (span + n - 1) / n;
    let q = dialect.quote(pk);
    let mut out = Vec::new();
    let mut lo = min as i128;
    while lo <= max as i128 {
        let hi = (lo + chunk - 1).min(max as i128);
        out.push(match extra {
            Some(w) => format!("({w}) AND {q} >= {lo} AND {q} <= {hi}"),
            None => format!("{q} >= {lo} AND {q} <= {hi}"),
        });
        lo = hi + 1;
    }
    Ok(out)
}
