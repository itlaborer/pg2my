//! 增量同步（CDC）。
//!
//! 这里采用「水位列轮询」的方式：每轮只读取水位列大于上次位点的行，然后 upsert 到目标。
//! 默认自动寻找 updated_at / update_time 等列，没有的话退回主键（只能捕获新增，捕获不到更新）。
//!
//! 明确的限制：轮询方式无法捕获 DELETE；需要物理删除同步时应改用逻辑复制/binlog。

use std::path::Path;
use std::time::Duration;

use anyhow::Result;

use crate::checkpoint::Checkpoint;
use crate::model::{Endpoint, IncrementalFilter, Table};
use crate::sink::Sink;
use crate::source::Source;

pub struct CdcOptions {
    pub interval_secs: u64,
    pub column: Option<String>,
    pub batch_size: usize,
}

/// 选择水位列：显式指定 > 常见更新时间列 > 主键第一列。
fn watermark_column(table: &Table, explicit: Option<&str>) -> Option<String> {
    if let Some(name) = explicit {
        return table
            .columns
            .iter()
            .any(|c| c.name == name)
            .then(|| name.to_string());
    }
    for candidate in ["updated_at", "update_time", "modified_at", "gmt_modified", "last_modified"] {
        if let Some(col) = table.columns.iter().find(|c| c.name.eq_ignore_ascii_case(candidate)) {
            return Some(col.name.clone());
        }
    }
    table.primary_key.first().cloned()
}

/// 全量搬运之前记录每个表的水位起点。
///
/// 必须早于全量开始：如果在全量结束后才取 `max(水位列)`，全量期间发生的更新
/// 时间戳会小于该最大值，增量阶段用 `>` 比较就永远扫不到。提前记录后，
/// 全量期间的变化都大于起点，会在第一轮增量里被补上（重复行由 upsert 吸收）。
pub async fn prime_watermarks(
    src_ep: &Endpoint,
    tables: &[Table],
    opts: &CdcOptions,
    checkpoint: &mut Checkpoint,
    checkpoint_path: Option<&Path>,
) -> Result<()> {
    let mut src = Source::connect(src_ep).await?;
    for table in tables {
        let Some(col) = watermark_column(table, opts.column.as_deref()) else {
            continue;
        };
        // 没有主键就无法 upsert，和正式增量阶段保持一致直接跳过
        if table.primary_key.is_empty() || checkpoint.watermark(&table.name).is_some() {
            continue;
        }
        if let Some(max) = src.max_text(table, &col).await? {
            checkpoint.set_watermark(&table.name, &max);
            println!("  增量起点 {:<28} {} = {}", table.name, col, max);
        }
    }
    if let Some(path) = checkpoint_path {
        checkpoint.save(path)?;
    }
    Ok(())
}

pub async fn run(
    src_ep: &Endpoint,
    dst_ep: &Endpoint,
    tables: &[Table],
    opts: &CdcOptions,
    checkpoint: &mut Checkpoint,
    checkpoint_path: Option<&Path>,
) -> Result<()> {
    let mut src = Source::connect(src_ep).await?;
    let mut sink = Sink::connect(dst_ep).await?;

    // 初始化水位：全量迁移后从当前最大值开始，避免重复搬全表
    for table in tables {
        let Some(col) = watermark_column(table, opts.column.as_deref()) else {
            println!("  跳过 {}：没有可用作水位的列", table.name);
            continue;
        };
        if table.primary_key.is_empty() {
            println!("  跳过 {}：没有主键，无法 upsert", table.name);
            continue;
        }
        if checkpoint.watermark(&table.name).is_none() {
            if let Some(max) = src.max_text(table, &col).await? {
                checkpoint.set_watermark(&table.name, &max);
            }
        }
        println!("  表 {:<32} 水位列 {}", table.name, col);
    }
    if let Some(path) = checkpoint_path {
        checkpoint.save(path)?;
    }

    println!("\n进入增量同步，每 {}s 轮询一次，Ctrl-C 退出", opts.interval_secs);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("收到中断信号，退出增量同步");
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_secs(opts.interval_secs.max(1))) => {}
        }

        for table in tables {
            let Some(col) = watermark_column(table, opts.column.as_deref()) else {
                continue;
            };
            if table.primary_key.is_empty() {
                continue;
            }
            let filter = checkpoint
                .watermark(&table.name)
                .map(|after| IncrementalFilter { column: col.clone(), after: after.clone() });
            let stat = match src
                .copy(
                    table,
                    dst_ep.dialect,
                    opts.batch_size,
                    filter.as_ref(),
                    &mut sink,
                    true,
                    Some(&col),
                    false,
                    None,
                    None,
                )
                .await
            {
                Ok(stat) => stat,
                Err(e) => {
                    eprintln!("  cdc {:<32} 失败: {e:#}", table.name);
                    continue;
                }
            };
            if stat.rows > 0 {
                if let Some(last) = &stat.last_key {
                    checkpoint.set_watermark(&table.name, last);
                }
                println!("  cdc {:<32} {:>8} 行", table.name, stat.rows);
                if let Some(path) = checkpoint_path {
                    checkpoint.save(path)?;
                }
            }
        }
    }
}
