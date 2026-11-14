//! PostgreSQL 作为目标：执行 DDL、批量写入、统计行数。

use anyhow::{Context, Result};
use futures_util::{SinkExt, pin_mut};
use tokio_postgres::{Client, NoTls};

use crate::ddl;
use crate::model::{Dialect, Table, Value};
use crate::value::bytes_to_pg_hex;

pub struct PgTarget {
    pub client: Client,
    pub ns: String,
}

pub async fn connect(url: &str, ns: &str) -> Result<PgTarget> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .context("连接目标 PostgreSQL 失败")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("目标 PostgreSQL 连接中断: {e}");
        }
    });
    // 固定 UTC，保证 timestamptz 文本值解释一致
    client
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .context("初始化目标 PostgreSQL 失败")?;
    // 并行任务会各自建连接，schema 已存在时忽略并发冲突
    let _ = client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            Dialect::Postgres.quote(ns)
        ))
        .await;
    Ok(PgTarget { client, ns: ns.to_string() })
}

/// 执行一条（可含多个语句的）SQL。
pub async fn exec(target: &PgTarget, sql: &str) -> Result<()> {
    target.client.batch_execute(sql).await.with_context(|| format!("执行 SQL 失败: {sql}"))
}

/// 摘掉目标库里这些表上的外键（TRUNCATE / 重建外键前都要先做），失败忽略。
pub async fn drop_foreign_keys(target: &PgTarget, tables: &[Table]) -> Result<()> {
    for table in tables {
        for sql in ddl::drop_foreign_key_statements(Dialect::Postgres, &target.ns, table) {
            let _ = exec(target, &sql).await;
        }
    }
    Ok(())
}

pub async fn truncate(target: &PgTarget, table: &Table) -> Result<()> {
    let sql = format!(
        "TRUNCATE TABLE {}",
        Dialect::Postgres.qualified(&target.ns, &table.name)
    );
    exec(target, &sql).await
}

pub async fn count(target: &PgTarget, table: &Table) -> Result<i64> {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}",
        Dialect::Postgres.qualified(&target.ns, &table.name)
    );
    let row = target.client.query_one(&sql, &[]).await.context("目标 PostgreSQL 统计行数失败")?;
    Ok(row.get(0))
}

/// 用 COPY 文本格式批量装载（仅 PG 目标）：比多行 INSERT 快一个量级。
/// 值都以文本传入，COPY 直接按列类型解析，不需要建临时表。
pub async fn copy_batch(target: &PgTarget, table: &Table, rows: &[Vec<Value>]) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let cols = table
        .columns
        .iter()
        .map(|c| Dialect::Postgres.quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "COPY {} ({cols}) FROM STDIN",
        Dialect::Postgres.qualified(&target.ns, &table.name)
    );

    let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * table.columns.len() * 8);
    for row in rows {
        for (i, value) in row.iter().enumerate() {
            if i > 0 {
                buf.push(b'\t');
            }
            match value {
                Value::Null => buf.extend_from_slice(b"\\N"),
                Value::Text(s) => copy_escape_text(&mut buf, s),
                Value::Bytes(b) => {
                    // COPY 文本格式里反斜杠要转义，bytea 的十六进制前缀写成 \\x
                    buf.extend_from_slice(b"\\\\x");
                    buf.extend_from_slice(hex::encode(b).as_bytes());
                }
            }
        }
        buf.push(b'\n');
    }

    let sink = target
        .client
        .copy_in(sql.as_str())
        .await
        .with_context(|| format!("启动 COPY 失败: {sql}"))?;
    pin_mut!(sink);
    sink.send(bytes::Bytes::from(buf))
        .await
        .with_context(|| format!("COPY 发送数据失败: {}", table.name))?;
    let n = sink
        .finish()
        .await
        .with_context(|| format!("COPY 写入 PostgreSQL 表 {} 失败", table.name))?;
    Ok(n)
}

/// COPY 文本格式的转义：\\ \t \n \r
fn copy_escape_text(out: &mut Vec<u8>, s: &str) {
    for b in s.as_bytes() {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            _ => out.push(*b),
        }
    }
}

/// 批量写入，参数统一以文本传输再转成目标列类型。
pub async fn write_batch(
    target: &PgTarget,
    table: &Table,
    rows: &[Vec<Value>],
    upsert: bool,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let ncols = table.columns.len().max(1);
    // PG 扩展协议最多 65535 个参数
    let max_rows = (60000 / ncols).max(1);
    let mut total = 0;
    for chunk in rows.chunks(max_rows) {
        total += write_chunk(target, table, chunk, upsert).await?;
    }
    Ok(total)
}

async fn write_chunk(
    target: &PgTarget,
    table: &Table,
    rows: &[Vec<Value>],
    upsert: bool,
) -> Result<u64> {
    let cols = table
        .columns
        .iter()
        .map(|c| Dialect::Postgres.quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    let mut tuples = Vec::with_capacity(rows.len());
    let mut params: Vec<Option<String>> = Vec::with_capacity(rows.len() * table.columns.len());
    let mut idx = 1usize;
    for row in rows {
        let mut items = Vec::with_capacity(table.columns.len());
        for (col, value) in table.columns.iter().zip(row.iter()) {
            let ty = ddl::pg_column_type(&target.ns, &table.name, col);
            items.push(format!("${idx}::text::{ty}"));
            params.push(match value {
                Value::Null => None,
                Value::Text(s) => Some(s.clone()),
                Value::Bytes(b) => Some(bytes_to_pg_hex(b)),
            });
            idx += 1;
        }
        tuples.push(format!("({})", items.join(", ")));
    }

    let mut sql = format!(
        "INSERT INTO {} ({cols}) VALUES {}",
        Dialect::Postgres.qualified(&target.ns, &table.name),
        tuples.join(", ")
    );
    if upsert && !table.primary_key.is_empty() {
        let pk = table
            .primary_key
            .iter()
            .map(|c| Dialect::Postgres.quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        let updates = table
            .columns
            .iter()
            .filter(|c| !table.primary_key.contains(&c.name))
            .map(|c| {
                let q = Dialect::Postgres.quote(&c.name);
                format!("{q} = EXCLUDED.{q}")
            })
            .collect::<Vec<_>>();
        if updates.is_empty() {
            sql.push_str(&format!(" ON CONFLICT ({pk}) DO NOTHING"));
        } else {
            sql.push_str(&format!(
                " ON CONFLICT ({pk}) DO UPDATE SET {}",
                updates.join(", ")
            ));
        }
    }

    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    target
        .client
        .execute(sql.as_str(), &refs)
        .await
        .with_context(|| format!("写入 PostgreSQL 表 {} 失败", table.name))?;
    Ok(rows.len() as u64)
}

/// 目标表已有列名，用于结构漂移时补列。
pub async fn existing_columns(target: &PgTarget, table: &Table) -> Result<Vec<String>> {
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&target.ns, &table.name];
    let rows = target
        .client
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2",
            &params,
        )
        .await
        .with_context(|| format!("读取目标 {} 的列失败", table.name))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// 目标表已有索引名（含主键），用于只补缺失索引。
pub async fn existing_indexes(target: &PgTarget, table: &Table) -> Result<Vec<String>> {
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&target.ns, &table.name];
    let rows = target
        .client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
            &params,
        )
        .await
        .with_context(|| format!("读取目标 {} 的索引失败", table.name))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// binlog CDC：按主键删除一行。主键值缺失时跳过，避免误删全表。
pub async fn delete_by_pk(target: &PgTarget, table: &Table, pk: &[Value]) -> Result<()> {
    let mut conds = Vec::new();
    let mut params: Vec<Option<String>> = Vec::new();
    for (i, name) in table.primary_key.iter().enumerate() {
        let Some(value) = pk.get(i) else { continue };
        let Some(col) = table.columns.iter().find(|c| c.name == *name) else { continue };
        let ty = ddl::pg_column_type(&target.ns, &table.name, col);
        conds.push(format!(
            "{} = ${}::text::{ty}",
            Dialect::Postgres.quote(name),
            params.len() + 1
        ));
        params.push(match value {
            Value::Null => None,
            Value::Text(s) => Some(s.clone()),
            Value::Bytes(b) => Some(bytes_to_pg_hex(b)),
        });
    }
    if conds.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM {} WHERE {}",
        Dialect::Postgres.qualified(&target.ns, &table.name),
        conds.join(" AND ")
    );
    let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
        .iter()
        .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    target
        .client
        .execute(sql.as_str(), &refs)
        .await
        .with_context(|| format!("按主键删除 {} 失败", table.name))?;
    Ok(())
}
