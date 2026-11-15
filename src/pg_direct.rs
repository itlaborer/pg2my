//! PG → PG 的 COPY 直通：源端 `COPY TO STDOUT` 的字节流直接灌入目标端 `COPY FROM STDIN`。
//!
//! 与先 `SELECT ...::text` 再逐行转换相比，省掉了全部文本解析与参数绑定，
//! 是 pg→pg 场景下最快的搬运方式。只在无过滤、无续搬、非 upsert 时启用。

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt, pin_mut};

use crate::model::{Dialect, Table};
use crate::pg_source::PgSource;
use crate::pg_target::PgTarget;

pub async fn run(src: &PgSource, dst: &PgTarget, table: &Table) -> Result<u64> {
    let q = Dialect::Postgres;
    let cols = table
        .columns
        .iter()
        .map(|c| q.quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let src_sql = format!(
        "COPY (SELECT {cols} FROM {}) TO STDOUT",
        q.qualified(&src.ns, &table.name)
    );
    let dst_sql = format!(
        "COPY {} ({cols}) FROM STDIN",
        q.qualified(&dst.ns, &table.name)
    );

    // 目标端整表一个事务：中途失败回滚，不留半张表的数据
    dst.client
        .batch_execute("BEGIN")
        .await
        .with_context(|| format!("开启目标 {} 事务失败", table.name))?;
    match pipe(src, dst, &src_sql, &dst_sql, &table.name).await {
        Ok(rows) => {
            dst.client
                .batch_execute("COMMIT")
                .await
                .with_context(|| format!("提交目标 {} 失败", table.name))?;
            Ok(rows)
        }
        Err(e) => {
            let _ = dst.client.batch_execute("ROLLBACK").await;
            Err(e)
        }
    }
}

async fn pipe(
    src: &PgSource,
    dst: &PgTarget,
    src_sql: &str,
    dst_sql: &str,
    table: &str,
) -> Result<u64> {
    let out = src
        .client
        .copy_out(src_sql)
        .await
        .with_context(|| format!("启动源 COPY 失败: {table}"))?;
    let sink = dst
        .client
        .copy_in(dst_sql)
        .await
        .with_context(|| format!("启动目标 COPY 失败: {table}"))?;
    pin_mut!(out, sink);

    let mut rows = 0u64;
    while let Some(chunk) = out.next().await {
        let chunk = chunk.with_context(|| format!("读取源 {table} 的 COPY 数据失败"))?;
        // COPY 文本格式一行一条记录，正文里的换行已被转义为 \n
        rows += chunk.iter().filter(|b| **b == b'\n').count() as u64;
        sink.send(chunk)
            .await
            .with_context(|| format!("写入目标 {table} 失败"))?;
    }
    sink.finish()
        .await
        .with_context(|| format!("完成目标 {table} 的 COPY 失败"))?;
    Ok(rows)
}
