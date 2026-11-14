//! MySQL 作为目标：执行 DDL、批量写入、统计行数。

use anyhow::{Context, Result};
use mysql_async::{
    Conn, Opts, OptsBuilder, Params, Value as MyValue, prelude::Queryable,
};

use crate::model::{Dialect, Table, Value};

pub struct MyTarget {
    pub conn: Conn,
    pub db: String,
}

pub async fn connect(url: &str, db: &str) -> Result<MyTarget> {
    let opts = Opts::from_url(url).context("MySQL 连接串格式不对")?;
    let opts = OptsBuilder::from_opts(opts).db_name(None::<String>);
    let mut conn = Conn::new(opts).await.context("连接目标 MySQL 失败")?;
    conn.query_drop(format!(
        "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4",
        Dialect::Mysql.quote(db)
    ))
    .await
    .context("创建目标库失败")?;
    conn.query_drop(format!("USE {}", Dialect::Mysql.quote(db)))
        .await
        .context("切换目标库失败")?;
    // 时区固定 UTC；NO_AUTO_VALUE_ON_ZERO 保证显式写入 0 时不会被当成自增
    conn.query_drop("SET SESSION time_zone = '+00:00'")
        .await
        .context("设置 MySQL 会话时区失败")?;
    conn.query_drop("SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_AUTO_VALUE_ON_ZERO')")
        .await
        .context("设置 MySQL sql_mode 失败")?;
    Ok(MyTarget { conn, db: db.to_string() })
}

pub async fn exec(target: &mut MyTarget, sql: &str) -> Result<()> {
    target.conn
        .query_drop(sql)
        .await
        .with_context(|| format!("执行 SQL 失败: {sql}"))
}

/// 摘掉目标库里这些表上的全部外键（TRUNCATE / 重建外键前都要先做）。
/// 按 information_schema 的实际名字删除，能兼容历史遗留的约束名。
pub async fn drop_foreign_keys(target: &mut MyTarget, tables: &[Table]) -> Result<()> {
    let rows: Vec<(String, String)> = target
        .conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL \
             GROUP BY TABLE_NAME, CONSTRAINT_NAME",
            Params::Positional(vec![MyValue::from(target.db.clone())]),
        )
        .await
        .context("读取目标 MySQL 外键列表失败")?;
    for (table, name) in rows {
        if !tables.iter().any(|t| t.name == table) {
            continue;
        }
        let sql = format!(
            "ALTER TABLE {} DROP FOREIGN KEY {}",
            Dialect::Mysql.qualified(&target.db, &table),
            Dialect::Mysql.quote(&name)
        );
        // 并发或已删除时会失败，忽略即可
        let _ = target.conn.query_drop(&sql).await;
    }
    Ok(())
}

pub async fn truncate(target: &mut MyTarget, table: &Table) -> Result<()> {
    let sql = format!(
        "TRUNCATE TABLE {}",
        Dialect::Mysql.qualified(&target.db, &table.name)
    );
    exec(target, &sql).await
}

pub async fn count(target: &mut MyTarget, table: &Table) -> Result<i64> {
    let sql = format!(
        "SELECT count(*) FROM {}",
        Dialect::Mysql.qualified(&target.db, &table.name)
    );
    let value: Option<i64> = target.conn.query_first(sql).await.context("目标 MySQL 统计行数失败")?;
    Ok(value.unwrap_or(0))
}

pub async fn write_batch(
    target: &mut MyTarget,
    table: &Table,
    rows: &[Vec<Value>],
    upsert: bool,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let ncols = table.columns.len().max(1);
    // MySQL 预处理语句最多 65535 个参数
    let max_rows = (60000 / ncols).max(1);
    let mut total = 0;
    for chunk in rows.chunks(max_rows) {
        total += write_chunk(target, table, chunk, upsert).await?;
    }
    Ok(total)
}

async fn write_chunk(
    target: &mut MyTarget,
    table: &Table,
    rows: &[Vec<Value>],
    upsert: bool,
) -> Result<u64> {
    let cols = table
        .columns
        .iter()
        .map(|c| Dialect::Mysql.quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholder = format!("({})", vec!["?"; table.columns.len()].join(", "));
    let values = vec![placeholder; rows.len()].join(", ");

    let mut sql = format!(
        "INSERT INTO {} ({cols}) VALUES {values}",
        Dialect::Mysql.qualified(&target.db, &table.name)
    );
    if upsert {
        let updates = table
            .columns
            .iter()
            .filter(|c| !table.primary_key.contains(&c.name))
            .map(|c| {
                let q = Dialect::Mysql.quote(&c.name);
                format!("{q} = VALUES({q})")
            })
            .collect::<Vec<_>>();
        if updates.is_empty() {
            // 只有主键列时写一个空更新，保证重复键不报错
            if let Some(pk) = table.primary_key.first() {
                let q = Dialect::Mysql.quote(pk);
                sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {q} = {q}"));
            }
        } else {
            sql.push_str(&format!(
                " ON DUPLICATE KEY UPDATE {}",
                updates.join(", ")
            ));
        }
    }

    let mut params: Vec<MyValue> = Vec::with_capacity(rows.len() * table.columns.len());
    for row in rows {
        for value in row {
            params.push(match value {
                Value::Null => MyValue::NULL,
                Value::Text(s) => MyValue::from(s.clone()),
                Value::Bytes(b) => MyValue::Bytes(b.clone()),
            });
        }
    }

    target.conn
        .exec_drop(sql, Params::Positional(params))
        .await
        .with_context(|| format!("写入 MySQL 表 {} 失败", table.name))?;
    Ok(rows.len() as u64)
}

/// 目标表已有列名，用于结构漂移时补列。
pub async fn existing_columns(target: &mut MyTarget, table: &Table) -> Result<Vec<String>> {
    let params = Params::Positional(vec![
        MyValue::from(target.db.clone()),
        MyValue::from(table.name.clone()),
    ]);
    let rows: Vec<mysql_async::Row> = target
        .conn
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            params,
        )
        .await
        .with_context(|| format!("读取目标 {} 的列失败", table.name))?;
    Ok(rows.iter().filter_map(|r| r.get::<String, _>(0)).collect())
}

/// 目标表已有索引名（含主键），用于只补缺失索引。
pub async fn existing_indexes(target: &mut MyTarget, table: &Table) -> Result<Vec<String>> {
    let params = Params::Positional(vec![
        MyValue::from(target.db.clone()),
        MyValue::from(table.name.clone()),
    ]);
    let rows: Vec<mysql_async::Row> = target
        .conn
        .exec(
            "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            params,
        )
        .await
        .with_context(|| format!("读取目标 {} 的索引失败", table.name))?;
    Ok(rows.iter().filter_map(|r| r.get::<String, _>(0)).collect())
}

/// binlog CDC：按主键删除一行。主键值缺失时跳过，避免误删全表。
pub async fn delete_by_pk(target: &mut MyTarget, table: &Table, pk: &[Value]) -> Result<()> {
    let mut conds = Vec::new();
    let mut params: Vec<MyValue> = Vec::new();
    for (i, name) in table.primary_key.iter().enumerate() {
        let Some(value) = pk.get(i) else { continue };
        if !table.columns.iter().any(|c| c.name == *name) {
            continue;
        }
        conds.push(format!("{} = ?", Dialect::Mysql.quote(name)));
        params.push(match value {
            Value::Null => MyValue::NULL,
            Value::Text(s) => MyValue::from(s.clone()),
            Value::Bytes(b) => MyValue::Bytes(b.clone()),
        });
    }
    if conds.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM {} WHERE {}",
        Dialect::Mysql.qualified(&target.db, &table.name),
        conds.join(" AND ")
    );
    target
        .conn
        .exec_drop(sql, Params::Positional(params))
        .await
        .with_context(|| format!("按主键删除 {} 失败", table.name))?;
    Ok(())
}
