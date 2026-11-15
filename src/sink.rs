//! 目标端抽象：统一调度 PG / MySQL 的写入与 DDL 执行。

use anyhow::Result;

use crate::model::{ColumnState, Dialect, Endpoint, Table, Value};
use crate::{my_target, pg_target};

pub enum Sink {
    Pg(pg_target::PgTarget),
    My(my_target::MyTarget),
}

impl Sink {
    pub async fn connect(ep: &Endpoint) -> Result<Self> {
        match ep.dialect {
            Dialect::Postgres => Ok(Sink::Pg(
                pg_target::connect(&ep.url, &ep.namespace).await?,
            )),
            Dialect::Mysql => Ok(Sink::My(
                my_target::connect(&ep.url, &ep.namespace).await?,
            )),
        }
    }

    pub async fn exec(&mut self, sql: &str) -> Result<()> {
        match self {
            Sink::Pg(t) => pg_target::exec(t, sql).await,
            Sink::My(t) => my_target::exec(t, sql).await,
        }
    }

    pub async fn drop_foreign_keys(&mut self, tables: &[Table]) -> Result<()> {
        match self {
            Sink::Pg(t) => pg_target::drop_foreign_keys(t, tables).await,
            Sink::My(t) => my_target::drop_foreign_keys(t, tables).await,
        }
    }

    pub async fn truncate(&mut self, table: &Table) -> Result<()> {
        match self {
            Sink::Pg(t) => pg_target::truncate(t, table).await,
            Sink::My(t) => my_target::truncate(t, table).await,
        }
    }

    pub async fn count(&mut self, table: &Table) -> Result<i64> {
        match self {
            Sink::Pg(t) => pg_target::count(t, table).await,
            Sink::My(t) => my_target::count(t, table).await,
        }
    }

    /// 一批数据落库：copy 为真且不是 upsert 时优先走 PG COPY。
    pub async fn write_rows(
        &mut self,
        table: &Table,
        rows: &[Vec<Value>],
        upsert: bool,
        copy: bool,
    ) -> Result<u64> {
        if copy && !upsert && matches!(self, Sink::Pg(_)) {
            let mut attempt = 0;
            loop {
                attempt += 1;
                let result = match self {
                    Sink::Pg(t) => pg_target::copy_batch(t, table, rows).await,
                    Sink::My(_) => unreachable!(),
                };
                match result {
                    Ok(n) => return Ok(n),
                    Err(e) if attempt < 3 && retryable(&e) => {
                        eprintln!(
                            "  重试 {}（第 {} 次）：{}",
                            table.name,
                            attempt,
                            format!("{e:#}")
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(200 * attempt)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        self.write_batch(table, rows, upsert).await
    }

    /// 目标表已有的列名，用于结构漂移时补列。
    pub async fn existing_columns(&mut self, table: &Table) -> Result<Vec<String>> {
        match self {
            Sink::Pg(t) => pg_target::existing_columns(t, table).await,
            Sink::My(t) => my_target::existing_columns(t, table).await,
        }
    }

    /// 目标表已有列的类型与非空状态，用于结构漂移比对。
    pub async fn column_states(&mut self, table: &Table) -> Result<Vec<ColumnState>> {
        match self {
            Sink::Pg(t) => pg_target::column_states(t, table).await,
            Sink::My(t) => my_target::column_states(t, table).await,
        }
    }

    /// 目标表已有的索引名，用于只补缺失索引。
    pub async fn existing_indexes(&mut self, table: &Table) -> Result<Vec<String>> {
        match self {
            Sink::Pg(t) => pg_target::existing_indexes(t, table).await,
            Sink::My(t) => my_target::existing_indexes(t, table).await,
        }
    }

    /// 按主键删除一行（binlog CDC 的 DELETE，以及 UPDATE 改主键时清理旧行）。
    pub async fn delete_by_pk(&mut self, table: &Table, pk: &[Value]) -> Result<()> {
        match self {
            Sink::Pg(t) => pg_target::delete_by_pk(t, table, pk).await,
            Sink::My(t) => my_target::delete_by_pk(t, table, pk).await,
        }
    }

    /// 开启目标事务：binlog 按事务聚合后整体提交，保证目标端不会出现半个事务。
    pub async fn begin(&mut self) -> Result<()> {
        self.exec("BEGIN").await
    }

    pub async fn commit(&mut self) -> Result<()> {
        self.exec("COMMIT").await
    }

    /// 回滚失败不再上抛，真正的错误由调用方保留。
    pub async fn rollback(&mut self) {
        let _ = self.exec("ROLLBACK").await;
    }

    pub async fn write_batch(
        &mut self,
        table: &Table,
        rows: &[Vec<Value>],
        upsert: bool,
    ) -> Result<u64> {
        // 死锁/锁等待超时属于瞬时错误，退避重试；其它错误直接上抛
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = match self {
                Sink::Pg(t) => pg_target::write_batch(t, table, rows, upsert).await,
                Sink::My(t) => my_target::write_batch(t, table, rows, upsert).await,
            };
            match result {
                Ok(n) => return Ok(n),
                Err(e) if attempt < 3 && retryable(&e) => {
                    eprintln!("  重试 {}（第 {} 次）：{}", table.name, attempt, format!("{e:#}"));
                    tokio::time::sleep(std::time::Duration::from_millis(200 * attempt)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// 只重试死锁、序列化失败、锁等待超时，按数据库返回的错误码判断，避免依赖错误文本。
fn retryable(e: &anyhow::Error) -> bool {
    // PG：40P01=死锁，40001=序列化失败，55P03=锁不可用
    if let Some(err) = e.downcast_ref::<tokio_postgres::Error>() {
        return err
            .as_db_error()
            .is_some_and(|db| matches!(db.code().code(), "40P01" | "40001" | "55P03"));
    }
    // MySQL：1213=死锁，1205=锁等待超时
    if let Some(mysql_async::Error::Server(err)) = e.downcast_ref::<mysql_async::Error>() {
        return matches!(err.code, 1213 | 1205);
    }
    false
}
