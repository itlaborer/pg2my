//! 源端抽象：统一调度 PG / MySQL 的内省与读取。

use anyhow::{Result, bail};

use crate::model::{CopyStat, Dialect, Endpoint, IncrementalFilter, RowResume, Table};
use crate::sink::Sink;
use crate::{my_source, pg_direct, pg_source};

pub enum Source {
    Pg(pg_source::PgSource),
    My(my_source::MySource),
}

impl Source {
    pub async fn connect(ep: &Endpoint) -> Result<Self> {
        match ep.dialect {
            Dialect::Postgres => Ok(Source::Pg(
                pg_source::connect(&ep.url, &ep.namespace).await?,
            )),
            Dialect::Mysql => Ok(Source::My(
                my_source::connect(&ep.url, &ep.namespace).await?,
            )),
        }
    }

    /// 搬运专用连接：PG 加入共享快照，MySQL 开一致性快照事务。
    pub async fn connect_snapshot(ep: &Endpoint, snapshot: Option<&str>) -> Result<Self> {
        match ep.dialect {
            Dialect::Postgres => match snapshot {
                Some(id) => Ok(Source::Pg(
                    pg_source::connect_snapshot(&ep.url, &ep.namespace, id).await?,
                )),
                None => Ok(Source::Pg(
                    pg_source::connect(&ep.url, &ep.namespace).await?,
                )),
            },
            Dialect::Mysql => Ok(Source::My(
                my_source::connect_consistent(&ep.url, &ep.namespace).await?,
            )),
        }
    }

    pub async fn introspect(&mut self, filter: Option<&[String]>) -> Result<Vec<Table>> {
        match self {
            Source::Pg(s) => pg_source::introspect(s, filter).await,
            Source::My(s) => my_source::introspect(s, filter).await,
        }
    }

    pub async fn copy(
        &mut self,
        table: &Table,
        target: Dialect,
        batch_size: usize,
        filter: Option<&IncrementalFilter>,
        sink: &mut Sink,
        upsert: bool,
        watermark: Option<&str>,
        copy: bool,
        resume: Option<&RowResume>,
        where_clause: Option<&str>,
    ) -> Result<CopyStat> {
        match self {
            Source::Pg(s) => {
                pg_source::copy_table(
                    s,
                    table,
                    target,
                    batch_size,
                    filter,
                    sink,
                    upsert,
                    watermark,
                    copy,
                    resume,
                    where_clause,
                )
                .await
            }
            Source::My(s) => {
                my_source::copy_table(
                    s,
                    table,
                    batch_size,
                    filter,
                    sink,
                    upsert,
                    watermark,
                    copy,
                    resume,
                    where_clause,
                )
                .await
            }
        }
    }

    /// PG → PG 的 COPY 直通：源端字节流直接写入目标，仅用于无过滤、无续搬的整表搬运。
    pub async fn copy_direct(&self, table: &Table, sink: &mut Sink) -> Result<u64> {
        match (self, sink) {
            (Source::Pg(src), Sink::Pg(dst)) => pg_direct::run(src, dst, table).await,
            _ => bail!("COPY 直通仅支持 PostgreSQL → PostgreSQL"),
        }
    }

    pub async fn count(&mut self, table: &Table) -> Result<i64> {
        match self {
            Source::Pg(s) => pg_source::count(s, table).await,
            Source::My(s) => my_source::count(s, table).await,
        }
    }

    pub async fn max_text(&mut self, table: &Table, column: &str) -> Result<Option<String>> {
        match self {
            Source::Pg(s) => pg_source::max_text(s, table, column).await,
            Source::My(s) => my_source::max_text(s, table, column).await,
        }
    }

    /// 单表分片用：整数主键的 min/max。
    pub async fn pk_bounds(
        &mut self,
        table: &Table,
        filter: Option<&str>,
    ) -> Result<Option<(i64, i64)>> {
        match self {
            Source::Pg(s) => pg_source::pk_bounds(s, table, filter).await,
            Source::My(s) => my_source::pk_bounds(s, table, filter).await,
        }
    }
}
