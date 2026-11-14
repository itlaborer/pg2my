//! PostgreSQL 作为源：内省表结构、流式读取数据。

use std::collections::HashMap;

use anyhow::{Context, Result};
use futures_util::{TryStreamExt, pin_mut};
use tokio_postgres::{Client, NoTls};

use crate::model::{
    Column, ColumnKind, CopyStat, Dialect, ForeignKey, GeneratedColumn, IncrementalFilter, Index,
    RowResume, Table, Value,
};
use crate::sink::Sink;
use crate::type_map;

pub struct PgSource {
    pub client: Client,
    pub ns: String,
}

pub async fn connect(url: &str, ns: &str) -> Result<PgSource> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .context("连接 PostgreSQL 失败")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL 连接中断: {e}");
        }
    });
    client
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .context("设置 PostgreSQL 时区失败")?;
    Ok(PgSource { client, ns: ns.to_string() })
}

/// 搬运专用连接：在导出的快照上开一个只读事务，保证多张表看到同一时间点。
pub async fn connect_snapshot(url: &str, ns: &str, snapshot: &str) -> Result<PgSource> {
    let src = connect(url, ns).await?;
    src.client
        .batch_execute(&format!(
            "BEGIN ISOLATION LEVEL REPEATABLE READ; \
             SET TRANSACTION SNAPSHOT '{}'",
            snapshot.replace('\'', "''")
        ))
        .await
        .context("在导出快照上开启事务失败")?;
    Ok(src)
}

/// 导出一个可被其他连接共享的快照。返回的 PgSource 必须保持存活到搬运结束，否则快照失效。
pub async fn export_snapshot(url: &str) -> Result<(PgSource, String)> {
    let src = connect(url, "public").await?;
    src.client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .context("开启快照事务失败")?;
    let row = src
        .client
        .query_one("SELECT pg_export_snapshot()", &[])
        .await
        .context("导出 PostgreSQL 快照失败")?;
    let id: String = row.get(0);
    Ok((src, id))
}

const TABLES_SQL: &str = r#"
SELECT c.relname::text
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND NOT c.relispartition
  AND n.nspname = $1
  AND ($2::text[] IS NULL OR c.relname = ANY($2))
ORDER BY c.relname
"#;

const COLUMNS_SQL: &str = r#"
SELECT a.attname::text,
       format_type(a.atttypid, a.atttypmod),
       a.attnotnull,
       pg_get_expr(d.adbin, d.adrelid),
       (a.attidentity <> ''),
       t.typtype::text,
       t.typcategory::text,
       et.typtype::text,
       CASE WHEN t.typelem <> 0 THEN format_type(t.typelem, NULL) END,
       col_description(a.attrelid, a.attnum)
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_type et ON et.oid = t.typelem
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = $1::text::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attgenerated = ''
ORDER BY a.attnum
"#;

const GENERATED_SQL: &str = r#"
SELECT a.attname::text,
       format_type(a.atttypid, a.atttypmod),
       t.typtype::text,
       t.typcategory::text,
       et.typtype::text,
       CASE WHEN t.typelem <> 0 THEN format_type(t.typelem, NULL) END,
       pg_get_expr(d.adbin, d.adrelid),
       col_description(a.attrelid, a.attnum)
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_type et ON et.oid = t.typelem
JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = $1::text::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND a.attgenerated = 's'
ORDER BY a.attnum
"#;

const ENUM_SQL: &str = r#"
SELECT a.attname::text, e.enumlabel::text
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
JOIN pg_enum e ON e.enumtypid = t.oid OR e.enumtypid = t.typelem
WHERE a.attrelid = $1::text::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum, e.enumsortorder
"#;

const PK_SQL: &str = r#"
SELECT a.attname::text
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = $1::text::regclass AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)
"#;

const INDEX_SQL: &str = r#"
SELECT i.relname::text,
       ix.indisunique,
       (SELECT array_agg(a.attname::text ORDER BY k.ord)
          FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum)
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
WHERE ix.indrelid = $1::text::regclass
  AND NOT ix.indisprimary
  AND ix.indisvalid
  AND ix.indpred IS NULL
  AND ix.indexprs IS NULL
ORDER BY i.relname
"#;

const FK_SQL: &str = r#"
SELECT con.conname::text,
       (SELECT array_agg(a.attname::text ORDER BY k.ord)
          FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum),
       rt.relname::text,
       (SELECT array_agg(a.attname::text ORDER BY k.ord)
          FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum),
       con.confdeltype::text,
       con.confupdtype::text
FROM pg_constraint con
JOIN pg_class rt ON rt.oid = con.confrelid
WHERE con.contype = 'f' AND con.conrelid = $1::text::regclass
ORDER BY con.conname
"#;

/// 读取指定 schema 下所有普通表的完整结构。
pub async fn introspect(src: &PgSource, filter: Option<&[String]>) -> Result<Vec<Table>> {
    let filter: Option<Vec<String>> = filter.map(|f| f.to_vec());
    let rows = src
        .client
        .query(TABLES_SQL, &[&src.ns, &filter])
        .await
        .context("读取 PostgreSQL 表列表失败")?;

    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let regclass = Dialect::Postgres.qualified(&src.ns, &name);

        // 主键
        let pk_rows = src
            .client
            .query(PK_SQL, &[&regclass])
            .await
            .with_context(|| format!("读取 {name} 的主键失败"))?;
        let primary_key: Vec<String> = pk_rows.iter().map(|r| r.get(0)).collect();

        // 枚举标签（包含数组元素枚举）
        let enum_rows = src
            .client
            .query(ENUM_SQL, &[&regclass])
            .await
            .with_context(|| format!("读取 {name} 的枚举标签失败"))?;
        let mut enum_map: HashMap<String, Vec<String>> = HashMap::new();
        for r in enum_rows {
            let col: String = r.get(0);
            let label: String = r.get(1);
            enum_map.entry(col).or_default().push(label);
        }

        // 列
        let col_rows = src
            .client
            .query(COLUMNS_SQL, &[&regclass])
            .await
            .with_context(|| format!("读取 {name} 的列信息失败"))?;
        let mut columns = Vec::with_capacity(col_rows.len());
        for r in col_rows {
            let col_name: String = r.get(0);
            let full_type: String = r.get(1);
            let not_null: bool = r.get(2);
            let default: Option<String> = r.get(3);
            let identity: bool = r.get(4);
            let typtype: String = r.get(5);
            let typecategory: String = r.get(6);
            let elem_typtype: Option<String> = r.get(7);
            let elem_type: Option<String> = r.get(8);
            let comment: Option<String> = r.get(9);

            let labels = enum_map.remove(&col_name).unwrap_or_default();
            let kind = type_map::parse_pg(
                &full_type,
                &typtype,
                &typecategory,
                elem_typtype.as_deref(),
                elem_type.as_deref(),
                labels,
            );
            let auto_increment = identity
                || default
                    .as_deref()
                    .is_some_and(|d| d.trim_start().starts_with("nextval("));

            columns.push(Column {
                name: col_name,
                kind,
                not_null,
                default,
                auto_increment,
                on_update_timestamp: false,
                comment,
            });
        }

        // 生成列：只用于建表，不参与数据搬运
        let gen_rows = src
            .client
            .query(GENERATED_SQL, &[&regclass])
            .await
            .with_context(|| format!("读取 {name} 的生成列失败"))?;
        let mut generated_columns = Vec::new();
        for r in gen_rows {
            let col_name: String = r.get(0);
            let full_type: String = r.get(1);
            let typtype: String = r.get(2);
            let typecategory: String = r.get(3);
            let elem_typtype: Option<String> = r.get(4);
            let elem_type: Option<String> = r.get(5);
            let expression: String = r.get(6);
            let comment: Option<String> = r.get(7);
            let labels = enum_map.remove(&col_name).unwrap_or_default();
            let kind = type_map::parse_pg(
                &full_type,
                &typtype,
                &typecategory,
                elem_typtype.as_deref(),
                elem_type.as_deref(),
                labels,
            );
            generated_columns.push(GeneratedColumn {
                name: col_name,
                kind,
                expression,
                comment,
            });
        }

        // 索引
        let idx_rows = src
            .client
            .query(INDEX_SQL, &[&regclass])
            .await
            .with_context(|| format!("读取 {name} 的索引失败"))?;
        let mut indexes = Vec::new();
        for r in idx_rows {
            let idx_name: String = r.get(0);
            let unique: bool = r.get(1);
            let cols: Option<Vec<String>> = r.get(2);
            let cols = cols.unwrap_or_default();
            if !cols.is_empty() {
                indexes.push(Index { name: idx_name, columns: cols, unique });
            }
        }

        // 外键
        let fk_rows = src
            .client
            .query(FK_SQL, &[&regclass])
            .await
            .with_context(|| format!("读取 {name} 的外键失败"))?;
        let mut foreign_keys = Vec::new();
        for r in fk_rows {
            let fk_name: String = r.get(0);
            let cols: Option<Vec<String>> = r.get(1);
            let ref_table: String = r.get(2);
            let ref_cols: Option<Vec<String>> = r.get(3);
            let del: String = r.get(4);
            let upd: String = r.get(5);
            foreign_keys.push(ForeignKey {
                name: fk_name,
                columns: cols.unwrap_or_default(),
                ref_table,
                ref_columns: ref_cols.unwrap_or_default(),
                on_delete: pg_action(&del),
                on_update: pg_action(&upd),
            });
        }

        // 表注释
        let comment = src
            .client
            .query_opt(
                "SELECT obj_description($1::text::regclass, 'pg_class')",
                &[&regclass],
            )
            .await
            .with_context(|| format!("读取 {name} 的注释失败"))?
            .and_then(|r| r.get::<_, Option<String>>(0));

        tables.push(Table {
            schema: src.ns.clone(),
            name,
            columns,
            primary_key,
            indexes,
            foreign_keys,
            generated_columns,
            comment,
        });
    }
    Ok(tables)
}

fn pg_action(code: &str) -> Option<String> {
    match code {
        "r" => Some("RESTRICT".to_string()),
        "c" => Some("CASCADE".to_string()),
        "n" => Some("SET NULL".to_string()),
        "d" => Some("SET DEFAULT".to_string()),
        _ => None,
    }
}

/// 统计源表行数。
pub async fn count(src: &PgSource, table: &Table) -> Result<i64> {
    let sql = format!(
        "SELECT count(*)::bigint FROM {}",
        Dialect::Postgres.qualified(&src.ns, &table.name)
    );
    let row = src.client.query_one(&sql, &[]).await.context("PostgreSQL 统计行数失败")?;
    Ok(row.get(0))
}

/// 取水位列当前最大值，用于初始化增量同步位点。
/// 时间类列按与查询相同的微秒精度格式化，否则 "…:00" 与 "…:00.000000"
/// 在文本比较下不相等，会让边界行每轮被重复读取。
pub async fn max_text(src: &PgSource, table: &Table, column: &str) -> Result<Option<String>> {
    let c = Dialect::Postgres.quote(column);
    let kind = table.columns.iter().find(|col| col.name == column).map(|col| &col.kind);
    let expr = match kind {
        Some(ColumnKind::TimestampTz) => {
            format!("to_char(max({c}) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')")
        }
        Some(ColumnKind::Timestamp) => format!("to_char(max({c}), 'YYYY-MM-DD HH24:MI:SS.US')"),
        Some(ColumnKind::Date) => format!("to_char(max({c}), 'YYYY-MM-DD')"),
        Some(ColumnKind::Time) => format!("to_char(max({c}), 'HH24:MI:SS.US')"),
        _ => format!("max({c})::text"),
    };
    let sql = format!(
        "SELECT {expr} FROM {}",
        Dialect::Postgres.qualified(&src.ns, &table.name)
    );
    let row = src.client.query_one(&sql, &[]).await.context("读取水位最大值失败")?;
    Ok(row.get(0))
}

/// 把一张表流式读出来，按批交给目标写入。
pub async fn copy_table(
    src: &PgSource,
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
    let exprs = table
        .columns
        .iter()
        .map(|c| select_expr(c, target))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {exprs} FROM {}",
        Dialect::Postgres.qualified(&src.ns, &table.name)
    );

    // 行级断点只支持单列主键
    let resume_idx = resume
        .filter(|r| table.primary_key.len() == 1 && table.primary_key[0] == r.column)
        .and_then(|r| table.columns.iter().position(|c| c.name == r.column));

    let mut params_owned: Vec<String> = Vec::new();
    let mut conditions: Vec<String> = Vec::new();
    // --where 是用户提供的原始 SQL，直接内联（工具面向可信运维场景）
    if let Some(w) = where_clause {
        let w = w.trim();
        if !w.is_empty() {
            conditions.push(format!("({w})"));
        }
    }
    if let Some(f) = filter {
        if let Some(col) = table.columns.iter().find(|c| c.name == f.column) {
            let n = params_owned.len() + 1;
            conditions.push(filter_condition(col, &format!("${n}")));
            params_owned.push(f.after.clone());
        }
    }
    if let (Some(r), Some(idx)) = (resume, resume_idx) {
        if let Some(after) = &r.after {
            let n = params_owned.len() + 1;
            conditions.push(filter_condition(&table.columns[idx], &format!("${n}")));
            params_owned.push(after.clone());
        }
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    // 有行级断点时必须按主键顺序读，才能用“大于上个主键”续搬。
    // 这里必须用表名限定，否则 ORDER BY "id" 会命中 SELECT 里的 "id"::text 输出列，变成字符串排序。
    // 排序表达式也要和 filter_condition 的比较语义一致（文本比较的类型按 ::text 排序）。
    if let Some(idx) = resume_idx {
        let col = &table.columns[idx];
        let qualified = format!(
            "{}.{}",
            Dialect::Postgres.qualified(&src.ns, &table.name),
            Dialect::Postgres.quote(&col.name)
        );
        let order = match &col.kind {
            ColumnKind::SmallInt
            | ColumnKind::Int
            | ColumnKind::BigInt
            | ColumnKind::UnsignedBigInt
            | ColumnKind::Real
            | ColumnKind::Double
            | ColumnKind::Numeric { .. }
            | ColumnKind::Bool
            | ColumnKind::Date
            | ColumnKind::Time
            | ColumnKind::Timestamp
            | ColumnKind::TimestampTz => qualified,
            _ => format!("{qualified}::text"),
        };
        sql.push_str(&format!(" ORDER BY {order}"));
    }

    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params_owned.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    let stream = src
        .client
        .query_raw(&sql, params)
        .await
        .with_context(|| format!("读取 {} 数据失败", table.name))?;
    pin_mut!(stream);

    let mut buf: Vec<Vec<Value>> = Vec::with_capacity(batch_size.max(1));
    let mut stat = CopyStat::default();
    let mut last_pk: Option<String> = None;

    while let Some(row) = stream.try_next().await.context("流式读取中断")? {
        let mut values = Vec::with_capacity(table.columns.len());
        for (idx, col) in table.columns.iter().enumerate() {
            let text: Option<String> = row.try_get(idx)?;
            let value = pg_value(&col.kind, target, text)?;
            if Some(col.name.as_str()) == watermark {
                if let Some(t) = value.as_text() {
                    stat.last_key = Some(t.to_string());
                }
            }
            values.push(value);
        }
        buf.push(values);
        if buf.len() >= batch_size.max(1) {
            stat.rows += sink.write_rows(table, &buf, upsert, copy).await?;
            report_progress(&buf, resume_idx, resume, &mut last_pk);
            buf.clear();
        }
    }
    if !buf.is_empty() {
        stat.rows += sink.write_rows(table, &buf, upsert, copy).await?;
        report_progress(&buf, resume_idx, resume, &mut last_pk);
    }
    Ok(stat)
}

/// 一批写入成功后，把该批最大的主键回报给断点。
fn report_progress(
    batch: &[Vec<Value>],
    pk_idx: Option<usize>,
    resume: Option<&RowResume>,
    last: &mut Option<String>,
) {
    let (Some(idx), Some(r)) = (pk_idx, resume) else {
        return;
    };
    if let Some(key) = batch.last().and_then(|row| row.get(idx)).and_then(|v| v.as_text()) {
        *last = Some(key.to_string());
    }
    if let (Some(key), Some(cb)) = (last.as_deref(), &r.on_progress) {
        cb(key);
    }
}

/// 每列都转成文本读出，时间/字节/布尔在这里做特殊处理。
fn select_expr(col: &Column, target: Dialect) -> String {
    let c = Dialect::Postgres.quote(&col.name);
    match &col.kind {
        ColumnKind::Bool => format!("CASE WHEN {c} THEN '1' ELSE '0' END"),
        ColumnKind::Bytes => format!("encode({c}, 'hex')"),
        ColumnKind::TimestampTz => {
            if target == Dialect::Postgres {
                // 带上 +00，避免目标会话时区影响
                format!("to_char({c} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00'")
            } else {
                format!("to_char({c} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')")
            }
        }
        ColumnKind::Timestamp => format!("to_char({c}, 'YYYY-MM-DD HH24:MI:SS.US')"),
        ColumnKind::Date => format!("to_char({c}, 'YYYY-MM-DD')"),
        ColumnKind::Time => format!("to_char({c}, 'HH24:MI:SS.US')"),
        _ => format!("{c}::text"),
    }
}

fn pg_value(kind: &ColumnKind, target: Dialect, text: Option<String>) -> Result<Value> {
    let Some(text) = text else {
        return Ok(Value::Null);
    };
    match kind {
        ColumnKind::Bytes => {
            let hex_text = text.trim_start_matches("\\x");
            Ok(Value::Bytes(hex::decode(hex_text).context("bytea 十六进制解码失败")?))
        }
        // MySQL 没有数组类型，落成 JSON 文本
        ColumnKind::Array { elem } if target == Dialect::Mysql => {
            Ok(Value::Text(crate::value::pg_array_to_json(&text, elem)))
        }
        _ => Ok(Value::Text(text)),
    }
}

/// 增量过滤条件。统一用文本/数值比较，避免依赖源类型在 search_path 里可见。
fn filter_condition(col: &Column, param: &str) -> String {
    let c = Dialect::Postgres.quote(&col.name);
    match &col.kind {
        ColumnKind::TimestampTz => {
            format!("to_char({c} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') > {param}")
        }
        ColumnKind::Timestamp => format!("to_char({c}, 'YYYY-MM-DD HH24:MI:SS.US') > {param}"),
        ColumnKind::Date => format!("to_char({c}, 'YYYY-MM-DD') > {param}"),
        ColumnKind::Time => format!("to_char({c}, 'HH24:MI:SS.US') > {param}"),
        ColumnKind::SmallInt
        | ColumnKind::Int
        | ColumnKind::BigInt
        | ColumnKind::UnsignedBigInt
        | ColumnKind::Real
        | ColumnKind::Double
        | ColumnKind::Numeric { .. } => format!("{c} > {param}::text::numeric"),
        ColumnKind::Bool => format!("{c} > {param}::text::boolean"),
        _ => format!("{c}::text > {param}"),
    }
}
