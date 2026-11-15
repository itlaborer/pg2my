//! MySQL 作为源：内省表结构、流式读取数据。

use std::collections::HashMap;

use anyhow::{Context, Result};
use mysql_async::{
    Conn, Opts, Params, Row, Value as MyValue, prelude::Queryable,
};

use crate::model::{
    Column, ColumnKind, CopyStat, Dialect, ForeignKey, IncrementalFilter, Index, RowResume, Table,
    Value, View,
};
use crate::sink::Sink;
use crate::type_map;

pub struct MySource {
    pub conn: Conn,
    pub ns: String,
}

pub async fn connect(url: &str, ns: &str) -> Result<MySource> {
    if ns.is_empty() {
        anyhow::bail!("MySQL 源需要在连接串里带库名，或显式指定 --schema");
    }
    let opts = Opts::from_url(url).context("MySQL 连接串格式不对")?;
    let mut conn = Conn::new(opts).await.context("连接 MySQL 失败")?;
    // 统一用 UTC，TIMESTAMP 的文本值才和目标一致
    conn.query_drop("SET SESSION time_zone = '+00:00'")
        .await
        .context("设置 MySQL 会话时区失败")?;
    Ok(MySource { conn, ns: ns.to_string() })
}

/// 搬运专用连接：开一个一致性快照事务，避免单表读到不同时间点的数据。
pub async fn connect_consistent(url: &str, ns: &str) -> Result<MySource> {
    let mut src = connect(url, ns).await?;
    src.conn
        .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .context("设置 MySQL 隔离级别失败")?;
    src.conn
        .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .context("开启 MySQL 一致性快照失败")?;
    Ok(src)
}

const TABLES_SQL: &str = r#"
SELECT TABLE_NAME, TABLE_COMMENT
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME
"#;

const COLUMNS_SQL: &str = r#"
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA,
       COLUMN_COMMENT, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION
"#;

const INDEX_SQL: &str = r#"
SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX
"#;

const FK_SQL: &str = r#"
SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME,
       k.REFERENCED_COLUMN_NAME, r.DELETE_RULE, r.UPDATE_RULE
FROM information_schema.KEY_COLUMN_USAGE k
JOIN information_schema.REFERENTIAL_CONSTRAINTS r
  ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
"#;

fn params2(a: &str, b: &str) -> Params {
    Params::Positional(vec![MyValue::from(a.to_string()), MyValue::from(b.to_string())])
}

/// MariaDB 的 JSON 实际是 longtext + CHECK(json_valid(`列`))，靠约束反推。
const JSON_CHECK_SQL: &str = r#"
SELECT cc.CHECK_CLAUSE
FROM information_schema.CHECK_CONSTRAINTS cc
JOIN information_schema.TABLE_CONSTRAINTS tc
  ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
 AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
WHERE cc.CONSTRAINT_SCHEMA = ? AND tc.TABLE_NAME = ?
"#;

pub async fn introspect(src: &mut MySource, filter: Option<&[String]>) -> Result<Vec<Table>> {
    let rows: Vec<Row> = src
        .conn
        .exec(TABLES_SQL, Params::Positional(vec![MyValue::from(src.ns.clone())]))
        .await
        .context("读取 MySQL 表列表失败")?;

    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        let name = row.get::<String, _>(0).unwrap_or_default();
        if filter.is_some_and(|f| !f.contains(&name)) {
            continue;
        }
        let table_comment = row
            .get::<String, _>(1)
            .filter(|c| !c.is_empty());

        let col_rows: Vec<Row> = src
            .conn
            .exec(COLUMNS_SQL, params2(&src.ns, &name))
            .await
            .with_context(|| format!("读取 {name} 的列信息失败"))?;

        // 收集 json_valid(`列`) 形式的检查约束，用于识别 MariaDB 的 JSON 列
        let check_rows: Vec<Row> = src
            .conn
            .exec(JSON_CHECK_SQL, params2(&src.ns, &name))
            .await
            .unwrap_or_default();
        let json_checks: Vec<String> = check_rows
            .iter()
            .filter_map(|r| r.get::<String, _>(0))
            .map(|s| s.to_ascii_lowercase())
            .collect();

        let mut columns = Vec::with_capacity(col_rows.len());
        for r in col_rows {
            let col_name = r.get::<String, _>(0).unwrap_or_default();
            let data_type = r.get::<String, _>(1).unwrap_or_default();
            let column_type = r.get::<String, _>(2).unwrap_or_default();
            let nullable = r.get::<String, _>(3).unwrap_or_default();
            let default = r.get::<MyValue, _>(4).and_then(value_to_plain_string);
            let extra = r.get::<String, _>(5).unwrap_or_default();
            // 生成列不能显式写入，直接跳过
            let extra_lower = extra.to_ascii_lowercase();
            if extra_lower.contains("virtual generated") || extra_lower.contains("stored generated") {
                eprintln!("  跳过 {name}.{col_name}：生成列不参与迁移");
                continue;
            }
            let comment = r.get::<String, _>(6).filter(|c| !c.is_empty());
            let char_len = r.get::<MyValue, _>(7).and_then(|v| value_u64(&v));
            let precision = r.get::<MyValue, _>(8).and_then(|v| value_u64(&v));
            let scale = r.get::<MyValue, _>(9).and_then(|v| value_u64(&v));

            let kind = type_map::parse_mysql(
                &data_type,
                &column_type,
                char_len,
                precision,
                scale,
            );
            // MariaDB：longtext + CHECK(json_valid(`col`)) 等价于 JSON 列
            let kind = if matches!(kind, ColumnKind::Text { .. })
                && json_checks.iter().any(|c| {
                    c.contains("json_valid(") && c.contains(&format!("`{}`", col_name.to_lowercase()))
                })
            {
                ColumnKind::Json
            } else {
                kind
            };
            if matches!(kind, ColumnKind::Other { .. }) {
                eprintln!(
                    "  警告 {name}.{col_name}：类型 {column_type} 没有对应映射，按文本搬运"
                );
            }
            columns.push(Column {
                name: col_name,
                kind,
                not_null: nullable.eq_ignore_ascii_case("NO"),
                default,
                auto_increment: extra_lower.contains("auto_increment"),
                on_update_timestamp: extra_lower.contains("on update current_timestamp"),
                comment,
            });
        }

        // 索引（含主键）
        let idx_rows: Vec<Row> = src
            .conn
            .exec(INDEX_SQL, params2(&src.ns, &name))
            .await
            .with_context(|| format!("读取 {name} 的索引失败"))?;
        let mut primary_key = Vec::new();
        let mut indexes: Vec<Index> = Vec::new();
        let mut idx_map: HashMap<String, (bool, Vec<String>)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for r in idx_rows {
            let idx_name = r.get::<String, _>(0).unwrap_or_default();
            let non_unique = r.get::<MyValue, _>(1).and_then(|v| value_u64(&v)).unwrap_or(1);
            let Some(col) = r.get::<String, _>(2) else {
                continue;
            };
            if idx_name == "PRIMARY" {
                primary_key.push(col);
                continue;
            }
            if !idx_map.contains_key(&idx_name) {
                order.push(idx_name.clone());
            }
            let entry = idx_map.entry(idx_name).or_insert((non_unique == 0, Vec::new()));
            entry.1.push(col);
        }
        for idx_name in order {
            if let Some((unique, cols)) = idx_map.remove(&idx_name) {
                indexes.push(Index { name: idx_name, columns: cols, unique });
            }
        }

        // 外键
        let fk_rows: Vec<Row> = src
            .conn
            .exec(FK_SQL, params2(&src.ns, &name))
            .await
            .with_context(|| format!("读取 {name} 的外键失败"))?;
        let mut fk_map: HashMap<String, ForeignKey> = HashMap::new();
        let mut fk_order: Vec<String> = Vec::new();
        for r in fk_rows {
            let fk_name = r.get::<String, _>(0).unwrap_or_default();
            let Some(col) = r.get::<String, _>(1) else {
                continue;
            };
            let ref_table = r.get::<String, _>(2).unwrap_or_default();
            let Some(ref_col) = r.get::<String, _>(3) else {
                continue;
            };
            let del = r.get::<String, _>(4).unwrap_or_default();
            let upd = r.get::<String, _>(5).unwrap_or_default();
            if !fk_map.contains_key(&fk_name) {
                fk_order.push(fk_name.clone());
            }
            let entry = fk_map.entry(fk_name.clone()).or_insert_with(|| ForeignKey {
                name: fk_name,
                columns: Vec::new(),
                ref_table,
                ref_columns: Vec::new(),
                on_delete: action(&del),
                on_update: action(&upd),
            });
            entry.columns.push(col);
            entry.ref_columns.push(ref_col);
        }
        let foreign_keys = fk_order
            .into_iter()
            .filter_map(|n| fk_map.remove(&n))
            .collect();

        tables.push(Table {
            schema: src.ns.clone(),
            name,
            columns,
            primary_key,
            indexes,
            foreign_keys,
            // MySQL 生成列的表达式是 MySQL 方言，跨库不重建（内省时已跳过）
            generated_columns: Vec::new(),
            comment: table_comment,
        });
    }
    Ok(tables)
}

fn action(rule: &str) -> Option<String> {
    match rule.to_ascii_uppercase().as_str() {
        "CASCADE" => Some("CASCADE".to_string()),
        "SET NULL" => Some("SET NULL".to_string()),
        "SET DEFAULT" => Some("SET DEFAULT".to_string()),
        "RESTRICT" => Some("RESTRICT".to_string()),
        _ => None,
    }
}

/// 单表分片用：取整数主键的最小/最大值（受 where 过滤影响）。
pub async fn pk_bounds(
    src: &mut MySource,
    table: &Table,
    filter: Option<&str>,
) -> Result<Option<(i64, i64)>> {
    let q = Dialect::Mysql;
    let col = q.quote(&table.primary_key[0]);
    let mut sql = format!(
        "SELECT MIN({col}), MAX({col}) FROM {}",
        q.qualified(&src.ns, &table.name)
    );
    if let Some(w) = filter {
        sql.push_str(&format!(" WHERE {w}"));
    }
    let row: Option<mysql_async::Row> =
        src.conn.query_first(sql).await.context("读取主键范围失败")?;
    let Some(row) = row else { return Ok(None) };
    let lo: Option<i64> = row.get(0);
    let hi: Option<i64> = row.get(1);
    Ok(match (lo, hi) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    })
}

/// 视图定义保留 MySQL 方言，只在 MySQL → MySQL 时迁移。
pub async fn views(src: &mut MySource, filter: Option<&[String]>) -> Result<Vec<View>> {
    let rows: Vec<Row> = src
        .conn
        .exec(
            "SELECT TABLE_NAME, VIEW_DEFINITION FROM information_schema.VIEWS \
             WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
            (src.ns.as_str(),),
        )
        .await
        .context("读取 MySQL 视图失败")?;
    let mut out = Vec::new();
    for row in rows {
        let name: String = row.get(0).unwrap_or_default();
        if let Some(f) = filter {
            if !f.iter().any(|t| t == &name) {
                continue;
            }
        }
        // 权限不足时 VIEW_DEFINITION 为 NULL，跳过并交给调用方统计
        let definition: Option<String> = row.get(1);
        if let Some(definition) = definition.filter(|d| !d.trim().is_empty()) {
            out.push(View { name, definition });
        }
    }
    Ok(out)
}

pub async fn count(src: &mut MySource, table: &Table) -> Result<i64> {
    let sql = format!(
        "SELECT count(*) FROM {}",
        Dialect::Mysql.qualified(&src.ns, &table.name)
    );
    let value: Option<i64> = src.conn.query_first(sql).await.context("MySQL 统计行数失败")?;
    Ok(value.unwrap_or(0))
}

pub async fn max_text(src: &mut MySource, table: &Table, column: &str) -> Result<Option<String>> {
    let sql = format!(
        "SELECT MAX({}) FROM {}",
        Dialect::Mysql.quote(column),
        Dialect::Mysql.qualified(&src.ns, &table.name)
    );
    let value: Option<MyValue> = src.conn.query_first(sql).await.context("读取水位最大值失败")?;
    let kind = table
        .columns
        .iter()
        .find(|c| c.name == column)
        .map(|c| c.kind.clone())
        .unwrap_or(ColumnKind::Text { max_len: None });
    match value {
        None | Some(MyValue::NULL) => Ok(None),
        Some(v) => Ok(Some(value_as_text(&kind, v)?)),
    }
}

pub async fn copy_table(
    src: &mut MySource,
    table: &Table,
    batch_size: usize,
    filter: Option<&IncrementalFilter>,
    sink: &mut Sink,
    upsert: bool,
    watermark: Option<&str>,
    copy: bool,
    resume: Option<&RowResume>,
    where_clause: Option<&str>,
) -> Result<CopyStat> {
    let cols = table
        .columns
        .iter()
        .map(|c| Dialect::Mysql.quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {cols} FROM {}",
        Dialect::Mysql.qualified(&src.ns, &table.name)
    );

    // 行级断点只支持单列主键
    let resume_idx = resume
        .filter(|r| table.primary_key.len() == 1 && table.primary_key[0] == r.column)
        .and_then(|r| table.columns.iter().position(|c| c.name == r.column));

    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<MyValue> = Vec::new();
    // --where 是用户提供的原始 SQL，直接内联（工具面向可信运维场景）
    if let Some(w) = where_clause {
        let w = w.trim();
        if !w.is_empty() {
            conditions.push(format!("({w})"));
        }
    }
    if let Some(f) = filter {
        if table.columns.iter().any(|c| c.name == f.column) {
            conditions.push(format!("{} > ?", Dialect::Mysql.quote(&f.column)));
            params.push(MyValue::from(f.after.clone()));
        }
    }
    if let (Some(r), Some(idx)) = (resume, resume_idx) {
        if let Some(after) = &r.after {
            conditions.push(format!("{} > ?", Dialect::Mysql.quote(&table.columns[idx].name)));
            params.push(MyValue::from(after.clone()));
        }
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    if let Some(idx) = resume_idx {
        sql.push_str(&format!(
            " ORDER BY {}",
            Dialect::Mysql.quote(&table.columns[idx].name)
        ));
    }
    let params = if params.is_empty() { Params::Empty } else { Params::Positional(params) };

    let mut result = src
        .conn
        .exec_iter(sql, params)
        .await
        .with_context(|| format!("读取 {} 数据失败", table.name))?;

    let mut buf: Vec<Vec<Value>> = Vec::with_capacity(batch_size.max(1));
    let mut stat = CopyStat::default();
    let mut zero_dates = 0u64;
    let mut last_pk: Option<String> = None;
    while let Some(row) = result.next().await.context("流式读取中断")? {
        let mut values = Vec::with_capacity(table.columns.len());
        for (idx, col) in table.columns.iter().enumerate() {
            let raw: Option<MyValue> = row.get(idx);
            let value = match raw {
                // mysql_async 的 Option<Value> 对 NULL 会给出 Some(Value::NULL)
                None | Some(MyValue::NULL) => Value::Null,
                // 零日期在严格模式的目标库写不进去，统一按 NULL 处理
                Some(v) if is_zero_date(&col.kind, &v) => {
                    zero_dates += 1;
                    Value::Null
                }
                // 二进制列保留字节，避免 UTF-8 转换破坏数据
                Some(MyValue::Bytes(b)) if matches!(col.kind, ColumnKind::Bytes) => {
                    Value::Bytes(b)
                }
                Some(v) => Value::Text(
                    value_as_text(&col.kind, v)
                        .with_context(|| format!("列 {} 的值无法转成文本", col.name))?,
                ),
            };
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
    if zero_dates > 0 {
        eprintln!(
            "  {} 有 {} 个零日期值（0000-00-00），已按 NULL 写入",
            table.name, zero_dates
        );
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

/// MySQL 的零日期（0000-00-00）在严格模式的目标库无法写入。
pub(crate) fn is_zero_date(kind: &ColumnKind, value: &MyValue) -> bool {
    matches!(
        kind,
        ColumnKind::Date | ColumnKind::Timestamp | ColumnKind::TimestampTz
    ) && matches!(value, MyValue::Date(0, 0, 0, _, _, _, _))
}

/// 把 MySQL 的二进制/数值/时间值统一转成文本（二进制列保留字节）。
pub(crate) fn value_as_text(kind: &ColumnKind, value: MyValue) -> Result<String> {
    Ok(match value {
        MyValue::NULL => return Ok(String::new()),
        MyValue::Bytes(b) => String::from_utf8(b).context("MySQL 文本列不是合法的 UTF-8")?,
        MyValue::Int(i) => i.to_string(),
        MyValue::UInt(u) => u.to_string(),
        MyValue::Float(f) => f.to_string(),
        MyValue::Double(d) => d.to_string(),
        MyValue::Date(y, mo, d, h, mi, s, us) => {
            if matches!(kind, ColumnKind::Date) {
                format!("{y:04}-{mo:02}-{d:02}")
            } else {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
            }
        }
        MyValue::Time(neg, days, h, mi, s, us) => {
            let total = days * 24 + h as u32;
            format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                if neg { "-" } else { "" },
                total,
                mi,
                s,
                us
            )
        }
    })
}

/// 内省用的宽松字符串转换。
fn value_to_plain_string(value: MyValue) -> Option<String> {
    match value_as_text(&ColumnKind::Text { max_len: None }, value) {
        Ok(s) if !s.is_empty() => Some(s),
        _ => None,
    }
}

fn value_u64(value: &MyValue) -> Option<u64> {
    match value {
        MyValue::Int(i) if *i >= 0 => Some(*i as u64),
        MyValue::UInt(u) => Some(*u),
        MyValue::Bytes(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        MyValue::Double(d) if *d >= 0.0 => Some(*d as u64),
        MyValue::Float(f) if *f >= 0.0 => Some(*f as u64),
        _ => None,
    }
}
