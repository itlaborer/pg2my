//! 纯客户端的 CSV 导出 / 导入。
//!
//! 面向不允许服务端读写本地文件的托管数据库实例：导出用客户端流式读取
//! （PG 走 `COPY ... TO STDOUT`，MySQL 走普通查询），导入走普通批量 INSERT，
//! 不依赖服务端文件权限，也不需要 `LOAD DATA` / `COPY FROM`。
//!
//! 字段约定与 PostgreSQL 的 CSV 模式一致：
//! - NULL 写成空字段（逗号之间什么都没有）
//! - 空字符串写成 `""`
//! - 含逗号、双引号、换行的字段整体加双引号，内部双引号翻倍（RFC 4180）

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use futures_util::{TryStreamExt, pin_mut};
use mysql_async::{Conn, Opts, Params, Value as MyValue, prelude::Queryable};
use tokio_postgres::NoTls;

use crate::model::{Dialect, Endpoint, Value};
use crate::sink::Sink;
use crate::source::Source;

/// CSV 的一个字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvField {
    /// 空字段：导入时还原成 NULL
    Null,
    /// 非 NULL 文本，空串也走这里（写出时加引号成 `""`）
    Text(String),
}

/// 按自定义 SQL 把查询结果导出成 CSV，返回数据行数（不含表头）。
pub async fn export(
    url: &str,
    dialect: Dialect,
    schema: Option<&str>,
    sql: &str,
    path: &Path,
) -> Result<u64> {
    let query = strip_terminator(sql);
    if query.is_empty() {
        bail!("--export-sql 不能为空");
    }
    match dialect {
        Dialect::Postgres => export_pg(url, schema, query, path).await,
        Dialect::Mysql => export_mysql(url, schema, query, path).await,
    }
}

/// 去掉 SQL 末尾的空白与分号，便于拼进 `COPY (...)`。
fn strip_terminator(sql: &str) -> &str {
    let mut s = sql.trim();
    while let Some(t) = s.strip_suffix(';') {
        s = t.trim();
    }
    s
}

async fn export_pg(url: &str, schema: Option<&str>, query: &str, path: &Path) -> Result<u64> {
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
    if let Some(ns) = schema {
        client
            .batch_execute(&format!(
                "SET search_path TO {}",
                Dialect::Postgres.quote(ns)
            ))
            .await
            .with_context(|| format!("设置 search_path 到 {ns} 失败"))?;
    }

    // COPY TO STDOUT 的字节流由客户端接收，不涉及服务端文件；
    // CSV 模式下 NULL 是空字段、空串是 ""，与导入约定天然一致。
    let copy_sql = format!("COPY ({query}) TO STDOUT WITH (FORMAT csv, HEADER true)");
    let stream = client
        .copy_out(&copy_sql)
        .await
        .with_context(|| format!("执行导出 SQL 失败：{query}"))?;
    pin_mut!(stream);

    let mut out = BufWriter::new(
        File::create(path).with_context(|| format!("创建 {} 失败", path.display()))?,
    );
    let mut counter = RecordCounter::default();
    while let Some(chunk) = stream.try_next().await.context("读取导出结果失败")? {
        counter.feed(&chunk);
        out.write_all(&chunk).context("写入 CSV 失败")?;
    }
    out.flush().context("写入 CSV 失败")?;
    // 首行是表头
    Ok(counter.records.saturating_sub(1))
}

async fn export_mysql(url: &str, schema: Option<&str>, query: &str, path: &Path) -> Result<u64> {
    let opts = Opts::from_url(url).context("MySQL 连接串格式不对")?;
    let mut conn = Conn::new(opts).await.context("连接 MySQL 失败")?;
    conn.query_drop("SET SESSION time_zone = '+00:00'")
        .await
        .context("设置 MySQL 会话时区失败")?;
    // 自定义 SQL 里的表名可能不带库名，--schema 指定时切过去
    if let Some(ns) = schema {
        conn.query_drop(format!("USE {}", Dialect::Mysql.quote(ns)))
            .await
            .with_context(|| format!("切换到库 {ns} 失败"))?;
    }

    let mut result = conn
        .exec_iter(query, Params::Empty)
        .await
        .with_context(|| format!("执行导出 SQL 失败：{query}"))?;
    let header: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();

    let mut out = BufWriter::new(
        File::create(path).with_context(|| format!("创建 {} 失败", path.display()))?,
    );
    if !header.is_empty() {
        let fields: Vec<CsvField> = header.iter().cloned().map(CsvField::Text).collect();
        write_record(&mut out, &fields)?;
    }

    let mut rows = 0u64;
    while let Some(row) = result.next().await.context("读取导出结果失败")? {
        let mut fields = Vec::with_capacity(header.len());
        for idx in 0..header.len() {
            // mysql_async 对 NULL 给的是 Some(Value::NULL)
            fields.push(match row.get::<MyValue, _>(idx) {
                None | Some(MyValue::NULL) => CsvField::Null,
                Some(v) => CsvField::Text(my_value_text(v)),
            });
        }
        write_record(&mut out, &fields)?;
        rows += 1;
    }
    out.flush().context("写入 CSV 失败")?;
    Ok(rows)
}

/// 自定义 SQL 的结果没有列类型信息，统一按驱动给出的值转文本。
fn my_value_text(value: MyValue) -> String {
    match value {
        MyValue::NULL => String::new(),
        MyValue::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        MyValue::Int(i) => i.to_string(),
        MyValue::UInt(u) => u.to_string(),
        MyValue::Float(f) => f.to_string(),
        MyValue::Double(d) => d.to_string(),
        MyValue::Date(y, mo, d, h, mi, s, us) => {
            if h == 0 && mi == 0 && s == 0 && us == 0 {
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
    }
}

/// 统计 CSV 记录数：只数引号外的换行，字段内的换行不算。
#[derive(Default)]
struct RecordCounter {
    in_quotes: bool,
    records: u64,
}

impl RecordCounter {
    fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            match b {
                // 连续两个引号会在两次切换后回到原状态，语义正确
                b'"' => self.in_quotes = !self.in_quotes,
                b'\n' if !self.in_quotes => self.records += 1,
                _ => {}
            }
        }
    }
}

/// 把本地 CSV 导入目标库的表，返回写入行数。
///
/// 只写入 CSV 表头里出现的列，其余列留给目标表的默认值 / 自增。
pub async fn import(
    ep: &Endpoint,
    table_name: &str,
    path: &Path,
    batch_size: usize,
    truncate: bool,
) -> Result<u64> {
    let mut src = Source::connect(ep)
        .await
        .context("连接目标库失败：导入时以 --target 为准")?;
    let filter = [table_name.to_string()];
    let mut tables = src.introspect(Some(&filter)).await?;
    drop(src);
    let mut table = tables
        .pop()
        .ok_or_else(|| anyhow::anyhow!("目标库里没有表 {table_name}"))?;

    let file = File::open(path).with_context(|| format!("打开 {} 失败", path.display()))?;
    let mut reader = CsvReader::new(BufReader::new(file));
    let header = reader
        .next_record()?
        .ok_or_else(|| anyhow::anyhow!("{} 是空文件，缺少表头", path.display()))?;
    let names: Vec<String> = header
        .iter()
        .map(|f| match f {
            CsvField::Null => String::new(),
            CsvField::Text(s) => s.clone(),
        })
        .collect();

    // 表头名 -> 目标列：优先精确匹配，再退回大小写不敏感
    let mut positions = Vec::new();
    let mut columns = Vec::new();
    for col in &table.columns {
        let pos = names
            .iter()
            .position(|n| n == &col.name)
            .or_else(|| names.iter().position(|n| n.eq_ignore_ascii_case(&col.name)));
        if let Some(pos) = pos {
            positions.push(pos);
            columns.push(col.clone());
        }
    }
    if columns.is_empty() {
        bail!(
            "CSV 表头（{}）与目标表 {} 的列名没有交集",
            names.join(","),
            table_name
        );
    }
    for (i, name) in names.iter().enumerate() {
        if !positions.contains(&i) {
            eprintln!(
                "  提示 CSV 列 {name} 在目标表 {} 中不存在，已忽略",
                table.name
            );
        }
    }
    let missing: Vec<&str> = table
        .columns
        .iter()
        .filter(|c| !columns.iter().any(|k| k.name == c.name))
        .map(|c| c.name.as_str())
        .collect();
    if !missing.is_empty() {
        println!("  CSV 未包含列：{}（使用目标表默认值）", missing.join(", "));
    }

    // 只保留 CSV 覆盖的列，Sink 便只写这些列，其余列才会吃默认值
    table.columns = columns;

    let mut sink = Sink::connect(ep).await?;
    if truncate {
        sink.truncate(&table).await.context("清空目标表失败")?;
        println!("  已清空 {}", table.name);
    }

    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(batch_size.max(1));
    let mut total = 0u64;
    while let Some(record) = reader.next_record()? {
        // 多列文件里的空行是排版残留；单列时它表示这一行就是 NULL
        if record.len() == 1 && record[0] == CsvField::Null && names.len() != 1 {
            continue;
        }
        if record.len() != names.len() {
            bail!(
                "第 {} 条记录有 {} 个字段，表头是 {} 个",
                reader.record_no,
                record.len(),
                names.len()
            );
        }
        let mut row = Vec::with_capacity(positions.len());
        for &pos in &positions {
            row.push(match &record[pos] {
                CsvField::Null => Value::Null,
                CsvField::Text(s) => Value::Text(s.clone()),
            });
        }
        batch.push(row);
        if batch.len() >= batch_size.max(1) {
            total += sink.write_rows(&table, &batch, false, false).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        total += sink.write_rows(&table, &batch, false, false).await?;
    }
    Ok(total)
}

/// 流式 CSV 读取器：逐条返回记录，不把整个文件读进内存。
pub struct CsvReader<R: BufRead> {
    reader: R,
    /// 已读出的记录数（含表头），用于报错定位
    pub record_no: u64,
    bom_checked: bool,
    done: bool,
}

impl<R: BufRead> CsvReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            record_no: 0,
            bom_checked: false,
            done: false,
        }
    }

    fn next_byte(&mut self) -> Result<Option<u8>> {
        let buf = self.reader.fill_buf().context("读取 CSV 失败")?;
        if buf.is_empty() {
            return Ok(None);
        }
        let b = buf[0];
        self.reader.consume(1);
        Ok(Some(b))
    }

    /// 吃了 `\r` 之后，紧跟着的 `\n` 属于同一个换行。
    fn eat_lf(&mut self) -> Result<()> {
        let buf = self.reader.fill_buf().context("读取 CSV 失败")?;
        if buf.first() == Some(&b'\n') {
            self.reader.consume(1);
        }
        Ok(())
    }

    /// 读下一条记录；文件结束返回 None。空行会被跳过。
    pub fn next_record(&mut self) -> Result<Option<Vec<CsvField>>> {
        if self.done {
            return Ok(None);
        }
        if !self.bom_checked {
            self.bom_checked = true;
            let buf = self.reader.fill_buf().context("读取 CSV 失败")?;
            if buf.starts_with(&[0xEF, 0xBB, 0xBF]) {
                self.reader.consume(3);
            }
        }

        let mut fields: Vec<CsvField> = Vec::new();
        let mut buf: Vec<u8> = Vec::new();
        // 字段是否由引号包围：引号包围的空串是 ""，未包围的空字段才是 NULL
        let mut quoted_field = false;

        #[derive(Clone, Copy)]
        enum State {
            Start,
            Unquoted,
            Quoted,
            AfterQuote,
        }
        let mut state = State::Start;

        loop {
            let byte = self.next_byte()?;
            match (state, byte) {
                (_, None) => {
                    self.done = true;
                    match state {
                        // 文件恰好以换行结束，没有半截记录
                        State::Start if fields.is_empty() => return Ok(None),
                        State::Quoted => bail!("第 {} 条记录引号未闭合", self.record_no + 1),
                        State::Start => fields.push(CsvField::Null),
                        State::Unquoted | State::AfterQuote => {
                            fields.push(finish_field(buf, quoted_field)?)
                        }
                    }
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                (State::Start, Some(b'\n')) => {
                    // 单独一个空行是“1 列 NULL”，与 PG COPY 的行为一致；
                    // 多列文件的空行由导入侧按字段数容错跳过。
                    fields.push(CsvField::Null);
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                (State::Start, Some(b'\r')) => {
                    self.eat_lf()?;
                    fields.push(CsvField::Null);
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                (State::Start, Some(b',')) => fields.push(CsvField::Null),
                (State::Start, Some(b'"')) => {
                    quoted_field = true;
                    state = State::Quoted;
                }
                (State::Start, Some(b)) => {
                    buf.push(b);
                    state = State::Unquoted;
                }

                (State::Unquoted, Some(b',')) => {
                    fields.push(finish_field(buf, quoted_field)?);
                    buf = Vec::new();
                    quoted_field = false;
                    state = State::Start;
                }
                (State::Unquoted, Some(b'\n')) => {
                    fields.push(finish_field(buf, quoted_field)?);
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                (State::Unquoted, Some(b'\r')) => {
                    self.eat_lf()?;
                    fields.push(finish_field(buf, quoted_field)?);
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                // 未加引号的字段里出现引号按普通字符处理
                (State::Unquoted, Some(b)) => buf.push(b),

                (State::Quoted, Some(b'"')) => state = State::AfterQuote,
                (State::Quoted, Some(b)) => buf.push(b),

                (State::AfterQuote, Some(b'"')) => {
                    // "" 是一个转义后的双引号
                    buf.push(b'"');
                    state = State::Quoted;
                }
                (State::AfterQuote, Some(b',')) => {
                    fields.push(finish_field(buf, true)?);
                    buf = Vec::new();
                    quoted_field = false;
                    state = State::Start;
                }
                (State::AfterQuote, Some(b'\n')) => {
                    fields.push(finish_field(buf, true)?);
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                (State::AfterQuote, Some(b'\r')) => {
                    self.eat_lf()?;
                    fields.push(finish_field(buf, true)?);
                    self.record_no += 1;
                    return Ok(Some(fields));
                }
                // 宽松处理：闭合引号后又出现字符，按字段内容继续
                (State::AfterQuote, Some(b)) => {
                    buf.push(b);
                    state = State::Quoted;
                }
            }
        }
    }
}

fn finish_field(buf: Vec<u8>, quoted: bool) -> Result<CsvField> {
    if !quoted && buf.is_empty() {
        return Ok(CsvField::Null);
    }
    let s = String::from_utf8(buf).context("CSV 不是合法的 UTF-8 编码")?;
    Ok(CsvField::Text(s))
}

fn write_record<W: Write>(out: &mut W, fields: &[CsvField]) -> Result<()> {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            out.write_all(b",")?;
        }
        match field {
            CsvField::Null => {}
            CsvField::Text(s) => {
                let needs_quote = s.is_empty()
                    || s.as_bytes()
                        .iter()
                        .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'));
                if needs_quote {
                    out.write_all(b"\"")?;
                    let bytes = s.as_bytes();
                    let mut start = 0;
                    for (idx, b) in bytes.iter().enumerate() {
                        if *b == b'"' {
                            out.write_all(&bytes[start..idx])?;
                            out.write_all(b"\"\"")?;
                            start = idx + 1;
                        }
                    }
                    out.write_all(&bytes[start..])?;
                    out.write_all(b"\"")?;
                } else {
                    out.write_all(s.as_bytes())?;
                }
            }
        }
    }
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn parse(text: &str) -> Vec<Vec<CsvField>> {
        let mut reader = CsvReader::new(Cursor::new(text.as_bytes().to_vec()));
        let mut out = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            out.push(record);
        }
        out
    }

    fn text(s: &str) -> CsvField {
        CsvField::Text(s.to_string())
    }

    #[test]
    fn distinguishes_null_and_empty_string() {
        assert_eq!(
            parse("a,,\"\",b\n"),
            vec![vec![text("a"), CsvField::Null, text(""), text("b")]]
        );
        // 空行等价于“1 列 NULL”，与 PG COPY 一致
        assert_eq!(
            parse("a\n\nb\n"),
            vec![vec![text("a")], vec![CsvField::Null], vec![text("b")]]
        );
        assert_eq!(parse("\n"), vec![vec![CsvField::Null]]);
    }

    #[test]
    fn parses_quoted_commas_quotes_and_newlines() {
        let input = "1,\"a,b\",\"say \"\"hi\"\"\",\"line1\nline2\"\r\n";
        assert_eq!(
            parse(input),
            vec![vec![
                text("1"),
                text("a,b"),
                text("say \"hi\""),
                text("line1\nline2"),
            ]]
        );
    }

    #[test]
    fn handles_crlf_bom_and_missing_trailing_newline() {
        assert_eq!(
            parse("\u{feff}id,name\r\n1,alice\r\n2,bob"),
            vec![
                vec![text("id"), text("name")],
                vec![text("1"), text("alice")],
                vec![text("2"), text("bob")],
            ]
        );
    }

    #[test]
    fn reports_unclosed_quote() {
        let mut reader = CsvReader::new(Cursor::new(b"\"abc".to_vec()));
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn writer_round_trip() {
        let record = vec![
            CsvField::Null,
            text(""),
            text("a,b"),
            text("say \"hi\""),
            text("x\ny"),
            text("plain"),
        ];
        let mut buf = Vec::new();
        write_record(&mut buf, &record).unwrap();
        let encoded = String::from_utf8(buf).unwrap();
        assert_eq!(parse(&encoded), vec![record]);
    }
}
