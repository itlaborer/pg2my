//! MySQL binlog 增量同步（ROW 格式）。
//!
//! 相比水位轮询，binlog 能捕获 UPDATE / DELETE，也不会漏掉中间状态。
//! 使用前提：源库开启 log_bin、binlog_format=ROW、binlog_row_image=FULL，
//! 账号具备 REPLICATION SLAVE / REPLICATION CLIENT 权限。

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, pin_mut};
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::{BinlogStreamRequest, Conn, Opts, Value as MyValue, prelude::Queryable};

use crate::checkpoint::{BinlogPos, Checkpoint};
use crate::model::{ColumnKind, Endpoint, Table, Value};
use crate::my_source;
use crate::sink::Sink;

/// 读取当前 binlog 位点。全量迁移前调用，保证迁移期间的变更也能补上。
pub async fn current_position(url: &str) -> Result<BinlogPos> {
    let mut conn = Conn::new(Opts::from_url(url).context("MySQL 连接串格式不对")?)
        .await
        .context("连接 MySQL 失败")?;
    let row: Option<mysql_async::Row> = conn
        .query_first("SHOW MASTER STATUS")
        .await
        .context("读取 binlog 位点失败（需要 REPLICATION CLIENT 权限）")?;
    let row = row.context("源库没有开启 binlog（SHOW MASTER STATUS 为空）")?;
    let file: String = row.get(0).context("binlog 文件名为空")?;
    let raw: MyValue = row.get(1).unwrap_or(MyValue::NULL);
    let pos = match raw {
        MyValue::Int(i) => i.max(0) as u64,
        MyValue::UInt(u) => u,
        MyValue::Bytes(b) => String::from_utf8_lossy(&b).parse().unwrap_or(0),
        _ => 0,
    };
    Ok(BinlogPos { file, pos })
}

/// 持续消费 binlog 并写入目标，Ctrl-C 退出。
pub async fn run(
    src_ep: &Endpoint,
    dst_ep: &Endpoint,
    tables: &[Table],
    checkpoint: &mut Checkpoint,
    checkpoint_path: Option<&Path>,
    server_id: u32,
) -> Result<()> {
    let start = match &checkpoint.binlog {
        Some(p) => p.clone(),
        None => current_position(&src_ep.url).await?,
    };
    checkpoint.binlog = Some(start.clone());
    if let Some(path) = checkpoint_path {
        checkpoint.save(path)?;
    }

    let mut sink = Sink::connect(dst_ep).await?;
    let index: HashMap<String, Table> =
        tables.iter().map(|t| (t.name.clone(), t.clone())).collect();

    println!(
        "\n进入 binlog 增量同步：{}:{}，Ctrl-C 退出",
        start.file, start.pos
    );

    let mut file = start.file;
    let mut pos = start.pos;
    let mut last_save = Instant::now();

    loop {
        let conn = Conn::new(Opts::from_url(&src_ep.url).context("MySQL 连接串格式不对")?)
            .await
            .context("连接源 MySQL 失败")?;
        let request = BinlogStreamRequest::new(server_id)
            .with_filename(file.as_bytes())
            .with_pos(pos);
        let stream = conn
            .get_binlog_stream(request)
            .await
            .context("订阅 binlog 失败（需要 REPLICATION SLAVE 权限）")?;
        pin_mut!(stream);

        loop {
            let next = tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    checkpoint.binlog = Some(BinlogPos { file: file.clone(), pos });
                    if let Some(path) = checkpoint_path {
                        checkpoint.save(path)?;
                    }
                    println!("收到中断信号，退出 binlog 同步");
                    return Ok(());
                }
                next = stream.next() => next,
            };
            let Some(event) = next else {
                // 源库轮转或重启导致流结束，保存位点后重连
                break;
            };
            let event = event.context("读取 binlog 失败")?;
            let log_pos = event.header().log_pos() as u64;
            match event.read_data().context("解析 binlog 事件失败")? {
                Some(EventData::RotateEvent(ev)) => {
                    // MariaDB 的 rotate 事件可能带校验和尾巴，文件名只取合法字符
                    file = clean_binlog_name(&ev.name());
                    // header.log_pos 为 0 的是建流时的 fake rotate，不能用它的 position 覆盖进度
                    if log_pos > 0 {
                        pos = ev.position();
                    }
                }
                Some(EventData::RowsEvent(ev)) => {
                    let Some(tme) = stream.get_tme(ev.table_id()) else {
                        continue;
                    };
                    let db = tme.database_name();
                    if !db.eq_ignore_ascii_case(&src_ep.namespace) {
                        continue;
                    }
                    let name = tme.table_name().to_string();
                    let Some(table) = index.get(&name) else {
                        continue;
                    };
                    apply_rows(&ev, tme, table, &mut sink).await?;
                }
                Some(EventData::XidEvent(_)) => {
                    // 事务提交，位点可以安全前移
                    pos = log_pos;
                }
                _ => {}
            }
            if last_save.elapsed() >= Duration::from_secs(1) {
                checkpoint.binlog = Some(BinlogPos { file: file.clone(), pos });
                if let Some(path) = checkpoint_path {
                    checkpoint.save(path)?;
                }
                last_save = Instant::now();
            }
        }

        checkpoint.binlog = Some(BinlogPos { file: file.clone(), pos });
        if let Some(path) = checkpoint_path {
            checkpoint.save(path)?;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// 按事件类型应用 INSERT / UPDATE / DELETE。
async fn apply_rows(
    ev: &RowsEventData<'_>,
    tme: &TableMapEvent<'_>,
    table: &Table,
    sink: &mut Sink,
) -> Result<()> {
    if ev.num_columns() as usize != table.columns.len() {
        eprintln!(
            "  binlog 跳过 {}：列数 {} 与内省的 {} 不一致（源表结构可能已变更）",
            table.name,
            ev.num_columns(),
            table.columns.len()
        );
        return Ok(());
    }

    let kind = match ev {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => "insert",
        RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => "update",
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => "delete",
        RowsEventData::PartialUpdateRowsEvent(_) => {
            eprintln!("  binlog 跳过 {}：暂不支持 PARTIAL_UPDATE_ROWS_EVENT", table.name);
            return Ok(());
        }
    };

    for row in ev.rows(tme) {
        let (before, after) = row.context("解析 binlog 行失败")?;
        match kind {
            "delete" => {
                if table.primary_key.is_empty() {
                    continue;
                }
                if let Some(b) = &before {
                    let pk = pk_values(table, b)?;
                    sink.delete_by_pk(table, &pk).await?;
                }
            }
            "insert" => {
                if let Some(a) = &after {
                    let values = full_values(table, a)?;
                    sink.write_batch(table, &[values], true).await?;
                }
            }
            _ => {
                let (Some(b), Some(a)) = (&before, &after) else {
                    continue;
                };
                let old_pk = if table.primary_key.is_empty() {
                    Vec::new()
                } else {
                    pk_values(table, b)?
                };
                let new_pk = if table.primary_key.is_empty() {
                    Vec::new()
                } else {
                    pk_values(table, a)?
                };
                // 主键被更新时，先把旧行删掉，避免目标库留一条幽灵行
                if !old_pk.is_empty() && old_pk != new_pk {
                    sink.delete_by_pk(table, &old_pk).await?;
                }
                let values = full_values(table, a)?;
                sink.write_batch(table, &[values], true).await?;
            }
        }
    }
    Ok(())
}

/// 取一行的全部列；要求 binlog_row_image=FULL，否则缺列无法安全写入。
fn full_values(table: &Table, row: &BinlogRow) -> Result<Vec<Value>> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let value = row.as_ref(i).with_context(|| {
                format!(
                    "{} 的列 {} 在 binlog 行镜像里缺失，请把源库 binlog_row_image 设为 FULL",
                    table.name, col.name
                )
            })?;
            convert(&col.kind, value)
        })
        .collect()
}

/// 取主键列的值，顺序与 table.primary_key 一致。
fn pk_values(table: &Table, row: &BinlogRow) -> Result<Vec<Value>> {
    table
        .primary_key
        .iter()
        .map(|name| {
            let idx = table
                .columns
                .iter()
                .position(|c| &c.name == name)
                .with_context(|| format!("{} 找不到主键列 {}", table.name, name))?;
            let value = row
                .as_ref(idx)
                .with_context(|| format!("{} 主键 {} 不在 binlog 行镜像里", table.name, name))?;
            convert(&table.columns[idx].kind, value)
        })
        .collect()
}

/// binlog 值 → 与全量迁移一致的中间表示。
fn convert(kind: &ColumnKind, value: &BinlogValue<'_>) -> Result<Value> {
    let raw: MyValue = value
        .clone()
        .try_into()
        .map_err(|e| anyhow::anyhow!("binlog 值转换失败: {e}"))?;
    // SET 在 binlog 里是原始位图，ENUM 是序号，必须按定义还原，不能当普通文本
    match (kind, &raw) {
        (ColumnKind::Set { labels }, MyValue::Bytes(b)) => {
            return Ok(Value::Text(set_text(labels, b)));
        }
        (ColumnKind::Enum { labels }, MyValue::Int(i)) => {
            return Ok(Value::Text(enum_text(labels, *i)));
        }
        (ColumnKind::Enum { labels }, MyValue::UInt(u)) => {
            return Ok(Value::Text(enum_text(labels, *u as i64)));
        }
        // BIGINT UNSIGNED 在 binlog 里按有符号位模式解码，需要还原成 u64
        (ColumnKind::UnsignedBigInt, MyValue::Int(i)) => {
            return Ok(Value::Text((*i as u64).to_string()));
        }
        _ => {}
    }
    Ok(match raw {
        MyValue::NULL => Value::Null,
        // MariaDB 的 TIMESTAMP 在 binlog 里是 epoch 秒，需要还原成日期文本
        MyValue::Int(i)
            if matches!(kind, ColumnKind::Timestamp | ColumnKind::TimestampTz) =>
        {
            Value::Text(epoch_to_datetime(i))
        }
        MyValue::UInt(u)
            if matches!(kind, ColumnKind::Timestamp | ColumnKind::TimestampTz) =>
        {
            Value::Text(epoch_to_datetime(u as i64))
        }
        // TIMESTAMP2 在 mysql_common 里被解码成“epoch 秒的十进制字节串”
        MyValue::Bytes(b)
            if matches!(kind, ColumnKind::Timestamp | ColumnKind::TimestampTz) =>
        {
            let text = String::from_utf8_lossy(&b);
            match text.parse::<f64>() {
                Ok(secs) => Value::Text(epoch_to_datetime_frac(secs)),
                Err(_) if text.is_empty() => Value::Null,
                Err(_) => Value::Text(text.into_owned()),
            }
        }
        v if my_source::is_zero_date(kind, &v) => Value::Null,
        MyValue::Bytes(b) if matches!(kind, ColumnKind::Bytes) => Value::Bytes(b),
        v => Value::Text(my_source::value_as_text(kind, v)?),
    })
}

/// 过滤 binlog 文件名里的二进制尾巴（MariaDB 会在事件末尾带 4 字节校验和）。
/// 文件名一般形如 `mysql-bin.000001`，截到“最后一个点后面的连续数字”为止。
fn clean_binlog_name(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    if let Some(dot) = chars.iter().rposition(|c| *c == '.') {
        let mut end = dot + 1;
        while end < chars.len() && chars[end].is_ascii_digit() {
            end += 1;
        }
        if end > dot + 1 {
            return chars[..end].iter().collect();
        }
    }
    chars
        .into_iter()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        .collect()
}

/// SET 位图 → "a,b,c"。
fn set_text(labels: &[String], raw: &[u8]) -> String {
    let mut picked = Vec::new();
    for (i, label) in labels.iter().enumerate() {
        let byte = raw.get(i / 8).copied().unwrap_or(0);
        if byte & (1 << (i % 8)) != 0 {
            picked.push(label.clone());
        }
    }
    picked.join(",")
}

/// ENUM 序号 → 标签，0 表示空字符串。
fn enum_text(labels: &[String], index: i64) -> String {
    if index <= 0 {
        return String::new();
    }
    labels
        .get(index as usize - 1)
        .cloned()
        .unwrap_or_default()
}

/// epoch 秒（可带小数）→ UTC 的 "YYYY-MM-DD HH:MM:SS.ffffff"。
fn epoch_to_datetime_frac(value: f64) -> String {
    let secs = value.floor() as i64;
    let usec = ((value - value.floor()) * 1_000_000.0).round() as u32;
    let base = epoch_to_datetime(secs);
    base.replace(".000000", &format!(".{usec:06}"))
}

/// epoch 秒 → UTC 的 "YYYY-MM-DD HH:MM:SS.000000"。
fn epoch_to_datetime(secs: i64) -> String {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.000000",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant 的 civil_from_days：把 1970-01-01 起的天数还原成年月日。
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// 校验 binlog 模式的前置条件，提前给出明确错误。
pub fn ensure_supported(src: &Endpoint) -> Result<()> {
    if src.dialect != crate::model::Dialect::Mysql {
        bail!("--cdc-mode binlog 目前只支持 MySQL 源；PG 源请使用默认的水位轮询或外接 Debezium");
    }
    Ok(())
}
