//! 类型归一化与目标方言类型生成。

use crate::model::ColumnKind;

/// 把 PG 的 `format_type()` 结果解析成语义类型。
///
/// - `typtype`：`e` 表示枚举
/// - `typecategory`：`A` 表示数组
/// - `elem_*`：数组元素类型信息
pub fn parse_pg(
    full_type: &str,
    typtype: &str,
    typecategory: &str,
    elem_typtype: Option<&str>,
    elem_type: Option<&str>,
    labels: Vec<String>,
) -> ColumnKind {
    if typecategory == "A" || full_type.ends_with("[]") {
        let elem = match elem_type {
            Some(et) if !et.is_empty() => {
                let nested = et.ends_with("[]");
                parse_pg(
                    et,
                    elem_typtype.unwrap_or("b"),
                    if nested { "A" } else { "N" },
                    None,
                    None,
                    labels,
                )
            }
            _ => parse_pg_base(full_type.trim_end_matches("[]"), Vec::new()),
        };
        return ColumnKind::Array { elem: Box::new(elem) };
    }
    if typtype == "e" {
        return ColumnKind::Enum { labels };
    }
    parse_pg_base(full_type, labels)
}

fn parse_pg_base(full_type: &str, labels: Vec<String>) -> ColumnKind {
    let (base, args) = split_type(full_type);
    let base = base.trim().to_ascii_lowercase();
    let p = args.first().copied();
    let s = args.get(1).copied();

    match base.as_str() {
        "boolean" | "bool" => ColumnKind::Bool,
        "smallint" | "int2" => ColumnKind::SmallInt,
        "integer" | "int" | "int4" => ColumnKind::Int,
        "bigint" | "int8" | "oid" => ColumnKind::BigInt,
        "real" | "float4" => ColumnKind::Real,
        "double precision" | "float8" => ColumnKind::Double,
        "numeric" | "decimal" => ColumnKind::Numeric { precision: p, scale: s },
        "money" => ColumnKind::Numeric { precision: Some(19), scale: Some(2) },
        "character varying" | "varchar" => ColumnKind::Text { max_len: p },
        "character" | "char" | "bpchar" => ColumnKind::FixedText { len: p.unwrap_or(1) },
        "text" | "citext" | "name" | "xml" | "tsvector" => ColumnKind::Text { max_len: None },
        "bytea" => ColumnKind::Bytes,
        "date" => ColumnKind::Date,
        "time without time zone" | "time" => ColumnKind::Time,
        "timestamp without time zone" | "timestamp" => ColumnKind::Timestamp,
        "timestamp with time zone" | "timestamptz" => ColumnKind::TimestampTz,
        "uuid" => ColumnKind::Uuid,
        "json" | "jsonb" | "hstore" => ColumnKind::Json,
        "enum" => ColumnKind::Enum { labels },
        // 这些 PG 类型在 MySQL 里没有等价物，但 PG→PG 时可以原样保留
        "interval" | "inet" | "cidr" | "macaddr" | "macaddr8" | "bit" | "bit varying" | "varbit"
        | "time with time zone" | "timetz" => ColumnKind::Other {
            source: full_type.to_string(),
            pg_type: Some(full_type.to_string()),
        },
        _ => ColumnKind::Other { source: full_type.to_string(), pg_type: None },
    }
}

/// 把 MySQL 的 `information_schema.COLUMNS` 信息解析成语义类型。
pub fn parse_mysql(
    data_type: &str,
    column_type: &str,
    char_len: Option<u64>,
    precision: Option<u64>,
    scale: Option<u64>,
) -> ColumnKind {
    let lower = data_type.to_ascii_lowercase();
    let column_lower = column_type.to_ascii_lowercase();
    let unsigned = column_lower.contains("unsigned");
    let char_len = char_len.map(|n| n.min(u32::MAX as u64) as u32);
    let p = precision.map(|n| n.min(u32::MAX as u64) as u32);
    let s = scale.map(|n| n.min(u32::MAX as u64) as u32);

    match lower.as_str() {
        "tinyint" => {
            if column_lower.starts_with("tinyint(1)") && !unsigned {
                ColumnKind::Bool
            } else if unsigned {
                ColumnKind::Int
            } else {
                ColumnKind::SmallInt
            }
        }
        "smallint" => {
            if unsigned {
                ColumnKind::Int
            } else {
                ColumnKind::SmallInt
            }
        }
        "mediumint" | "int" | "integer" => {
            if unsigned {
                ColumnKind::BigInt
            } else {
                ColumnKind::Int
            }
        }
        "bigint" => {
            if unsigned {
                // PG 的 bigint 放不下 0..2^64-1，用 numeric(20,0) 承载
                ColumnKind::UnsignedBigInt
            } else {
                ColumnKind::BigInt
            }
        }
        "float" => ColumnKind::Real,
        "double" | "real" => ColumnKind::Double,
        "decimal" | "numeric" => ColumnKind::Numeric { precision: p, scale: s },
        "char" => ColumnKind::FixedText { len: char_len.unwrap_or(1) },
        "varchar" => ColumnKind::Text { max_len: char_len },
        "tinytext" | "text" | "mediumtext" | "longtext" => ColumnKind::Text { max_len: None },
        // 二进制与空间类型都按字节搬运，避免 UTF-8 转换失败
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit"
        | "geometry" | "point" | "linestring" | "polygon" | "multipoint"
        | "multilinestring" | "multipolygon" | "geomcollection" | "geometrycollection" => {
            ColumnKind::Bytes
        }
        "date" => ColumnKind::Date,
        "time" => ColumnKind::Time,
        "datetime" => ColumnKind::Timestamp,
        "timestamp" => ColumnKind::TimestampTz,
        "year" => ColumnKind::SmallInt,
        "json" => ColumnKind::Json,
        "enum" => ColumnKind::Enum { labels: parse_mysql_labels(column_type) },
        "set" => ColumnKind::Set { labels: parse_mysql_labels(column_type) },
        _ => ColumnKind::Other { source: column_type.to_string(), pg_type: None },
    }
}

/// 解析 MySQL 的 `enum('a','b')` 标签，处理 `''` 与 `\\` 转义。
fn parse_mysql_labels(column_type: &str) -> Vec<String> {
    let Some(open) = column_type.find('(') else {
        return Vec::new();
    };
    let inner: Vec<char> = column_type[open + 1..].trim_end_matches(')').chars().collect();
    let mut labels = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        if in_quote {
            match c {
                '\\' if i + 1 < inner.len() => {
                    cur.push(inner[i + 1]);
                    i += 2;
                    continue;
                }
                '\'' if i + 1 < inner.len() && inner[i + 1] == '\'' => {
                    cur.push('\'');
                    i += 2;
                    continue;
                }
                '\'' => in_quote = false,
                _ => cur.push(c),
            }
        } else if c == '\'' {
            in_quote = true;
        } else if c == ',' {
            labels.push(std::mem::take(&mut cur));
        }
        i += 1;
    }
    if !cur.is_empty() {
        labels.push(cur);
    }
    labels
}

/// 生成 PG 的目标类型。`enum_type` 是已经带引号的枚举类型全名。
pub fn pg_type(kind: &ColumnKind, enum_type: &str) -> String {
    match kind {
        ColumnKind::Bool => "boolean".to_string(),
        ColumnKind::SmallInt => "smallint".to_string(),
        ColumnKind::Int => "integer".to_string(),
        ColumnKind::BigInt => "bigint".to_string(),
        // 无符号 64 位整数超出 PG bigint 范围，用 numeric 承载
        ColumnKind::UnsignedBigInt => "numeric(20,0)".to_string(),
        ColumnKind::Real => "real".to_string(),
        ColumnKind::Double => "double precision".to_string(),
        ColumnKind::Numeric { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) => format!("numeric({p},{s})"),
            (Some(p), None) => format!("numeric({p})"),
            _ => "numeric".to_string(),
        },
        ColumnKind::Text { max_len } => match max_len {
            Some(n) => format!("character varying({n})"),
            None => "text".to_string(),
        },
        ColumnKind::FixedText { len } => format!("character({len})"),
        ColumnKind::Bytes => "bytea".to_string(),
        ColumnKind::Date => "date".to_string(),
        ColumnKind::Time => "time".to_string(),
        ColumnKind::Timestamp => "timestamp".to_string(),
        ColumnKind::TimestampTz => "timestamptz".to_string(),
        ColumnKind::Json => "jsonb".to_string(),
        ColumnKind::Uuid => "uuid".to_string(),
        ColumnKind::Enum { .. } => enum_type.to_string(),
        ColumnKind::Set { .. } => "text".to_string(),
        ColumnKind::Array { elem } => format!("{}[]", pg_type(elem, enum_type)),
        ColumnKind::Other { pg_type: Some(t), .. } => t.clone(),
        ColumnKind::Other { .. } => "text".to_string(),
    }
}

/// 生成 MySQL 的目标类型。
pub fn mysql_type(kind: &ColumnKind) -> String {
    match kind {
        ColumnKind::Bool => "TINYINT(1)".to_string(),
        ColumnKind::SmallInt => "SMALLINT".to_string(),
        ColumnKind::Int => "INT".to_string(),
        ColumnKind::BigInt => "BIGINT".to_string(),
        ColumnKind::UnsignedBigInt => "BIGINT UNSIGNED".to_string(),
        ColumnKind::Real => "FLOAT".to_string(),
        ColumnKind::Double => "DOUBLE".to_string(),
        ColumnKind::Numeric { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) => format!("DECIMAL({},{})", (*p).min(65), (*s).min(30)),
            (Some(p), None) => format!("DECIMAL({},0)", (*p).min(65)),
            _ => "DECIMAL(65,30)".to_string(),
        },
        ColumnKind::Text { max_len } => match max_len {
            Some(n) if *n <= 16383 => format!("VARCHAR({n})"),
            _ => "LONGTEXT".to_string(),
        },
        ColumnKind::FixedText { len } => {
            if *len <= 255 {
                format!("CHAR({len})")
            } else {
                "LONGTEXT".to_string()
            }
        }
        ColumnKind::Bytes => "LONGBLOB".to_string(),
        ColumnKind::Date => "DATE".to_string(),
        ColumnKind::Time => "TIME(6)".to_string(),
        ColumnKind::Timestamp | ColumnKind::TimestampTz => "DATETIME(6)".to_string(),
        ColumnKind::Json => "JSON".to_string(),
        ColumnKind::Uuid => "CHAR(36)".to_string(),
        ColumnKind::Enum { labels } => {
            if labels.is_empty() {
                "VARCHAR(191)".to_string()
            } else {
                let vals = labels
                    .iter()
                    .map(|l| format!("'{}'", escape_mysql_literal(l)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ENUM({vals})")
            }
        }
        ColumnKind::Set { labels } => {
            if labels.is_empty() {
                "VARCHAR(191)".to_string()
            } else {
                let vals = labels
                    .iter()
                    .map(|l| format!("'{}'", escape_mysql_literal(l)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("SET({vals})")
            }
        }
        ColumnKind::Array { .. } => "JSON".to_string(),
        ColumnKind::Other { .. } => "LONGTEXT".to_string(),
    }
}

/// MySQL 字符串字面量转义。
pub fn escape_mysql_literal(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

/// 拆出 `numeric(10,2)` 这类类型的基础名和精度参数。
fn split_type(pg_type: &str) -> (&str, Vec<u32>) {
    let Some(open) = pg_type.find('(') else {
        return (pg_type, Vec::new());
    };
    let Some(close) = pg_type[open..].find(')').map(|i| open + i) else {
        return (pg_type, Vec::new());
    };
    let args = pg_type[open + 1..close]
        .split(',')
        .filter_map(|a| a.trim().parse::<u32>().ok())
        .collect();
    (pg_type[..open].trim(), args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_varchar_keeps_length() {
        assert_eq!(
            parse_pg("character varying(64)", "b", "S", None, None, vec![]),
            ColumnKind::Text { max_len: Some(64) }
        );
    }

    #[test]
    fn pg_timestamptz() {
        assert_eq!(
            parse_pg("timestamp with time zone", "b", "D", None, None, vec![]),
            ColumnKind::TimestampTz
        );
    }

    #[test]
    fn pg_array_of_enum() {
        let kind = parse_pg("mood[]", "b", "A", Some("e"), Some("mood"), vec!["a".into(), "b".into()]);
        assert_eq!(
            kind,
            ColumnKind::Array {
                elem: Box::new(ColumnKind::Enum { labels: vec!["a".into(), "b".into()] })
            }
        );
    }

    #[test]
    fn pg_array_renders_with_type() {
        let kind = ColumnKind::Array { elem: Box::new(ColumnKind::Int) };
        assert_eq!(pg_type(&kind, "\"public\".\"t_c\""), "integer[]");
    }

    #[test]
    fn mysql_enum_labels() {
        assert_eq!(
            parse_mysql("enum", "enum('a','b,c','d''e')", None, None, None),
            ColumnKind::Enum { labels: vec!["a".into(), "b,c".into(), "d'e".into()] }
        );
    }

    #[test]
    fn mysql_tinyint1_is_bool() {
        assert_eq!(
            parse_mysql("tinyint", "tinyint(1)", None, None, None),
            ColumnKind::Bool
        );
        assert_eq!(
            parse_mysql("tinyint", "tinyint(4)", None, None, None),
            ColumnKind::SmallInt
        );
    }

    #[test]
    fn mysql_bigint_unsigned_becomes_numeric() {
        assert_eq!(
            parse_mysql("bigint", "bigint unsigned", None, None, None),
            ColumnKind::UnsignedBigInt
        );
        assert_eq!(mysql_type(&ColumnKind::UnsignedBigInt), "BIGINT UNSIGNED");
        assert_eq!(
            pg_type(&ColumnKind::UnsignedBigInt, "unused"),
            "numeric(20,0)"
        );
    }

    #[test]
    fn mysql_spatial_becomes_bytes() {
        for ty in ["point", "geometry", "polygon", "multilinestring"] {
            assert_eq!(
                parse_mysql(ty, ty, None, None, None),
                ColumnKind::Bytes,
                "{ty} 应按字节搬运"
            );
        }
    }

    #[test]
    fn mysql_enum_renders_escaped() {
        let kind = ColumnKind::Enum { labels: vec!["a".into(), "d'e".into()] };
        assert_eq!(mysql_type(&kind), "ENUM('a', 'd''e')");
    }
}
