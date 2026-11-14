//! 值的格式转换：PG 数组文本 → JSON，字符串转义等。

use crate::model::ColumnKind;

/// 把 PG 的数组文本（如 `{1,2,3}`、`{{1,2},{3,4}}`）转成 JSON 数组文本。
///
/// MySQL 没有数组类型，统一落成 JSON。元素类型只用于决定数字/布尔是否加引号。
pub fn pg_array_to_json(text: &str, elem: &ColumnKind) -> String {
    let s = text.trim();
    // 多维数组可能带 `[1:2]=` 前缀
    let s = if s.starts_with('[') {
        match s.find('=') {
            Some(i) => &s[i + 1..],
            None => s,
        }
    } else {
        s
    };
    let chars: Vec<char> = s.chars().collect();
    let mut parser = Parser { chars: &chars, pos: 0 };
    let out = parser.array(elem);
    if parser.pos == 0 { "[]".to_string() } else { out }
}

struct Parser<'a> {
    chars: &'a [char],
    pos: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.pos += 1;
        }
    }

    fn array(&mut self, elem: &ColumnKind) -> String {
        self.skip_ws();
        if self.peek() != Some('{') {
            return "[]".to_string();
        }
        self.pos += 1;
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                Some('}') => {
                    self.pos += 1;
                    break;
                }
                Some('{') => items.push(self.array(elem)),
                Some(_) => items.push(self.element(elem)),
                None => break,
            }
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some('}') => {
                    self.pos += 1;
                    break;
                }
                _ => break,
            }
        }
        format!("[{}]", items.join(","))
    }

    fn element(&mut self, elem: &ColumnKind) -> String {
        // 多维数组递归时，标量要按最内层元素类型解析
        let elem = base_elem(elem);
        if self.peek() == Some('"') {
            self.pos += 1;
            let mut s = String::new();
            while let Some(c) = self.peek() {
                self.pos += 1;
                match c {
                    '\\' => {
                        if let Some(next) = self.peek() {
                            s.push(next);
                            self.pos += 1;
                        }
                    }
                    '"' => break,
                    _ => s.push(c),
                }
            }
            return json_scalar(elem, &s);
        }

        let mut token = String::new();
        while let Some(c) = self.peek() {
            if c == ',' || c == '}' {
                break;
            }
            token.push(c);
            self.pos += 1;
        }
        let token = token.trim();
        if token.eq_ignore_ascii_case("NULL") {
            "null".to_string()
        } else {
            json_scalar(elem, token)
        }
    }
}

/// 取数组最内层的元素类型。
fn base_elem(kind: &ColumnKind) -> &ColumnKind {
    match kind {
        ColumnKind::Array { elem } => base_elem(elem),
        other => other,
    }
}

/// 按元素类型把单个元素写成 JSON 值。
fn json_scalar(elem: &ColumnKind, raw: &str) -> String {
    match elem {
        ColumnKind::Bool => match raw.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => "true".to_string(),
            "f" | "false" | "0" => "false".to_string(),
            _ => json_string(raw),
        },
        ColumnKind::SmallInt
        | ColumnKind::Int
        | ColumnKind::BigInt
        | ColumnKind::UnsignedBigInt
        | ColumnKind::Real
        | ColumnKind::Double
        | ColumnKind::Numeric { .. } => {
            if raw.parse::<f64>().is_ok() { raw.to_string() } else { json_string(raw) }
        }
        // 即使原数组里带引号，数字/布尔也按语义还原
        _ => json_string(raw),
    }
}

/// JSON 字符串字面量。
pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// 转成 PG 的 bytea 文本输入格式（`\x` + 十六进制）。
pub fn bytes_to_pg_hex(bytes: &[u8]) -> String {
    format!("\\x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_dimension_int_array() {
        assert_eq!(pg_array_to_json("{1,2,3}", &ColumnKind::Int), "[1,2,3]");
    }

    #[test]
    fn text_array_with_quotes() {
        assert_eq!(
            pg_array_to_json(r#"{"a,b","say \"hi\""}"#, &ColumnKind::Text { max_len: None }),
            r#"["a,b","say \"hi\""]"#
        );
    }

    #[test]
    fn null_and_bool() {
        assert_eq!(
            pg_array_to_json("{t,f,NULL}", &ColumnKind::Bool),
            "[true,false,null]"
        );
    }

    #[test]
    fn nested_array() {
        let elem = ColumnKind::Array { elem: Box::new(ColumnKind::Int) };
        assert_eq!(pg_array_to_json("{{1,2},{3,4}}", &elem), "[[1,2],[3,4]]");
    }

    #[test]
    fn empty_array() {
        assert_eq!(pg_array_to_json("{}", &ColumnKind::Int), "[]");
    }

    #[test]
    fn dimension_prefix_is_stripped() {
        assert_eq!(pg_array_to_json("[1:2]={1,2}", &ColumnKind::Int), "[1,2]");
    }
}
