//! 数据库方言、表结构与值的中间模型。
//!
//! 源库和目标库的内省结果都会归一化成这里的 [`Table`]，DDL 与数据搬运再按目标方言输出。

use anyhow::{Result, bail};

/// 参与迁移的数据库方言。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mysql,
}

impl Dialect {
    /// 根据连接串猜方言：`postgres://`、`host=...` 视为 PG，`mysql://` 视为 MySQL。
    pub fn detect(url: &str) -> Result<Self> {
        let lower = url.trim().to_ascii_lowercase();
        if lower.starts_with("postgres://")
            || lower.starts_with("postgresql://")
            || lower.starts_with("host=")
            || lower.starts_with("dbname=")
            || lower.starts_with("user=")
        {
            return Ok(Dialect::Postgres);
        }
        if lower.starts_with("mysql://") || lower.starts_with("mariadb://") {
            return Ok(Dialect::Mysql);
        }
        bail!("无法识别连接串的数据库类型，请以 postgres:// 或 mysql:// 开头：{url}")
    }

    pub fn label(self) -> &'static str {
        match self {
            Dialect::Postgres => "PostgreSQL",
            Dialect::Mysql => "MySQL",
        }
    }

    /// 引用标识符。
    pub fn quote(self, ident: &str) -> String {
        match self {
            Dialect::Postgres => format!("\"{}\"", ident.replace('"', "\"\"")),
            Dialect::Mysql => format!("`{}`", ident.replace('`', "``")),
        }
    }

    pub fn qualified(self, ns: &str, table: &str) -> String {
        format!("{}.{}", self.quote(ns), self.quote(table))
    }
}

/// 一个端点（源或目标）的描述，参数可克隆给并行任务各自建连接。
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub dialect: Dialect,
    pub url: String,
    /// PG 是 schema，MySQL 是库名
    pub namespace: String,
}

/// 连接串没写密码时，从环境变量补齐（PG 用 PGPASSWORD，MySQL 用 MYSQL_PWD），
/// 避免密码出现在命令行历史与进程列表里。
pub fn apply_env_password(url: &str, dialect: Dialect) -> String {
    let var = match dialect {
        Dialect::Postgres => "PGPASSWORD",
        Dialect::Mysql => "MYSQL_PWD",
    };
    match std::env::var(var) {
        Ok(password) if !password.is_empty() => inject_password(url, &password),
        _ => url.to_string(),
    }
}

/// 把密码按百分号编码写进 URL 的 userinfo；已有密码或没有用户名时原样返回。
fn inject_password(url: &str, password: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let Some(at) = authority.rfind('@') else {
        return url.to_string();
    };
    let userinfo = &authority[..at];
    if userinfo.contains(':') {
        return url.to_string();
    }
    format!(
        "{}://{}:{}@{}{}",
        &url[..scheme_end],
        userinfo,
        percent_encode_userinfo(password),
        &authority[at + 1..],
        &rest[authority_end..]
    )
}

/// userinfo 里只保留 RFC 3986 的非保留字符，其余按 UTF-8 字节转义。
fn percent_encode_userinfo(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// 归一化后的列类型。只保留迁移需要的语义，具体类型名由目标方言生成。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnKind {
    Bool,
    SmallInt,
    Int,
    BigInt,
    /// MySQL 的 BIGINT UNSIGNED：PG 用 numeric(20,0) 承载，MySQL 侧保持原类型
    UnsignedBigInt,
    Real,
    Double,
    Numeric {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Text {
        max_len: Option<u32>,
    },
    FixedText {
        len: u32,
    },
    Bytes,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Json,
    Uuid,
    Enum {
        labels: Vec<String>,
    },
    /// MySQL 的 SET：MySQL 目标保留枚举约束，PG 目标用 text 承载
    Set {
        labels: Vec<String>,
    },
    Array {
        elem: Box<ColumnKind>,
    },
    /// 未识别的类型，按文本搬运；PG 源会保留原始类型名，PG→PG 时不丢语义。
    Other {
        source: String,
        pg_type: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub kind: ColumnKind,
    pub not_null: bool,
    /// 源库默认值表达式，翻译阶段再处理
    pub default: Option<String>,
    /// serial / identity / AUTO_INCREMENT
    pub auto_increment: bool,
    /// MySQL 的 ON UPDATE CURRENT_TIMESTAMP
    pub on_update_timestamp: bool,
    pub comment: Option<String>,
}

/// 生成列：数据不参与搬运，目标库按表达式重算。
#[derive(Debug, Clone)]
pub struct GeneratedColumn {
    pub name: String,
    pub kind: ColumnKind,
    /// 源库里的生成表达式（PG 语法）
    pub expression: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Table {
    /// 源命名空间
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<Index>,
    pub foreign_keys: Vec<ForeignKey>,
    /// 生成列（只用于 DDL，不参与数据搬运）
    pub generated_columns: Vec<GeneratedColumn>,
    pub comment: Option<String>,
}

/// 目标库现有列的状态，用于结构漂移比对。
#[derive(Debug, Clone)]
pub struct ColumnState {
    pub name: String,
    /// 目标方言的类型文本（PG 来自 format_type，MySQL 来自 COLUMN_TYPE）
    pub data_type: String,
    pub not_null: bool,
}

/// 普通视图。定义保留源方言 SQL，只在同方言之间迁移。
#[derive(Debug, Clone)]
pub struct View {
    pub name: String,
    /// `CREATE VIEW ... AS` 之后的查询主体
    pub definition: String,
}

/// 独立序列（不依附于任何列），PG 专用。
#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start: i64,
    pub cache: i64,
    pub cycle: bool,
    /// 源库当前位点；空序列（从未 nextval）时为 None
    pub last_value: Option<i64>,
    pub is_called: bool,
}

/// 搬运过程中的统一值表示。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Text(String),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }
}

/// 增量同步的过滤条件：只读取 `column > after` 的行。
#[derive(Debug, Clone)]
pub struct IncrementalFilter {
    pub column: String,
    pub after: String,
}

/// 行级断点：按单列主键顺序搬运，每批写入成功后回报已搬到的最大主键。
#[derive(Clone)]
pub struct RowResume {
    pub column: String,
    /// 上次中断时的位置，None 表示从头开始
    pub after: Option<String>,
    /// 进度回报（主键文本值），由调用方负责落盘
    pub on_progress: Option<ProgressFn>,
}

/// 进度回调：拿到当前批的最大主键文本值。
pub type ProgressFn = std::sync::Arc<dyn Fn(&str) + Send + Sync>;

impl std::fmt::Debug for RowResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowResume")
            .field("column", &self.column)
            .field("after", &self.after)
            .finish_non_exhaustive()
    }
}

/// 一张表的搬运结果。
#[derive(Debug, Clone, Default)]
pub struct CopyStat {
    pub rows: u64,
    /// 水位列的最后一个值，用于推进增量位点
    pub last_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::inject_password;

    #[test]
    fn injects_percent_encoded_password() {
        assert_eq!(
            inject_password("postgres://app@db:5432/prod", "p@ss word"),
            "postgres://app:p%40ss%20word@db:5432/prod"
        );
        assert_eq!(
            inject_password("mysql://root@127.0.0.1:3306/db", "a:b"),
            "mysql://root:a%3Ab@127.0.0.1:3306/db"
        );
    }

    #[test]
    fn keeps_url_when_password_or_user_missing() {
        // 已经带密码
        let url = "postgres://app:secret@db/prod";
        assert_eq!(inject_password(url, "other"), url);
        // 没有用户名，无法注入 userinfo
        let url = "postgres://db:5432/prod";
        assert_eq!(inject_password(url, "other"), url);
        // 不是 URL
        assert_eq!(
            inject_password("host=db user=app", "other"),
            "host=db user=app"
        );
    }
}
