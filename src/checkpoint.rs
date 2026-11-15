//! 断点续传：记录已完成的表与增量同步水位。

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// 表名 → 已搬运行数
    #[serde(default)]
    pub completed: BTreeMap<String, u64>,
    /// 表名 → 增量同步水位
    #[serde(default)]
    pub watermarks: BTreeMap<String, String>,
    /// 表名 → 行级断点（已搬到的最大主键）
    #[serde(default)]
    pub progresses: BTreeMap<String, String>,
    /// MySQL binlog 增量同步位点
    #[serde(default)]
    pub binlog: Option<BinlogPos>,
}

/// MySQL binlog 位点。
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BinlogPos {
    pub file: String,
    pub pos: u64,
}

impl Checkpoint {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = fs::read_to_string(path)
            .with_context(|| format!("读取断点文件 {} 失败", path.display()))?;
        if text.trim().is_empty() {
            return Ok(Self::default());
        }
        serde_json::from_str(&text).context("断点文件不是合法的 JSON")
    }

    /// 原子写入：先写临时文件再改名，避免中断留下半个文件。
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("写入断点文件 {} 失败", tmp.display()))?;
        fs::rename(&tmp, path).with_context(|| format!("更新断点文件 {} 失败", path.display()))?;
        Ok(())
    }

    pub fn is_done(&self, table: &str) -> bool {
        self.completed.contains_key(table)
    }

    pub fn mark_done(&mut self, table: &str, rows: u64) {
        self.completed.insert(table.to_string(), rows);
    }

    pub fn watermark(&self, table: &str) -> Option<&String> {
        self.watermarks.get(table)
    }

    pub fn set_watermark(&mut self, table: &str, value: &str) {
        self.watermarks.insert(table.to_string(), value.to_string());
    }

    /// 行级断点：该表已经搬到的最大主键。
    pub fn progress(&self, table: &str) -> Option<&String> {
        self.progresses.get(table)
    }

    pub fn set_progress(&mut self, table: &str, value: &str) {
        self.progresses.insert(table.to_string(), value.to_string());
    }
}
