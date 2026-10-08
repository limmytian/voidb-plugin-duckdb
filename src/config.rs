//! DuckDB plugin configuration structures

use serde::{Deserialize, Serialize};

/// DuckDB connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct DuckDbConfig {
    /// Database file path (or ":memory:" for in-memory)
    pub path: String,

    /// Open in read-only mode
    #[serde(default)]
    pub read_only: bool,

    /// Extensions to auto-load on connect
    #[serde(default)]
    pub extensions: Vec<String>,

    /// Memory limit (e.g. "2GB", "512MB")
    #[serde(default)]
    pub memory_limit: Option<String>,

    /// Number of threads (None = auto)
    #[serde(default)]
    pub threads: Option<u32>,
}

impl DuckDbConfig {
    pub fn new(path: String) -> Self {
        Self {
            path,
            read_only: false,
            extensions: Vec::new(),
            memory_limit: None,
            threads: None,
        }
    }
}

