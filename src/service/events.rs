//! DuckDB service event types.
//!
//! Events are sent from the SyncWorker back to the TUI plugin.
//! The TUI polls events via `DuckDbService::poll_event()`.

use voidb_core::{ColumnInfo, Row};
use voidb_core::database::types::ForeignKeyInfo;

/// Top-level event enum for the DuckDB service.
#[derive(Debug)]
pub enum DuckDbEvent {
    /// Schema introspection results.
    Schema(SchemaEvent),

    /// CRUD operation results.
    Crud(CrudEvent),

    /// Data loading results.
    Load(LoadEvent),

    /// Browser operation results.
    Browser(BrowserEvent),

    /// An error occurred during an operation.
    Error(String),
}

/// Schema introspection events.
#[derive(Debug)]
pub enum SchemaEvent {
    /// Tables and views loaded.
    TablesLoaded {
        tables: Vec<TableMeta>,
        views: Vec<String>,
    },

    /// CREATE TABLE DDL for a table.
    CreateTableDdl { table: String, ddl: String },

    /// Combined structure info (columns + indexes + foreign keys).
    StructureLoaded {
        table: String,
        columns: Vec<ColumnInfo>,
        indexes: Vec<IndexInfo>,
        foreign_keys: Vec<ForeignKeyInfo>,
    },
}

/// CRUD operation events.
#[derive(Debug)]
pub enum CrudEvent {
    /// Single query result (columns + rows).
    QueryResult {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
    },

    /// Multiple statement results.
    MultiQueryResult { results: Vec<StatementResult> },

    /// Cell edits and row deletions applied.
    ChangesSaved { updates: usize, deletes: usize },

    /// Paste operation completed.
    PasteComplete { inserted: usize },

    /// CSV import completed.
    ImportComplete,
}

/// Result of a single SQL statement within a multi-query execution.
#[derive(Debug)]
pub enum StatementResult {
    /// A SELECT result.
    Select {
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
    },
    /// An INSERT/UPDATE/DELETE result with affected row count.
    Affected(u64),
    /// Empty result set.
    Empty,
    /// Statement execution error.
    Error(String),
}

/// Data loading events.
#[derive(Debug)]
pub enum LoadEvent {
    /// A page of table data with primary keys and total row count.
    TableDataLoaded {
        table: String,
        pks: Vec<String>,
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
        total: u64,
    },

    /// Total row count for a table.
    RowCountLoaded { table: String, count: u64 },

    /// Primary key column names for a table.
    PrimaryKeysLoaded { table: String, pks: Vec<String> },
}

/// Browser / DDL operation events.
#[derive(Debug)]
pub enum BrowserEvent {
    /// Tables loaded for browser display.
    TablesLoaded {
        tables: Vec<TableMeta>,
        views: Vec<String>,
    },

    /// Preview data loaded for a table.
    PreviewLoaded {
        table: String,
        columns: Vec<ColumnInfo>,
        rows: Vec<Row>,
        row_count: u64,
        ddl: String,
    },

    /// Copy data ready for clipboard write (internal use by facade).
    CopyDataReady { data: Vec<super::commands::TableCopyData> },

    /// Copy operation completed.
    CopyDone,

    /// Paste operation completed.
    PasteDone,

    /// Raw SQL executed in browser context.
    SqlExecuted { result: String },

    /// Table dropped successfully.
    TableDropped { table: String },
}

/// Table metadata for browser display.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// Table name.
    pub name: String,
    /// Table type (e.g., "table", "view").
    pub table_type: String,
    /// Row count (if available).
    pub rows: Option<u64>,
}

/// Index metadata (service-layer copy, decoupled from voidb_core::IndexInfo).
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Columns in the index.
    pub columns: Vec<String>,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Index type (e.g., "BTREE").
    pub index_type: String,
}
