mod agent_session;
mod capabilities;
mod cli_plugin;
mod config;
pub mod service;

use async_trait::async_trait;
use duckdb::Connection;

use voidb_core::connection::{ConnectionConfig, DatabaseType};
use voidb_core::database::types::*;
use voidb_core::database::DatabaseAdapter;
use voidb_core::error::VoidbError;

pub use capabilities::{duckdb_capabilities, invoke_duckdb_capability};
pub use agent_session::DuckDbAgentSessionFactory;
pub use cli_plugin::create_duckdb_cli_plugin;
pub use config::DuckDbConfig;
pub use service::{DuckDbCommand, DuckDbEvent, DuckDbService};

/// Open a DuckDB connection from config.
pub fn open_duckdb(config: &DuckDbConfig) -> Result<Connection, String> {
    let conn = if config.path == ":memory:" || config.path.is_empty() {
        Connection::open_in_memory()
    } else if config.read_only {
        let cfg = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .map_err(|e| format!("Config error: {}", e))?;
        Connection::open_with_flags(&config.path, cfg)
    } else {
        Connection::open(&config.path)
    }
    .map_err(|e| format!("Failed to open DuckDB: {}", e))?;

    // Apply settings
    if let Some(ref limit) = config.memory_limit {
        let _ = conn.execute_batch(&format!("SET memory_limit = '{}';", limit));
    }
    if let Some(threads) = config.threads {
        let _ = conn.execute_batch(&format!("SET threads = {};", threads));
    }

    // Load extensions
    for ext in &config.extensions {
        let _ = conn.execute_batch(&format!("INSTALL '{}'; LOAD '{}';", ext, ext));
    }

    Ok(conn)
}

/// Query table schema from DuckDB using information_schema.
pub fn duckdb_describe_table(conn: &Connection, table: &str) -> Result<TableSchema, String> {
    // Columns via information_schema
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type, is_nullable, column_default
             FROM information_schema.columns
             WHERE table_name = ?
             ORDER BY ordinal_position",
        )
        .map_err(|e| format!("describe columns failed: {}", e))?;

    // Get primary key columns
    let pk_cols = duckdb_pk_columns(conn, table);

    let columns: Vec<ColumnInfo> = stmt
        .query_map([table], |row| {
            let name: String = row.get(0)?;
            let data_type: String = row.get(1)?;
            let nullable: String = row.get(2)?;
            let default_value: Option<String> = row.get(3)?;
            Ok((name, data_type, nullable, default_value))
        })
        .map_err(|e| format!("columns query failed: {}", e))?
        .filter_map(|r| r.ok())
        .map(|(name, data_type, nullable, default_value)| {
            let is_pk = pk_cols.contains(&name);
            ColumnInfo {
                name,
                data_type,
                nullable: nullable == "YES",
                is_primary_key: is_pk,
                default_value,
                max_length: None,
                extra: if is_pk {
                    "PRIMARY KEY".to_string()
                } else {
                    String::new()
                },
            }
        })
        .collect();

    // Indexes
    let indexes = duckdb_list_indexes(conn, table);

    // DuckDB doesn't have foreign_key_list via PRAGMA, query information_schema
    let foreign_keys = duckdb_list_foreign_keys(conn, table);

    Ok(TableSchema {
        database: String::new(),
        table_name: table.to_string(),
        columns,
        indexes,
        foreign_keys,
        engine: None,
        comment: None,
    })
}

pub fn duckdb_pk_columns(conn: &Connection, table: &str) -> Vec<String> {
    let sql = "SELECT column_name FROM information_schema.key_column_usage kcu
               JOIN information_schema.table_constraints tc
               ON kcu.constraint_name = tc.constraint_name
               WHERE tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'
               ORDER BY kcu.ordinal_position";
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    match stmt.query_map([table], |row| row.get::<_, String>(0)) {
        Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
        Err(_) => Vec::new(),
    }
}

pub fn duckdb_list_indexes(conn: &Connection, table: &str) -> Vec<IndexInfo> {
    let sql = "SELECT index_name, is_unique, sql
               FROM duckdb_indexes()
               WHERE table_name = ?";
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let mapped = match stmt.query_map([table], |row| {
        let name: String = row.get(0)?;
        let unique: bool = row.get(1)?;
        let sql: Option<String> = row.get(2)?;
        Ok((name, unique, sql))
    }) {
        Ok(rows) => rows.filter_map(|r| r.ok()).collect::<Vec<_>>(),
        Err(_) => return Vec::new(),
    };
    mapped.into_iter()
    .map(|(name, unique, _sql)| IndexInfo {
        name,
        columns: Vec::new(),
        unique,
        index_type: "BTREE".to_string(),
    })
    .collect()
}

pub fn duckdb_list_foreign_keys(conn: &Connection, table: &str) -> Vec<ForeignKeyInfo> {
    let sql = "SELECT tc.constraint_name,
                      kcu.column_name,
                      ccu.table_name AS ref_table,
                      ccu.column_name AS ref_column
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
               JOIN information_schema.constraint_column_usage ccu
                 ON tc.constraint_name = ccu.constraint_name
               WHERE tc.table_name = ? AND tc.constraint_type = 'FOREIGN KEY'";
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let rows: Vec<(String, String, String, String)> = match stmt
        .query_map([table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        }) {
        Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
        Err(_) => return Vec::new(),
    };

    let mut fk_map: std::collections::HashMap<String, ForeignKeyInfo> =
        std::collections::HashMap::new();
    for (name, col, ref_table, ref_col) in rows {
        let entry = fk_map.entry(name.clone()).or_insert_with(|| ForeignKeyInfo {
            name,
            columns: Vec::new(),
            referenced_table: ref_table,
            referenced_columns: Vec::new(),
            on_update: String::new(),
            on_delete: String::new(),
        });
        entry.columns.push(col);
        entry.referenced_columns.push(ref_col);
    }
    fk_map.into_values().collect()
}

/// Generate CREATE TABLE SQL for DuckDB from a TableSchema.
pub fn duckdb_generate_create_table(schema: &TableSchema) -> String {
    let mut parts = Vec::new();
    let mut pk_cols: Vec<&str> = Vec::new();

    for col in &schema.columns {
        let mut def = format!("\"{}\" {}", col.name.replace('"', "\"\""), col.data_type);
        if !col.nullable {
            def.push_str(" NOT NULL");
        }
        if let Some(ref dv) = col.default_value {
            def.push_str(&format!(" DEFAULT {}", dv));
        }
        if col.is_primary_key {
            pk_cols.push(&col.name);
        }
        parts.push(def);
    }

    if !pk_cols.is_empty() {
        let cols_str = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!("PRIMARY KEY ({})", cols_str));
    }

    for fk in &schema.foreign_keys {
        let from = fk
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let to = fk
            .referenced_columns
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!(
            "FOREIGN KEY ({}) REFERENCES \"{}\" ({})",
            from,
            fk.referenced_table.replace('"', "\"\""),
            to
        ));
    }

    format!(
        "CREATE TABLE \"{}\" (\n  {}\n)",
        schema.table_name.replace('"', "\"\""),
        parts.join(",\n  ")
    )
}

pub struct DuckDbAdapter {
    connection_id: String,
}

#[async_trait]
impl DatabaseAdapter for DuckDbAdapter {
    async fn connect(config: &ConnectionConfig) -> Result<Box<dyn DatabaseAdapter>, VoidbError> {
        let _duckdb_config: DuckDbConfig = config
            .plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid DuckDB config: {}", e)))
            })?;

        Err(VoidbError::Other(
            "DuckDB adapter: use plugin directly".to_string(),
        ))
    }
    async fn ping(&self) -> Result<(), VoidbError> {
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), VoidbError> {
        Ok(())
    }
    fn db_type(&self) -> DatabaseType {
        DatabaseType::Plugin
    }
    fn connection_id(&self) -> String {
        self.connection_id.clone()
    }
    async fn list_databases(&self) -> Result<Vec<String>, VoidbError> {
        Ok(Vec::new())
    }
    async fn list_tables(&self, _database: &str) -> Result<Vec<TableInfo>, VoidbError> {
        Ok(Vec::new())
    }
    async fn list_views(&self, _database: &str) -> Result<Vec<ViewInfo>, VoidbError> {
        Ok(Vec::new())
    }
    async fn describe_table(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<TableSchema, VoidbError> {
        Err(VoidbError::Other("Not implemented".to_string()))
    }
    async fn list_indexes(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Vec<IndexInfo>, VoidbError> {
        Ok(Vec::new())
    }
    async fn list_foreign_keys(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>, VoidbError> {
        Ok(Vec::new())
    }
    async fn query_rows(
        &self,
        _sql: &str,
        _params: &[QueryParam],
        _offset: u64,
        _limit: u64,
    ) -> Result<QueryResult, VoidbError> {
        Err(VoidbError::Other("Not implemented".to_string()))
    }
    async fn execute(
        &self,
        _sql: &str,
        _params: &[QueryParam],
    ) -> Result<ExecuteResult, VoidbError> {
        Err(VoidbError::Other("Not implemented".to_string()))
    }
    async fn execute_query(
        &self,
        _sql: &str,
        _database: Option<&str>,
    ) -> Result<QueryResult, VoidbError> {
        Err(VoidbError::Other("Not implemented".to_string()))
    }
    async fn count_rows(
        &self,
        _database: &str,
        _table: &str,
        _filter: Option<&str>,
    ) -> Result<u64, VoidbError> {
        Ok(0)
    }
    async fn get_create_table_sql(
        &self,
        _database: &str,
        table: &str,
    ) -> Result<String, VoidbError> {
        Err(VoidbError::Other(format!(
            "get_create_table_sql not implemented for DuckDB table: {}",
            table
        )))
    }
    fn generate_create_table_sql(&self, schema: &TableSchema) -> String {
        duckdb_generate_create_table(schema)
    }
    fn quote_identifier(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Test connection by opening and running SELECT 1.
pub fn test_connection(config: &DuckDbConfig) -> Result<String, String> {
    let conn = open_duckdb(config)?;
    let version: String = conn
        .query_row("SELECT version()", [], |row| row.get(0))
        .map_err(|e| format!("Query failed: {}", e))?;
    Ok(format!("DuckDB {}", version))
}
