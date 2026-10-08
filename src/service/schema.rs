//! Schema introspection handler functions for the DuckDB SyncWorker.
//!
//! These functions run on the worker thread with direct `&Connection` access.
//! DuckDB uses information_schema queries (not PRAGMA like SQLite).

use duckdb::Connection;
use voidb_core::ColumnInfo;
use voidb_core::database::types::ForeignKeyInfo;

use super::events::IndexInfo;

/// (columns, indexes, foreign_keys)
type StructureResult = (Vec<ColumnInfo>, Vec<IndexInfo>, Vec<ForeignKeyInfo>);

/// List all tables and views in the database.
pub fn handle_list_tables(conn: &Connection) -> Result<(Vec<super::events::TableMeta>, Vec<String>), String> {
    let mut stmt = conn
        .prepare(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'main' \
             ORDER BY table_name"
        )
        .map_err(|e| format!("list_tables: {}", e))?;

    let entries: Vec<(String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| format!("list_tables query: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    let mut tables = Vec::new();
    let mut views = Vec::new();

    for (name, obj_type) in entries {
        if obj_type == "VIEW" {
            views.push(name);
        } else {
            let count: Option<u64> = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM \"{}\"", name.replace('"', "\"\"")),
                    [],
                    |r| r.get(0),
                )
                .ok();
            tables.push(super::events::TableMeta {
                name,
                table_type: "table".to_string(),
                rows: count,
            });
        }
    }

    Ok((tables, views))
}

/// Get the CREATE TABLE DDL for a table.
pub fn handle_show_create_table(conn: &Connection, table: &str) -> Result<String, String> {
    let schema = crate::duckdb_describe_table(conn, table)?;
    Ok(crate::duckdb_generate_create_table(&schema))
}

/// Load full structure info (columns + indexes + foreign keys).
pub fn handle_load_structure(
    conn: &Connection,
    table: &str,
) -> Result<StructureResult, String> {
    let schema = crate::duckdb_describe_table(conn, table)?;

    let indexes: Vec<IndexInfo> = crate::duckdb_list_indexes(conn, table)
        .into_iter()
        .map(|idx| IndexInfo {
            name: idx.name,
            columns: idx.columns,
            unique: idx.unique,
            index_type: idx.index_type,
        })
        .collect();

    let foreign_keys = crate::duckdb_list_foreign_keys(conn, table);

    Ok((schema.columns, indexes, foreign_keys))
}
