//! Browser service handler functions for the DuckDB SyncWorker.
//!
//! All functions execute on the worker thread with `&Connection` access.
//! They handle table management operations for the db_browser plugin.

use duckdb::Connection;

use voidb_core::{ColumnInfo, Row};

use super::commands::{BrowserCommand, TableCopyData};
use super::events::{BrowserEvent, TableMeta};
use super::value_convert::{decode_duckdb_value, quote_duckdb_value};

/// Handle a browser command. Returns an event or error string.
pub fn handle_browser(
    conn: &Connection,
    cmd: BrowserCommand,
) -> Result<BrowserEvent, String> {
    match cmd {
        BrowserCommand::LoadTables => handle_load_tables(conn),
        BrowserCommand::LoadPreview { table } => handle_load_preview(conn, &table),
        BrowserCommand::CopyTables { tables } => handle_copy_tables(conn, &tables),
        BrowserCommand::PasteTables { table_data } => handle_paste_tables(conn, &table_data),
        BrowserCommand::ExecuteSql { sql } => handle_execute_sql(conn, &sql),
        BrowserCommand::DropTable { table } => handle_drop_table(conn, &table),
    }
}

/// Load all tables and views for the browser tree.
pub fn handle_load_tables(conn: &Connection) -> Result<BrowserEvent, String> {
    let mut stmt = conn
        .prepare(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = 'main' \
             ORDER BY table_name",
        )
        .map_err(|e| format!("Query failed: {}", e))?;

    let entries: Vec<(String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| format!("Query failed: {}", e))?
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
            tables.push(TableMeta {
                name,
                table_type: "table".into(),
                rows: count,
            });
        }
    }

    Ok(BrowserEvent::TablesLoaded { tables, views })
}

/// Load preview data for a table (first 50 rows + columns + DDL + row count).
pub fn handle_load_preview(conn: &Connection, table: &str) -> Result<BrowserEvent, String> {
    let quoted = format!("\"{}\"", table.replace('"', "\"\""));

    // Get columns via describe
    let schema = crate::duckdb_describe_table(conn, table)?;
    let columns: Vec<ColumnInfo> = schema.columns;

    // Get row count
    let row_count: u64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {}", quoted), [], |r| r.get(0))
        .map_err(|e| format!("Count failed: {}", e))?;

    // Get first 50 rows
    let sql = format!("SELECT * FROM {} LIMIT 50", quoted);
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("Prepare failed: {}", e))?;

    let raw_rows: Vec<Vec<voidb_core::CellValue>> = stmt
        .query_map([], |row| {
            let mut values = Vec::new();
            for i in 0.. {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(_) => values.push(decode_duckdb_value(row, i)),
                    Err(_) => break,
                }
            }
            Ok(values)
        })
        .map_err(|e| format!("Query failed: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    let rows: Vec<Row> = raw_rows.into_iter().map(|values| Row { values }).collect();

    // Get DDL
    let ddl = crate::duckdb_generate_create_table(&crate::duckdb_describe_table(conn, table)?);

    Ok(BrowserEvent::PreviewLoaded {
        table: table.to_string(),
        columns,
        rows,
        row_count,
        ddl,
    })
}

/// Copy table structures and data for clipboard operations.
pub fn handle_copy_tables(
    _conn: &Connection,
    _tables: &[String],
) -> Result<BrowserEvent, String> {
    // Copy data collection is handled via collect_copy_data called from mod.rs.
    // This returns CopyDone; the facade handles the rest.
    Ok(BrowserEvent::CopyDone)
}

/// Internal: collect table data for a copy operation.
/// Called from mod.rs to get the actual data for clipboard.
pub fn collect_copy_data(
    conn: &Connection,
    tables: &[String],
) -> Result<Vec<TableCopyData>, String> {
    let mut result = Vec::new();
    for table in tables {
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));

        // Get CREATE TABLE DDL
        let schema = crate::duckdb_describe_table(conn, table)?;
        let create_sql = crate::duckdb_generate_create_table(&schema);

        // Get column names
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        // Get all rows
        let sql = format!("SELECT * FROM {}", quoted);
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("SELECT from '{}' failed: {}", table, e))?;

        let raw_rows: Vec<Vec<voidb_core::CellValue>> = stmt
            .query_map([], |row| {
                let mut values = Vec::new();
                for i in 0.. {
                    match row.get::<_, duckdb::types::Value>(i) {
                        Ok(_) => values.push(decode_duckdb_value(row, i)),
                        Err(_) => break,
                    }
                }
                Ok(values)
            })
            .map_err(|e| format!("Query '{}' failed: {}", table, e))?
            .filter_map(|r| r.ok())
            .collect();

        let rows: Vec<Row> = raw_rows.into_iter().map(|values| Row { values }).collect();

        result.push(TableCopyData {
            table_name: table.clone(),
            create_sql,
            rows,
            column_names,
        });
    }
    Ok(result)
}

/// Paste previously copied tables into the database.
pub fn handle_paste_tables(
    conn: &Connection,
    table_data: &[TableCopyData],
) -> Result<BrowserEvent, String> {
    for td in table_data {
        // Execute the CREATE TABLE DDL
        conn.execute_batch(&td.create_sql)
            .map_err(|e| format!("CREATE TABLE '{}' failed: {}", td.table_name, e))?;

        // Insert rows if any
        if !td.rows.is_empty() && !td.column_names.is_empty() {
            let col_names: Vec<String> = td
                .column_names
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect();
            let cols_str = col_names.join(", ");

            for chunk in td.rows.chunks(500) {
                let mut values_parts = Vec::new();
                for row in chunk {
                    let vals: Vec<String> = row.values.iter().map(quote_duckdb_value).collect();
                    values_parts.push(format!("({})", vals.join(", ")));
                }
                let insert_sql = format!(
                    "INSERT INTO \"{}\" ({}) VALUES {}",
                    td.table_name.replace('"', "\"\""),
                    cols_str,
                    values_parts.join(", ")
                );
                conn.execute_batch(&insert_sql)
                    .map_err(|e| format!("INSERT into '{}' failed: {}", td.table_name, e))?;
            }
        }
    }
    Ok(BrowserEvent::PasteDone)
}

/// Execute raw SQL in the browser context.
pub fn handle_execute_sql(conn: &Connection, sql: &str) -> Result<BrowserEvent, String> {
    let stmts: Vec<&str> = sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    for stmt in &stmts {
        conn.execute_batch(stmt)
            .map_err(|e| format!("Failed: {}", e))?;
    }
    Ok(BrowserEvent::SqlExecuted {
        result: format!("OK ({} statements)", stmts.len()),
    })
}

/// Drop a table.
pub fn handle_drop_table(conn: &Connection, table: &str) -> Result<BrowserEvent, String> {
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS \"{}\"",
        table.replace('"', "\"\"")
    ))
    .map_err(|e| format!("Drop failed: {}", e))?;
    Ok(BrowserEvent::TableDropped {
        table: table.to_string(),
    })
}
