//! Paginated data loading handler functions for the DuckDB SyncWorker.

use duckdb::Connection;
use voidb_core::{CellValue, ColumnInfo, Row};

use super::value_convert::{decode_duckdb_value, extract_columns};

/// (pks, columns, rows, total_count)
type TableDataResult = (Vec<String>, Vec<ColumnInfo>, Vec<Row>, u64);

/// Load a page of table data with PKs and total count.
pub fn handle_load_table_data(
    conn: &Connection,
    table: &str,
    page_size: usize,
    offset: u64,
) -> Result<TableDataResult, String> {
    let qtable = format!("\"{}\"", table.replace('"', "\"\""));

    // Get PKs
    let pks = handle_load_primary_keys(conn, table)?;

    // Get total count
    let total = handle_load_row_count(conn, table)?;

    // Query page
    let sql = format!(
        "SELECT * FROM {} LIMIT {} OFFSET {}",
        qtable, page_size, offset
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| format!("Prepare: {}", e))?;

    // DuckDB panics on column_count()/column_name() before execution.
    // query_map() executes the statement; after collect() the mutable
    // borrow is released and extract_columns() becomes safe.
    let raw_rows: Vec<Vec<CellValue>> = stmt
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
        .map_err(|e| format!("Query: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    // Mutable borrow released — extract column metadata
    let columns = extract_columns(&stmt);
    let rows: Vec<Row> = raw_rows.into_iter().map(|values| Row { values }).collect();

    Ok((pks, columns, rows, total))
}

/// Load the total row count for a table.
pub fn handle_load_row_count(conn: &Connection, table: &str) -> Result<u64, String> {
    let qtable = format!("\"{}\"", table.replace('"', "\"\""));
    let count: u64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {}", qtable), [], |r| {
            r.get(0)
        })
        .map_err(|e| format!("count_rows: {}", e))?;
    Ok(count)
}

/// Load primary key column names for a table.
pub fn handle_load_primary_keys(conn: &Connection, table: &str) -> Result<Vec<String>, String> {
    Ok(crate::duckdb_pk_columns(conn, table))
}
