//! CRUD operation handler functions for the DuckDB SyncWorker.
//!
//! These functions run on the worker thread with direct `&Connection` access.

use std::collections::HashMap;

use duckdb::Connection;
use voidb_core::{CellValue, ColumnInfo, Row};

use super::commands::PasteMode;
use super::events::StatementResult;
use super::value_convert::{build_pk_where, decode_duckdb_value, extract_columns, quote_duckdb_value};

/// Execute a single SQL query and return columns + rows.
///
/// For DDL/DML statements (CREATE, INSERT, UPDATE, DELETE, etc.),
/// uses `execute()` and returns empty columns/rows.
/// For SELECT/VALUES, uses `query_map()` and extracts columns after execution
/// (DuckDB requires statement execution before column_name() is accessible).
pub fn handle_execute_query(conn: &Connection, sql: &str) -> Result<(Vec<ColumnInfo>, Vec<Row>), String> {
    let upper = sql.trim_start().to_uppercase();
    let is_select = upper.starts_with("SELECT")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("VALUES")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW");

    if !is_select {
        // DDL/DML: use execute, return empty result
        conn.execute(sql, [])
            .map_err(|e| format!("Execute: {}", e))?;
        return Ok((Vec::new(), Vec::new()));
    }

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("Prepare: {}", e))?;

    // DuckDB panics on column_count()/column_name() before execution.
    // query_map() executes the statement; after collect() the mutable
    // borrow is released and extract_columns() becomes safe.
    //
    // Since we don't know column count beforehand, the closure probes
    // by trying successive column indices until get() fails.
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

    Ok((columns, rows))
}

/// Execute multiple SQL statements sequentially.
pub fn handle_execute_multi_query(conn: &Connection, sql: &str) -> Vec<StatementResult> {
    let statements = voidb_core::sql_split::split_statements(sql);
    let mut results = Vec::new();

    for stmt_sql in &statements {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() {
            continue;
        }

        let upper = trimmed.to_uppercase();
        let is_select = upper.starts_with("SELECT")
            || upper.starts_with("EXPLAIN")
            || upper.starts_with("VALUES")
            || upper.starts_with("WITH")
            || upper.starts_with("SHOW")
            || upper.starts_with("DESCRIBE");

        if is_select {
            match handle_execute_query(conn, trimmed) {
                Ok((columns, rows)) => {
                    results.push(StatementResult::Select { columns, rows });
                }
                Err(e) => {
                    results.push(StatementResult::Error(e));
                }
            }
        } else {
            match conn.execute(trimmed, []) {
                Ok(affected) => {
                    results.push(StatementResult::Affected(affected as u64));
                }
                Err(e) => {
                    results.push(StatementResult::Error(e.to_string()));
                }
            }
        }
    }

    if results.is_empty() {
        results.push(StatementResult::Empty);
    }

    results
}

/// Save pending cell edits and row deletions.
pub fn handle_save_changes(
    conn: &Connection,
    table: &str,
    columns: &[ColumnInfo],
    rows: &[Row],
    pk_columns: &[String],
    pending: &HashMap<(usize, usize), CellValue>,
    deleted: &[usize],
) -> Result<(usize, usize), String> {
    let qtable = format!("\"{}\"", table.replace('"', "\"\""));

    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| format!("Begin transaction: {}", e))?;

    // Apply updates
    let mut update_count = 0;
    let mut row_edits: HashMap<usize, Vec<(usize, &CellValue)>> = HashMap::new();
    for (&(row_idx, col_idx), value) in pending {
        row_edits.entry(row_idx).or_default().push((col_idx, value));
    }

    for (row_idx, edits) in &row_edits {
        if deleted.contains(row_idx) {
            continue;
        }
        let row_values = &rows[*row_idx].values;
        let where_clause = build_pk_where(pk_columns, row_values, columns)
            .inspect_err(|_| {
                let _ = conn.execute_batch("ROLLBACK");
            })?;

        let set_parts: Vec<String> = edits
            .iter()
            .map(|(col_idx, value)| {
                let col_name = &columns[*col_idx].name;
                format!(
                    "\"{}\" = {}",
                    col_name.replace('"', "\"\""),
                    quote_duckdb_value(value)
                )
            })
            .collect();

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            qtable,
            set_parts.join(", "),
            where_clause
        );
        conn.execute(&sql, []).map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            format!("Update row {}: {}", row_idx, e)
        })?;
        update_count += 1;
    }

    // Apply deletes
    let mut delete_count = 0;
    for &row_idx in deleted {
        let row_values = &rows[row_idx].values;
        let where_clause = build_pk_where(pk_columns, row_values, columns)
            .inspect_err(|_| {
                let _ = conn.execute_batch("ROLLBACK");
            })?;
        let sql = format!("DELETE FROM {} WHERE {}", qtable, where_clause);
        conn.execute(&sql, []).map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            format!("Delete row {}: {}", row_idx, e)
        })?;
        delete_count += 1;
    }

    conn.execute_batch("COMMIT")
        .map_err(|e| format!("Commit: {}", e))?;

    Ok((update_count, delete_count))
}

/// Paste rows into a table.
pub fn handle_paste_rows(
    conn: &Connection,
    table: &str,
    columns: &[ColumnInfo],
    data_rows: &[Row],
    mode: &PasteMode,
) -> Result<usize, String> {
    let qtable = format!("\"{}\"", table.replace('"', "\"\""));

    conn.execute_batch("BEGIN TRANSACTION")
        .map_err(|e| format!("Begin transaction: {}", e))?;

    if *mode == PasteMode::Overwrite {
        conn.execute(&format!("DELETE FROM {}", qtable), [])
            .map_err(|e| {
                let _ = conn.execute_batch("ROLLBACK");
                format!("Delete existing rows: {}", e)
            })?;
    }

    let col_names: Vec<String> = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
        .collect();

    let mut inserted = 0;
    for row in data_rows {
        let val_strs: Vec<String> = row.values.iter().map(quote_duckdb_value).collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            qtable,
            col_names.join(", "),
            val_strs.join(", ")
        );
        conn.execute(&sql, []).map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            format!("Insert row: {}", e)
        })?;
        inserted += 1;
    }

    conn.execute_batch("COMMIT")
        .map_err(|e| format!("Commit: {}", e))?;

    Ok(inserted)
}

/// Import CSV data via pre-built SQL batch.
pub fn handle_import_csv(conn: &Connection, _table: &str, sql_batch: &str) -> Result<(), String> {
    conn.execute_batch(sql_batch)
        .map_err(|e| format!("Import CSV: {}", e))
}
