//! Centralized DuckDB value conversion utilities.
//!
//! Consolidates value decoding and SQL generation helpers. These
//! functions run on the SyncWorker thread inside handler_fn.

use voidb_core::{CellValue, ColumnInfo};

/// Decode a duckdb row value to CellValue by probing with try_get.
///
/// DuckDB's Rust API uses typed `get` calls. We probe for common types
/// in order of likelihood: Integer, Float, String, Blob, Bool.
pub fn decode_duckdb_value(row: &duckdb::Row, idx: usize) -> CellValue {
    // Try integer first
    if let Ok(v) = row.get::<_, i64>(idx) {
        return CellValue::Int(v);
    }
    // Try float
    if let Ok(v) = row.get::<_, f64>(idx) {
        return CellValue::Float(v);
    }
    // Try bool
    if let Ok(v) = row.get::<_, bool>(idx) {
        return CellValue::Bool(v);
    }
    // Try string
    if let Ok(v) = row.get::<_, String>(idx) {
        return CellValue::Text(v);
    }
    // Try blob
    if let Ok(v) = row.get::<_, Vec<u8>>(idx) {
        return CellValue::Blob(v);
    }
    // Check for NULL explicitly
    if let Ok(None) = row.get::<_, Option<String>>(idx) {
        return CellValue::Null;
    }
    CellValue::Null
}

/// SQL-quote a CellValue for embedding in generated SQL statements.
///
/// Produces a SQL literal: NULL, numeric, or single-quoted string
/// with escaped inner quotes.
pub fn quote_duckdb_value(value: &CellValue) -> String {
    match value {
        CellValue::Null => "NULL".into(),
        CellValue::Int(i) => i.to_string(),
        CellValue::Float(f) => f.to_string(),
        CellValue::Bool(b) => if *b { "true" } else { "false" }.into(),
        CellValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
        CellValue::Date(s) | CellValue::Time(s) | CellValue::DateTime(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        CellValue::Blob(b) => format!("'\\x{}'", hex::encode(b)),
        CellValue::Json(s) => format!("'{}'", s.replace('\'', "''")),
        CellValue::Uuid(s) | CellValue::Unknown(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Build a WHERE clause from primary key columns and a data row.
///
/// Produces expressions like `"id" = 42 AND "name" = 'foo'`.
/// NULL PKs are handled with `IS NULL`.
///
/// # Errors
///
/// Returns an error if `pk_columns` is empty or a PK column is not
/// found in the column list.
pub fn build_pk_where(
    pk_columns: &[String],
    row: &[CellValue],
    columns: &[ColumnInfo],
) -> Result<String, String> {
    if pk_columns.is_empty() {
        return Err("No primary key -- cannot identify row".to_string());
    }

    let mut conditions = Vec::new();
    for pk in pk_columns {
        let idx = columns
            .iter()
            .position(|c| c.name == *pk)
            .ok_or_else(|| format!("PK column '{}' not found", pk))?;

        let val = &row[idx];
        if matches!(val, CellValue::Null) {
            conditions.push(format!("\"{}\" IS NULL", pk));
        } else {
            conditions.push(format!("\"{}\" = {}", pk, quote_duckdb_value(val)));
        }
    }

    Ok(conditions.join(" AND "))
}

/// Extract column metadata from a prepared duckdb Statement.
pub fn extract_columns(stmt: &duckdb::Statement) -> Vec<ColumnInfo> {
    (0..stmt.column_count())
        .map(|i| ColumnInfo {
            name: stmt.column_name(i).map_or("?".to_string(), |v| v.to_string()),
            data_type: String::new(),
            nullable: true,
            is_primary_key: false,
            default_value: None,
            max_length: None,
            extra: String::new(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_duckdb_value_null() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (a INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (NULL)", []).unwrap();
        let mut stmt = conn.prepare("SELECT a FROM t").unwrap();
        let val = stmt
            .query_row([], |row| Ok(decode_duckdb_value(row, 0)))
            .unwrap();
        assert_eq!(val, CellValue::Null);
    }

    #[test]
    fn test_decode_duckdb_value_integer() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (a INTEGER)").unwrap();
        conn.execute("INSERT INTO t VALUES (42)", []).unwrap();
        let mut stmt = conn.prepare("SELECT a FROM t").unwrap();
        let val = stmt
            .query_row([], |row| Ok(decode_duckdb_value(row, 0)))
            .unwrap();
        assert_eq!(val, CellValue::Int(42));
    }

    #[test]
    fn test_decode_duckdb_value_text() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (a VARCHAR)").unwrap();
        conn.execute("INSERT INTO t VALUES ('hello')", []).unwrap();
        let mut stmt = conn.prepare("SELECT a FROM t").unwrap();
        let val = stmt
            .query_row([], |row| Ok(decode_duckdb_value(row, 0)))
            .unwrap();
        assert_eq!(val, CellValue::Text("hello".into()));
    }

    #[test]
    fn test_quote_duckdb_value_null() {
        assert_eq!(quote_duckdb_value(&CellValue::Null), "NULL");
    }

    #[test]
    fn test_quote_duckdb_value_int() {
        assert_eq!(quote_duckdb_value(&CellValue::Int(42)), "42");
    }

    #[test]
    fn test_quote_duckdb_value_text_with_quotes() {
        assert_eq!(
            quote_duckdb_value(&CellValue::Text("it's".into())),
            "'it''s'"
        );
    }

    #[test]
    fn test_quote_duckdb_value_float() {
        let result = quote_duckdb_value(&CellValue::Float(12.5));
        assert!(result.starts_with("12.5"));
    }

    #[test]
    fn test_quote_duckdb_value_blob() {
        assert_eq!(
            quote_duckdb_value(&CellValue::Blob(vec![0xDE, 0xAD])),
            "'\\xdead'"
        );
    }

    #[test]
    fn test_quote_duckdb_value_bool() {
        assert_eq!(quote_duckdb_value(&CellValue::Bool(true)), "true");
        assert_eq!(quote_duckdb_value(&CellValue::Bool(false)), "false");
    }

    #[test]
    fn test_quote_duckdb_value_datetime() {
        assert_eq!(
            quote_duckdb_value(&CellValue::DateTime("2024-01-15 10:30:00".into())),
            "'2024-01-15 10:30:00'"
        );
    }

    #[test]
    fn test_build_pk_where_single() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            data_type: "INTEGER".into(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            max_length: None,
            extra: String::new(),
        }];
        let row = vec![CellValue::Int(42)];
        let result = build_pk_where(&["id".into()], &row, &columns).unwrap();
        assert_eq!(result, "\"id\" = 42");
    }

    #[test]
    fn test_build_pk_where_empty_pks() {
        let columns = vec![ColumnInfo {
            name: "id".into(),
            data_type: "INTEGER".into(),
            nullable: false,
            is_primary_key: true,
            default_value: None,
            max_length: None,
            extra: String::new(),
        }];
        let row = vec![CellValue::Int(1)];
        let result = build_pk_where(&[], &row, &columns);
        assert!(result.is_err());
    }
}
