use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};
use voidb_core::database::types::QueryResult;
use voidb_core::formatters::{cell_value_to_csv, cell_value_to_json, print_table};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::VoidbError;

use crate::config::DuckDbConfig;
use crate::service::{DuckDbService, StatementResult};

pub struct DuckDbCliPlugin;

pub fn create_duckdb_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(DuckDbCliPlugin)
}

#[async_trait]
impl CliPlugin for DuckDbCliPlugin {
    fn plugin_id(&self) -> &str {
        "duckdb"
    }

    fn name(&self) -> &str {
        "DuckDB"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        vec![
            Command::new("query")
                .about("Execute a SQL query")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("format")
                        .short('f')
                        .long("format")
                        .value_parser(["table", "json", "csv"])
                        .default_value("table")
                        .help("Output format"),
                )
                .arg(Arg::new("sql").required(true).help("SQL query to execute")),
            Command::new("tables")
                .about("List tables and views")
                .arg(conn_arg.clone()),
            Command::new("describe")
                .about("Describe table schema")
                .arg(conn_arg.clone())
                .arg(Arg::new("table").required(true).help("Table name")),
            Command::new("schemas")
                .about("List schemas")
                .arg(conn_arg.clone()),
            Command::new("import")
                .about("Import data from file (CSV/Parquet/JSON)")
                .arg(conn_arg.clone())
                .arg(Arg::new("file").required(true).help("File path"))
                .arg(Arg::new("table").required(true).help("Target table name")),
            Command::new("export")
                .about("Export query result to file")
                .arg(conn_arg.clone())
                .arg(Arg::new("sql").required(true).help("SQL query"))
                .arg(
                    Arg::new("output")
                        .required(true)
                        .help("Output file path (extension determines format)"),
                ),
            Command::new("extensions")
                .about("List installed extensions")
                .arg(conn_arg.clone()),
            Command::new("install-ext")
                .about("Install and load an extension")
                .arg(conn_arg.clone())
                .arg(Arg::new("name").required(true).help("Extension name")),
            Command::new("test")
                .about("Test connection")
                .arg(conn_arg),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "query" => self.handle_query(matches, ctx).await,
            "tables" => self.handle_tables(matches, ctx).await,
            "describe" => self.handle_describe(matches, ctx).await,
            "schemas" => self.handle_schemas(matches, ctx).await,
            "import" => self.handle_import(matches, ctx).await,
            "export" => self.handle_export(matches, ctx).await,
            "extensions" => self.handle_extensions(matches, ctx).await,
            "install-ext" => self.handle_install_ext(matches, ctx).await,
            "test" => self.handle_test(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl DuckDbCliPlugin {
    fn parse_config(&self, conn_name: &str, ctx: &CliContext) -> Result<DuckDbConfig, VoidbError> {
        let config = ctx.find_connection(conn_name).ok_or_else(|| {
            VoidbError::Plugin(format!("Connection '{}' not found", conn_name))
        })?;

        if config.effective_plugin_id() != "duckdb" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not a DuckDB connection (plugin: {})",
                conn_name,
                config.effective_plugin_id()
            )));
        }

        serde_json::from_value(
            config
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Config("Missing plugin_config".into()))?
                .clone(),
        )
        .map_err(|e| VoidbError::Config(e.to_string()))
    }

    fn create_service(&self, conn_name: &str, ctx: &CliContext) -> Result<DuckDbService, VoidbError> {
        let duckdb_config = self.parse_config(conn_name, ctx)?;
        DuckDbService::new_direct(&duckdb_config).map_err(VoidbError::Connection)
    }

    async fn handle_query(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let sql = matches.get_one::<String>("sql").unwrap();
        let format = matches
            .get_one::<String>("format")
            .map(|s| s.as_str())
            .unwrap_or("table");

        let mut service = self.create_service(conn_name, ctx)?;
        let results = service
            .execute_query(sql)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        for stmt_result in &results {
            match stmt_result {
                StatementResult::Select { columns, rows } => {
                    let result = QueryResult {
                        columns: columns.clone(),
                        rows: rows.clone(),
                        execution_time: std::time::Duration::ZERO,
                        rows_affected: None,
                    };

                    match format {
                        "json" => {
                            let json_rows: Vec<serde_json::Value> = result
                                .rows
                                .iter()
                                .map(|row| {
                                    let mut obj = serde_json::Map::new();
                                    for (i, col) in result.columns.iter().enumerate() {
                                        let val = row
                                            .values
                                            .get(i)
                                            .map(cell_value_to_json)
                                            .unwrap_or(serde_json::Value::Null);
                                        obj.insert(col.name.clone(), val);
                                    }
                                    serde_json::Value::Object(obj)
                                })
                                .collect();
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&json_rows)
                                    .map_err(|e| VoidbError::Plugin(e.to_string()))?
                            );
                        }
                        "csv" => {
                            let header: Vec<&str> =
                                result.columns.iter().map(|c| c.name.as_str()).collect();
                            println!("{}", header.join(","));
                            for row in &result.rows {
                                let values: Vec<String> =
                                    row.values.iter().map(cell_value_to_csv).collect();
                                println!("{}", values.join(","));
                            }
                        }
                        _ => {
                            print_table(&result);
                        }
                    }
                    eprintln!("({} rows)", rows.len());
                }
                StatementResult::Affected(n) => {
                    eprintln!("{} rows affected", n);
                }
                StatementResult::Empty => {
                    eprintln!("(empty result)");
                }
                StatementResult::Error(e) => {
                    return Err(VoidbError::Query(e.clone()));
                }
            }
        }

        Ok(())
    }

    async fn handle_tables(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;

        // Use SQL query via service for schema+table+type info
        let results = service
            .execute_query(
                "SELECT table_schema, table_name, table_type \
                 FROM information_schema.tables \
                 WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
                 ORDER BY table_schema, table_name",
            )
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("{:<20} {:<40} {:<15}", "SCHEMA", "NAME", "TYPE");
        println!("{}", "-".repeat(75));
        for stmt_result in &results {
            if let StatementResult::Select { rows, .. } = stmt_result {
                for row in rows {
                    let schema = row.values.first().map(|v| voidb_core::formatters::cell_value_to_display(v).to_string()).unwrap_or_default();
                    let name = row.values.get(1).map(|v| voidb_core::formatters::cell_value_to_display(v).to_string()).unwrap_or_default();
                    let table_type = row.values.get(2).map(|v| voidb_core::formatters::cell_value_to_display(v).to_string()).unwrap_or_default();
                    println!("{:<20} {:<40} {:<15}", schema, name, table_type);
                }
            }
        }
        Ok(())
    }

    async fn handle_describe(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let table = matches.get_one::<String>("table").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;
        let (columns, indexes, foreign_keys) = service
            .describe_table(table)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!(
            "{:<30} {:<20} {:<8} {:<5} DEFAULT",
            "COLUMN", "TYPE", "NULL", "KEY"
        );
        println!("{}", "-".repeat(80));
        for col in &columns {
            println!(
                "{:<30} {:<20} {:<8} {:<5} {}",
                col.name,
                col.data_type,
                if col.nullable { "YES" } else { "NO" },
                if col.is_primary_key { "PRI" } else { "" },
                col.default_value.as_deref().unwrap_or(""),
            );
        }

        if !indexes.is_empty() {
            println!("\nIndexes:");
            for idx in &indexes {
                println!(
                    "  {} [{}]",
                    idx.name,
                    if idx.unique { "UNIQUE" } else { &idx.index_type },
                );
            }
        }

        if !foreign_keys.is_empty() {
            println!("\nForeign Keys:");
            for fk in &foreign_keys {
                println!(
                    "  {} ({}) -> {} ({})",
                    fk.name,
                    fk.columns.join(", "),
                    fk.referenced_table,
                    fk.referenced_columns.join(", "),
                );
            }
        }

        Ok(())
    }

    async fn handle_schemas(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;
        let results = service
            .execute_query("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("SCHEMA");
        println!("{}", "-".repeat(30));
        for stmt_result in &results {
            if let StatementResult::Select { rows, .. } = stmt_result {
                for row in rows {
                    let name = row.values.first().map(voidb_core::formatters::cell_value_to_display).unwrap_or_default();
                    println!("{}", name);
                }
            }
        }
        Ok(())
    }

    async fn handle_import(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let file = matches.get_one::<String>("file").unwrap();
        let table = matches.get_one::<String>("table").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;

        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        let sql = format!(
            "CREATE TABLE {} AS SELECT * FROM '{}'",
            quoted,
            file.replace('\'', "''")
        );
        service
            .execute_query(&sql)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        let count_results = service
            .execute_query(&format!("SELECT COUNT(*) FROM {}", quoted))
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        let count = count_results.first().and_then(|r| {
            if let StatementResult::Select { rows, .. } = r {
                rows.first().and_then(|row| row.values.first().map(voidb_core::formatters::cell_value_to_display))
            } else {
                None
            }
        }).unwrap_or_else(|| "?".to_string());

        println!("Imported {} rows into '{}'", count, table);
        Ok(())
    }

    async fn handle_export(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let sql = matches.get_one::<String>("sql").unwrap();
        let output = matches.get_one::<String>("output").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;

        let export_sql = format!(
            "COPY ({}) TO '{}' (AUTO_DETECT TRUE)",
            sql,
            output.replace('\'', "''")
        );
        service
            .execute_query(&export_sql)
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("Exported to '{}'", output);
        Ok(())
    }

    async fn handle_extensions(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;
        let results = service
            .execute_query(
                "SELECT extension_name, installed, install_path \
                 FROM duckdb_extensions() \
                 WHERE installed = true \
                 ORDER BY extension_name",
            )
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("{:<30} {:<10} PATH", "NAME", "LOADED");
        println!("{}", "-".repeat(70));
        for stmt_result in &results {
            if let StatementResult::Select { rows, .. } = stmt_result {
                for row in rows {
                    let name = row.values.first().map(voidb_core::formatters::cell_value_to_display).unwrap_or_default();
                    let installed = row.values.get(1).map(voidb_core::formatters::cell_value_to_display).unwrap_or_default();
                    let path = row.values.get(2).map(voidb_core::formatters::cell_value_to_display).unwrap_or_default();
                    println!("{:<30} {:<10} {}", name, installed, path);
                }
            }
        }
        Ok(())
    }

    async fn handle_install_ext(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let ext_name = matches.get_one::<String>("name").unwrap();
        let mut service = self.create_service(conn_name, ctx)?;

        service
            .execute_query(&format!("INSTALL '{}'; LOAD '{}';", ext_name, ext_name))
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("Extension '{}' installed and loaded", ext_name);
        Ok(())
    }

    async fn handle_test(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let duckdb_config = self.parse_config(conn_name, ctx)?;

        let result = crate::test_connection(&duckdb_config);
        match result {
            Ok(msg) => println!("OK: {}", msg),
            Err(e) => println!("FAIL: {}", e),
        }
        Ok(())
    }
}
