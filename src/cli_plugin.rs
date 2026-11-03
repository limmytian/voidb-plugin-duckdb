use std::fs;

use async_trait::async_trait;
use chrono::Utc;
use clap::{Arg, ArgAction, ArgMatches, Command};
use voidb_core::database::types::QueryResult;
use voidb_core::formatters::{cell_value_to_csv, cell_value_to_json, print_table};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::{ConnectionProfileRef, TuiLaunchRequest, VoidbError, build_tui_launch_plan};

use crate::config::DuckDbConfig;
use crate::service::{DuckDbService, StatementResult};
use crate::tui::{
    DuckDbTuiLaunch, DuckDbTuiSource, build_duckdb_tui_evidence, run_duckdb_tui,
    write_duckdb_tui_preflight,
};

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
                .arg(conn_arg.required(false))
                .arg(
                    Arg::new("stdin-config")
                        .long("stdin-config")
                        .action(clap::ArgAction::SetTrue)
                        .help("Read connection configuration JSON from stdin"),
                ),
            Command::new("tui")
                .about("Launch the standalone DuckDB data-inspector TUI (pilot)")
                .arg(
                    Arg::new("profile")
                        .long("profile")
                        .value_name("PROFILE")
                        .conflicts_with("connection")
                        .help("Profile name, id:<id>, or name:<name>"),
                )
                .arg(
                    Arg::new("connection")
                        .short('c')
                        .long("connection")
                        .value_name("CONNECTION")
                        .conflicts_with("profile")
                        .help("Legacy connection name"),
                )
                .arg(
                    Arg::new("fixture").long("fixture").value_name("PATH").help(
                        "Load deterministic DuckDB TUI fixture JSON instead of opening a file",
                    ),
                )
                .arg(
                    Arg::new("purpose")
                        .long("purpose")
                        .value_name("PURPOSE")
                        .default_value("operations")
                        .help("Launch purpose, for example operations or triage"),
                )
                .arg(
                    Arg::new("readonly")
                        .long("readonly")
                        .action(ArgAction::SetTrue)
                        .help("Stage operation plans but block mutating service commands"),
                )
                .arg(
                    Arg::new("no-restore")
                        .long("no-restore")
                        .action(ArgAction::SetTrue)
                        .help("Start without restoring plugin-owned UI state"),
                )
                .arg(
                    Arg::new("format")
                        .long("format")
                        .value_parser(["json"])
                        .help("Emit secret-free preflight JSON and exit"),
                )
                .arg(
                    Arg::new("evidence")
                        .long("evidence")
                        .value_name("PATH")
                        .help("Write fixture-backed standalone DuckDB TUI evidence JSON and exit"),
                ),
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
            "tui" => self.handle_tui(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl DuckDbCliPlugin {
    fn parse_config(&self, conn_name: &str, ctx: &CliContext) -> Result<DuckDbConfig, VoidbError> {
        let config = if let Some(config) = ctx.find_connection(conn_name) {
            config.clone()
        } else if let Ok((_profile, config)) =
            ctx.resolve_profile_connection(conn_name, Some("duckdb"))
        {
            config
        } else {
            return Err(VoidbError::Plugin(format!(
                "Connection or profile '{}' not found",
                conn_name
            )));
        };

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

    fn create_service(
        &self,
        conn_name: &str,
        ctx: &CliContext,
    ) -> Result<DuckDbService, VoidbError> {
        let duckdb_config = self.parse_config(conn_name, ctx)?;
        DuckDbService::new_direct(&duckdb_config).map_err(VoidbError::Connection)
    }

    fn parse_tui_launch(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<DuckDbTuiLaunch, VoidbError> {
        let fixture_path = matches.get_one::<String>("fixture").cloned();
        let purpose = matches
            .get_one::<String>("purpose")
            .cloned()
            .unwrap_or_else(|| "operations".to_string());
        let readonly = matches.get_flag("readonly");
        let restore = !matches.get_flag("no-restore");

        if let Some(profile_ref) = matches.get_one::<String>("profile") {
            let (profile, connection) =
                ctx.resolve_profile_connection(profile_ref, Some("duckdb"))?;
            let config: DuckDbConfig = connection
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
                .and_then(|pc| {
                    serde_json::from_value(pc.clone()).map_err(|e| {
                        VoidbError::Connection(format!("Invalid DuckDB config: {}", e))
                    })
                })?;
            let profile_arg = profile_ref_arg(profile_ref, &profile.id);
            let request = TuiLaunchRequest::new("duckdb", profile_arg, purpose.clone())
                .readonly(readonly)
                .restore(restore)
                .raw_input(false);
            let launch_plan = build_tui_launch_plan(&profile, request, "voidb-cli", Utc::now())?;

            return Ok(DuckDbTuiLaunch {
                profile_label: profile.name,
                config: Some(config),
                source: DuckDbTuiSource::Profile,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: Some(launch_plan),
            });
        }

        if let Some(conn_name) = matches.get_one::<String>("connection") {
            let config = self.parse_config(conn_name, ctx)?;
            return Ok(DuckDbTuiLaunch {
                profile_label: conn_name.clone(),
                config: Some(config),
                source: DuckDbTuiSource::Connection,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        if fixture_path.is_some() {
            return Ok(DuckDbTuiLaunch {
                profile_label: "fixture".to_string(),
                config: None,
                source: DuckDbTuiSource::Fixture,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        Err(VoidbError::Plugin(
            "duckdb tui requires --profile, --connection, or --fixture".to_string(),
        ))
    }

    async fn handle_tui(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let launch = self.parse_tui_launch(matches, ctx)?;
        if let Some(path) = matches.get_one::<String>("evidence") {
            let evidence = build_duckdb_tui_evidence(&launch)
                .map_err(|e| VoidbError::Plugin(format!("DuckDB TUI evidence failed: {}", e)))?;
            let rendered = serde_json::to_string_pretty(&evidence).map_err(|e| {
                VoidbError::Plugin(format!("DuckDB TUI evidence serialization failed: {}", e))
            })?;
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|e| {
                    VoidbError::Plugin(format!(
                        "Failed to create DuckDB TUI evidence directory '{}': {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            fs::write(path, rendered).map_err(|e| {
                VoidbError::Plugin(format!(
                    "Failed to write DuckDB TUI evidence '{}': {}",
                    path, e
                ))
            })?;
            println!("Wrote DuckDB TUI evidence to {path}");
            return Ok(());
        }

        if matches
            .get_one::<String>("format")
            .is_some_and(|format| format == "json")
        {
            write_duckdb_tui_preflight(&launch)
                .map_err(|e| VoidbError::Plugin(format!("DuckDB TUI preflight failed: {}", e)))?;
            return Ok(());
        }

        run_duckdb_tui(launch)
            .await
            .map_err(|e| VoidbError::Plugin(format!("DuckDB TUI failed: {}", e)))
    }

    async fn handle_query(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
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
                    let schema = row
                        .values
                        .first()
                        .map(|v| voidb_core::formatters::cell_value_to_display(v).to_string())
                        .unwrap_or_default();
                    let name = row
                        .values
                        .get(1)
                        .map(|v| voidb_core::formatters::cell_value_to_display(v).to_string())
                        .unwrap_or_default();
                    let table_type = row
                        .values
                        .get(2)
                        .map(|v| voidb_core::formatters::cell_value_to_display(v).to_string())
                        .unwrap_or_default();
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
                    if idx.unique {
                        "UNIQUE"
                    } else {
                        &idx.index_type
                    },
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
            .execute_query(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            )
            .await
            .map_err(|e| VoidbError::Query(e.to_string()))?;

        println!("SCHEMA");
        println!("{}", "-".repeat(30));
        for stmt_result in &results {
            if let StatementResult::Select { rows, .. } = stmt_result {
                for row in rows {
                    let name = row
                        .values
                        .first()
                        .map(voidb_core::formatters::cell_value_to_display)
                        .unwrap_or_default();
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

        let count = count_results
            .first()
            .and_then(|r| {
                if let StatementResult::Select { rows, .. } = r {
                    rows.first().and_then(|row| {
                        row.values
                            .first()
                            .map(voidb_core::formatters::cell_value_to_display)
                    })
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "?".to_string());

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
                    let name = row
                        .values
                        .first()
                        .map(voidb_core::formatters::cell_value_to_display)
                        .unwrap_or_default();
                    let installed = row
                        .values
                        .get(1)
                        .map(voidb_core::formatters::cell_value_to_display)
                        .unwrap_or_default();
                    let path = row
                        .values
                        .get(2)
                        .map(voidb_core::formatters::cell_value_to_display)
                        .unwrap_or_default();
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

    async fn handle_test(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let duckdb_config = if matches.get_flag("stdin-config") {
            use std::io::Read;
            let mut input = String::new();
            std::io::stdin()
                .read_to_string(&mut input)
                .map_err(|e| VoidbError::Config(format!("Failed to read stdin: {}", e)))?;
            let val: serde_json::Value = serde_json::from_str(&input)
                .map_err(|e| VoidbError::Config(format!("Invalid JSON on stdin: {}", e)))?;
            let plugin_val = val.get("plugin_config").unwrap_or(&val);
            serde_json::from_value(plugin_val.clone())
                .map_err(|e| VoidbError::Config(format!("Invalid DuckDB config: {}", e)))?
        } else if let Some(conn_name) = matches.get_one::<String>("connection") {
            self.parse_config(conn_name, ctx)?
        } else {
            return Err(VoidbError::Plugin(
                "Either --connection or --stdin-config is required".into(),
            ));
        };

        let result = crate::test_connection(&duckdb_config);
        match result {
            Ok(msg) => {
                println!("OK: {}", msg);
                Ok(())
            }
            Err(e) => {
                println!("FAIL: {}", e);
                Err(VoidbError::Connection(e))
            }
        }
    }
}

fn profile_ref_arg(input: &str, profile_id: &str) -> ConnectionProfileRef {
    if let Some(id) = input.strip_prefix("id:") {
        ConnectionProfileRef::Id(id.to_string())
    } else if let Some(name) = input
        .strip_prefix("name:")
        .or_else(|| input.strip_prefix("alias:"))
    {
        ConnectionProfileRef::Name(name.to_string())
    } else if input == profile_id {
        ConnectionProfileRef::Id(input.to_string())
    } else {
        ConnectionProfileRef::Name(input.to_string())
    }
}
