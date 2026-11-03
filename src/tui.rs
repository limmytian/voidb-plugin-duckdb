//! Standalone DuckDB analytical data-inspector TUI (Requirement 83 pilot).
//!
//! The pilot follows the retained family TUI pattern (see the Redis pilot,
//! the accepted gate precedent, and the MySQL/PostgreSQL SQL adaptations):
//! overlay-first routing, a non-modal error banner, per-view selection
//! memory, a persistent hint footer, full-screen scrollable viewers,
//! confirm-quit, loading markers, Length(4) header/status panels, fixture
//! launch with `build_duckdb_tui_evidence`, a secret-leak scan, and
//! secret-free preflight JSON. All reads go through the existing
//! `DuckDbService` channel contract (`SchemaCommand` + `LoadCommand`); the
//! only mutation is a single PK-anchored row delete sent through the
//! existing `CrudCommand::SaveChanges` path with an empty pending-edit set
//! and one deleted row index. The TUI adds no backend operations.
//!
//! Scope is deliberately bounded to analytical file triage: tables/views
//! (row counts) → bounded data pages → typed row inspection → structure →
//! typed row delete. A DuckDB profile is one embedded file, so there is no
//! databases view (the service listing has no database or schema parameter);
//! cell edits, inserts/imports, DDL, a SQL console, and a server view are
//! deferred as `later candidate`; the CLI remains the supported general path.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use voidb_core::database::types::ForeignKeyInfo;
use voidb_core::{
    CellValue, ColumnInfo, Row, TabInfo, TabManager, TuiLaunchPlan, retained_tui_quality_gate,
};

use crate::config::DuckDbConfig;
use crate::service::{
    CrudCommand, CrudEvent, DuckDbCommand, DuckDbEvent, DuckDbService, LoadCommand, LoadEvent,
    SchemaCommand, SchemaEvent, TableMeta,
};

const TABLES_HINTS: &str = "1 tables 2 structure j/k / filter i data r refresh ? q quit";
const STRUCTURE_HINTS: &str = "1 tables 2 structure j/k r refresh ? q quit";
const DATA_HINTS: &str =
    "j/k PgUp/PgDn n page b back / filter i row detail d del row r refetch Esc close";
const ROW_HINTS: &str = "row detail: j/k PgUp/PgDn scroll d del row Esc close";
const PAGE_SIZE: usize = 5;
const VIEWER_LINE_LIMIT: usize = 2_000;
const VIEWER_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const INPUT_LIMIT: usize = 128;
const PAGE_STEP: isize = 10;
const CELL_MAX_WIDTH: usize = 28;

/// Where the TUI launch came from. Mirrors the family `K8sTuiSource`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DuckDbTuiSource {
    Profile,
    Connection,
    Fixture,
}

/// Launch description for the standalone DuckDB data-inspector TUI. Mirrors
/// the family `K8sTuiLaunch` shape.
#[derive(Debug, Clone)]
pub struct DuckDbTuiLaunch {
    pub profile_label: String,
    pub config: Option<DuckDbConfig>,
    pub source: DuckDbTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

// ---------------------------------------------------------------------------
// Fixture model
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct DuckDbTuiFixture {
    profile_label: Option<String>,
    server_label: String,
    connection_kind: String,
    #[serde(default)]
    database_file: Option<String>,
    status: Option<String>,
    target_error: Option<String>,
    #[serde(default)]
    error_tables: Vec<String>,
    error_message: Option<String>,
    #[serde(default)]
    tables: Vec<FixtureTableMeta>,
    #[serde(default)]
    views: Vec<String>,
    #[serde(default)]
    table_data: BTreeMap<String, FixtureTableData>,
    #[serde(default)]
    structures: BTreeMap<String, FixtureStructure>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureTableMeta {
    name: String,
    #[serde(default)]
    table_type: String,
    rows: Option<u64>,
}

/// Fixture table payload. Rows are bare cell arrays decoded into `Row`s so
/// the viewer code is shared with the live service path.
#[derive(Debug, Deserialize)]
struct FixtureTableData {
    #[serde(default)]
    columns: Vec<ColumnInfo>,
    #[serde(default)]
    pks: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<CellValue>>,
}

#[derive(Debug, Deserialize)]
struct FixtureStructure {
    #[serde(default)]
    columns: Vec<ColumnInfo>,
    #[serde(default)]
    indexes: Vec<IndexEntry>,
    #[serde(default)]
    foreign_keys: Vec<ForeignKeyEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct IndexEntry {
    name: String,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    index_type: String,
}

#[derive(Debug, Deserialize, Clone)]
struct ForeignKeyEntry {
    name: String,
    #[serde(default)]
    columns: Vec<String>,
    referenced_table: String,
    #[serde(default)]
    referenced_columns: Vec<String>,
}

/// Internal fixture table with decoded rows.
#[derive(Debug, Clone)]
struct FixtureTable {
    columns: Vec<ColumnInfo>,
    pks: Vec<String>,
    rows: Vec<Row>,
}

/// In-memory fixture state. Fixture actions mutate this state directly so
/// the "row gone from the refetched page" success state is observable
/// without a server.
struct DuckDbTuiData {
    profile_label: String,
    server_label: String,
    connection_kind: String,
    database_file: Option<String>,
    status: String,
    target_error: Option<String>,
    error_tables: Vec<String>,
    error_message: String,
    tables: Vec<FixtureTableMeta>,
    views: Vec<String>,
    table_data: BTreeMap<String, FixtureTable>,
    structures: BTreeMap<String, FixtureStructure>,
}

impl DuckDbTuiData {
    /// Tables and views are sorted so fixture evidence is deterministic and
    /// matches the `information_schema` ordering the live service reports.
    /// Views are appended after tables so the browser shows one merged list
    /// with a `view` type marker.
    fn resolve_entries(&self) -> Vec<TableMeta> {
        let mut tables: Vec<TableMeta> = self
            .tables
            .iter()
            .map(|meta| TableMeta {
                name: meta.name.clone(),
                table_type: if meta.table_type.is_empty() {
                    "table".to_string()
                } else {
                    meta.table_type.clone()
                },
                rows: meta.rows,
            })
            .collect();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        for view in &self.views {
            tables.push(TableMeta {
                name: view.clone(),
                table_type: "view".to_string(),
                rows: None,
            });
        }
        tables
    }

    /// Resolve one bounded data page. Unknown tables return an empty page so
    /// the TUI always has a visible empty state to render.
    fn resolve_page(
        &self,
        table: &str,
        offset: u64,
        page_size: usize,
    ) -> (Vec<ColumnInfo>, Vec<Row>, u64, Vec<String>) {
        let Some(fixture_table) = self.table_data.get(table) else {
            return (Vec::new(), Vec::new(), 0, Vec::new());
        };
        let start = offset as usize;
        let end = start
            .saturating_add(page_size)
            .min(fixture_table.rows.len());
        let rows = if start >= fixture_table.rows.len() {
            Vec::new()
        } else {
            fixture_table.rows[start..end].to_vec()
        };
        (
            fixture_table.columns.clone(),
            rows,
            fixture_table.rows.len() as u64,
            fixture_table.pks.clone(),
        )
    }

    fn resolve_structure(&self, table: &str) -> Result<StructureView, String> {
        let Some(structure) = self.structures.get(table) else {
            return Err(format!(
                "target error: no fixture structure for table {table} (fixture)"
            ));
        };
        Ok(StructureView {
            table: table.to_string(),
            columns: structure.columns.clone(),
            indexes: structure
                .indexes
                .iter()
                .map(|entry| crate::service::IndexInfo {
                    name: entry.name.clone(),
                    columns: entry.columns.clone(),
                    unique: entry.unique,
                    index_type: if entry.index_type.is_empty() {
                        "ART".to_string()
                    } else {
                        entry.index_type.clone()
                    },
                })
                .collect(),
            foreign_keys: structure
                .foreign_keys
                .iter()
                .map(|entry| ForeignKeyInfo {
                    name: entry.name.clone(),
                    columns: entry.columns.clone(),
                    referenced_table: entry.referenced_table.clone(),
                    referenced_columns: entry.referenced_columns.clone(),
                    on_update: String::new(),
                    on_delete: String::new(),
                })
                .collect(),
        })
    }

    fn is_error_table(&self, table: &str) -> bool {
        self.error_tables.iter().any(|candidate| candidate == table)
    }

    /// Apply a staged row delete to the fixture table. Tables listed in
    /// `error_tables` deliberately fail so the TUI can prove a target error
    /// keeps it responsive.
    fn apply_row_delete(
        &mut self,
        plan: &OperationPlanView,
    ) -> std::result::Result<String, String> {
        if self.is_error_table(&plan.table) {
            return Err(self.error_message.clone());
        }
        let Some(fixture_table) = self.table_data.get_mut(&plan.table) else {
            return Err(format!("target error: no fixture data for {}", plan.table));
        };
        if fixture_table.pks.is_empty() {
            return Err(format!(
                "target error: {} has no primary key; bounded row delete unavailable",
                plan.table
            ));
        }
        let index = plan.row_offset as usize;
        if index >= fixture_table.rows.len() {
            return Err(format!(
                "target error: staged row {} is no longer present in the fixture table",
                plan.pk_summary
            ));
        }
        fixture_table.rows.remove(index);
        Ok(format!(
            "row {} removed from the fixture table",
            plan.pk_summary
        ))
    }
}

fn load_fixture(path: &str) -> Result<DuckDbTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: DuckDbTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    Ok(DuckDbTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-duckdb".to_string()),
        server_label: fixture.server_label,
        connection_kind: fixture.connection_kind,
        database_file: fixture.database_file,
        status: fixture
            .status
            .unwrap_or_else(|| "fixture duckdb operations ready".to_string()),
        target_error: fixture.target_error,
        error_tables: fixture.error_tables,
        error_message: fixture
            .error_message
            .unwrap_or_else(|| "target error: fixture action rejected".to_string()),
        tables: fixture.tables,
        views: fixture.views,
        table_data: fixture
            .table_data
            .into_iter()
            .map(|(name, data)| {
                (
                    name,
                    FixtureTable {
                        columns: data.columns,
                        pks: data.pks,
                        rows: data.rows.into_iter().map(|values| Row { values }).collect(),
                    },
                )
            })
            .collect(),
        structures: fixture.structures,
    })
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Tables,
    Structure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
}

/// Full-screen or popup surfaces stacked on top of the browser. Input is
/// routed to the active overlay first, so each surface owns its key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    None,
    Help,
    DataViewer,
    RowViewer,
    ConfirmDeleteRow,
    ConfirmQuit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    DeleteRow,
}

impl OperationKind {
    fn label(&self) -> &'static str {
        match self {
            OperationKind::DeleteRow => "delete row",
        }
    }

    fn risk(&self) -> &'static str {
        match self {
            OperationKind::DeleteRow => "destructive",
        }
    }
}

/// Staged mutation plan rendered in the details panel before execution. The
/// only slice 1 mutation is a single PK-anchored row delete.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationPlanView {
    kind: OperationKind,
    table: String,
    /// Absolute row index inside the table (page offset + index in page).
    row_offset: u64,
    /// Offset of the page the row was staged from, so a stale plan is
    /// detected before execution.
    page_offset: u64,
    /// Primary-key summary rendered in the plan panel (for example `id=7`).
    pk_summary: String,
    risk: &'static str,
    confirmations_required: u8,
    confirmations: u8,
}

impl OperationPlanView {
    /// Index of the staged row within its page (the `SaveChanges` deleted
    /// index).
    fn local_index(&self) -> usize {
        self.row_offset.saturating_sub(self.page_offset) as usize
    }
}

/// Current bounded page of table data in the full-screen data viewer. Only
/// one page is held at a time; pages are replaced, never appended.
struct TableDataState {
    table: String,
    columns: Vec<ColumnInfo>,
    pks: Vec<String>,
    rows: Vec<Row>,
    total: u64,
    offset: u64,
    row_selection: usize,
    filter: String,
    loading: bool,
}

impl TableDataState {
    /// Client-side filter over the current page. Returns the page index of
    /// each match so staging can map back to the absolute row offset.
    fn filtered_rows(&self) -> Vec<(usize, &Row)> {
        let needle = self.filter.to_lowercase();
        self.rows
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                needle.is_empty()
                    || row
                        .values
                        .iter()
                        .any(|value| cell_display(value).to_lowercase().contains(&needle))
            })
            .collect()
    }

    fn selected_row(&self) -> Option<(usize, Row)> {
        let matches = self.filtered_rows();
        if matches.is_empty() {
            return None;
        }
        let index = self.row_selection.min(matches.len() - 1);
        let (page_index, row) = matches[index];
        Some((page_index, row.clone()))
    }

    fn trim_selection(&mut self) {
        let matches = self.filtered_rows();
        self.row_selection = self.row_selection.min(matches.len().saturating_sub(1));
    }
}

/// Structure snapshot cached for the table the structure view shows (and
/// reused for typed row-detail rendering when it matches the data viewer's
/// table).
#[derive(Debug, Clone)]
struct StructureView {
    table: String,
    columns: Vec<ColumnInfo>,
    indexes: Vec<crate::service::IndexInfo>,
    foreign_keys: Vec<ForeignKeyInfo>,
}

/// Bounded text buffer for viewer bodies (same contract as the family TUIs).
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_duckdb_tui_preflight(launch: &DuckDbTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_duckdb_tui_evidence(launch: &DuckDbTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("duckdb tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "duckdb_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "duckdb",
            &["fixture-duckdb-ops", "fixture-duckdb-local"],
            &["target error"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "server_label": data.server_label,
            "connection_kind": data.connection_kind,
            "database_file": data.database_file,
            "status": data.status,
            "target_error": data.target_error,
            "tables": data
                .resolve_entries()
                .iter()
                .map(|meta| {
                    json!({
                        "name": meta.name,
                        "table_type": meta.table_type,
                        "rows": meta.rows
                    })
                })
                .collect::<Vec<_>>(),
            "table_data": data
                .table_data
                .iter()
                .map(|(name, table)| {
                    json!({
                        "table": name,
                        "columns": table.columns,
                        "pks": table.pks,
                        "total_rows": table.rows.len(),
                        "rows": table
                            .rows
                            .iter()
                            .map(|row| {
                                Value::Array(
                                    row.values.iter().map(cell_value_json).collect(),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>(),
            "page_size": PAGE_SIZE,
            "viewer_bounds": {
                "max_lines": VIEWER_LINE_LIMIT,
                "max_bytes": VIEWER_BYTE_LIMIT
            }
        },
        "plan_transcript": [
            {
                "action": "delete_row",
                "risk": "destructive",
                "typed_confirmation": "exact_table_name_required",
                "second_confirmation": "explicit_y",
                "primary_key_required": true,
                "bounded_to": "single PK-anchored DELETE via CrudCommand::SaveChanges with empty pending edits and one deleted row index",
                "readonly_behavior": "plan_visible_but_blocked",
                "post_action": "current_page_refetch"
            }
        ],
        "coverage": [
            "startup",
            "tables_structure_tabs",
            "tables_browser_row_counts",
            "views_listed_without_counts",
            "table_data_paging",
            "partial_page_state",
            "empty_table_state",
            "client_filter_match_count",
            "row_viewer_typed_columns",
            "null_value_rendering",
            "json_text_rendering",
            "structure_viewer_columns_indexes_fks",
            "typed_delete_confirmation",
            "delete_requires_primary_key",
            "readonly_block",
            "mutation_refetch",
            "erroring_mutation_responsive",
            "non_modal_error_banner",
            "target_error_responsive",
            "quit_confirmation",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "deferred": {
            "cell_edits": "later candidate: CrudCommand::SaveChanges pending cell updates need their own fixture, confirmation, and redaction evidence",
            "row_inserts_and_imports": "later candidate: CrudCommand::PasteRows and CrudCommand::ImportCsv need the same evidence",
            "ddl_and_table_management": "later candidate: BrowserCommand DDL (including DropTable) blast radius exceeds the slice 1 mutation gate",
            "sql_console": "later candidate: CrudCommand::ExecuteQuery executes arbitrary statements including writes; the CLI remains the supported ad-hoc SQL path",
            "server_view": "later candidate: the duckdb service exposes no bounded diagnostics command and the pilot adds no backend operations",
            "schema_level_browsing": "later candidate: the service listing filters table_schema = 'main' and exposes no schema switch command",
            "decode_surface": "later candidate: value_convert probes Int/Float/Bool/String/Blob, so LIST/STRUCT columns surface as NULL and TIMESTAMP/DATE surface raw values; widening the probe is a service-layer concern",
            "outcome": "later candidate within the accepted pilot",
            "supported_mutations": ["delete_row"],
            "cli_remains_supported_general_path": true
        },
        "mutation_rescan_policy": {
            "page_semantics": "one bounded LIMIT/OFFSET page at a time; the next page is fetched with offset + page_size and pages are replaced, never appended",
            "after_mutation": "the current page offset is refetched so the deleted row disappears and totals stay accurate",
            "fixture_mutations": "applied to the in-memory fixture rows so paging evidence stays deterministic"
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_duckdb_tui(launch: DuckDbTuiLaunch) -> Result<()> {
    let mut app = DuckDbTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &DuckDbTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": data.connection_kind,
                    "server_label": data.server_label,
                    "database_file": data.database_file,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "fixture",
                "server_label": "fixture",
                "secret_material": "redacted"
            })
        });
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "duckdb tui",
        "plugin_id": "duckdb",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "connection": connection,
        "privacy": {
            "diagnostics_include_credential_material": false,
            "diagnostics_include_connection_url": false,
            "diagnostics_include_row_values": false
        },
        "stream_bounds": {
            "table_page": { "page_size": PAGE_SIZE },
            "row_viewer": { "max_lines": VIEWER_LINE_LIMIT, "max_bytes": VIEWER_BYTE_LIMIT }
        },
        "service_boundary": "DuckDbService::Channel",
        "modes": [
            "tables",
            "structure",
            "filter",
            "data_viewer",
            "row_viewer",
            "typed_delete_confirmation",
            "confirm_quit",
            "target_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut DuckDbTuiApp) -> Result<()> {
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized duckdb view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn config_data(profile_label: String, config: &DuckDbConfig) -> DuckDbTuiData {
    DuckDbTuiData {
        profile_label,
        server_label: server_label(config),
        connection_kind: connection_kind(config).to_string(),
        database_file: Some(config.path.clone()),
        status: "connecting through DuckDbService channel mode".to_string(),
        target_error: None,
        error_tables: Vec::new(),
        error_message: "target error: fixture action rejected".to_string(),
        tables: Vec::new(),
        views: Vec::new(),
        table_data: BTreeMap::new(),
        structures: BTreeMap::new(),
    }
}

fn server_label(config: &DuckDbConfig) -> String {
    if config.path == ":memory:" || config.path.is_empty() {
        "in-memory".to_string()
    } else {
        config.path.clone()
    }
}

fn connection_kind(config: &DuckDbConfig) -> &'static str {
    if config.path == ":memory:" || config.path.is_empty() {
        "memory"
    } else if config.read_only {
        "file-readonly"
    } else {
        "file"
    }
}

fn connection_value(config: &DuckDbConfig) -> Value {
    json!({
        "kind": connection_kind(config),
        "database_file": config.path,
        "read_only": config.read_only,
        "extension_count": config.extensions.len(),
        "memory_limit": config.memory_limit,
        "threads": config.threads,
        "secret_material": "redacted"
    })
}

struct DuckDbTuiApp {
    profile_label: String,
    server_label: String,
    connection_kind: String,
    source: DuckDbTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    view: View,
    mode: Mode,
    /// Browser-level filter over the tables/structure views; the data
    /// viewer keeps its own filter.
    browser_filter: String,
    overlay: Overlay,
    error_banner: Option<String>,
    entries: Vec<TableMeta>,
    views: Vec<String>,
    entries_loaded: bool,
    entry_selection: usize,
    tables_pending: bool,
    structure: Option<StructureView>,
    structure_request: Option<String>,
    structure_pending: bool,
    structure_selection: usize,
    data: Option<TableDataState>,
    data_pending: bool,
    data_filtering: bool,
    row_viewer_table: Option<String>,
    row_viewer_offset: u64,
    row_viewer_lines: BoundedLines,
    row_viewer_scroll: usize,
    typed_table_input: String,
    operation_plan: Option<OperationPlanView>,
    fixture: Option<DuckDbTuiData>,
    service: Option<DuckDbService>,
    status: String,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl DuckDbTuiApp {
    fn new(launch: DuckDbTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("duckdb tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("duckdb tui requires config outside fixture mode")?;
            let tabs = Arc::new(StandaloneDuckDbTabManager::new(render_quit.clone()));
            // The SyncWorker opens the database on its own thread; an open
            // failure surfaces here as a launch error before any frame.
            let service = DuckDbService::new(config, tabs)
                .map_err(|e| anyhow::anyhow!("open DuckDB database: {e}"))?;
            Some(service)
        } else {
            None
        };

        let is_fixture = launch.fixture_path.is_some();
        let mut app = Self {
            profile_label: data.profile_label.clone(),
            server_label: data.server_label.clone(),
            connection_kind: data.connection_kind.clone(),
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            view: View::Tables,
            mode: Mode::Browser,
            browser_filter: String::new(),
            overlay: Overlay::None,
            error_banner: data.target_error.clone(),
            entries: Vec::new(),
            views: Vec::new(),
            entries_loaded: false,
            entry_selection: 0,
            tables_pending: false,
            structure: None,
            structure_request: None,
            structure_pending: false,
            structure_selection: 0,
            data: None,
            data_pending: false,
            data_filtering: false,
            row_viewer_table: None,
            row_viewer_offset: 0,
            row_viewer_lines: BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT),
            row_viewer_scroll: 0,
            typed_table_input: String::new(),
            operation_plan: None,
            fixture: if is_fixture { Some(data) } else { None },
            service,
            status: String::new(),
            should_quit: false,
            render_quit,
        };
        // Fixture mode loads the table list eagerly so the first frame
        // already shows the catalog; live mode sends the first ListTables
        // request through the service channel (the pending marker stays on
        // until TablesLoaded arrives).
        app.request_tables();
        if let Some(error) = app
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.target_error.clone())
        {
            app.error_banner = Some(error.clone());
            app.status = format!("{} | {}", app.status, safe_error_summary(&error));
        }
        Ok(app)
    }

    /// The SyncWorker thread stops on its own when the service (and its
    /// command sender) drops; the duckdb service contract has no explicit
    /// disconnect command for the pilot to send.
    fn shutdown(&mut self) {
        self.service = None;
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        // Collect first and restore the service before handling: follow-up
        // commands sent inside handlers (the post-mutation page refetch)
        // must see the live service, and their responses drain on the next
        // loop iteration.
        let mut events = Vec::new();
        while let Some(event) = service.poll_event() {
            changed = true;
            events.push(event);
        }
        self.service = Some(service);
        for event in events {
            self.handle_service_event(event);
        }
        changed
    }

    fn handle_service_event(&mut self, event: DuckDbEvent) {
        match event {
            DuckDbEvent::Schema(SchemaEvent::TablesLoaded { tables, views }) => {
                self.tables_pending = false;
                self.entries_loaded = true;
                self.views = views;
                let mut entries = tables;
                for view in self.views.clone() {
                    entries.push(TableMeta {
                        name: view,
                        table_type: "view".to_string(),
                        rows: None,
                    });
                }
                self.entries = entries;
                self.trim_entry_selection();
                self.status = format!(
                    "loaded {} tables and {} views from {}",
                    self.entries.len().saturating_sub(self.views.len()),
                    self.views.len(),
                    self.server_label
                );
            }
            DuckDbEvent::Schema(SchemaEvent::StructureLoaded {
                table,
                columns,
                indexes,
                foreign_keys,
            }) => {
                self.structure_pending = false;
                let matches = self
                    .structure_request
                    .as_ref()
                    .is_some_and(|requested| requested == &table);
                if matches {
                    self.structure_request = None;
                    self.structure = Some(StructureView {
                        table,
                        columns,
                        indexes,
                        foreign_keys,
                    });
                    self.structure_selection = 0;
                    self.status = format!(
                        "structure loaded: {} columns, {} indexes, {} foreign keys",
                        self.structure.as_ref().map_or(0, |s| s.columns.len()),
                        self.structure.as_ref().map_or(0, |s| s.indexes.len()),
                        self.structure.as_ref().map_or(0, |s| s.foreign_keys.len())
                    );
                }
            }
            DuckDbEvent::Load(LoadEvent::TableDataLoaded {
                table,
                pks,
                columns,
                rows,
                total,
            }) => {
                self.data_pending = false;
                let matches_request = self
                    .data
                    .as_ref()
                    .is_some_and(|state| state.table == table && state.loading);
                if matches_request && let Some(state) = self.data.as_mut() {
                    state.loading = false;
                    state.columns = columns;
                    state.pks = pks;
                    state.rows = rows;
                    state.total = total;
                    state.trim_selection();
                    self.status = format!(
                        "loaded {} rows at offset {} of {total}",
                        state.rows.len(),
                        state.offset
                    );
                }
            }
            DuckDbEvent::Crud(CrudEvent::ChangesSaved { updates, deletes }) => {
                self.operation_plan = None;
                self.status = format!("saved: {updates} update(s), {deletes} delete(s)");
                self.refetch_current_page();
            }
            // CreateTableDdl and the remaining Crud/Browser events belong to
            // other surfaces; the pilot never sends those commands.
            DuckDbEvent::Schema(_)
            | DuckDbEvent::Load(_)
            | DuckDbEvent::Crud(_)
            | DuckDbEvent::Browser(_) => {
                self.tables_pending = false;
                self.structure_pending = false;
                self.data_pending = false;
                if let Some(state) = self.data.as_mut() {
                    state.loading = false;
                }
                self.status = "service event outside the pilot read path".to_string();
            }
            DuckDbEvent::Error(message) => {
                // Background and request failures surface as a non-modal
                // banner so a failing fetch or mutation never hijacks the
                // browser.
                self.tables_pending = false;
                self.structure_pending = false;
                self.data_pending = false;
                if let Some(state) = self.data.as_mut() {
                    state.loading = false;
                }
                self.error_banner = Some(message);
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            }
        }
    }

    // === Input routing ===

    fn handle_key(&mut self, key: KeyEvent) {
        // The error banner is informational: any key acknowledges it while
        // the key itself keeps its normal meaning, so background failures
        // never trap the operator in a modal.
        self.error_banner = None;
        match self.overlay {
            Overlay::None => {}
            Overlay::Help => {
                self.overlay = Overlay::None;
                self.status = "help closed".to_string();
                return;
            }
            Overlay::DataViewer => {
                self.handle_data_viewer_key(key);
                return;
            }
            Overlay::RowViewer => {
                self.handle_row_viewer_key(key);
                return;
            }
            Overlay::ConfirmDeleteRow => {
                self.handle_confirm_delete_key(key);
                return;
            }
            Overlay::ConfirmQuit => {
                self.handle_confirm_quit_key(key);
                return;
            }
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('1') => self.set_view(View::Tables),
            KeyCode::Char('2') => self.set_view(View::Structure),
            KeyCode::Tab => {
                let next = match self.view {
                    View::Tables => View::Structure,
                    View::Structure => View::Tables,
                };
                self.set_view(next);
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_selected(0),
            KeyCode::End | KeyCode::Char('G') => {
                let len = self.current_items_len();
                self.set_selected(len.saturating_sub(1));
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                self.mode = Mode::Filter;
                self.status = "filter: type to narrow live, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('r') => self.refresh_view(),
            KeyCode::Char('d') => {
                self.status = "no table-level mutations in this pilot; row deletes live in the table data viewer (i)"
                    .to_string();
            }
            KeyCode::Char('?') => self.show_help(),
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Esc => self.cancel_plan(),
            _ => {
                self.status = format!(
                    "duckdb: {}",
                    match self.view {
                        View::Tables => TABLES_HINTS,
                        View::Structure => STRUCTURE_HINTS,
                    }
                );
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = format!(
                    "filter applied: {} ({} matches)",
                    self.filter_label(),
                    self.current_items_len()
                );
            }
            KeyCode::Esc => {
                self.browser_filter.clear();
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.browser_filter.pop();
                self.set_selected(0);
            }
            KeyCode::Char(ch) => {
                if self.browser_filter.len() < INPUT_LIMIT {
                    self.browser_filter.push(ch);
                }
                self.set_selected(0);
            }
            _ => {}
        }
    }

    fn handle_data_viewer_key(&mut self, key: KeyEvent) {
        // While the viewer filter input is open every character belongs to
        // the filter, mirroring the family filter-mode routing.
        if self.data_filtering {
            match key.code {
                KeyCode::Esc => {
                    self.data_filtering = false;
                    if let Some(state) = self.data.as_mut() {
                        state.filter.clear();
                        state.trim_selection();
                    }
                    self.status = "viewer filter cancelled".to_string();
                }
                KeyCode::Backspace => {
                    if let Some(state) = self.data.as_mut() {
                        state.filter.pop();
                        state.trim_selection();
                    }
                }
                KeyCode::Enter => {
                    self.data_filtering = false;
                    let (matches, filter) = {
                        let Some(state) = self.data.as_ref() else {
                            return;
                        };
                        (state.filtered_rows().len(), state.filter.clone())
                    };
                    self.status = format!(
                        "filter applied: {} ({} matches)",
                        if filter.is_empty() {
                            "none".to_string()
                        } else {
                            filter
                        },
                        matches
                    );
                }
                KeyCode::Char(ch) => {
                    if let Some(state) = self.data.as_mut()
                        && state.filter.len() < INPUT_LIMIT
                    {
                        state.filter.push(ch);
                        state.trim_selection();
                    }
                }
                _ => {}
            }
            return;
        }
        let matches = self
            .data
            .as_ref()
            .map(|state| state.filtered_rows().len())
            .unwrap_or(0);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.data = None;
                self.data_filtering = false;
                self.status = "table data viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_data_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_data_selection(1),
            KeyCode::PageUp => self.move_data_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_data_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_data_selection(0),
            KeyCode::End | KeyCode::Char('G') => self.set_data_selection(matches.saturating_sub(1)),
            KeyCode::Char('n') => self.next_data_page(),
            KeyCode::Char('b') => self.previous_data_page(),
            KeyCode::Char('/') => {
                self.data_filtering = true;
                self.status =
                    "viewer filter: type to narrow the current page, Enter applies, Esc clears"
                        .to_string();
            }
            KeyCode::Char('r') => {
                self.refetch_current_page();
                self.status = "refetching current page".to_string();
            }
            KeyCode::Enter | KeyCode::Char('i') => self.open_row_viewer(),
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('y') => self.confirm_plan(),
            _ => {
                self.status = format!("duckdb data viewer: {DATA_HINTS}");
            }
        }
    }

    fn handle_row_viewer_key(&mut self, key: KeyEvent) {
        let max_scroll = self.row_viewer_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::DataViewer;
                self.row_viewer_table = None;
                self.status = "row viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.row_viewer_scroll = self.row_viewer_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.row_viewer_scroll = (self.row_viewer_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.row_viewer_scroll = self.row_viewer_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.row_viewer_scroll =
                    (self.row_viewer_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.row_viewer_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.row_viewer_scroll = max_scroll,
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('y') => self.confirm_plan(),
            _ => {}
        }
    }

    fn handle_confirm_delete_key(&mut self, key: KeyEvent) {
        let Some(state) = self.data.as_ref() else {
            self.overlay = Overlay::None;
            return;
        };
        let table = state.table.clone();
        match key.code {
            KeyCode::Esc => {
                self.typed_table_input.clear();
                self.overlay = Overlay::DataViewer;
                self.status = "delete cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.typed_table_input.pop();
            }
            KeyCode::Enter => {
                if self.typed_table_input != table {
                    self.status = format!("typed name does not match {table}; delete not staged");
                    self.typed_table_input.clear();
                    return;
                }
                match self.staged_row_delete() {
                    Some((table, row_offset, page_offset, pk_summary)) => {
                        let staged_table = table.clone();
                        let staged_summary = pk_summary.clone();
                        self.typed_table_input.clear();
                        // Return to the data viewer the confirm was opened
                        // from. The typed table name is the first of two
                        // confirmations; `y` remains the explicit
                        // non-default acknowledgement.
                        self.overlay = Overlay::DataViewer;
                        self.operation_plan = Some(OperationPlanView {
                            kind: OperationKind::DeleteRow,
                            table,
                            row_offset,
                            page_offset,
                            pk_summary,
                            risk: OperationKind::DeleteRow.risk(),
                            confirmations_required: 2,
                            confirmations: 1,
                        });
                        self.status = format!(
                            "delete plan staged for row {staged_summary} in {staged_table} (typed confirmation recorded); press y to execute"
                        );
                    }
                    None => {
                        self.overlay = Overlay::DataViewer;
                        self.status = "row delete unavailable without a primary key".to_string();
                    }
                }
            }
            KeyCode::Char(ch) if self.typed_table_input.len() < INPUT_LIMIT => {
                self.typed_table_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_confirm_quit_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.should_quit = true;
            }
            _ => {
                self.overlay = Overlay::None;
                self.status = "quit cancelled".to_string();
            }
        }
    }

    // === Selection and views ===

    fn filtered_entries(&self) -> Vec<TableMeta> {
        let needle = self.filter_needle();
        self.entries
            .iter()
            .filter(|meta| needle.is_empty() || meta.name.to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    /// The browser-level filter is shared between the tables and structure
    /// views; the data viewer keeps its own filter.
    fn filter_needle(&self) -> String {
        self.browser_filter.to_lowercase()
    }

    fn filter_label(&self) -> String {
        if self.browser_filter.is_empty() {
            "none".to_string()
        } else {
            self.browser_filter.clone()
        }
    }

    fn current_items_len(&self) -> usize {
        match self.view {
            View::Tables => self.filtered_entries().len(),
            View::Structure => self
                .structure
                .as_ref()
                .map(|view| view.columns.len())
                .unwrap_or(0),
        }
    }

    fn selected(&self) -> usize {
        match self.view {
            View::Tables => self.entry_selection,
            View::Structure => self.structure_selection,
        }
    }

    fn set_selected(&mut self, value: usize) {
        let len = self.current_items_len();
        let clamped = value.min(len.saturating_sub(1));
        match self.view {
            View::Tables => self.entry_selection = clamped,
            View::Structure => self.structure_selection = clamped,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.current_items_len();
        if len == 0 {
            self.set_selected(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.selected() as isize + delta).clamp(0, last) as usize;
        self.set_selected(next);
    }

    fn trim_entry_selection(&mut self) {
        let len = self.filtered_entries().len();
        self.entry_selection = self.entry_selection.min(len.saturating_sub(1));
    }

    fn set_view(&mut self, view: View) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.status = format!(
            "view: {}",
            match view {
                View::Tables => "tables",
                View::Structure => "structure",
            }
        );
        if view == View::Structure {
            // Per-view selection memory: only (re)load when the cached
            // structure does not already match the selected table.
            let selected = self.selected_table_label();
            if self.structure.as_ref().is_none_or(|s| s.table != selected) {
                self.request_structure_for_selected();
            }
        }
    }

    // === Service requests ===

    fn request_tables(&mut self) {
        if let Some(fixture) = &self.fixture {
            self.entries = fixture.resolve_entries();
            self.views = fixture.views.clone();
            self.entries_loaded = true;
            self.trim_entry_selection();
            self.status = format!(
                "fixture loaded {} tables and {} views",
                self.entries.len() - self.views.len(),
                self.views.len()
            );
            return;
        }
        self.tables_pending = true;
        if let Some(service) = &self.service {
            self.status = "loading tables from the database file".to_string();
            service.send(DuckDbCommand::Schema(SchemaCommand::ListTables));
        } else {
            self.tables_pending = false;
        }
    }

    /// Request the structure of the table selected in the tables browser so
    /// the structure view (and typed row detail) has column types, indexes,
    /// and foreign keys. Fixture mode resolves synchronously.
    fn request_structure_for_selected(&mut self) {
        let Some(entry) = self.filtered_entries().get(self.entry_selection).cloned() else {
            self.status = "select a table in tables view (1) to load its structure".to_string();
            return;
        };
        self.request_structure(entry.name);
    }

    fn request_structure(&mut self, table: String) {
        if let Some(fixture) = &self.fixture {
            match fixture.resolve_structure(&table) {
                Ok(view) => {
                    self.structure = Some(view);
                    self.structure_selection = 0;
                    self.status = format!("fixture loaded structure for {table}");
                }
                Err(message) => {
                    self.error_banner = Some(message);
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
            return;
        }
        if let Some(service) = &self.service {
            self.structure_pending = true;
            self.structure_request = Some(table.clone());
            self.status = format!("loading structure for {table}");
            service.send(DuckDbCommand::Schema(SchemaCommand::LoadStructure {
                table,
            }));
        } else {
            self.structure_pending = false;
        }
    }

    /// Send one bounded page request (`LIMIT page_size OFFSET n`). Fixture
    /// mode resolves the page synchronously; live mode sends
    /// `LoadCommand::LoadTableData`.
    fn request_table_data(&mut self, table: String, offset: u64) {
        if let Some(state) = self.data.as_mut() {
            state.table = table.clone();
            state.offset = offset;
            state.row_selection = 0;
            state.loading = true;
        }
        if let Some(fixture) = &self.fixture {
            let (columns, rows, total, pks) = fixture.resolve_page(&table, offset, PAGE_SIZE);
            if let Some(state) = self.data.as_mut() {
                state.columns = columns;
                state.rows = rows;
                state.total = total;
                state.pks = pks;
                state.loading = false;
                state.trim_selection();
            }
            let loaded = self
                .data
                .as_ref()
                .map(|state| state.rows.len())
                .unwrap_or(0);
            self.status = format!("fixture loaded {loaded} rows at offset {offset} of {total}");
        } else if let Some(service) = &self.service {
            self.data_pending = true;
            service.send(DuckDbCommand::Load(LoadCommand::LoadTableData {
                table,
                page_size: PAGE_SIZE,
                offset,
            }));
            self.status = format!("loading rows at offset {offset}");
        } else if let Some(state) = self.data.as_mut() {
            state.loading = false;
        }
    }

    fn open_table_data(&mut self, table: String) {
        self.data = Some(TableDataState {
            table: table.clone(),
            columns: Vec::new(),
            pks: Vec::new(),
            rows: Vec::new(),
            total: 0,
            offset: 0,
            row_selection: 0,
            filter: String::new(),
            loading: true,
        });
        self.overlay = Overlay::DataViewer;
        self.data_filtering = false;
        self.request_table_data(table.clone(), 0);
        // Structure feeds the typed row-detail viewer; the request is
        // best-effort and the detail falls back to page column metadata.
        self.request_structure(table.clone());
        self.status = format!("table data viewer: {table} (bounded page of {PAGE_SIZE})");
    }

    /// `n`: replace the current page with the page at `offset + page_size`.
    /// Pages are replaced (not appended) so the viewport stays bounded.
    fn next_data_page(&mut self) {
        let Some(state) = self.data.as_ref() else {
            return;
        };
        let next_offset = state.offset + PAGE_SIZE as u64;
        if next_offset >= state.total {
            self.status = "end of table reached; press b to go back or / to filter".to_string();
            return;
        }
        let table = state.table.clone();
        self.request_table_data(table, next_offset);
    }

    fn previous_data_page(&mut self) {
        let Some(state) = self.data.as_ref() else {
            return;
        };
        if state.offset == 0 {
            self.status = "no previous page".to_string();
            return;
        }
        let previous = state.offset.saturating_sub(PAGE_SIZE as u64);
        let table = state.table.clone();
        self.request_table_data(table, previous);
    }

    /// After a mutation the current page offset is refetched so the deleted
    /// row disappears and totals stay accurate without leaving the TUI.
    fn refetch_current_page(&mut self) {
        let Some(state) = self.data.as_ref() else {
            return;
        };
        let table = state.table.clone();
        let offset = state.offset;
        self.request_table_data(table, offset);
    }

    fn refresh_view(&mut self) {
        match self.view {
            View::Tables => {
                self.request_tables();
                self.status = "refreshing tables".to_string();
            }
            View::Structure => {
                self.request_structure_for_selected();
            }
        }
    }

    // === Inspect ===

    fn inspect_selected(&mut self) {
        match self.view {
            View::Tables => {
                let Some(entry) = self.filtered_entries().get(self.selected()).cloned() else {
                    self.status = "nothing selected".to_string();
                    return;
                };
                self.open_table_data(entry.name);
            }
            View::Structure => {
                self.status =
                    "structure view shows columns/indexes/FKs panels; press 1 to browse data"
                        .to_string();
            }
        }
    }

    /// Declared type for a data-viewer column: the structure cache wins when
    /// it matches the viewer's table (information_schema types), otherwise
    /// the page column metadata (empty for duckdb's runtime extraction).
    fn structure_type_for(&self, table: &str, column_name: &str, fallback: &str) -> String {
        if let Some(view) = &self.structure
            && view.table == table
            && let Some(column) = view.columns.iter().find(|c| c.name == column_name)
            && !column.data_type.is_empty()
        {
            return column.data_type.clone();
        }
        fallback.to_string()
    }

    fn open_row_viewer(&mut self) {
        let Some(state) = self.data.as_ref() else {
            return;
        };
        let Some((page_index, row)) = state.selected_row() else {
            self.status = "nothing selected".to_string();
            return;
        };
        let columns = state.columns.clone();
        let pks = state.pks.clone();
        let total = state.total;
        let offset = state.offset + page_index as u64;
        let table_ref = state.table.clone();

        let mut lines = BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT);
        lines.push_line(format!("row {} of {total} in {table_ref}", offset + 1));
        lines.push_line(String::new());
        for (index, column) in columns.iter().enumerate() {
            let value = row.values.get(index).unwrap_or(&CellValue::Null);
            let mut flags = Vec::new();
            if column.is_primary_key || pks.contains(&column.name) {
                flags.push("PK");
            }
            flags.push(if column.nullable {
                "nullable"
            } else {
                "NOT NULL"
            });
            let data_type = self.structure_type_for(&state.table, &column.name, &column.data_type);
            lines.push_line(format!(
                "{:<22} {:<18} {:<16} {}",
                column.name,
                data_type,
                flags.join(" "),
                cell_display(value)
            ));
        }
        self.row_viewer_table = Some(table_ref);
        self.row_viewer_offset = offset;
        self.row_viewer_lines = lines;
        self.row_viewer_scroll = 0;
        self.overlay = Overlay::RowViewer;
        self.status = format!("row viewer: row {} of {total}", offset + 1);
    }

    // === Mutations ===

    fn open_delete_prompt(&mut self) {
        let Some(state) = self.data.as_ref() else {
            self.status = "no table data loaded".to_string();
            return;
        };
        if state.pks.is_empty() {
            self.status = format!(
                "{} has no primary key; bounded row delete unavailable (CLI remains the path)",
                state.table
            );
            return;
        }
        if state.selected_row().is_none() {
            self.status = "select a row to delete".to_string();
            return;
        }
        self.typed_table_input.clear();
        self.overlay = Overlay::ConfirmDeleteRow;
        self.status = format!(
            "delete row from {}: type the exact table name to confirm, Esc cancels",
            state.table
        );
    }

    /// Derive the staging payload for the currently selected row. Returns
    /// `(table, absolute_row_offset, page_offset, pk_summary)`.
    fn staged_row_delete(&self) -> Option<(String, u64, u64, String)> {
        let state = self.data.as_ref()?;
        if state.pks.is_empty() {
            return None;
        }
        let (page_index, row) = state.selected_row()?;
        let pk_summary = pk_summary(&state.columns, &row, &state.pks);
        Some((
            state.table.clone(),
            state.offset + page_index as u64,
            state.offset,
            pk_summary,
        ))
    }

    fn confirm_plan(&mut self) {
        let Some(mut plan) = self.operation_plan.take() else {
            self.status = "no operation plan to confirm".to_string();
            return;
        };
        plan.confirmations = plan.confirmations.saturating_add(1);
        if plan.confirmations < plan.confirmations_required {
            self.status = format!(
                "{} on {} is {}; press y again to execute",
                plan.kind.label(),
                plan.table,
                plan.risk
            );
            self.operation_plan = Some(plan);
            return;
        }

        if self.readonly {
            self.status = format!(
                "readonly launch blocked {} on {}",
                plan.kind.label(),
                plan.table
            );
            self.operation_plan = Some(plan);
            return;
        }

        if let Some(fixture) = self.fixture.as_mut() {
            match fixture.apply_row_delete(&plan) {
                Ok(summary) => {
                    // Refetch first so the mutation summary is the final
                    // status the operator sees on the refreshed page.
                    self.refetch_current_page();
                    self.status = format!(
                        "fixture executed {} on {} ({}): {}",
                        plan.kind.label(),
                        plan.table,
                        plan.pk_summary,
                        summary
                    );
                }
                Err(message) => {
                    self.error_banner = Some(message);
                    self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                }
            }
        } else if let Some(service) = &self.service {
            let stale = self.data.as_ref().is_none_or(|state| {
                state.table != plan.table
                    || state.offset != plan.page_offset
                    || plan.local_index() >= state.rows.len()
            });
            if stale {
                self.status = "staged page no longer current; delete plan dropped".to_string();
                return;
            }
            let Some(state) = self.data.as_ref() else {
                return;
            };
            // The bounded delete path: the current page rows plus a single
            // deleted index; pending cell edits stay empty so the service
            // generates exactly one PK-anchored DELETE.
            service.send(DuckDbCommand::Crud(CrudCommand::SaveChanges {
                table: plan.table.clone(),
                columns: state.columns.clone(),
                rows: state.rows.clone(),
                pk_columns: state.pks.clone(),
                pending: HashMap::new(),
                deleted: vec![plan.local_index()],
            }));
            self.status = format!("{} sent to DuckDbService", plan.kind.label());
        } else {
            self.status = "no service connection; plan dropped".to_string();
        }
    }

    fn cancel_plan(&mut self) {
        if self.operation_plan.take().is_some() {
            self.status = "operation plan cancelled".to_string();
        } else {
            self.mode = Mode::Browser;
            self.status = "browser mode".to_string();
        }
    }

    fn request_quit(&mut self) {
        let work_active = self.tables_pending
            || self.structure_pending
            || self.data_pending
            || self.operation_plan.is_some();
        if work_active {
            self.overlay = Overlay::ConfirmQuit;
            self.status =
                "quit? a table/structure/data fetch or staged plan is active: y quits, any other key stays"
                    .to_string();
            return;
        }
        self.should_quit = true;
    }

    fn show_help(&mut self) {
        self.overlay = Overlay::Help;
        self.status = "help open; any key closes".to_string();
    }

    // === Data viewer selection ===

    fn data_row_selection(&self) -> usize {
        self.data.as_ref().map_or(0, |state| state.row_selection)
    }

    fn set_data_selection(&mut self, value: usize) {
        if let Some(state) = self.data.as_mut() {
            let len = state.filtered_rows().len();
            state.row_selection = value.min(len.saturating_sub(1));
        }
    }

    fn move_data_selection(&mut self, delta: isize) {
        let len = self
            .data
            .as_ref()
            .map(|state| state.filtered_rows().len())
            .unwrap_or(0);
        if len == 0 {
            self.set_data_selection(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.data_row_selection() as isize + delta).clamp(0, last) as usize;
        self.set_data_selection(next);
    }

    // === Drawing ===

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                // Header and status each carry two content rows (title+tabs,
                // status+hints) on top of their borders.
                Constraint::Length(4),
                Constraint::Min(12),
                Constraint::Length(4),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(vertical[1]);
        match self.view {
            View::Tables => {
                frame.render_widget(self.table_list(body[0]), body[0]);
                frame.render_widget(self.table_details_panel(), body[1]);
            }
            View::Structure => {
                frame.render_widget(self.structure_columns(body[0]), body[0]);
                frame.render_widget(self.structure_indexes_panel(), body[1]);
            }
        }
        frame.render_widget(self.status_panel(), vertical[2]);

        match self.overlay {
            Overlay::None => {}
            Overlay::Help => self.draw_help(frame, area),
            Overlay::DataViewer => self.draw_data_viewer(frame, area),
            Overlay::RowViewer => self.draw_row_viewer(frame, area),
            Overlay::ConfirmDeleteRow => self.draw_confirm_delete(frame, area),
            Overlay::ConfirmQuit => self.draw_confirm_quit(frame, area),
        }
        if let Some(error) = &self.error_banner {
            self.draw_error_banner(frame, area, error);
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let tables_mark = if self.tables_pending || self.data_pending {
            "\u{27f3}"
        } else {
            ""
        };
        let structure_mark = if self.structure_pending {
            "\u{27f3}"
        } else {
            ""
        };
        let tables_tab = format!("1:tables{tables_mark}");
        let structure_tab = format!("2:structure{structure_mark}");
        let tabs = match self.view {
            View::Tables => format!("[{tables_tab}] {structure_tab}"),
            View::Structure => format!("{tables_tab} [{structure_tab}]"),
        };
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "DuckDB Data TUI (pilot)",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} file={}",
                    self.profile_label, self.server_label
                )),
            ]),
            Line::from(format!(
                "{}  source={} kind={} purpose={} readonly={} restore={} filter={}",
                tabs,
                source,
                self.connection_kind,
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label()
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn table_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_entries();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.selected(), visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, meta)| {
                let marker = if idx == self.selected() { ">" } else { " " };
                let style = if idx == self.selected() {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<24} {:<8} {:>10}",
                        meta.name,
                        meta.table_type,
                        meta.rows
                            .map(|rows| rows.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if !lines.is_empty() {
            lines
        } else if self.tables_pending {
            vec![Line::from("loading\u{2026}")]
        } else {
            vec![Line::from(
                "No tables visible. Press r to refresh or / to filter.",
            )]
        })
        .block(
            Block::default()
                .title(format!(
                    " tables in {} ({}/{}) views={} ",
                    self.server_label,
                    self.selected().saturating_add(1).min(items.len().max(1)),
                    items.len(),
                    self.views.len()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn table_details_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.filtered_entries().get(self.selected()) {
            Some(meta) => {
                let rows_label = meta
                    .rows
                    .map(|rows| rows.to_string())
                    .unwrap_or_else(|| "-".to_string());
                vec![
                    Line::from(format!("table: {}", meta.name)),
                    Line::from(format!("type: {}  rows: {}", meta.table_type, rows_label)),
                    Line::from("i/Enter opens the bounded data viewer"),
                    Line::from("row delete: d inside the viewer (typed table name + y)"),
                    Line::from("views open read-only; they have no primary key"),
                ]
            }
            None => vec![Line::from(
                "Select a table to page through its rows. Press r to refresh.",
            )],
        };

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!(
                "target: {} row {}",
                plan.table, plan.pk_summary
            )));
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn structure_columns(&self, area: Rect) -> Paragraph<'_> {
        let Some(view) = &self.structure else {
            let hint = if self.structure_pending {
                "loading\u{2026}"
            } else {
                "No structure loaded. Select a table in tables view (1), then press 2."
            };
            return Paragraph::new(vec![Line::from(hint)]).block(
                Block::default()
                    .title(format!(" columns \u{b7} {} ", self.selected_table_label()))
                    .borders(Borders::ALL),
            );
        };
        let count = view.columns.len();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.structure_selection, visible, count);
        let mut lines: Vec<Line> = view
            .columns
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, column)| {
                let marker = if idx == self.structure_selection {
                    ">"
                } else {
                    " "
                };
                let style = if idx == self.structure_selection {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let mut flags = Vec::new();
                if column.is_primary_key {
                    flags.push("PK");
                }
                flags.push(if column.nullable { "null" } else { "not null" });
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<22} {:<18} {:<16} {}",
                        column.name,
                        column.data_type,
                        flags.join(" "),
                        column.default_value.as_deref().unwrap_or("")
                    ),
                    style,
                ))
            })
            .collect();
        if lines.is_empty() {
            lines.push(Line::from("no columns reported for this table"));
        }
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(format!(
                        " columns \u{b7} {} ({}/{}) ",
                        view.table,
                        self.structure_selection.saturating_add(1).min(count.max(1)),
                        count
                    ))
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false })
    }

    fn selected_table_label(&self) -> String {
        self.filtered_entries()
            .get(self.selected())
            .map(|meta| meta.name.clone())
            .unwrap_or_else(|| "-".to_string())
    }

    fn structure_indexes_panel(&self) -> Paragraph<'_> {
        let mut lines: Vec<Line> = Vec::new();
        match &self.structure {
            Some(view) => {
                lines.push(Line::from(Span::styled(
                    format!("table: {}", view.table),
                    Style::default().add_modifier(Modifier::BOLD),
                )));
                lines.push(Line::from("indexes:"));
                if view.indexes.is_empty() {
                    lines.push(Line::from("  (none reported)"));
                }
                for index in &view.indexes {
                    lines.push(Line::from(format!(
                        "  {} [{}] {}",
                        index.name,
                        if index.unique {
                            "UNIQUE"
                        } else {
                            &index.index_type
                        },
                        index.columns.join(", ")
                    )));
                }
                lines.push(Line::from("foreign keys:"));
                if view.foreign_keys.is_empty() {
                    lines.push(Line::from("  (none reported)"));
                }
                for fk in &view.foreign_keys {
                    lines.push(Line::from(format!(
                        "  {} ({}) -> {} ({})",
                        fk.name,
                        fk.columns.join(", "),
                        fk.referenced_table,
                        fk.referenced_columns.join(", ")
                    )));
                }
            }
            None => {
                lines.push(Line::from(
                    "No structure loaded. Select a table in tables view (1), then press 2.",
                ));
            }
        }
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(" indexes & foreign keys ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        let hints = if self.data_filtering {
            let (filter, matches) = self
                .data
                .as_ref()
                .map(|state| (state.filter.clone(), state.filtered_rows().len()))
                .unwrap_or_default();
            format!(
                "viewer filter: {}\u{258f} Enter apply \u{b7} Esc clear \u{b7} {matches} match(es)",
                if filter.is_empty() { "" } else { &filter }
            )
        } else if self.mode == Mode::Filter {
            format!(
                "filter: {}\u{258f} Enter apply \u{b7} Esc clear \u{b7} {} match(es)",
                self.filter_label(),
                self.current_items_len()
            )
        } else {
            match self.overlay {
                Overlay::RowViewer => ROW_HINTS.to_string(),
                Overlay::DataViewer => DATA_HINTS.to_string(),
                _ => match self.view {
                    View::Tables => TABLES_HINTS.to_string(),
                    View::Structure => STRUCTURE_HINTS.to_string(),
                },
            }
        };
        let (data_window, data_total, data_matches) = self
            .data
            .as_ref()
            .map(|state| {
                (
                    if state.rows.is_empty() {
                        "0".to_string()
                    } else {
                        format!(
                            "{}-{}",
                            state.offset + 1,
                            state.offset + state.rows.len() as u64
                        )
                    },
                    state.total,
                    state.filtered_rows().len(),
                )
            })
            .unwrap_or(("-".to_string(), 0, 0));
        Paragraph::new(vec![
            Line::from(format!(
                "{} | mode={} | tables {} (+{} views) | data rows {} of {} (match {}) | viewer {}/{} dropped {} | pending tables={} structure={} data={}",
                self.status,
                mode_label(self.mode),
                self.entries.len().saturating_sub(self.views.len()),
                self.views.len(),
                data_window,
                data_total,
                data_matches,
                self.row_viewer_lines.len(),
                self.row_viewer_lines.max_lines,
                self.row_viewer_lines.dropped_lines,
                self.tables_pending,
                self.structure_pending,
                self.data_pending,
            )),
            Line::from(Span::styled(hints, Style::default().fg(Color::DarkGray))),
        ])
        .block(Block::default().title(" Status ").borders(Borders::ALL))
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(78, 84, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from(Span::styled(
                "Navigation",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("1 tables browser \u{b7} 2 structure (Tab cycles)"),
            Line::from("j/k or arrows move \u{b7} PgUp/PgDn page \u{b7} g/G home/end"),
            Line::from("/ client-side filter (live, Esc clears)"),
            Line::from(Span::styled(
                "Schema & paging",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("i/Enter on a table or view: bounded data viewer (LIMIT/OFFSET pages)"),
            Line::from("r refresh current view"),
            Line::from(Span::styled(
                "Inspect",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(
                "viewer: j/k PgUp/PgDn move \u{b7} n next page \u{b7} b back \u{b7} / filter page \u{b7} i row detail \u{b7} Esc close",
            ),
            Line::from("row viewer: typed columns with NULL/PK flags \u{b7} Esc back"),
            Line::from("structure: typed columns \u{b7} indexes \u{b7} foreign keys"),
            Line::from(Span::styled(
                "Mutations (staged plans, y confirms)",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("d on a row: delete (type the exact table name, then y)"),
            Line::from(
                "single PK-anchored delete only \u{b7} tables without a primary key refuse \u{b7} readonly launches block",
            ),
            Line::from(Span::styled(
                "Sessions",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("q: quit (asks for confirmation while a fetch or plan is active)"),
            Line::from("errors appear in a non-modal banner; any key acknowledges"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_data_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(92, 84, area);
        frame.render_widget(Clear, popup);
        let Some(state) = self.data.as_ref() else {
            frame.render_widget(
                Paragraph::new("no table data loaded")
                    .block(Block::default().title(" data ").borders(Borders::ALL)),
                popup,
            );
            return;
        };
        let matches = state.filtered_rows();
        let visible = popup.height.saturating_sub(2).max(4) as usize;
        let header_rows = 2usize;
        let body_visible = visible.saturating_sub(header_rows).max(1);
        let start = window_start(self.data_row_selection(), body_visible, matches.len());

        let row_refs: Vec<&Row> = matches.iter().map(|(_, row)| *row).collect();
        let widths = column_widths(&state.columns, &row_refs);
        let header_line = Line::from(Span::styled(
            format!("  {}", grid_row(&state.columns, None, &widths)),
            Style::default().add_modifier(Modifier::BOLD),
        ));
        let separator = Line::from(format!(
            "  {}",
            widths
                .iter()
                .map(|width| "\u{2500}".repeat(*width))
                .collect::<Vec<_>>()
                .join("  ")
        ));

        let mut body: Vec<Line> = vec![header_line, separator];
        for (position, (_, row)) in matches.iter().enumerate().skip(start).take(body_visible) {
            let marker = if position == self.data_row_selection() {
                ">"
            } else {
                " "
            };
            let style = if position == self.data_row_selection() {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            body.push(Line::from(Span::styled(
                format!("{marker}{}", grid_row(&state.columns, Some(*row), &widths)),
                style,
            )));
        }
        if state.loading && state.rows.is_empty() {
            body.push(Line::from("loading\u{2026}"));
        } else if state.total == 0 {
            body.push(Line::from(
                "No rows in this table (total 0). Press Esc to close.",
            ));
        } else if state.rows.is_empty() {
            body.push(Line::from(
                "No rows on this page. Press b to go back, n for the next page, or r to refetch.",
            ));
        } else if matches.is_empty() {
            body.push(Line::from(
                "No rows match the filter on this page. Esc in the filter clears it.",
            ));
        }

        if self.data_filtering {
            body.push(Line::from(Span::styled(
                format!("filter: {}\u{258f}", state.filter),
                Style::default().fg(Color::Cyan),
            )));
        }

        let window = if state.rows.is_empty() {
            "0".to_string()
        } else {
            format!(
                "{}-{}",
                state.offset + 1,
                state.offset + state.rows.len() as u64
            )
        };
        let pk_label = if state.pks.is_empty() {
            String::new()
        } else {
            format!(" \u{b7} pk {}", state.pks.join(","))
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " data {} \u{b7} rows {window} of {} \u{b7} offset {}{} \u{b7} j/k move \u{b7} n page \u{b7} b back \u{b7} i row detail \u{b7} d del row \u{b7} Esc close ",
                            state.table, state.total, state.offset, pk_label
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_row_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.row_viewer_lines.len();
        let body: Vec<Line> = if total == 0 {
            vec![Line::from("no row selected; press Enter on a row")]
        } else {
            let start = self.row_viewer_scroll.min(total.saturating_sub(1));
            self.row_viewer_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " row detail \u{b7} {} \u{b7} row {} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} d del row \u{b7} Esc close ",
                            self.row_viewer_table.as_deref().unwrap_or("(none)"),
                            self.row_viewer_offset + 1,
                            (self.row_viewer_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_delete(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(64, 26, area);
        frame.render_widget(Clear, popup);
        let target = self
            .data
            .as_ref()
            .map(|state| {
                let row = state
                    .selected_row()
                    .map(|(_, row)| pk_summary(&state.columns, &row, &state.pks))
                    .unwrap_or_else(|| "(no row selected)".to_string());
                format!("{} row {}", state.table, row)
            })
            .unwrap_or_else(|| "no table data loaded".to_string());
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Delete row (destructive)",
                    Style::default()
                        .fg(Color::Red)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!("target: {target}")),
                Line::from(
                    "One PK-anchored DELETE is sent; the whole table is never targeted.",
                ),
                Line::from("Type the exact table name to confirm:"),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.typed_table_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(
                    "Enter stages the plan (typed confirmation) \u{b7} y executes \u{b7} Esc cancels",
                ),
            ])
            .block(
                Block::default()
                    .title(" Confirm Delete Row ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_quit(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Quit with active work?",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(self.active_work_summary()),
                Line::from("y/Enter: quit \u{b7} any other key: stay"),
            ])
            .block(
                Block::default()
                    .title(" Confirm Quit ")
                    .borders(Borders::ALL),
            ),
            popup,
        );
    }

    fn active_work_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.tables_pending {
            parts.push("table fetch in flight".to_string());
        }
        if self.structure_pending {
            parts.push("structure fetch in flight".to_string());
        }
        if self.data_pending {
            parts.push("data page fetch in flight".to_string());
        }
        if let Some(plan) = &self.operation_plan {
            parts.push(format!("{} plan on {}", plan.kind.label(), plan.table));
        }
        if parts.is_empty() {
            "no active fetches or plans".to_string()
        } else {
            parts.join(", ")
        }
    }

    fn draw_error_banner(&self, frame: &mut Frame, area: Rect, error: &str) {
        let banner = Rect {
            x: area.x + 1,
            y: area.height.saturating_sub(6),
            width: area.width.saturating_sub(2),
            height: 3,
        };
        frame.render_widget(Clear, banner);
        frame.render_widget(
            Paragraph::new(vec![Line::from(Span::styled(
                safe_error_summary(error),
                Style::default().fg(Color::Red),
            ))])
            .block(
                Block::default()
                    .title(" Error (any key acknowledges) ")
                    .borders(Borders::ALL)
                    .style(Style::default().fg(Color::Red)),
            )
            .wrap(Wrap { trim: false }),
            banner,
        );
    }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn range(&self, start: usize, end: usize) -> Vec<String> {
        self.lines
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect()
    }
}

fn cell_display(value: &CellValue) -> String {
    let text = value.display();
    text.chars()
        .map(|ch| if ch.is_control() { '?' } else { ch })
        .collect()
}

fn cell_value_json(value: &CellValue) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn pk_summary(columns: &[ColumnInfo], row: &Row, pks: &[String]) -> String {
    if pks.is_empty() {
        return "(no primary key)".to_string();
    }
    pks.iter()
        .map(|pk| {
            let value = columns
                .iter()
                .position(|column| column.name == *pk)
                .and_then(|index| row.values.get(index))
                .map(cell_display)
                .unwrap_or_else(|| "?".to_string());
            format!("{pk}={value}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn column_widths(columns: &[ColumnInfo], rows: &[&Row]) -> Vec<usize> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let mut width = column.name.chars().count();
            for row in rows {
                if let Some(value) = row.values.get(index) {
                    width = width.max(cell_display(value).chars().count());
                }
            }
            width.clamp(4, CELL_MAX_WIDTH)
        })
        .collect()
}

fn fit_cell(text: &str, width: usize) -> String {
    let count = text.chars().count();
    if count <= width {
        format!("{text:<width$}")
    } else {
        let truncated: String = text.chars().take(width.saturating_sub(1)).collect();
        format!("{truncated}\u{2026}")
    }
}

fn grid_row(columns: &[ColumnInfo], row: Option<&Row>, widths: &[usize]) -> String {
    widths
        .iter()
        .enumerate()
        .map(|(index, width)| {
            let text = match row {
                Some(row) => row.values.get(index).map(cell_display).unwrap_or_default(),
                None => columns
                    .get(index)
                    .map(|column| column.name.clone())
                    .unwrap_or_default(),
            };
            fit_cell(&text, *width)
        })
        .collect::<Vec<_>>()
        .join("  ")
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "duckdb_password_value",
        "duckdb_secret_data_value",
        "plaintext_credential_value",
        "raw_connection_url",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

// ---------------------------------------------------------------------------
// Standalone tab manager
// ---------------------------------------------------------------------------

struct StandaloneDuckDbTabManager {
    render_tx: tokio::sync::mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneDuckDbTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow::anyhow!(
            "DuckDB TUI does not host plugin tabs; use plugin-owned CLI commands instead"
        )
    }
}

impl TabManager for StandaloneDuckDbTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "DuckDB".to_string(),
            plugin_id: "duckdb".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow::anyhow!("DuckDB TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("DuckDB TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn fixture_launch(readonly: bool) -> DuckDbTuiLaunch {
        DuckDbTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: DuckDbTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/duckdb_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    fn app() -> DuckDbTuiApp {
        DuckDbTuiApp::new(fixture_launch(false)).unwrap()
    }

    fn press(app: &mut DuckDbTuiApp, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn type_text(app: &mut DuckDbTuiApp, text: &str) {
        for ch in text.chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
        }
    }

    /// Open the data viewer for the entry at `index` (fixture tables are
    /// sorted by name, views appended after).
    fn open_entry(app: &mut DuckDbTuiApp, index: usize) {
        app.set_selected(index);
        press(app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_duckdb_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("duckdb_tui_fixture_evidence"));
        assert!(rendered.contains("typed_delete_confirmation"));
        assert!(rendered.contains("table_data_paging"));
        assert!(rendered.contains("delete_requires_primary_key"));
        assert!(rendered.contains("row_viewer_typed_columns"));
        assert!(rendered.contains("structure_viewer_columns_indexes_fks"));
        assert!(rendered.contains("later candidate"));
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
        assert_eq!(evidence["quality_gate"]["plugin_id"], json!("duckdb"));
        assert_eq!(
            evidence["quality_gate"]["layout"]["minimum_viewport"]["width"],
            json!(80)
        );
    }

    #[test]
    fn preflight_reports_redacted_file_target() {
        let launch = DuckDbTuiLaunch {
            profile_label: "live-duckdb".to_string(),
            config: Some(DuckDbConfig::new("/tmp/warehouse.duckdb".to_string())),
            source: DuckDbTuiSource::Profile,
            fixture_path: None,
            purpose: "triage".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert_eq!(preflight_value(&launch)["ok"], json!(true));
        assert_eq!(preflight_value(&launch)["command"], json!("duckdb tui"));
        assert_eq!(preflight_value(&launch)["plugin_id"], json!("duckdb"));
        assert_eq!(preflight_value(&launch)["raw_input"], json!(false));
        assert_eq!(
            preflight_value(&launch)["connection"]["kind"],
            json!("file")
        );
        assert_eq!(
            preflight_value(&launch)["connection"]["read_only"],
            json!(false)
        );
        assert_eq!(
            preflight_value(&launch)["connection"]["secret_material"],
            json!("redacted")
        );
        assert!(!rendered.contains("raw_connection_url"));
    }

    #[test]
    fn startup_target_error_banner_is_non_modal() {
        let mut app = app();
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("target error")
        );
        assert_eq!(app.mode, Mode::Browser, "errors must not hijack the mode");
        assert_eq!(app.view, View::Tables);
        assert_eq!(
            app.entries.len(),
            7,
            "fixture tables and views load despite the banner"
        );
        assert_eq!(app.views.len(), 1);
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "any key acknowledges");
        assert_eq!(app.entry_selection, 1, "the key keeps its normal meaning");
    }

    #[test]
    fn hotkeys_switch_views_and_tab_cycles() {
        let mut app = app();
        assert_eq!(app.view, View::Tables);
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Structure);
        assert!(
            app.status.contains("structure"),
            "structure view loads the selected table: {}",
            app.status
        );
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.view, View::Tables);
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Tables);
    }

    #[test]
    fn tables_browser_lists_views_without_counts() {
        let app = app();
        // Sorted tables then views appended: dim_customers, empty_archive,
        // fact_sales, keyvalue, locked_events, stg_events, v_daily_revenue.
        let names: Vec<&str> = app.entries.iter().map(|meta| meta.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "dim_customers",
                "empty_archive",
                "fact_sales",
                "keyvalue",
                "locked_events",
                "stg_events",
                "v_daily_revenue"
            ]
        );
        let view_entry = app.entries.last().unwrap();
        assert_eq!(view_entry.table_type, "view");
        assert!(view_entry.rows.is_none(), "views carry no row counts");
        assert_eq!(app.entries[0].rows, Some(6));
        assert_eq!(app.entries[1].rows, Some(0));
    }

    #[test]
    fn table_data_paging_tracks_offset_and_total() {
        let mut app = app();
        open_entry(&mut app, 5); // stg_events
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.table, "stg_events");
        assert_eq!(state.total, 12);
        assert_eq!(state.offset, 0);
        assert_eq!(state.rows.len(), 5, "one bounded page, not the whole table");
        assert_eq!(state.pks, vec!["event_id".to_string()]);
        press(&mut app, KeyCode::Char('n'));
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.offset, 5);
        assert_eq!(state.rows.len(), 5);
        press(&mut app, KeyCode::Char('n'));
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.offset, 10);
        assert_eq!(state.rows.len(), 2, "partial last page is visible");
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of table"));
        press(&mut app, KeyCode::Char('b'));
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.offset, 5);
        press(&mut app, KeyCode::Char('b'));
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.offset, 0);
        press(&mut app, KeyCode::Char('b'));
        assert!(app.status.contains("no previous page"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        assert!(app.data.is_none(), "closing the viewer drops the page");
    }

    #[test]
    fn view_opens_read_only_in_data_viewer() {
        let mut app = app();
        open_entry(&mut app, 6); // v_daily_revenue (view)
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.table, "v_daily_revenue");
        assert_eq!(state.total, 2);
        assert_eq!(state.rows.len(), 2);
        assert!(state.pks.is_empty());
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains("no primary key"),
            "deletes refuse on views"
        );
        assert!(app.operation_plan.is_none());
    }

    #[test]
    fn filter_reports_match_count_in_browser_and_viewer() {
        let mut app = app();
        press(&mut app, KeyCode::Char('/'));
        assert_eq!(app.mode, Mode::Filter);
        type_text(&mut app, "stg");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.mode, Mode::Browser);
        assert_eq!(app.filtered_entries().len(), 1);
        assert!(app.status.contains("(1 matches)"));
        press(&mut app, KeyCode::Enter); // open the filtered stg_events table
        assert_eq!(app.overlay, Overlay::DataViewer);
        press(&mut app, KeyCode::Char('/'));
        assert!(app.data_filtering);
        type_text(&mut app, "import_batch");
        press(&mut app, KeyCode::Enter);
        assert!(!app.data_filtering);
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.filtered_rows().len(), 5);
        assert!(app.status.contains("(5 matches)"));
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        let state = app.data.as_ref().expect("data viewer state");
        assert!(state.filter.is_empty());
        assert!(app.status.contains("viewer filter cancelled"));
    }

    #[test]
    fn row_viewer_renders_typed_columns_with_null_json_and_blob() {
        let mut app = app();
        open_entry(&mut app, 5); // stg_events
        press(&mut app, KeyCode::Enter); // row 1
        assert_eq!(app.overlay, Overlay::RowViewer);
        let rendered = app
            .row_viewer_lines
            .range(0, app.row_viewer_lines.len())
            .join("\n");
        assert!(rendered.contains("event_id"));
        assert!(
            rendered.contains("BIGINT"),
            "structure types feed the detail"
        );
        assert!(rendered.contains("JSON"));
        assert!(rendered.contains("PK"));
        assert!(
            rendered.contains("{\"batch\":\"nightly-2026-11-01\",\"seq\":1}"),
            "JSON documents render as text"
        );
        assert!(rendered.contains("notes"));
        assert!(rendered.contains("NULL"), "NULL values render literally");
        press(&mut app, KeyCode::Esc);
        assert_eq!(
            app.overlay,
            Overlay::DataViewer,
            "row viewer returns to the data"
        );
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        // Blob cells render with the bounded display form.
        open_entry(&mut app, 3); // keyvalue
        press(&mut app, KeyCode::Char('j'));
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::RowViewer);
        let rendered = app
            .row_viewer_lines
            .range(0, app.row_viewer_lines.len())
            .join("\n");
        assert!(
            rendered.contains("[BLOB 5 bytes]"),
            "blob cells render through the shared display form"
        );
    }

    #[test]
    fn structure_view_shows_columns_indexes_and_fks() {
        let mut app = app();
        app.set_selected(5); // stg_events
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Structure);
        let view = app.structure.as_ref().expect("structure cached");
        assert_eq!(view.table, "stg_events");
        assert_eq!(view.columns.len(), 6);
        assert!(view.columns[0].is_primary_key);
        assert_eq!(view.columns[3].data_type, "TIMESTAMP");
        assert_eq!(view.indexes.len(), 0);
        assert_eq!(view.foreign_keys.len(), 1);
        assert_eq!(view.foreign_keys[0].referenced_table, "dim_customers");
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Tables);
        // fact_sales carries one ART index.
        app.set_selected(2);
        press(&mut app, KeyCode::Char('2'));
        let view = app.structure.as_ref().expect("structure cached");
        assert_eq!(view.table, "fact_sales");
        assert_eq!(view.indexes.len(), 1);
        assert_eq!(view.indexes[0].name, "idx_fact_sales_region");
        assert_eq!(view.indexes[0].index_type, "ART");
    }

    #[test]
    fn typed_delete_requires_exact_table_name() {
        let mut app = app();
        open_entry(&mut app, 5); // stg_events
        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::ConfirmDeleteRow);
        type_text(&mut app, "dim_customers");
        press(&mut app, KeyCode::Enter);
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("typed name does not match stg_events"));
        assert_eq!(app.overlay, Overlay::ConfirmDeleteRow);
        type_text(&mut app, "stg_events");
        press(&mut app, KeyCode::Enter);
        assert_eq!(
            app.overlay,
            Overlay::DataViewer,
            "staging returns to the data viewer the confirm came from"
        );
        let plan = app.operation_plan.clone().expect("delete plan staged");
        assert_eq!(plan.kind, OperationKind::DeleteRow);
        assert_eq!(plan.table, "stg_events");
        assert_eq!(plan.row_offset, 0);
        assert_eq!(plan.confirmations_required, 2);
        assert_eq!(
            plan.confirmations, 1,
            "typed entry is the first confirmation"
        );
        assert_eq!(plan.pk_summary, "event_id=1");
        assert!(app.status.contains("typed confirmation recorded"));
    }

    #[test]
    fn delete_executes_after_y_and_refetches_page() {
        let mut app = app();
        open_entry(&mut app, 5); // stg_events
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "stg_events");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed delete row"));
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.total, 11, "the refetched page reports the new total");
        assert_eq!(
            state.rows[0].values[0],
            CellValue::Int(2),
            "the deleted row is gone from the refetched page"
        );
    }

    #[test]
    fn delete_refused_without_primary_key() {
        let mut app = app();
        open_entry(&mut app, 3); // keyvalue has no PK
        press(&mut app, KeyCode::Char('d'));
        assert!(app.status.contains("no primary key"));
        assert_eq!(
            app.overlay,
            Overlay::DataViewer,
            "the refusal keeps the operator in the data viewer"
        );
        assert!(app.operation_plan.is_none());
    }

    #[test]
    fn readonly_launch_blocks_confirmed_plan() {
        let mut app = DuckDbTuiApp::new(fixture_launch(true)).unwrap();
        open_entry(&mut app, 5); // stg_events
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "stg_events");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("readonly launch blocked"));
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(
            state.total, 12,
            "readonly launches must not mutate the table"
        );
    }

    #[test]
    fn erroring_delete_shows_banner_and_stays_responsive() {
        let mut app = app();
        open_entry(&mut app, 4); // locked_events (delete fails at the fixture)
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "locked_events");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.error_banner.is_some());
        assert!(app.status.contains("target error"));
        assert!(app.operation_plan.is_none());
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.total, 1, "a failed mutation must not remove the row");
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "banner stays non-modal");
        assert_eq!(app.data_row_selection(), 0, "the row keeps its meaning");
        // The browser keeps working: open another table after the failure.
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        app.set_selected(5); // stg_events
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DataViewer);
    }

    #[test]
    fn empty_table_shows_empty_state() {
        let mut app = app();
        open_entry(&mut app, 1); // empty_archive
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.total, 0);
        assert!(state.rows.is_empty());
        assert!(!state.loading);
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of table"));
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains("select a row to delete"),
            "mutations refuse to stage with nothing selected"
        );
        assert!(app.operation_plan.is_none());
    }

    #[test]
    fn quit_with_active_plan_requires_confirmation() {
        let mut app = app();
        open_entry(&mut app, 5);
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "stg_events");
        press(&mut app, KeyCode::Enter);
        // Close the data viewer first; q inside the viewer closes it.
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        press(&mut app, KeyCode::Char('q'));
        assert_eq!(app.overlay, Overlay::ConfirmQuit);
        assert!(app.operation_plan.is_some(), "the staged plan survives");
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Char('q'));
        press(&mut app, KeyCode::Char('y'));
        assert!(app.should_quit);
    }

    #[test]
    fn page_keys_move_selection() {
        let mut app = app();
        press(&mut app, KeyCode::PageDown);
        assert_eq!(
            app.selected(),
            (PAGE_STEP as usize).min(app.filtered_entries().len() - 1),
            "PageDown clamps to the last entry"
        );
        press(&mut app, KeyCode::PageUp);
        assert_eq!(app.selected(), 0);
    }

    /// Pump service events until `pred` holds or the deadline lapses.
    fn pump_until(app: &mut DuckDbTuiApp, pred: impl Fn(&DuckDbTuiApp) -> bool, secs: u64) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
        while std::time::Instant::now() < deadline {
            app.drain_service();
            if pred(app) {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        pred(app)
    }

    #[test]
    fn live_service_roundtrip_lists_pages_and_deletes() {
        // Real embedded file through the live DuckDbService channel contract:
        // ListTables -> LoadTableData -> one PK-anchored SaveChanges delete
        // -> page refetch. This is the same surface a profile launch uses.
        let db_path = std::env::temp_dir().join(format!(
            "voidb-duckdb-tui-live-{}.duckdb",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&db_path);
        {
            let conn = duckdb::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE live_events (event_id BIGINT PRIMARY KEY, kind VARCHAR);
                 INSERT INTO live_events SELECT i, 'batch' FROM range(1, 13) t(i);",
            )
            .unwrap();
        }

        let launch = DuckDbTuiLaunch {
            profile_label: "live-test".to_string(),
            config: Some(DuckDbConfig::new(db_path.display().to_string())),
            source: DuckDbTuiSource::Connection,
            fixture_path: None,
            purpose: "operations".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let mut app = DuckDbTuiApp::new(launch).unwrap();
        assert!(app.service.is_some(), "live mode owns a service");
        assert!(app.tables_pending, "the first ListTables fetch is pending");

        let loaded = pump_until(
            &mut app,
            |app| !app.entries.is_empty() && !app.tables_pending,
            10,
        );
        assert!(loaded, "tables loaded through the live service");
        assert_eq!(app.entries.len(), 1);
        assert_eq!(app.entries[0].name, "live_events");
        assert_eq!(app.entries[0].rows, Some(12));

        press(&mut app, KeyCode::Enter);
        let page_loaded = pump_until(
            &mut app,
            |app| {
                app.data
                    .as_ref()
                    .is_some_and(|state| !state.loading && state.total == 12)
            },
            10,
        );
        assert!(page_loaded, "first bounded page loaded");
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.rows.len(), 5, "one bounded page of 5 rows");
        assert_eq!(state.pks, vec!["event_id".to_string()]);
        press(&mut app, KeyCode::Char('n'));
        let second_page = pump_until(
            &mut app,
            |app| {
                app.data
                    .as_ref()
                    .is_some_and(|state| !state.loading && state.offset == 5)
            },
            10,
        );
        assert!(second_page, "next page fetched through the service");

        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::ConfirmDeleteRow);
        type_text(&mut app, "live_events");
        press(&mut app, KeyCode::Enter);
        assert!(app.operation_plan.is_some(), "typed confirmation stages");
        press(&mut app, KeyCode::Char('y'));
        // The staged row was on the second page (offset 5, event_id 6); the
        // refetched page must drop the total to 11 and start at event_id 7.
        let saved = pump_until(
            &mut app,
            |app| {
                app.operation_plan.is_none()
                    && app
                        .data
                        .as_ref()
                        .is_some_and(|state| !state.loading && state.total == 11)
            },
            10,
        );
        assert!(saved, "SaveChanges delete applied: {}", app.status);
        let state = app.data.as_ref().expect("data viewer state");
        assert_eq!(state.total, 11, "the refetched page reports the new total");
        assert_eq!(
            state.rows[0].values[0],
            CellValue::Int(7),
            "the deleted event_id 6 is gone at offset 5"
        );

        app.shutdown();
        drop(app);
        {
            let conn = duckdb::Connection::open(&db_path).unwrap();
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM live_events", [], |r| r.get(0))
                .unwrap();
            eprintln!("DEBUG file row count after delete: {count}");
        }
        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn fixture_cells_use_service_decode_shapes() {
        // The fixture must only carry CellValue variants the live decode
        // (decode_duckdb_value: Int/Float/Bool/String/Blob/Null) can produce.
        let data = load_fixture(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/duckdb_tui_operations.json")
                .display()
                .to_string()
                .as_str(),
        )
        .unwrap();
        for (name, table) in &data.table_data {
            for row in &table.rows {
                for value in &row.values {
                    assert!(
                        matches!(
                            value,
                            CellValue::Int(_)
                                | CellValue::Float(_)
                                | CellValue::Bool(_)
                                | CellValue::Text(_)
                                | CellValue::Blob(_)
                                | CellValue::Null
                        ),
                        "{name} carries a cell the duckdb decode surface cannot produce: {value:?}"
                    );
                }
            }
        }
        // Spot-check the shared display forms the TUI renders.
        assert_eq!(cell_display(&CellValue::Null), "NULL");
        assert_eq!(
            cell_display(&CellValue::Blob(vec![0xDE, 0xAD])),
            "[BLOB 2 bytes]"
        );
        assert_eq!(fit_cell("stg_events", 8), "stg_eve\u{2026}");
        assert_eq!(
            pk_summary(
                &[ColumnInfo {
                    name: "event_id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    max_length: None,
                    extra: String::new(),
                }],
                &Row {
                    values: vec![CellValue::Int(7)]
                },
                &["event_id".to_string()]
            ),
            "event_id=7"
        );
    }
}
