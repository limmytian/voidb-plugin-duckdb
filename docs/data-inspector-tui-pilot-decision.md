# DuckDB Data-Inspector TUI Pilot Decision (Requirement 83)

- **Plugin / profile type**: `voidb-plugin-duckdb` / DuckDB file and in-memory
  profiles (`path` to a `.duckdb` database file or `:memory:`, optional
  `read_only` mode, optional auto-loaded extensions, memory/thread limits).
- **Selected state**: `Accepted pilot` (family precedent: the retained Redis
  data-TUI pilot is the canonical data-TUI implementation, and the MySQL and
  PostgreSQL pilots are the accepted SQL-data adaptations).
- **Command**: `voidb-plugin-duckdb tui` (plugin-owned, CLI-launched), also
  routed as `duckdb tui` through `voidb-cli`.
- **Scope**: analytical file triage. This is new plugin-owned UI work built on
  the existing `DuckDbService` channel contract; it adds no backend operations
  and does not depend on removed router-hosted factories, shared legacy data
  widgets, or shell-owned plugin UI state.

This record satisfies the Requirement 83 data inspector TUI acceptance gate in
`voidb/docs/data-inspector-cli-first-decisions.md`, which previously recorded
DuckDB as `CLI-only` ("native build cost and extension variability make it a
poor first inspector pilot"; "keep analytics workflows scriptable"). The
precondition the original no-pilot decision named — a bounded read path in the
plugin service layer — now exists (`SchemaCommand::ListTables` with row
counts, `LoadCommand::LoadTableData` with LIMIT/OFFSET), and the pilot TUI
rides that existing surface without adding backend operations. The native
build cost stays where it belongs: the pilot adds only `ratatui`/`crossterm`
and reuses the already-linked `duckdb` driver; no new database stack is
introduced. The pilot is implemented, validated, and accepted here with
fixture, target-error, and real PTY evidence produced in the same
requirement.

## Mutation scope decision (recorded up front)

**Slice 1 ships one bounded PK-anchored row delete — the MySQL precedent, not
the read-only slice.** The duckdb service layer already exposes
`CrudCommand::SaveChanges { table, columns, rows, pk_columns, pending,
deleted }`, and `service/crud.rs::handle_save_changes` generates exactly one
PK-anchored `DELETE FROM <table> WHERE <build_pk_where>` per deleted row
index inside a transaction, refusing up front (`build_pk_where` in
`service/value_convert.rs`) when the table has no primary key. The TUI sends
that path with an **empty pending-edit set and a single deleted row index**,
so the whole mutation surface is one typed, two-step-confirmed,
PK-anchored single-row delete — there is no bulk, table-level, unbounded, or
free-form mutation anywhere in the pilot. `--readonly` launches stage plans
but block at confirm. Row cell edits (`SaveChanges` pending updates), row
paste/insert (`PasteRows`), CSV import (`ImportCsv`), and all DDL/table
management (`BrowserCommand`, including `DropTable`) are **`later
candidate`** and unreachable from the TUI.

## Named human workflow

**Analytics file triage over one embedded database.** An operator responds to
a bad-import or data-quality incident on a DuckDB file:

1. **Start**: the operator opens an existing profile
   (`duckdb tui --profile sales-2026 --purpose triage`). The TUI opens
   read-only on the tables view; the first viewport is the file's table list
   with name/type/row-count columns (from the service's
   `information_schema` listing plus per-table `COUNT(*)`), so oversized
   tables are visible before any of them is opened. Views are listed with
   their own `view` type marker; the file itself is the database.
2. **Browse**: the operator filters the list with `/` (live match count) and
   lands on `stg_events`, a staging table inflated by a failed overnight
   import.
3. **Page**: pressing `i` opens the full-screen TABLE DATA VIEWER on the
   first bounded page (`LoadTableData` with `LIMIT 5 OFFSET 0` through the
   service's paged loader). `n` fetches the next page, `b` walks back; pages
   are **replaced**, not appended, so the viewport stays bounded.
4. **Inspect**: `/` narrows the loaded page live with a match count; Enter on
   a row opens the typed row-detail viewer — one line per column with the
   declared type from the service structure (`payload (JSON): {"k": 1}`),
   NULL rendered literally, and NULL markers for complex LIST/STRUCT cells
   whose decoding the service probe does not widen yet (recorded below).
5. **Structure**: pressing `2` shows the selected table's columns (typed,
   with nullable/PK/default markers), indexes, and foreign keys
   (`SchemaCommand::LoadStructure`) without leaving the workflow.
6. **Act**: for the duplicated import batch, the operator presses `d`,
   **types the exact table name**, and presses `y`; the delete is a single
   PK-anchored row delete through the service's existing `SaveChanges`
   path, and the current page is refetched so the row disappears. A failed
   delete (file lock held by another process — the classic DuckDB
   single-writer conflict) shows a redacted target error in a non-modal
   banner and the operator keeps working.
7. **Success state**: without leaving the TUI, the operator has walked
   file → table → page → row, confirmed the column types, and removed one
   identified row, with the refreshed page as confirmation. Edits, inserts,
   imports, and DDL are not part of this slice (explicitly `later candidate`
   above).

**Why the CLI is materially worse here**: the same loop requires
`duckdb tables`, then `duckdb describe stg_events`, then one
`duckdb query "SELECT * FROM stg_events LIMIT 5 OFFSET m"` invocation per
page, plus a hand-written `DELETE ... WHERE pk` per candidate row — each a
fresh process that re-opens the database file (and re-loads configured
extensions) and re-derives column names, types, and NULL/JSON rendering from
scratch, with no shared file, table, page, or cursor context. The TUI keeps
the table selection, offset paging state, per-page filter, and typed row
viewer in one viewport with persistent per-view selection memory. Generic
"browse data" is not the claim; the claim is cursor-preserving triage over
repeated page-inspect cycles on one analytical file, where process startup
(file open plus extension load) dominates a CLI loop.

## Gate-by-gate evidence

| Gate | Evidence in this implementation |
|---|---|
| Named human workflow | Above; exercised end to end in fixture mode and by the PTY transcript (open `stg_events` → page 1→2→3 with a partial final page → viewer filter → typed row detail with JSON/NULL cells → typed delete of row `event_id=1`, refetch shows 11 rows and the row gone). |
| Capability-backed bounded reads | Reads are the existing service-layer operations only: `SchemaCommand::ListTables` (`information_schema.tables` in the `main` schema plus per-table `COUNT(*)`, bound in `service/schema.rs`), `SchemaCommand::LoadStructure { table }` (typed columns, indexes, foreign keys), and `LoadCommand::LoadTableData { table, page_size, offset }` — `LIMIT page_size OFFSET offset` bound in `service/data_loader.rs`, with `LoadRowCount`/`LoadPrimaryKeys` piggybacked by the same command. The TUI adds no backend operations; `CrudCommand::ExecuteQuery`/`ExecuteMultiQuery` (arbitrary SQL) and every `BrowserCommand` are never sent, so there is no free-form SQL surface in the pilot. |
| Pagination and large-result safety | One page (`PAGE_SIZE = 5` rows) is rendered at a time; `n` fetches `offset + PAGE_SIZE` only and pages are replaced, never appended, so the viewport stays bounded. The viewer title reports `rows X-Y of T offset N`, so empty state (`empty_archive`: "No rows in this table (total 0)"), partial last page (`stg_events`: 12 rows over 3 pages), and end-of-table state are all visible. Table row counts are single `COUNT(*)` queries against the embedded file, not full scans rendered into the TUI. Row-viewer bodies are bounded by `BoundedLines` at 2,000 lines / 2 MiB with dropped-line counters in the status panel, and grid cells are capped at 28 chars with an ellipsis. |
| Redaction and error shape | Preflight (`--format json`) reports the profile label, target file label, `read_only` mode, and `"secret_material": "redacted"` with no connection URL and no credential material (DuckDB profiles carry no password; the launch plan's `credential_grant`/redaction summary is still surfaced when a profile launch plan exists); `secret_leak_markers` scan covers `duckdb_password_value`, `duckdb_secret_data_value`, `plaintext_credential_value`, `raw_connection_url` and passes (`passed: true`, 0 markers) in `build_duckdb_tui_evidence` output; errors render through `safe_error_summary` (180-char cap, newlines collapsed) in the non-modal banner and keep the service-layer error text from `service/*` (`Prepare:`/`Query:`/`Execute:` prefixes; validation/auth/permission/transport/timeout/plugin/target categories come from the shared capability error contract when errors arrive through capability surfaces). |
| Mutation safety | **First viewport is read-only** — the tables browser binds no mutation keys. Row `DELETE` requires **typed confirmation of the exact table name** plus explicit `y` (two confirmations, `destructive` risk), is refused up front when the table has no primary key (`build_pk_where` cannot anchor a bounded delete — this also refuses every view), refuses when the staged page is no longer current, and sends exactly one PK-anchored `DELETE FROM <table> WHERE …` through the existing `CrudCommand::SaveChanges` path with an empty pending-edit set and a single deleted row index — there is no bulk, table-level, or unbounded delete anywhere in the pilot. `--readonly` launch stages plans but blocks at confirm with `readonly launch blocked delete row on <table>`. Cell edits, paste/insert, CSV import, and all DDL (`BrowserCommand`, including `DropTable`) are `later candidate` and unreachable. Mutation policy is recorded in `evidence["mutation_rescan_policy"]` and `evidence["plan_transcript"]`. |
| Fixture and target-error evidence | `fixtures/duckdb_tui_operations.json` covers tables and views with row counts, 12-row paging over 5-row pages (full/partial/empty), NULL cells, a JSON document cell, a boolean/float/date mix, a no-primary-key table, an empty table, and a deliberately erroring table (`locked_events` → `target error: IO Error: Conflicting lock is held on the database file … (fixture)`). Startup carries a `target error: IO Error: Conflicting lock …` banner; the PTY proves the TUI stays fully responsive across it (all assertions pass with the banner present on the first frame). |
| PTY lifecycle evidence | Real PTY run (`pty.fork()` + `TIOCSWINSZ` `HHHH` at 36x130, winsize set pre-exec): 47/47 assertions across 3 consecutive clean runs, exit code 0 each run. First frame markers (`DuckDB Data TUI`, `1:tables`, `2:structure`, `fixture-duckdb-ops`, fixture rows, hint footer, target-error banner); navigation into the data viewer with paging forward/back and a partial final page; viewer filter with match count and clear; typed row detail; structure view with the FK panel; typed-delete confirmation and refetch; resize to 24x100 keeps content visible and reports `resized duckdb view to 100x24`; `q` exits 0 with terminal restore; raw-mode cleanup is delegated to `ratatui::restore()` after the loop (raw input is not used: `raw_input = false`). |
| No legacy dependency | The command lives in the plugin (`src/tui.rs`), is launched from the plugin CLI (`duckdb tui`), consumes only `DuckDbService` channel events, and contains no reference to removed router-hosted data-inspector adapters or shared legacy widgets. The agent-session/agent_live machinery is untouched. |

## Interaction surface

- Views and hotkeys: `1` tables browser, `2` structure (Tab cycles); pending
  table/structure/data fetches show the family `⟳` marker in the header tabs.
  **There is no databases view**: a DuckDB profile is one embedded file, and
  the service listing (`SchemaCommand::ListTables`) has no database or schema
  parameter — the file is the database and the pilot renders the `main`
  schema surface the service exposes (deferred note below).
- Tables browser: name/type/rows columns, tables first and views appended
  with a `view` type; `views=N` count in the list title; `/` filters the
  list client-side with a live match count; `r` refreshes; a details panel
  shows the selected entry and the staged operation plan when one exists.
- Enter/i on a table or view opens the full-screen TABLE DATA VIEWER: one
  bounded LIMIT/OFFSET page at a time (`LoadTableData` works for views as
  read-only queries) with `j/k`/PgUp/PgDn row cursor, `n` next page, `b`
  back (replace semantics, end-of-table and no-previous-page states), `/`
  live page filter with a match count, and Enter on a row opening the typed
  row-detail viewer (one line per column: name, declared type from the
  structure cache, PK/NULL flags, value; NULL renders literally; JSON
  documents render as text; LIST/STRUCT cells render the service decode
  result — currently NULL — per `value_convert`).
- Structure view: typed columns list (nullable/PK/default) plus an indexes
  and foreign-keys panel for the selected table; `r` reloads.
- Mutation: `d` on a row (data viewer or row viewer) stages a typed delete
  (type the exact table name, then `y`); readonly launches stage but block
  at confirm; the current page is refetched after execution.
- Confirm-quit fires while a table/structure/data fetch or a staged plan is
  active; the error banner is non-modal (any key acknowledges while the key
  keeps its meaning).

## Deferred inside the accepted pilot

- **Cell edits and row insert/paste** (`CrudCommand::SaveChanges` pending
  updates, `CrudCommand::PasteRows`, `CrudCommand::ImportCsv`): `later
  candidate` — value-entry UX needs its own fixture, confirmation, and
  redaction evidence before it can ship.
- **DDL and table management** (`BrowserCommand` DDL, `DropTable`, copy/
  paste tables): `later candidate`; blast radius exceeds any slice 1
  mutation gate.
- **Arbitrary SQL console**: intentionally not shipped. `CrudCommand::
  ExecuteQuery`/`ExecuteMultiQuery` execute arbitrary statements (including
  writes), so a TUI console over them would violate the mutation-safety
  gate; the CLI (`duckdb query`, `voidb-cli invoke run duckdb.exec …`)
  remains the supported path for ad-hoc SQL.
- **Server view (hotkey 3)**: `later candidate` — the duckdb service layer
  exposes no bounded diagnostics/variables command (Redis's
  `ServerCommand` has no duckdb counterpart), so there is nothing bounded
  to render.
- **Schema-level browsing**: the service listing filters
  `table_schema = 'main'` and exposes no schema switch command; attached
  databases and non-`main` schemas are `later candidate` behind a service
  command, not a TUI-side information_schema query.
- **Known decode surface (service-layer facts, verified by probe; the TUI
  renders them without changing the service)**: `decode_duckdb_value` probes
  Int → Float → Bool → String → Blob, so JSON documents surface as `Text`,
  LIST/STRUCT columns surface as NULL, and TIMESTAMP/DATE columns surface
  raw duckdb-rs values (epoch micros/days) — the fixture mirrors the
  decode-producible value shapes for non-temporal cells and carries readable
  text for temporal cells. Widening the probe (temporal, LIST/STRUCT, exact
  numeric decoding) is a `service/value_convert.rs` concern deferred as
  `later candidate`; the TUI adds no backend operations.
- Until these ship, docs and release copy must keep pointing to the CLI
  (`duckdb query/tables/describe/schemas/import/export/extensions`, and
  `voidb-cli invoke run duckdb.<capability> …`) as the supported general
  path for writes, arbitrary queries, imports/exports, and diagnostics;
  `plugin.toml` declares `[ui] tui = true, raw_input = false` for this
  bounded pilot only.

## Validation commands and evidence

All run in `voidb-plugin-duckdb` with `VOIDB_DISABLE_KEYRING=1` (no OS
keyring touch):

```bash
VOIDB_DISABLE_KEYRING=1 cargo test            # 60 passed (40 pre-existing + 20 TUI)
cargo clippy --all-targets -- -D warnings     # 0 warnings (CI runs -D warnings)
cargo fmt --all && cargo fmt --all -- --check # clean (CI enforces this)
cargo run --example export_contract -- --check # manifest/contract drift green
```

TUI interaction tests (in `src/tui.rs#[cfg(test)]`): fixture evidence +
secret-leak scan, preflight redaction, non-modal startup banner, view
hotkeys and tab cycling, table selection with views appended, table paging
with offset/total state, filter match count in browser and viewer, row
viewer typed columns with NULL and JSON, typed-delete exact-table-name
gate, delete-execute + page refetch, delete refusal without a primary key
(views included), readonly block, erroring-action responsiveness, empty
table state, confirm-quit, page-key movement, and fixture-cell conversion
parity with the service decode shapes.

Preflight and evidence:

```bash
target/debug/voidb-plugin-duckdb tui --fixture fixtures/duckdb_tui_operations.json --format json
target/debug/voidb-plugin-duckdb tui --fixture fixtures/duckdb_tui_operations.json --evidence evidence.json
```

Evidence JSON: `kind: duckdb_tui_fixture_evidence`, quality gate
`retained_tui_quality_gate("duckdb", ["fixture-duckdb-ops",
"fixture-duckdb-local"], ["target error"], 80, 24, 10_000)`, coverage
entries including the new interactions (`table_data_paging`,
`viewer_filter_match_count`, `row_detail_viewer`, `typed_cell_rendering`,
`typed_delete_confirmation`, `delete_requires_primary_key`,
`mutation_refetch`, `readonly_block`, …), `secret_leak_scan.passed = true`.

PTY lifecycle record (macOS, Python 3.9 stdlib only, `struct "HHHH"`, 36 rows
x 130 cols): assertions across consecutive clean runs, exit code 0 each run;
script retained out-of-repo at `/tmp/duckdb_tui_pty_smoke.py` (run
`python3 -I /tmp/duckdb_tui_pty_smoke.py`). Covered: first frame
(non-blank, marker-checked), banner acknowledgement, tables → data viewer
navigation, paging forward/back with partial final page, viewer filter with
match count and clear, typed row detail, typed-delete confirmation and
refetch, live resize with content retention, normal `q` quit with clean exit
0 and terminal restore. Local Python is 3.9 (no tomllib), so the python
provenance tests cannot run on this machine (CI runs 3.11+); this does not
affect the PTY evidence, which uses only stdlib.

## Migration note

This pilot is new plugin-owned UI work. It does not restore, wrap, or depend
on the router-hosted DuckDB data-inspector adapters removed in Requirement
77, and it does not reintroduce shared legacy data widgets or shell-owned
plugin UI state. The default TUI guidance for DuckDB profiles remains the
CLI/capability path; `duckdb tui` is a bounded, profile-scoped triage pilot
whose only supported mutation is a single PK-anchored row delete. Release
docs must not advertise the deferred cell-edit, insert/import, DDL, schema
switch, or SQL-console workflows as available in the TUI.
