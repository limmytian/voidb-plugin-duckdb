# VoidB DuckDB Plugin (`voidb-plugin-duckdb`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage DuckDB analytical databases.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Analytical SQL query execution, table inspection, schema description.

## Usage & Operational Modes

```bash
# Connectivity test
voidb-cli duckdb test --profile my-duckdb

# Run analytical SQL queries and schema inspection
voidb-cli duckdb tables --profile my-duckdb
voidb-cli duckdb describe analytics --profile my-duckdb
voidb-cli duckdb query "SELECT * FROM analytics LIMIT 10;" --profile my-duckdb

# Machine / Agent stdio-jsonrpc server
voidb-cli duckdb serve
```

## Optional local SDK development

Release and CI builds resolve SDK/core from the immutable Git revision in Cargo.toml. To test against a sibling `../voidb` checkout, explicitly opt in with `cargo --config .cargo/config.local.example.toml test --all-targets`. Do not commit an active `.cargo/config.toml` path override.
