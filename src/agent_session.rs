//! Persistent DuckDB agent sessions backed by the plugin's `SyncWorker`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use voidb_core::{
    AgentSessionCallRequest, AgentSessionCallResult, AgentSessionOpenContext, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, SqlDialect, SqlSessionTransactionTracker, sql_allows_read_only_query,
};

use crate::{DuckDbConfig, DuckDbService, service::StatementResult};

pub struct DuckDbAgentSessionFactory {
    config: DuckDbConfig,
}

impl DuckDbAgentSessionFactory {
    pub fn new(config: DuckDbConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for DuckDbAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "duckdb"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        if !matches!(
            context.binding.purpose,
            PluginSessionPurpose::DatabaseQuery | PluginSessionPurpose::DatabaseTransaction
        ) {
            return Err(error(
                PluginSessionErrorCode::Unsupported,
                "DuckDB persistent sessions require a database purpose.",
            ));
        }
        if context
            .binding
            .allowed_capabilities
            .iter()
            .any(|capability| {
                !matches!(
                    capability.as_str(),
                    "duckdb.query" | "query" | "duckdb.exec" | "exec"
                )
            })
        {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "DuckDB session capability scope is limited to query and exec.",
            ));
        }
        let mut service = DuckDbService::new_direct(&self.config).map_err(|_| {
            error(
                PluginSessionErrorCode::OwnerUnavailable,
                "DuckDB session connection could not be opened.",
            )
        })?;
        let transaction = SqlSessionTransactionTracker::from_open_input(
            SqlDialect::DuckDb,
            &context.request.input,
        )
        .map_err(|reason| {
            PluginSessionError::new(
                PluginSessionErrorCode::PolicyDenied,
                format!("DuckDB session transaction options are invalid: {reason}"),
            )
        })?;
        if let Some(sql) = transaction.isolation_setup_sql() {
            let results = service.execute_query(sql).await.map_err(|_| {
                error(
                    PluginSessionErrorCode::OwnerUnavailable,
                    "DuckDB session isolation could not be configured.",
                )
            })?;
            if results_have_error(&results) {
                return Err(error(
                    PluginSessionErrorCode::Unsupported,
                    "DuckDB session isolation is unsupported by the target.",
                ));
            }
        }
        Ok(Arc::new(DuckDbAgentSession {
            service: Mutex::new(Some(service)),
            transaction: Mutex::new(transaction),
        }))
    }
}

struct DuckDbAgentSession {
    service: Mutex<Option<DuckDbService>>,
    transaction: Mutex<SqlSessionTransactionTracker>,
}

#[async_trait]
impl PluginAgentSession for DuckDbAgentSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let sql = request
            .input
            .get("sql")
            .and_then(Value::as_str)
            .filter(|sql| !sql.trim().is_empty())
            .ok_or_else(|| error(PluginSessionErrorCode::PolicyDenied, "SQL is required."))?;
        match request.capability.as_str() {
            "duckdb.query" if !sql_allows_read_only_query(sql, SqlDialect::DuckDb) => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "duckdb.query accepts read-oriented SQL only; use duckdb.exec.",
                ));
            }
            "duckdb.exec" if !request.destructive_acknowledged => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "duckdb.exec requires destructive acknowledgement.",
                ));
            }
            "duckdb.query" | "duckdb.exec" => {}
            _ => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "DuckDB sessions accept duckdb.query and duckdb.exec.",
                ));
            }
        }
        let mut transaction = self.transaction.lock().await;
        let plan = transaction.plan(sql).map_err(|reason| {
            PluginSessionError::new(
                PluginSessionErrorCode::PolicyDenied,
                format!("DuckDB transaction transition is invalid: {reason}"),
            )
        })?;
        let mut guard = self.service.lock().await;
        let service = guard.as_mut().ok_or_else(|| {
            error(
                PluginSessionErrorCode::OwnerUnavailable,
                "DuckDB session is closed.",
            )
        })?;
        let results = service.execute_query(sql).await.map_err(|_| {
            error(
                PluginSessionErrorCode::OwnerUnavailable,
                "DuckDB session SQL failed.",
            )
        })?;
        if results_have_error(&results) {
            let rollback_succeeded = if transaction.failure_requires_rollback(&plan) {
                service
                    .execute_query("ROLLBACK")
                    .await
                    .is_ok_and(|results| !results_have_error(&results))
            } else {
                true
            };
            transaction.record_failure(&plan, rollback_succeeded);
        } else {
            transaction.record_success(plan);
        }
        let transaction_status = transaction.status();
        let max_rows = request
            .input
            .get("max_rows")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 1000) as usize;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({
                "statements": results_json(results, max_rows),
                "max_rows": max_rows,
                "transaction": transaction_status
            }),
            request.output_limit_bytes,
        )
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.service.lock().await.is_some() {
            PluginSessionHealth::Ready
        } else {
            PluginSessionHealth::Closed
        })
    }
    async fn cancel(&self, _call_id: &str) -> Result<(), PluginSessionError> {
        close_service(&self.transaction, &self.service).await;
        Ok(())
    }
    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        close_service(&self.transaction, &self.service).await;
        Ok(())
    }
}

async fn close_service(
    transaction: &Mutex<SqlSessionTransactionTracker>,
    service: &Mutex<Option<DuckDbService>>,
) {
    let _transaction = transaction.lock().await;
    if let Some(mut service) = service.lock().await.take() {
        let _ = service.execute_query("ROLLBACK").await;
    }
}

fn results_have_error(results: &[StatementResult]) -> bool {
    results
        .iter()
        .any(|result| matches!(result, StatementResult::Error(_)))
}

fn results_json(results: Vec<StatementResult>, max_rows: usize) -> Vec<Value> {
    results.into_iter().map(|result| match result {
        StatementResult::Select { columns, rows } => { let source_count = rows.len(); json!({ "kind": "rows", "columns": columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>(), "rows": rows.into_iter().take(max_rows).map(|row| row.values.into_iter().map(|value| voidb_core::formatters::cell_value_to_json(&value)).collect::<Vec<_>>()).collect::<Vec<_>>(), "source_count": source_count, "truncated": source_count > max_rows }) },
        StatementResult::Affected(rows) => json!({ "kind": "affected", "rows": rows }),
        StatementResult::Empty => json!({ "kind": "empty" }),
        StatementResult::Error(_) => json!({ "kind": "error", "message": "DuckDB statement failed." }),
    }).collect()
}

fn error(code: PluginSessionErrorCode, message: &str) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use voidb_core::{AgentSessionBinding, AgentSessionOpenRequest, AgentSessionRef};

    #[tokio::test]
    async fn memory_database_temp_state_survives_calls() {
        let factory = DuckDbAgentSessionFactory::new(DuckDbConfig::new(":memory:".into()));
        let session = factory.open(context()).await.expect("open");
        let created = call(
            &session,
            "duckdb.exec",
            "CREATE TEMP TABLE kept(value VARCHAR); INSERT INTO kept VALUES ('yes')",
            true,
        )
        .await;
        assert_eq!(created.output["transaction"]["state"], "idle");
        let result = call(&session, "duckdb.query", "SELECT value FROM kept", false).await;
        assert_eq!(result.output["statements"][0]["rows"][0][0], "yes");
        session.close("test".into()).await.expect("close");
        assert_eq!(session.health().await.unwrap(), PluginSessionHealth::Closed);
    }

    #[tokio::test]
    async fn close_rolls_back_open_file_transaction() {
        let path = std::env::temp_dir().join(format!(
            "voidb-duckdb-session-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let config = DuckDbConfig::new(path.to_string_lossy().into_owned());
        let factory = DuckDbAgentSessionFactory::new(config);
        let session = factory.open(context()).await.expect("open");
        let begun = call(
            &session,
            "duckdb.exec",
            "BEGIN; CREATE TABLE rolled_back(value VARCHAR)",
            true,
        )
        .await;
        assert_eq!(begun.output["transaction"]["state"], "active");
        session.close("test".into()).await.expect("close");

        let reopened = factory.open(context()).await.expect("reopen");
        let result = call(
            &reopened,
            "duckdb.query",
            "SELECT value FROM rolled_back",
            false,
        )
        .await;
        assert_eq!(result.output["statements"][0]["kind"], "error");
        reopened.close("test".into()).await.expect("close");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn cancel_rolls_back_open_file_transaction() {
        let path = std::env::temp_dir().join(format!(
            "voidb-duckdb-cancel-session-{}-{}.db",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let config = DuckDbConfig::new(path.to_string_lossy().into_owned());
        let factory = DuckDbAgentSessionFactory::new(config);
        let session = factory.open(context()).await.expect("open");
        call(
            &session,
            "duckdb.exec",
            "BEGIN; CREATE TABLE cancelled(value VARCHAR)",
            true,
        )
        .await;
        session
            .cancel("call:transaction")
            .await
            .expect("cancel transaction");
        assert_eq!(
            session.health().await.expect("cancelled health"),
            PluginSessionHealth::Closed
        );

        let reopened = factory.open(context()).await.expect("reopen");
        let result = call(
            &reopened,
            "duckdb.query",
            "SELECT value FROM cancelled",
            false,
        )
        .await;
        assert_eq!(result.output["statements"][0]["kind"], "error");
        reopened.close("test".into()).await.expect("close");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn statement_error_auto_rolls_back_and_reports_transaction_state() {
        let factory = DuckDbAgentSessionFactory::new(DuckDbConfig::new(":memory:".into()));
        let session = factory.open(context()).await.expect("open");
        let begun = call(
            &session,
            "duckdb.exec",
            "BEGIN; CREATE TABLE transient(value VARCHAR)",
            true,
        )
        .await;
        assert_eq!(begun.output["transaction"]["state"], "active");

        let failed = call(
            &session,
            "duckdb.exec",
            "INSERT INTO missing_table VALUES ('no')",
            true,
        )
        .await;

        assert_eq!(failed.output["statements"][0]["kind"], "error");
        assert_eq!(failed.output["transaction"]["state"], "idle");
        assert_eq!(
            failed.output["transaction"]["last_transition"],
            "auto_rollback_error"
        );
        let table = call(
            &session,
            "duckdb.query",
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'transient'",
            false,
        )
        .await;
        assert_eq!(table.output["statements"][0]["rows"][0][0], 0);
        session.close("test".into()).await.expect("close");
    }

    fn context() -> AgentSessionOpenContext {
        let purpose = PluginSessionPurpose::DatabaseTransaction;
        AgentSessionOpenContext {
            binding: AgentSessionBinding {
                grant_id: "grant".into(),
                profile_id: "profile".into(),
                plugin_id: "duckdb".into(),
                purpose: purpose.clone(),
                allowed_capabilities: vec!["duckdb.query".into(), "duckdb.exec".into()],
                host_generation: 1,
            },
            request: AgentSessionOpenRequest {
                purpose,
                capabilities: vec!["duckdb.query".into(), "duckdb.exec".into()],
                lease_seconds: 60,
                concurrency: Default::default(),
                destructive_acknowledged: false,
                input: Value::Null,
            },
            lease_expires_at: Utc::now() + Duration::seconds(60),
        }
    }

    async fn call(
        session: &Arc<dyn PluginAgentSession>,
        capability: &str,
        sql: &str,
        ack: bool,
    ) -> AgentSessionCallResult {
        session
            .call(AgentSessionCallRequest {
                session: AgentSessionRef::new("session", 1),
                call_id: sql.into(),
                capability: capability.into(),
                input: json!({"sql": sql}),
                destructive_acknowledged: ack,
                timeout_ms: Some(1000),
                output_limit_bytes: 64 * 1024,
            })
            .await
            .expect("call")
    }
}
