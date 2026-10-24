//! Native-history scanner — one pass over every backend's discoverable
//! native sessions, creating damon bookmark rows for the ones the
//! daemon has never seen (the PersistenceHandle philosophy: the
//! provider's session file is the source of truth; the DB row is a
//! bookmark the resume/import flow binds to). Never touches native
//! files beyond reading them through the backends' own discovery.
//!
//! Incremental: each discovered entry carries a stat signature (size,
//! mtime) — an unchanged signature skips the import entirely. A native
//! file that vanished gets its damon session archived (the provider's
//! truth is gone; history is kept, hidden). `session.delete` on an
//! imported session tombstones the handle so no sweep re-imports it;
//! re-binding the same handle clears the tombstone (explicit new
//! intent wins). Single-flight: one scan at a time, globally.

use std::sync::Arc;

use anyhow::Result;
use serde_json::json;

use crate::api::AppState;

/// One sweep's outcome — the `session.rescan` reply body.
#[derive(Debug, Clone, Default)]
pub struct ScanReport {
    /// Entries discovery returned.
    pub scanned: u32,
    /// Signature changed → the session row was refreshed.
    pub reparsed: u32,
    /// Signature unchanged → skipped entirely.
    pub reused: u32,
    /// Native file vanished → damon session archived.
    pub archived: u32,
    /// Tombstoned handles skipped (deleted by the user — never
    /// re-imported).
    pub tombstoned: u32,
    /// Backends whose discovery failed — best-effort, never aborts.
    pub skipped_backends: Vec<String>,
}

impl ScanReport {
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "scanned": self.scanned,
            "reparsed": self.reparsed,
            "reused": self.reused,
            "archived": self.archived,
            "tombstoned": self.tombstoned,
            "skippedBackends": self.skipped_backends,
        })
    }
}

/// Run one sweep. `progress` receives connection-scoped event payloads
/// (`backend` + running counters) — the requesting surface renders the
/// pipeline; other clients see the results via session.list.
///
/// Discovery honors per-backend env overrides (CLAUDE_CONFIG_DIR,
/// FACTORY_HOME_OVERRIDE, …) because it runs through each backend's
/// own `list_all_importable` — no per-engine logic lives here.
pub async fn scan_history(
    state: &Arc<AppState>,
    scan_id: &str,
    progress: &(dyn Fn(serde_json::Value) + Send + Sync),
) -> Result<ScanReport> {
    let mut report = ScanReport::default();
    let clients = state.sessions.clients();
    // (backend, native_handle) pairs seen THIS sweep — the vanish pass
    // reconciles the stored mappings against this set.
    let mut discovered: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();

    for (backend_id, client) in clients {
        progress(json!({
            "stage": "discovering", "backend": backend_id,
        }));
        let found = match client.list_all_importable().await {
            Ok(found) => found,
            Err(e) => {
                // Best-effort: a broken backend never aborts the scan.
                tracing::warn!(backend = %backend_id, error = %e, "history discovery failed");
                report.skipped_backends.push(backend_id.clone());
                continue;
            }
        };
        report.scanned += found.len() as u32;
        progress(json!({
            "stage": "importing", "backend": backend_id, "count": found.len(),
        }));
        for entry in found {
            let (backend, native) = (
                entry.handle.provider.clone(),
                entry.handle.native_handle.clone(),
            );
            if !discovered.insert((backend.clone(), native.clone())) {
                continue; // duplicate handle from the same backend
            }
            if state
                .store
                .is_tombstoned(&backend, &native)
                .await
                .unwrap_or(true)
            {
                // unwrap_or(true): a store error must never create a
                // row we cannot prove is wanted.
                report.tombstoned += 1;
                continue;
            }
            let mtime_ms = entry.modified_at.map(|s| s as i64);
            let size = entry.size;
            let sig_matches = match state
                .store
                .scanned_signature(&backend, &native)
                .await
                .unwrap_or(None)
            {
                Some((prev_size, prev_mtime)) => {
                    prev_size == size.map(|s| s as i64) && prev_mtime == mtime_ms
                }
                None => false,
            };
            if sig_matches {
                // Unchanged file: skip the import entirely — but a
                // session that reappeared after a vanish-archival
                // becomes visible again.
                report.reused += 1;
                continue;
            }
            // Mint the bookmark row through the same calls the
            // resume-by-handle flow uses. The record (and its
            // tombstone re-check) is one store call, so a concurrent
            // `session.delete` can never be re-upserted over.
            let session_id = uuid::Uuid::new_v4().to_string();
            let cwd = entry
                .cwd
                .as_ref()
                .map(|c| c.to_string_lossy().into_owned())
                .unwrap_or_default();
            if let Err(e) = state
                .store
                .create_session(&session_id, &cwd, Some(&backend))
                .await
            {
                tracing::warn!(error = %e, "history import: session row failed");
                continue;
            }
            if let Err(e) = state
                .store
                .set_agent_session(&session_id, &backend, &native)
                .await
            {
                tracing::warn!(error = %e, "history import: handle bind failed");
                let _ = state.store.delete_session(&session_id).await;
                continue;
            }
            if let Some(title) = &entry.title {
                let _ = state.store.set_title_if_empty(&session_id, title).await;
            }
            let recorded = state
                .store
                .record_scanned(&backend, &native, size, mtime_ms, &session_id)
                .await
                .unwrap_or(false);
            if recorded {
                report.reparsed += 1;
            } else {
                // Tombstoned between the check and the record: roll the
                // bookmark back — the user's delete wins.
                report.tombstoned += 1;
                let _ = state.store.delete_session(&session_id).await;
            }
        }
    }

    // Vanish pass: a scanned mapping whose native entry is gone this
    // sweep means the provider's truth disappeared — archive the damon
    // session (history kept, hidden). A tombstoned one drops instead.
    for (backend, native, session_id) in state.store.scanned_all().await? {
        if discovered.contains(&(backend.clone(), native.clone())) {
            continue;
        }
        if state
            .store
            .is_tombstoned(&backend, &native)
            .await
            .unwrap_or(true)
        {
            let _ = state.store.drop_scanned(&backend, &native).await;
            continue;
        }
        if let Ok(true) = state.store.set_archived(&session_id, true).await {
            report.archived += 1;
            progress(json!({
                "stage": "archived", "backend": backend, "sessionId": session_id,
            }));
        }
    }

    progress(json!({
        "stage": "done",
        "scanned": report.scanned,
        "reparsed": report.reparsed,
        "reused": report.reused,
        "archived": report.archived,
        "tombstoned": report.tombstoned,
    }));
    let _ = scan_id;
    Ok(report)
}

/// Fire-and-forget variant for background sweeps — no progress sink.
pub async fn scan_history_quiet(state: &Arc<AppState>) -> Result<ScanReport> {
    let noop = |_| {};
    scan_history(state, "background", &noop).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_json_carries_the_full_summary() {
        let report = ScanReport {
            scanned: 10,
            reparsed: 3,
            reused: 6,
            archived: 1,
            tombstoned: 2,
            skipped_backends: vec!["codex".into()],
        };
        let v = report.to_json();
        assert_eq!(v["scanned"], 10);
        assert_eq!(v["reused"], 6);
        assert_eq!(v["skippedBackends"], json!(["codex"]));
    }
}
