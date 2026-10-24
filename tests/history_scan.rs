// History sweep behavior tests over the real AppState + the listing-
// backed mock backend — no processes, no network (the pure noise
// filters and the bounded peek live in backend::tests).

use damon_core::backend::types::*;
use damon_core::history_scan::scan_history;

mod common;

/// Two discovered native sessions get bookmark rows (imported), the
/// second sweep reuses them (stat signature unchanged), an mtime bump
/// re-imports, a deleted session tombstones its handle so the sweep
/// skips it, and an entry removed from discovery archives its session.
#[tokio::test]
async fn sweep_imports_reuses_tombstones_and_archives() {
    use damon_core::config::Config;
    use std::sync::Arc;

    let session = |handle: &str, mtime: u64, title: Option<&str>| ImportableSession {
        handle: PersistenceHandle {
            provider: "mock".to_string(),
            native_handle: handle.to_string(),
            metadata: serde_json::Value::Null,
        },
        title: title.map(String::from),
        cwd: Some("/tmp/work".into()),
        modified_at: Some(mtime),
        size: Some(100),
    };

    let cfg: Config = {
        let mut c = common::mock_config(None);
        // Interval off — the background boot sweep would race the manual
        // one for the single-flight slot.
        c.history.scan_interval_secs = 0;
        c
    };
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(shared, store.clone()).await;
    let (client, listing) = common::mock_client_with_listing(vec![
        session("native-a", 1_000, Some("first plan")),
        session("native-b", 2_000, None),
    ]);
    // Scanner isolation: ONLY the mock — the machine's real CLIs (and
    // their real history files) must never enter a test's sweep.
    let mut clients: std::collections::HashMap<String, Arc<dyn damon_core::backend::AgentClient>> =
        std::collections::HashMap::new();
    clients.insert("mock".to_string(), client);
    state.sessions.set_clients(clients);

    // Sweep 1: both import.
    let events: std::sync::Arc<parking_lot::Mutex<Vec<serde_json::Value>>> = Default::default();
    let sink = events.clone();
    let progress = move |v: serde_json::Value| {
        sink.lock().push(v);
    };
    let report = scan_history(&state, "scan-1", &progress).await.unwrap();
    assert_eq!(report.scanned, 2, "{report:?}");
    assert_eq!(report.reparsed, 2, "{report:?}");
    assert_eq!(report.reused, 0, "{report:?}");
    assert!(events.lock().iter().any(|v| v["stage"] == "importing"));
    assert!(events.lock().iter().any(|v| v["stage"] == "done"));

    // The bookmarks exist, bound to the native handles, titled.
    let sessions = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(sessions.len(), 2, "{sessions:?}");
    assert!(sessions.iter().any(|s| s.title == "first plan"));
    assert!(sessions.iter().all(|s| s.backend == "mock"));
    // The scanner's mapping ties each damon id to its native handle.
    let mapped: Vec<(String, String)> = store
        .scanned_all()
        .await
        .unwrap()
        .into_iter()
        .map(|(b, h, _)| (b, h))
        .collect();
    assert!(mapped.contains(&("mock".to_string(), "native-a".to_string())));
    assert!(mapped.contains(&("mock".to_string(), "native-b".to_string())));

    // Sweep 2: unchanged signature → reuse only.
    let report = scan_history(&state, "scan-2", &|_| {}).await.unwrap();
    assert_eq!(report.reused, 2, "{report:?}");
    assert_eq!(report.reparsed, 0, "{report:?}");
    assert_eq!(
        store
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap()
            .len(),
        2,
        "no duplicate rows"
    );

    // mtime bump → re-import (the row refreshes; count stays 2).
    listing.lock().clear();
    listing
        .lock()
        .push(session("native-a", 9_000, Some("first plan")));
    let report = scan_history(&state, "scan-3", &|_| {}).await.unwrap();
    assert_eq!(report.reparsed, 1, "{report:?}");
    assert_eq!(report.reused, 0, "{report:?}");

    // Delete native-a's session → tombstone → the sweep (which still
    // discovers native-a) SKIPS it instead of re-importing.
    let doomed = store
        .scanned_all()
        .await
        .unwrap()
        .into_iter()
        .find(|(_, h, _)| h == "native-a")
        .map(|(_, _, sid)| sid)
        .unwrap();
    store.delete_session(&doomed).await.unwrap();
    let report = scan_history(&state, "scan-4", &|_| {}).await.unwrap();
    assert_eq!(
        report.tombstoned, 1,
        "deleted handle never re-imports: {report:?}"
    );
    assert_eq!(report.scanned, 1, "only native-a is discovered: {report:?}");
    // native-b left discovery back at scan-3 — its session archives.
    assert_eq!(report.archived, 1, "{report:?}");

    // A session re-resuming the same handle clears the tombstone — the
    // sweep imports it again (its stored signature died with the
    // delete, so the file counts as changed).
    store
        .create_session("renewed", "/tmp/work", Some("mock"))
        .await
        .unwrap();
    store
        .set_agent_session("renewed", "mock", "native-a")
        .await
        .unwrap();
    let report = scan_history(&state, "scan-5", &|_| {}).await.unwrap();
    assert_eq!(report.tombstoned, 0, "cleared tombstone: {report:?}");
    assert_eq!(report.reparsed, 1, "{report:?}");

    // Vanish (idempotent): native-b stays gone — its session keeps
    // re-archiving on every sweep while native-a stays visible.
    listing.lock().clear();
    listing
        .lock()
        .push(session("native-a", 9_000, Some("back again")));
    let report = scan_history(&state, "scan-6", &|_| {}).await.unwrap();
    assert_eq!(report.archived, 1, "{report:?}");
    assert_eq!(report.reused, 1, "{report:?}");
    let vanished_id = store
        .scanned_all()
        .await
        .unwrap()
        .into_iter()
        .find(|(_, h, _)| h == "native-b")
        .map(|(_, _, sid)| sid)
        .unwrap();
    let archived = store
        .list_sessions_paged(
            u32::MAX,
            0,
            damon_core::store::SessionFilter {
                include_archived: true,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id == vanished_id)
        .unwrap();
    assert!(
        archived.archived,
        "vanished native file archives its session"
    );
    // …and the still-bound one stays visible.
    let renewed_id = store
        .scanned_all()
        .await
        .unwrap()
        .into_iter()
        .find(|(_, h, _)| h == "native-a")
        .map(|(_, _, sid)| sid)
        .unwrap();
    let visible = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert!(visible.iter().any(|s| s.id == renewed_id), "{visible:?}");
}

/// A backend whose discovery fails is skipped — the sweep reports it
/// and the other backends still import (best-effort).
#[tokio::test]
async fn sweep_skips_a_failing_backend_without_aborting() {
    use damon_core::config::Config;
    use std::sync::Arc;

    // A broken backend = the mock's required surface with a failing
    // global discovery.
    struct BrokenClient {
        inner: Arc<dyn damon_core::backend::AgentClient>,
    }
    #[async_trait::async_trait]
    impl damon_core::backend::AgentClient for BrokenClient {
        fn provider(&self) -> &str {
            "broken"
        }
        fn capabilities(&self) -> &Capabilities {
            self.inner.capabilities()
        }
        async fn is_available(&self) -> bool {
            false
        }
        async fn fetch_catalog(
            &self,
            cwd: Option<&std::path::Path>,
        ) -> anyhow::Result<ProviderCatalog> {
            self.inner.fetch_catalog(cwd).await
        }
        async fn create_session(
            &self,
            config: SessionConfig,
        ) -> anyhow::Result<Arc<dyn damon_core::backend::AgentSession>> {
            self.inner.create_session(config).await
        }
        async fn resume_session(
            &self,
            handle: &PersistenceHandle,
            config: SessionConfig,
        ) -> anyhow::Result<Arc<dyn damon_core::backend::AgentSession>> {
            self.inner.resume_session(handle, config).await
        }
        async fn list_all_importable(&self) -> anyhow::Result<Vec<ImportableSession>> {
            anyhow::bail!("discovery exploded")
        }
    }

    let good_listing = vec![ImportableSession {
        handle: PersistenceHandle {
            provider: "mock".to_string(),
            native_handle: "n1".to_string(),
            metadata: serde_json::Value::Null,
        },
        title: Some("survives".into()),
        cwd: Some("/tmp".into()),
        modified_at: Some(1),
        size: Some(10),
    }];
    let cfg: Config = common::mock_config(None);
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(shared, store).await;
    let (broken_inner, _) = common::mock_client_with_listing(vec![]);
    let (good_client, _) = common::mock_client_with_listing(good_listing);
    let mut clients: std::collections::HashMap<String, Arc<dyn damon_core::backend::AgentClient>> =
        std::collections::HashMap::new();
    clients.insert(
        "broken".to_string(),
        Arc::new(BrokenClient {
            inner: broken_inner,
        }),
    );
    clients.insert("mock".to_string(), good_client);
    state.sessions.set_clients(clients);

    let report = scan_history(&state, "scan-x", &|_| {}).await.unwrap();
    assert_eq!(report.skipped_backends, ["broken".to_string()]);
    assert_eq!(report.reparsed, 1, "the healthy backend still imports");
}
