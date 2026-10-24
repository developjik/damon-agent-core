use damon_core::store::{PlanSubmit, SearchFilter, SessionFilter, Store};
use serde_json::json;

fn temp_db(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "damon-store-test-{name}-{}.db",
        uuid::Uuid::new_v4()
    ))
}

/// Wall-clock unix ms — bounds for message-ts assertions (the store's
/// own now_ms is private; tests re-derive it).
fn now_ms_test() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Backdate a session's created_at so cleanup_older_than can see it as old.
fn backdate(path: &std::path::Path, session_id: &str, days: u32) {
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute(
        "UPDATE sessions SET created_at = datetime('now', ?1),
                             last_active_at = datetime('now', ?1)
         WHERE id = ?2",
        rusqlite::params![format!("-{days} days"), session_id],
    )
    .unwrap();
}

/// Raw (cost_usd, cumulative_cost) rows in id order — the ground truth
/// under whatever the Store API reports.
fn usage_rows(path: &std::path::Path) -> Vec<(f64, Option<f64>)> {
    let c = rusqlite::Connection::open(path).unwrap();
    c.prepare("SELECT cost_usd, cumulative_cost FROM usage ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn usage_row_count(path: &std::path::Path) -> i64 {
    let c = rusqlite::Connection::open(path).unwrap();
    c.query_row("SELECT COUNT(*) FROM usage", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn delete_session_removes_usage_rows() {
    let path = temp_db("usage-delete");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .record_usage("s", "mock", 100, 200, 0.5)
        .await
        .unwrap();
    assert_eq!(usage_row_count(&path), 1);

    store.delete_session("s").await.unwrap();
    assert_eq!(
        usage_row_count(&path),
        0,
        "usage rows must not outlive their session"
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn retention_sweep_removes_usage_rows() {
    let path = temp_db("usage-retention");
    let store = Store::open(&path).await.unwrap();
    store.create_session("old", "/tmp", None).await.unwrap();
    store
        .append("old", "user", &json!({"content": "old"}))
        .await
        .unwrap();
    store
        .record_usage("old", "mock", 100, 200, 0.5)
        .await
        .unwrap();
    backdate(&path, "old", 30);

    let removed = store
        .cleanup_older_than(7, &Default::default())
        .await
        .unwrap();
    assert_eq!(removed, ["old"]);
    assert_eq!(
        usage_row_count(&path),
        0,
        "swept sessions must take their usage rows with them"
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn usage_costs_are_recorded_as_per_turn_deltas() {
    let path = temp_db("usage-delta");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();

    // ACP reports cumulative cost per turn: 0.50 after turn 1, 0.80
    // after turn 2. The rows must hold the per-turn deltas 0.50/0.30 so
    // SUM(cost_usd) equals the true session total (0.80), not 1.30.
    store
        .record_usage("s", "mock", 10, 100, 0.50)
        .await
        .unwrap();
    store
        .record_usage("s", "mock", 20, 100, 0.80)
        .await
        .unwrap();
    let rows = usage_rows(&path);
    assert_eq!(rows.len(), 2);
    assert!((rows[0].0 - 0.50).abs() < 1e-9);
    assert_eq!(rows[0].1, Some(0.50));
    assert!((rows[1].0 - 0.30).abs() < 1e-9, "delta row: {:?}", rows[1]);
    assert_eq!(rows[1].1, Some(0.80));

    let (_used, _size, cost, turns) = store.session_usage("s").await.unwrap();
    assert!((cost - 0.80).abs() < 1e-9, "SUM must be the real total");
    assert_eq!(turns, 2);

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn usage_cumulative_reset_clamps_to_raw_cost() {
    let path = temp_db("usage-reset");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();

    store
        .record_usage("s", "mock", 10, 100, 0.80)
        .await
        .unwrap();
    // Fresh agent session: the counter restarts below the previous
    // cumulative — the clamp keeps the raw value, never a negative
    // subtraction.
    store.record_usage("s", "mock", 5, 100, 0.10).await.unwrap();
    let rows = usage_rows(&path);
    assert!(
        (rows[1].0 - 0.10).abs() < 1e-9,
        "clamped row: {:?}",
        rows[1]
    );
    assert_eq!(rows[1].1, Some(0.10));
    let (_u, _s, cost, _t) = store.session_usage("s").await.unwrap();
    assert!((cost - 0.90).abs() < 1e-9);

    let _ = std::fs::remove_file(&path);
}

/// session_usage "turns" counts prompt turns (persisted user messages),
/// not usage rows — an agent that never emits usage_update still ran
/// the turn, and the UI's "N turns" must not read 0 after one.
#[tokio::test]
async fn usage_turns_counts_prompts_without_usage_rows() {
    let path = temp_db("usage-turns");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .append("s", "user", &json!({"role": "user", "content": "hi"}))
        .await
        .unwrap();
    store
        .append(
            "s",
            "assistant",
            &json!({"role": "assistant", "content": "echo:hi"}),
        )
        .await
        .unwrap();

    let (_u, _s, _cost, turns) = store.session_usage("s").await.unwrap();
    assert_eq!(turns, 1, "one prompt turn, zero usage rows");

    // Usage rows still win when they outnumber stored prompts (rows
    // recorded without messages — tests, tooling).
    store.record_usage("s", "mock", 10, 100, 0.5).await.unwrap();
    store.record_usage("s", "mock", 20, 100, 0.8).await.unwrap();
    let (_u, _s, _cost, turns) = store.session_usage("s").await.unwrap();
    assert_eq!(turns, 2);

    let _ = std::fs::remove_file(&path);
}

/// A pre-v2 database (cumulative costs stored raw, dead token columns
/// present) is migrated on open: columns dropped, history rewritten
/// into per-turn deltas with the cumulative kept as baseline.
#[tokio::test]
async fn usage_migration_converts_cumulative_history() {
    let path = temp_db("usage-migrate");
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "PRAGMA user_version = 1;
             CREATE TABLE usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0,
                context_used INTEGER NOT NULL DEFAULT 0,
                context_size INTEGER NOT NULL DEFAULT 0,
                cost_usd REAL NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
             );
             INSERT INTO usage (session_id, model, context_used, context_size, cost_usd) VALUES
                ('s', 'm', 10, 100, 0.50),
                ('s', 'm', 20, 100, 0.80),
                ('s', 'm', 30, 100, 0.10);",
        )
        .unwrap();
    }

    let store = Store::open(&path).await.unwrap();
    let cols: Vec<String> = {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.prepare("PRAGMA table_info(usage)")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert!(!cols.contains(&"input_tokens".to_string()));
    assert!(!cols.contains(&"output_tokens".to_string()));
    assert!(cols.contains(&"cumulative_cost".to_string()));

    let rows = usage_rows(&path);
    assert!((rows[0].0 - 0.50).abs() < 1e-9);
    assert!((rows[1].0 - 0.30).abs() < 1e-9);
    assert!((rows[2].0 - 0.10).abs() < 1e-9, "reset clamps: {rows:?}");
    assert_eq!(rows[1].1, Some(0.80));
    let (_u, _s, cost, turns) = store.session_usage("s").await.unwrap();
    assert!((cost - 0.90).abs() < 1e-9);
    assert_eq!(turns, 3);

    // Re-opening is a no-op — the conversion is not applied twice.
    drop(store);
    Store::open(&path).await.unwrap();
    let rows = usage_rows(&path);
    assert!(
        (rows[1].0 - 0.30).abs() < 1e-9,
        "re-run changed rows: {rows:?}"
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn cleanup_older_than_removes_old_sessions_and_messages() {
    let path = temp_db("cleanup");
    let store = Store::open(&path).await.unwrap();

    store.create_session("old", "/tmp", None).await.unwrap();
    store.create_session("new", "/tmp", None).await.unwrap();
    store
        .append("old", "user", &json!({"content": "old msg"}))
        .await
        .unwrap();
    store
        .append("new", "user", &json!({"content": "new msg"}))
        .await
        .unwrap();

    backdate(&path, "old", 30);

    let removed = store
        .cleanup_older_than(7, &Default::default())
        .await
        .unwrap();
    assert_eq!(removed, ["old"]);

    let ids: Vec<String> = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert!(!ids.iter().any(|id| id == "old"));
    assert!(ids.iter().any(|id| id == "new"));
    assert!(
        store
            .messages_paged("old", u32::MAX, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .messages_paged("new", u32::MAX, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    // FTS rows for the removed session are gone too.
    assert!(
        store
            .search_filtered("old msg", 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );

    // Second run is a no-op.
    assert!(
        store
            .cleanup_older_than(7, &Default::default())
            .await
            .unwrap()
            .is_empty()
    );

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn search_tokenizes_terms_phrases_and_operators() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .append("s", "user", &json!({"content": "error timeout in relay"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "error without timeout"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "unrelated message"}))
        .await
        .unwrap();

    // Bare terms AND together — both words required, order-free.
    let hits = store
        .search_filtered("error timeout", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);

    // Explicit operators pass through.
    let hits = store
        .search_filtered("error AND timeout", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    let hits = store
        .search_filtered("timeout OR unrelated", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    let hits = store
        .search_filtered("error NOT timeout", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 0);

    // Quoted phrase matches the exact sequence only.
    let hits = store
        .search_filtered("\"timeout in relay\"", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let hits = store
        .search_filtered("\"in timeout\"", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 0);

    // FTS5 metacharacters in terms stay literal — no parse error. Inside
    // quotes FTS5 tokenizes `*`/`:`/`,` away, so these degrade to the
    // contained terms rather than matching operators.
    assert_eq!(
        store
            .search_filtered("error*", 10, Default::default())
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        store
            .search_filtered("content:error", 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("NEAR(error, timeout)", 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );

    // Operator-only or empty queries return nothing instead of erroring.
    assert!(
        store
            .search_filtered("AND OR", 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("NOT", 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("\"\"", 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn list_sessions_paged_applies_limit_and_offset() {
    let store = Store::in_memory().await.unwrap();
    for i in 0..5 {
        store
            .create_session(&format!("s{i}"), "/tmp", None)
            .await
            .unwrap();
    }

    let all = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 5);

    let page = store
        .list_sessions_paged(2, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(page, all[..2]);

    let page = store
        .list_sessions_paged(2, 2, Default::default())
        .await
        .unwrap();
    assert_eq!(page, all[2..4]);

    let page = store
        .list_sessions_paged(2, 4, Default::default())
        .await
        .unwrap();
    assert_eq!(page, all[4..]);

    assert!(
        store
            .list_sessions_paged(2, 10, Default::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn messages_paged_applies_limit_and_offset() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    for i in 0..5 {
        store
            .append("s", "user", &json!({"content": format!("m{i}")}))
            .await
            .unwrap();
    }

    let all = store.messages_paged("s", u32::MAX, 0).await.unwrap();
    assert_eq!(all.len(), 5);

    let page = store.messages_paged("s", 2, 0).await.unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].id, all[0].id);
    assert_eq!(page[1].id, all[1].id);
    assert_eq!(page[0].data["content"], "m0");

    let page = store.messages_paged("s", 2, 4).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, all[4].id);

    assert!(store.messages_paged("s", 2, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn search_handles_unary_and_exclusion_not() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .append("s", "user", &json!({"content": "rust memory safe"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "rust gc pressure"}))
        .await
        .unwrap();

    // `X AND NOT Y` must keep the exclusion, not silently drop the NOT
    // and invert the result set.
    let hits = store
        .search_filtered("rust AND NOT gc", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let all = store.messages_paged("s", u32::MAX, 0).await.unwrap();
    let hit = all.iter().find(|m| m.id == hits[0].1).unwrap();
    assert_eq!(hit.data["content"], "rust memory safe");

    // Leading NOT: FTS5 has no unary NOT, so the query degrades to the
    // bare term — a parseable MATCH, not an error.
    let hits = store
        .search_filtered("NOT safe", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);

    // Trailing NOT dies without an operand; the term still matches.
    let hits = store
        .search_filtered("gc NOT", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn open_tightens_dir_and_db_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("damon-store-perms-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("sessions.db");
    let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

    let store = Store::open(&path).await.unwrap();
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&path), 0o600);

    // A pre-existing looser file is tightened again on the next open.
    drop(store);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
    Store::open(&path).await.unwrap();
    assert_eq!(mode(&path), 0o600);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn session_title_and_model_overrides() {
    let path = temp_db("title");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s1", "/tmp", None).await.unwrap();

    // First title wins; a second set_title_if_empty is a no-op.
    store.set_title_if_empty("s1", "first").await.unwrap();
    store.set_title_if_empty("s1", "second").await.unwrap();
    let sessions = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(sessions[0].title, "first");

    // Explicit rename overwrites.
    assert!(store.rename_session("s1", "renamed").await.unwrap());
    assert!(!store.rename_session("ghost", "x").await.unwrap());
    assert_eq!(
        store
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap()[0]
            .title,
        "renamed"
    );

    let _ = std::fs::remove_file(&path);
}

/// Tool I/O becomes searchable: tool-call names/arguments on assistant
/// messages, tool-result text (ACP content-block shape), and text
/// parts of array-form content. Image data stays out of the index.
#[tokio::test]
async fn search_finds_tool_text() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .append("s", "user", &json!({"content": "run the deploy step"}))
        .await
        .unwrap();
    // Assistant tool call (legacy OpenAI shape) — name + arguments.
    store
        .append(
            "s",
            "assistant",
            &json!({
                "role": "assistant",
                "content": "",
                "tool_calls": [{
                    "id": "t1",
                    "type": "function",
                    "function": {
                        "name": "run_tests",
                        "arguments": "{\"pattern\": \"flaky-widget\"}"
                    }
                }]
            }),
        )
        .await
        .unwrap();
    // Tool result — ACP tool_call_update content-block shape.
    store
        .append(
            "s",
            "tool",
            &json!({
                "role": "tool",
                "tool_call_id": "t1",
                "content": [{
                    "type": "content",
                    "content": {"type": "text", "text": "quux-42 passed with warnings"}
                }]
            }),
        )
        .await
        .unwrap();
    // Array-form user content: a text part alongside an image part.
    store
        .append(
            "s",
            "user",
            &json!({"content": [
                {"type": "text", "text": "multimodal note about zeta-9"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,QUFB"}}
            ]}),
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .search_filtered("flaky-widget", 10, Default::default())
            .await
            .unwrap()
            .len(),
        1,
        "tool arguments indexed"
    );
    assert_eq!(
        store
            .search_filtered("run_tests", 10, Default::default())
            .await
            .unwrap()
            .len(),
        1,
        "tool name indexed"
    );
    let tool_hits = store
        .search_filtered("quux-42", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(tool_hits.len(), 1, "tool result text indexed");
    assert_eq!(tool_hits[0].0, "s", "hit belongs to the prompting session");
    assert_eq!(
        store
            .search_filtered("zeta-9", 10, Default::default())
            .await
            .unwrap()
            .len(),
        1,
        "array text part indexed"
    );
    assert!(
        store
            .search_filtered("QUFB", 10, Default::default())
            .await
            .unwrap()
            .is_empty(),
        "image data not indexed"
    );
}

#[tokio::test]
async fn search_filters_session_backend_cwd_and_message_time() {
    let store = Store::in_memory().await.unwrap();
    store
        .create_session("claude1", "/work/a", Some("claude"))
        .await
        .unwrap();
    store
        .create_session("gpt1", "/work/b", Some("gpt"))
        .await
        .unwrap();
    let before = now_ms_test();
    store
        .append("claude1", "user", &json!({"content": "shared needle"}))
        .await
        .unwrap();
    store
        .append("gpt1", "user", &json!({"content": "shared needle"}))
        .await
        .unwrap();
    let after = now_ms_test();

    // No filters → all matches.
    assert_eq!(
        store
            .search_filtered("needle", 10, Default::default())
            .await
            .unwrap()
            .len(),
        2
    );

    // sessionId narrows to one session.
    let hits = store
        .search_filtered(
            "needle",
            10,
            SearchFilter {
                session_id: Some("gpt1".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "gpt1");

    // backend/cwd narrow by the owning session.
    let hits = store
        .search_filtered(
            "needle",
            10,
            SearchFilter {
                backend: Some("claude".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "claude1");
    let hits = store
        .search_filtered(
            "needle",
            10,
            SearchFilter {
                cwd: Some("/work/b".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "gpt1");

    // Message-time bounds (unix ms, inclusive): both rows are stamped
    // between `before` and `after`.
    let in_window = |since: Option<i64>, until: Option<i64>| SearchFilter {
        since_ms: since,
        until_ms: until,
        ..Default::default()
    };
    assert_eq!(
        store
            .search_filtered("needle", 10, in_window(Some(before), None))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .search_filtered("needle", 10, in_window(None, Some(after)))
            .await
            .unwrap()
            .len(),
        2
    );
    // A since in the future / an until in the past excludes both.
    assert!(
        store
            .search_filtered("needle", 10, in_window(Some(after + 60_000), None))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("needle", 10, in_window(None, Some(before - 60_000)))
            .await
            .unwrap()
            .is_empty()
    );
    // Filters AND together.
    assert!(
        store
            .search_filtered(
                "needle",
                10,
                SearchFilter {
                    backend: Some("claude".into()),
                    since_ms: Some(after + 60_000),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_empty()
    );
}

/// Rows written before the ts column existed (ts = 0) compare as their
/// session's created_at inside time-bounded search — the old schema's
/// only honest bound — instead of dropping out of every bounded query.
#[tokio::test]
async fn search_time_bounds_clamp_ts0_rows_to_session_creation() {
    let path = temp_db("search-ts0-clamp");
    let store = Store::open(&path).await.unwrap();
    store.create_session("old", "/tmp", None).await.unwrap();
    store
        .append("old", "user", &json!({"content": "legacy needle"}))
        .await
        .unwrap();
    // Make the row pre-ts: unknown stamp, session created 30 days back.
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute("UPDATE messages SET ts = 0", []).unwrap();
    }
    backdate(&path, "old", 30);

    let day = 24 * 3600 * 1000i64;
    let now = now_ms_test();
    // Effective time = created_at (~30 days ago), not "now": a since
    // of yesterday still excludes it, a since of 40 days back finds it.
    assert!(
        store
            .search_filtered(
                "legacy",
                10,
                SearchFilter {
                    since_ms: Some(now - day),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_empty(),
        "clamped row must compare as its session's creation"
    );
    assert_eq!(
        store
            .search_filtered(
                "legacy",
                10,
                SearchFilter {
                    since_ms: Some(now - 40 * day),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .len(),
        1
    );
    // Until bounds see the same clamp: yesterday excludes, 40 days
    // back includes.
    assert!(
        store
            .search_filtered(
                "legacy",
                10,
                SearchFilter {
                    until_ms: Some(now - 31 * day),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .search_filtered(
                "legacy",
                10,
                SearchFilter {
                    until_ms: Some(now - 29 * day),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .len(),
        1
    );

    let _ = std::fs::remove_file(&path);
}

/// VACUUM INTO backup: opening the result read-only shows the same
/// session/message/usage counts, and an existing target is refused.
#[tokio::test]
async fn backup_snapshots_sessions_and_messages() {
    let path = temp_db("backup");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s1", "/a", None).await.unwrap();
    store.create_session("s2", "/b", None).await.unwrap();
    store
        .append("s1", "user", &json!({"content": "hello backup"}))
        .await
        .unwrap();
    store.record_usage("s1", "m", 10, 100, 0.5).await.unwrap();

    let backup_path = path.with_extension("backup.db");
    store.backup(&backup_path).await.unwrap();

    let c = rusqlite::Connection::open_with_flags(
        &backup_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let count = |sql: &str| -> i64 { c.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(count("SELECT COUNT(*) FROM sessions"), 2);
    assert_eq!(count("SELECT COUNT(*) FROM messages"), 1);
    assert_eq!(count("SELECT COUNT(*) FROM usage"), 1);

    // A second backup onto the same path must refuse, not clobber.
    assert!(store.backup(&backup_path).await.is_err());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&backup_path);
}

/// fork_session with uptoMessageId copies only up to and including that
/// ORIGINAL message id; a missing session errors; and copied messages
/// are re-indexed under the fork's session id so search() finds them.
#[tokio::test]
async fn fork_upto_boundary_and_fts_reindex() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    let mut ids = Vec::new();
    for i in 0..4 {
        ids.push(
            store
                .append("s", "user", &json!({"content": format!("fork-marker-{i}")}))
                .await
                .unwrap(),
        );
    }

    // upto = second message id → exactly the first two messages copy.
    let fork = store.fork_session("s", Some(ids[1])).await.unwrap();
    let msgs = store.messages_paged(&fork, u32::MAX, 0).await.unwrap();
    assert_eq!(msgs.len(), 2, "upto must bound the copy: {msgs:?}");
    assert_eq!(msgs[0].data["content"], "fork-marker-0");
    assert_eq!(msgs[1].data["content"], "fork-marker-1");
    let fork2 = store.fork_session(&fork, Some(msgs[1].id)).await.unwrap();
    assert_eq!(
        store
            .messages_paged(&fork2, u32::MAX, 0)
            .await
            .unwrap()
            .len(),
        2
    );

    // Unknown session → error, no row created.
    assert!(store.fork_session("ghost", None).await.is_err());

    // Copied messages are re-indexed for FTS under the fork's session
    // id — a fork's history is searchable as its own.
    let hits = store
        .search_filtered("fork-marker-1", 10, Default::default())
        .await
        .unwrap();
    assert!(
        hits.iter().any(|(sid, _, _)| sid == &fork),
        "fork's copied messages must be searchable: {hits:?}"
    );
    // And the hit's message id belongs to the fork's copy, not the
    // original row.
    let fork_hit = hits.iter().find(|(sid, _, _)| sid == &fork).unwrap();
    assert!(
        msgs.iter().any(|m| m.id == fork_hit.1),
        "FTS hit must point at the fork's own message id: {fork_hit:?}"
    );
}

/// append() stamps each row with the persistence time; messages_paged
/// surfaces it. Fresh rows never read 0.
#[tokio::test]
async fn append_stamps_message_timestamps() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    let before = now_ms_test();
    let id1 = store
        .append("s", "user", &json!({"content": "one"}))
        .await
        .unwrap();
    let id2 = store
        .append("s", "assistant", &json!({"content": "two"}))
        .await
        .unwrap();
    let after = now_ms_test();

    let msgs = store.messages_paged("s", u32::MAX, 0).await.unwrap();
    let by_id = |id: i64| msgs.iter().find(|m| m.id == id).unwrap();
    for id in [id1, id2] {
        let ts = by_id(id).ts;
        assert!(ts > 0, "fresh rows must be stamped: {ts}");
        assert!(
            (before - 2_000..=after + 2_000).contains(&ts),
            "ts {ts} outside the append window [{before},{after}]"
        );
    }
    assert!(
        by_id(id2).ts >= by_id(id1).ts,
        "stamps must not go backwards"
    );
}

/// A pre-P2 database (no messages.ts, no sessions.tags, user_version 2)
/// is upgraded on open: columns appear, legacy rows read ts=0 and tags
/// [], and both keep working for new writes.
#[tokio::test]
async fn migration_adds_ts_and_tags_to_existing_db() {
    let path = temp_db("migrate-ts-tags");
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "PRAGMA user_version = 2;
             CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                last_active_at TEXT,
                cwd TEXT NOT NULL DEFAULT '',
                model TEXT,
                agent TEXT,
                agent_session TEXT,
                compacted_through INTEGER NOT NULL DEFAULT 0,
                summary TEXT,
                title TEXT
             );
             CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(id),
                role TEXT NOT NULL,
                data TEXT NOT NULL
             );
             CREATE VIRTUAL TABLE messages_fts USING fts5(
                content, session_id UNINDEXED, message_id UNINDEXED);
             CREATE TABLE usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                context_used INTEGER NOT NULL DEFAULT 0,
                context_size INTEGER NOT NULL DEFAULT 0,
                cost_usd REAL NOT NULL DEFAULT 0,
                cumulative_cost REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
             );
             INSERT INTO sessions (id, cwd, agent, title)
                VALUES ('old', '/legacy', 'claude', 'legacy title');
             INSERT INTO messages (session_id, role, data)
                VALUES ('old', 'user', '{\"content\":\"legacy needle\"}');
             INSERT INTO messages_fts (content, session_id, message_id)
                SELECT json_extract(data, '$.content'), 'old', id FROM messages;",
        )
        .unwrap();
    }

    let store = Store::open(&path).await.unwrap();
    let has_col = |table: &str, col: &str| {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.prepare(&format!("PRAGMA table_info({table})"))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .contains(&col.to_string())
    };
    assert!(has_col("messages", "ts"), "ts column must be added");
    assert!(has_col("sessions", "tags"), "tags column must be added");

    // Legacy rows: ts unknown, tags empty — not an error, not NULL.
    let legacy = store.messages_paged("old", u32::MAX, 0).await.unwrap();
    assert_eq!(legacy[0].ts, 0, "pre-column rows carry ts = 0");
    let overview = store.session_overview("old").await.unwrap().unwrap();
    assert_eq!(overview.tags, Vec::<String>::new());
    assert_eq!(overview.title, "legacy title");
    assert_eq!(overview.cwd, "/legacy");
    assert_eq!(overview.backend, "claude");

    // The legacy FTS row still matches after migration.
    assert_eq!(
        store
            .search_filtered("legacy", 10, Default::default())
            .await
            .unwrap()
            .len(),
        1
    );

    // New writes on the upgraded schema behave like fresh DBs.
    assert!(store.set_tags("old", &["kept".into()]).await.unwrap());
    let stamped = store
        .append("old", "user", &json!({"content": "fresh row"}))
        .await
        .unwrap();
    let msgs = store.messages_paged("old", u32::MAX, 0).await.unwrap();
    let fresh = msgs.iter().find(|m| m.id == stamped).unwrap();
    assert!(fresh.ts > 0);
    assert_eq!(
        store.session_overview("old").await.unwrap().unwrap().tags,
        ["kept"]
    );

    // Unknown session → None, not an error.
    assert!(store.session_overview("ghost").await.unwrap().is_none());

    let _ = std::fs::remove_file(&path);
}

/// list_sessions_paged narrows by backend, cwd, and tag membership —
/// set fields AND together and compose with limit/offset.
#[tokio::test]
async fn list_sessions_filters_backend_cwd_and_tag() {
    let store = Store::in_memory().await.unwrap();
    store
        .create_session("claude1", "/work/a", Some("claude"))
        .await
        .unwrap();
    store
        .create_session("claude2", "/work/b", Some("claude"))
        .await
        .unwrap();
    store
        .create_session("gpt1", "/work/a", Some("gpt"))
        .await
        .unwrap();
    store
        .set_tags("claude1", &["prod".into(), "urgent".into()])
        .await
        .unwrap();
    store.set_tags("gpt1", &["prod".into()]).await.unwrap();

    let ids = |filter: SessionFilter| {
        let store = &store;
        async move {
            store
                .list_sessions_paged(u32::MAX, 0, filter)
                .await
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect::<Vec<_>>()
        }
    };
    let all = ids(Default::default()).await;
    assert_eq!(all.len(), 3);
    assert_eq!(all, ["claude1", "claude2", "gpt1"], "ordered created_at,id");

    assert_eq!(
        ids(SessionFilter {
            backend: Some("claude".into()),
            ..Default::default()
        })
        .await,
        ["claude1", "claude2"]
    );
    assert_eq!(
        ids(SessionFilter {
            cwd: Some("/work/a".into()),
            ..Default::default()
        })
        .await,
        ["claude1", "gpt1"]
    );
    assert_eq!(
        ids(SessionFilter {
            tag: Some("prod".into()),
            ..Default::default()
        })
        .await,
        ["claude1", "gpt1"]
    );
    assert_eq!(
        ids(SessionFilter {
            tag: Some("urgent".into()),
            ..Default::default()
        })
        .await,
        ["claude1"]
    );
    assert!(
        ids(SessionFilter {
            tag: Some("nope".into()),
            ..Default::default()
        })
        .await
        .is_empty()
    );
    // Filters AND together, and compose with paging.
    assert_eq!(
        ids(SessionFilter {
            backend: Some("claude".into()),
            tag: Some("prod".into()),
            ..Default::default()
        })
        .await,
        ["claude1"]
    );
    let page = store
        .list_sessions_paged(
            1,
            1,
            SessionFilter {
                backend: Some("claude".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, "claude2");
}

/// set_tags replaces the whole list (rename semantics): [] clears,
/// unknown sessions report false, and rows/list reflect the change.
#[tokio::test]
async fn set_tags_replaces_whole_list() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();

    assert!(
        store
            .set_tags("s", &["a".into(), "b".into()])
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap()[0]
            .tags,
        ["a", "b"]
    );
    // Replace-all: the previous tags are gone, not merged.
    assert!(store.set_tags("s", &["c".into()]).await.unwrap());
    assert_eq!(
        store.session_overview("s").await.unwrap().unwrap().tags,
        ["c"]
    );
    // Clearing.
    assert!(store.set_tags("s", &[]).await.unwrap());
    assert_eq!(
        store.session_overview("s").await.unwrap().unwrap().tags,
        Vec::<String>::new()
    );
    // Unknown session: no row touched, honest false.
    assert!(!store.set_tags("ghost", &["x".into()]).await.unwrap());
}

/// Daily rollup: usage rows group by UTC day with summed cost, turn
/// counts, and the day's LAST context snapshot; the window bounds
/// which days appear.
#[tokio::test]
async fn daily_usage_groups_by_day_with_bounds_and_latest_context() {
    let path = temp_db("daily-usage");
    let store = Store::open(&path).await.unwrap();
    store.create_session("a", "/tmp", None).await.unwrap();
    store.create_session("b", "/tmp", None).await.unwrap();
    // Today, session a: two turns — deltas 0.10 then 0.30.
    store.record_usage("a", "m", 10, 100, 0.10).await.unwrap();
    store.record_usage("a", "m", 20, 100, 0.40).await.unwrap();
    // Session b's single row moves to two days ago.
    store.record_usage("b", "m", 50, 100, 0.10).await.unwrap();
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute(
            "UPDATE usage SET created_at = datetime('now', '-2 days')
             WHERE session_id = 'b'",
            [],
        )
        .unwrap();
    }
    // And one row 40 days back — outside every window but a 60-day one.
    store.record_usage("a", "m", 99, 100, 5.0).await.unwrap();
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute(
            "UPDATE usage SET created_at = datetime('now', '-40 days')
             WHERE context_used = 99",
            [],
        )
        .unwrap();
    }
    // The DB's own clock decides the expected day labels.
    let day = |modifier: &str| {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.query_row("SELECT date('now', ?1)", [modifier], |r| {
            r.get::<_, String>(0)
        })
        .unwrap()
    };

    let daily = store.daily_usage(7).await.unwrap();
    assert_eq!(
        daily.len(),
        2,
        "40-day-old row outside the window: {daily:?}"
    );
    assert_eq!(daily[0].0, day("-2 days"), "ordered oldest day first");
    assert_eq!(daily[0].1, 1, "turns");
    assert!((daily[0].2 - 0.10).abs() < 1e-9, "cost");
    assert_eq!(daily[0].3, 50, "that day's only row is its latest context");
    assert_eq!(daily[1].0, day("+0 days"));
    assert_eq!(daily[1].1, 2, "both of today's turns");
    assert!((daily[1].2 - 0.40).abs() < 1e-9, "summed deltas");
    assert_eq!(daily[1].3, 20, "latest snapshot, not the first");

    // days=1 → today only; days=60 → the old row joins.
    let today = store.daily_usage(1).await.unwrap();
    assert_eq!(today.len(), 1);
    assert_eq!(today[0].0, day("+0 days"));
    let wide = store.daily_usage(60).await.unwrap();
    assert_eq!(wide.len(), 3);
    assert_eq!(wide[0].0, day("-40 days"));
    assert!((wide[0].2 - 4.6).abs() < 1e-9, "delta vs the 0.40 baseline");

    let _ = std::fs::remove_file(&path);
}

/// fork_session copies the usage history retargeted to the new id and
/// the suffixed title (plus tags): the fork's rollups match, and its
/// NEXT delta subtracts the copied cumulative baseline — costs never
/// double-count across a fork.
#[tokio::test]
async fn fork_copies_usage_history_tags_and_title() {
    let store = Store::in_memory().await.unwrap();
    store
        .create_session("s", "/tmp", Some("claude"))
        .await
        .unwrap();
    store.set_title_if_empty("s", "original").await.unwrap();
    store.set_tags("s", &["keep".into()]).await.unwrap();
    store
        .append("s", "user", &json!({"content": "hello"}))
        .await
        .unwrap();
    store.record_usage("s", "m", 10, 100, 0.30).await.unwrap();
    store.record_usage("s", "m", 20, 100, 0.80).await.unwrap();

    let fork = store.fork_session("s", None).await.unwrap();
    let overview = store.session_overview(&fork).await.unwrap().unwrap();
    assert_eq!(overview.title, "original (fork)");
    assert_eq!(overview.tags, ["keep"]);
    let (used, _size, cost, turns) = store.session_usage(&fork).await.unwrap();
    assert_eq!(used, 20, "latest context snapshot copied");
    assert!((cost - 0.80).abs() < 1e-9, "per-turn deltas copied: {cost}");
    assert_eq!(turns, 2);

    // The fork's next report deltas against the COPIED baseline.
    store.record_usage(&fork, "m", 30, 100, 1.00).await.unwrap();
    let (_u, _s, cost, _t) = store.session_usage(&fork).await.unwrap();
    assert!(
        (cost - 1.00).abs() < 1e-9,
        "baseline must not double-count: {cost}"
    );

    // An untitled source forks untitled — no " (fork)" on nothing.
    store.create_session("t", "/tmp", None).await.unwrap();
    let fork_t = store.fork_session("t", None).await.unwrap();
    assert_eq!(
        store
            .session_overview(&fork_t)
            .await
            .unwrap()
            .unwrap()
            .title,
        ""
    );
}

/// The fork carries the persistence handle: agent + agent_session are
/// copied so session.resume on the fork reattaches the native session.
/// Pre-fix, the fork's agent_session stayed NULL and resume hit the
/// backend with an empty native handle.
#[tokio::test]
async fn fork_copies_persistence_handle() {
    let path = temp_db("fork-handle");
    let store = Store::open(&path).await.unwrap();
    store
        .create_session("src", "/w", Some("mock"))
        .await
        .unwrap();
    store
        .set_agent_session("src", "mock", "native-42")
        .await
        .unwrap();
    store
        .append("src", "user", &json!({"content": "hello"}))
        .await
        .unwrap();

    let fork = store.fork_session("src", None).await.unwrap();
    assert_ne!(fork, "src");
    assert_eq!(
        store.agent_session(&fork).await.unwrap(),
        Some(("mock".to_string(), "native-42".to_string())),
        "fork must be resumable"
    );
}

/// v3 FTS migration: legacy rows whose `content` was a JSON ARRAY
/// (text parts, tool blocks) were skipped by the v1 backfill — the
/// extractor handles them, the backfill SQL did not. Reopening the
/// store re-indexes every message missing from messages_fts.
#[tokio::test]
async fn v3_migration_reindexes_array_content_rows() {
    let path = temp_db("fts-v3");

    // Hand-build a pre-v3 database: schema without user_version 3, one
    // array-content message that never made it into the index.
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "CREATE TABLE sessions (
                 id TEXT PRIMARY KEY,
                 created_at TEXT NOT NULL DEFAULT (datetime('now')),
                 last_active_at TEXT,
                 cwd TEXT NOT NULL DEFAULT '',
                 model TEXT,
                 agent TEXT,
                 agent_session TEXT,
                 compacted_through INTEGER NOT NULL DEFAULT 0,
                 summary TEXT,
                 title TEXT,
                 tags TEXT NOT NULL DEFAULT '[]'
             );
             CREATE TABLE messages (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 role TEXT NOT NULL,
                 data TEXT NOT NULL,
                 ts INTEGER NOT NULL DEFAULT 0
             );
             CREATE VIRTUAL TABLE messages_fts USING fts5(
                 content, session_id UNINDEXED, message_id UNINDEXED);
             PRAGMA user_version = 2;
             INSERT INTO sessions (id, cwd) VALUES ('s', '/w');
             INSERT INTO messages (session_id, role, data)
             VALUES ('s', 'assistant',
                     '{\"content\":[{\"type\":\"text\",\"text\":\"zebrarray needle\"}]}');
             INSERT INTO messages (session_id, role, data)
             VALUES ('s', 'user', '{\"content\":\"plain indexed by v1 shape\"}');
             INSERT INTO messages_fts (content, session_id, message_id)
             VALUES ('plain indexed by v1 shape', 's', 2);",
        )
        .unwrap();
    }

    // Open runs the v3 migration over the legacy rows.
    let store = Store::open(&path).await.unwrap();
    let hits = store
        .search_filtered("zebrarray", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "array-content row must be searchable: {hits:?}"
    );
    assert_eq!(hits[0].0, "s");

    // Already-indexed rows are untouched (idempotent re-run safe).
    let hits = store
        .search_filtered("plain", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");

    let v: i64 = {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(
        v, 10,
        "migrations stamp user_version through the current one"
    );
}

/// Trailing `term*` is an FTS5 prefix query: `zebr*` matches both
/// `zebra` and `zebrules`. A bare `*` is nothing searchable — empty
/// result, not an error.
#[tokio::test]
async fn search_prefix_terms_match_prefixed_tokens() {
    let path = temp_db("fts-prefix");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s", "/w", None).await.unwrap();
    store
        .append("s", "user", &json!({"content": "the zebra runs"}))
        .await
        .unwrap();
    store
        .append(
            "s",
            "assistant",
            &json!({"content": "zebrules of the road"}),
        )
        .await
        .unwrap();

    let hits = store
        .search_filtered("zebr*", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 2, "prefix must match both tokens: {hits:?}");

    // Exact term still exact — no wildcard semantics without the star.
    let hits = store
        .search_filtered("zebra", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");

    // Bare star: no searchable token remains.
    let hits = store
        .search_filtered("*", 10, SearchFilter::default())
        .await
        .unwrap();
    assert!(hits.is_empty(), "{hits:?}");
}

/// Pinned and archived sessions survive the retention sweep; only the
/// unflagged old row is deleted.
#[tokio::test]
async fn pinned_and_archived_survive_retention() {
    let path = temp_db("retention-flags");
    let store = Store::open(&path).await.unwrap();
    store.create_session("plain", "/w", None).await.unwrap();
    store.create_session("pinned", "/w", None).await.unwrap();
    store.create_session("archived", "/w", None).await.unwrap();
    store.set_pinned("pinned", true).await.unwrap();
    store.set_archived("archived", true).await.unwrap();
    for id in ["plain", "pinned", "archived"] {
        backdate(&path, id, 90);
    }

    let removed = store
        .cleanup_older_than(30, &Default::default())
        .await
        .unwrap();
    assert_eq!(removed, vec!["plain".to_string()]);

    // Both protected rows still list (archived only on request).
    let visible: Vec<String> = store
        .list_sessions_paged(50, 0, SessionFilter::default())
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(visible, vec!["pinned".to_string()]);
    let all: Vec<String> = store
        .list_sessions_paged(
            50,
            0,
            SessionFilter {
                include_archived: true,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(all, vec!["pinned".to_string(), "archived".to_string()]);

    // Unpinning re-exposes the row to the sweep.
    store.set_pinned("pinned", false).await.unwrap();
    let removed = store
        .cleanup_older_than(30, &Default::default())
        .await
        .unwrap();
    assert_eq!(removed, vec!["pinned".to_string()]);
}

/// The trigram tokenizer makes CJK substring search work: a query
/// token matches inside a longer Korean/Chinese run. Under unicode61
/// the whole run indexed as ONE token, so `damon search "커밋"` could
/// never find "커밋 메시지" — the defect this fixes.
#[tokio::test]
async fn search_matches_cjk_substrings_via_trigram_tokenizer() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .append("s", "user", &json!({"content": "커밋 메시지를 남겼다"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "提交了新的代码"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "fix: handle timeout"}))
        .await
        .unwrap();

    // Sub-3-char queries take the LIKE fallback (see the next test).
    let hits = store
        .search_filtered("커밋", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "Korean substring must match: {hits:?}");
    let hits = store
        .search_filtered("提交", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "Chinese substring must match: {hits:?}");

    // A 3+ char CJK query inside a longer unspaced run must match on
    // the FTS5 path itself — the case unicode61 failed hardest: the
    // whole run indexed as one token, so nothing shorter matched.
    let hits = store
        .search_filtered("提交了新", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(
        hits.len(),
        1,
        "trigram windows must substring-match: {hits:?}"
    );
    let hits = store
        .search_filtered("메시지를", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
}

/// Terms shorter than the trigram minimum (3 chars) fall back to
/// exact-substring LIKE conjuncts with AND semantics; quoted phrases
/// of 3+ chars stay on the FTS5 path and AND with them.
#[tokio::test]
async fn search_short_tokens_fall_back_to_like_with_and_semantics() {
    let store = Store::in_memory().await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .append("s", "user", &json!({"content": "ab inside cd marker"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "ab alone"}))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "cd alone"}))
        .await
        .unwrap();

    // Both short tokens must match — AND, not OR: only the row with
    // both survives.
    let hits = store
        .search_filtered("ab cd", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].0, "s");

    // A quoted phrase (3+ chars) stays on the FTS5 path and ANDs with
    // the short token.
    let hits = store
        .search_filtered("\"cd marker\" ab", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
    // …and still works on its own (trigram phrase = substring match).
    let hits = store
        .search_filtered("\"cd marker\"", 10, Default::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
    let hits = store
        .search_filtered("\"marker cd\"", 10, Default::default())
        .await
        .unwrap();
    assert!(
        hits.is_empty(),
        "non-substring phrase matches nothing: {hits:?}"
    );
}

/// The v4 migration re-tokenizes messages_fts to trigram and rebuilds
/// the index, making pre-existing CJK rows searchable, then stamps
/// user_version 4.
#[tokio::test]
async fn v4_migration_rebuilds_fts_with_trigram_tokenizer() {
    let path = temp_db("fts-v4");

    // Hand-build a pre-v4 database: the unicode61 table with CJK rows
    // already indexed the old way (mirrors the v3 test's legacy schema).
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "CREATE TABLE sessions (
                 id TEXT PRIMARY KEY,
                 created_at TEXT NOT NULL DEFAULT (datetime('now')),
                 last_active_at TEXT,
                 cwd TEXT NOT NULL DEFAULT '',
                 model TEXT,
                 agent TEXT,
                 agent_session TEXT,
                 compacted_through INTEGER NOT NULL DEFAULT 0,
                 summary TEXT,
                 title TEXT,
                 tags TEXT NOT NULL DEFAULT '[]'
             );
             CREATE TABLE messages (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 role TEXT NOT NULL,
                 data TEXT NOT NULL,
                 ts INTEGER NOT NULL DEFAULT 0
             );
             CREATE VIRTUAL TABLE messages_fts USING fts5(
                 content, session_id UNINDEXED, message_id UNINDEXED);
             PRAGMA user_version = 3;
             INSERT INTO sessions (id, cwd) VALUES ('s', '/w');
             INSERT INTO messages (session_id, role, data)
             VALUES ('s', 'user', '{\"content\":\"커밋 메시지를 남겼다\"}');
             INSERT INTO messages (session_id, role, data)
             VALUES ('s', 'user', '{\"content\":\"提交了新的代码\"}');
             INSERT INTO messages_fts (content, session_id, message_id)
             VALUES ('커밋 메시지를 남겼다', 's', 1);
             INSERT INTO messages_fts (content, session_id, message_id)
             VALUES ('提交了新的代码', 's', 2);",
        )
        .unwrap();
        // Sanity: the unicode61 index cannot substring-match CJK —
        // each spaced run is a single token, so "메시지" matches
        // nothing ("메시지를" is a different token).
        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH '메시지'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 0, "pre-migration defect must be reproducible");
    }

    // Open runs the v4 migration over the legacy rows.
    let store = Store::open(&path).await.unwrap();
    let hits = store
        .search_filtered("메시지", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "rebuilt trigram index must match: {hits:?}");
    // Short-token LIKE fallback finds them too.
    let hits = store
        .search_filtered("커밋", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");
    let hits = store
        .search_filtered("提交", 10, SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "{hits:?}");

    let v: i64 = {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(
        v, 10,
        "migrations stamp user_version through the current one"
    );
    let _ = std::fs::remove_file(&path);
}

/// usage_aggregate groups per (day, backend, model) — two backends ×
/// two models give four groups — and sums a window total across them.
#[tokio::test]
async fn usage_aggregate_groups_by_day_backend_and_model() {
    let path = temp_db("usage-aggregate");
    let store = Store::open(&path).await.unwrap();
    store
        .create_session("a", "/tmp", Some("claude"))
        .await
        .unwrap();
    store
        .create_session("b", "/tmp", Some("codex"))
        .await
        .unwrap();
    // record_usage stores per-session cumulative-to-delta costs, so
    // each session's cumulative costs rise monotonically and the
    // groups hold the per-turn deltas.
    store
        .record_usage("a", "opus", 10, 100, 0.10)
        .await
        .unwrap();
    store
        .record_usage("a", "sonnet", 20, 100, 0.30)
        .await
        .unwrap();
    store
        .record_usage("b", "opus", 30, 100, 0.50)
        .await
        .unwrap();
    store
        .record_usage("b", "sonnet", 40, 100, 0.90)
        .await
        .unwrap();
    let today: String = {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.query_row("SELECT date('now')", [], |r| r.get(0)).unwrap()
    };

    let agg = store.usage_aggregate(7, 0).await.unwrap();
    assert_eq!(agg.rows.len(), 4, "2 backends × 2 models: {:?}", agg.rows);
    // Ordered by date, then backend, then model.
    assert_eq!(agg.rows[0].date, today);
    assert_eq!(agg.rows[0].backend, "claude");
    assert_eq!(agg.rows[0].model, "opus");
    assert_eq!(agg.rows[0].turns, 1);
    assert_eq!(agg.rows[0].context_used, 10);
    assert!((agg.rows[0].cost_usd - 0.10).abs() < 1e-9);
    assert_eq!(agg.rows[1].model, "sonnet");
    assert!((agg.rows[1].cost_usd - 0.20).abs() < 1e-9);
    assert_eq!(agg.rows[2].backend, "codex");
    assert!((agg.rows[2].cost_usd - 0.50).abs() < 1e-9);
    assert_eq!(agg.rows[3].backend, "codex");
    assert_eq!(agg.rows[3].model, "sonnet");
    assert!((agg.rows[3].cost_usd - 0.40).abs() < 1e-9);

    assert_eq!(agg.turns, 4, "window total turns");
    assert_eq!(agg.context_used, 100, "window total context");
    assert!((agg.cost_usd - 1.20).abs() < 1e-9, "window total cost");

    let _ = std::fs::remove_file(&path);
}

/// tz_offset_minutes moves UTC stamps across the local-day boundary:
/// a 01:00 UTC row reads as today in UTC but yesterday at UTC-3,
/// while a 12:00 UTC row stays on today — the buckets follow the
/// caller's calendar, and the window total is unaffected.
#[tokio::test]
async fn usage_aggregate_tz_offset_shifts_day_buckets() {
    let path = temp_db("usage-tz");
    let store = Store::open(&path).await.unwrap();
    store
        .create_session("s", "/tmp", Some("claude"))
        .await
        .unwrap();
    store.record_usage("s", "m", 10, 100, 0.10).await.unwrap();
    // Cumulative 0.30 → stored delta 0.20 (record_usage semantics).
    store.record_usage("s", "m", 20, 100, 0.30).await.unwrap();
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute(
            "UPDATE usage SET created_at = strftime('%Y-%m-%d', 'now') || ' 01:00:00'
             WHERE context_used = 10",
            [],
        )
        .unwrap();
        c.execute(
            "UPDATE usage SET created_at = strftime('%Y-%m-%d', 'now') || ' 12:00:00'
             WHERE context_used = 20",
            [],
        )
        .unwrap();
    }

    // UTC buckets: both rows are today — one group.
    let utc = store.usage_aggregate(7, 0).await.unwrap();
    assert_eq!(utc.rows.len(), 1, "{:?}", utc.rows);
    assert_eq!(utc.rows[0].turns, 2);

    // UTC-3: 01:00 UTC is 22:00 yesterday, 12:00 UTC stays today —
    // the group splits by local day.
    let shifted = store.usage_aggregate(7, -180).await.unwrap();
    assert_eq!(
        shifted.rows.len(),
        2,
        "one group per local day: {:?}",
        shifted.rows
    );
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        let yesterday: String = c
            .query_row("SELECT date('now', '-180 minutes', '-1 day')", [], |r| {
                r.get(0)
            })
            .unwrap();
        let today: String = c
            .query_row("SELECT date('now', '-180 minutes')", [], |r| r.get(0))
            .unwrap();
        // Ordered by date: yesterday's 01:00-UTC row first.
        assert_eq!(shifted.rows[0].date, yesterday);
        assert_eq!(shifted.rows[0].turns, 1);
        assert!((shifted.rows[0].cost_usd - 0.10).abs() < 1e-9);
        assert_eq!(shifted.rows[1].date, today);
        assert_eq!(shifted.rows[1].turns, 1);
        assert!((shifted.rows[1].cost_usd - 0.20).abs() < 1e-9);
    }
    // Bucketing never changes the window total.
    assert_eq!(shifted.turns, 2);
    assert!((shifted.cost_usd - 0.30).abs() < 1e-9);

    let _ = std::fs::remove_file(&path);
}

/// usage_clear deletes every usage row and nothing else.
#[tokio::test]
async fn usage_clear_empties_usage_rows_only() {
    let path = temp_db("usage-clear");
    let store = Store::open(&path).await.unwrap();
    store
        .create_session("s", "/tmp", Some("claude"))
        .await
        .unwrap();
    store
        .append("s", "user", &json!({"content": "kept"}))
        .await
        .unwrap();
    store.record_usage("s", "m", 10, 100, 0.5).await.unwrap();
    assert_eq!(store.usage_aggregate(7, 0).await.unwrap().turns, 1);

    store.usage_clear().await.unwrap();
    let agg = store.usage_aggregate(7, 0).await.unwrap();
    assert!(agg.rows.is_empty(), "rows must be gone: {:?}", agg.rows);
    assert_eq!(agg.turns, 0);

    // Sessions and messages are untouched.
    assert_eq!(store.messages_paged("s", 10, 0).await.unwrap().len(), 1);
    let sessions: i64 = {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(sessions, 1);

    let _ = std::fs::remove_file(&path);
}

/// Granted roots persist across reopen (the v5 table, global and
/// project-scoped rows), duplicates are idempotent, and revoke removes
/// exactly one (scope, root) pair — the other scope's grant survives.
#[tokio::test]
async fn grant_roots_persist_across_reopen_and_revoke_by_scope() {
    let path = temp_db("grants");
    let root_a = std::env::temp_dir().join(format!("damon-grant-a-{}", uuid::Uuid::new_v4()));
    let root_b = std::env::temp_dir().join(format!("damon-grant-b-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root_a).unwrap();
    std::fs::create_dir_all(&root_b).unwrap();

    {
        let store = Store::open(&path).await.unwrap();
        store
            .create_project("p1", "P1", "/tmp/p1", "{}")
            .await
            .unwrap();
        store
            .grant_root(None, root_a.to_string_lossy().into_owned())
            .await
            .unwrap();
        // Duplicate grant: idempotent, still one row.
        store
            .grant_root(None, root_a.to_string_lossy().into_owned())
            .await
            .unwrap();
        store
            .grant_root(Some("p1".into()), root_b.to_string_lossy().into_owned())
            .await
            .unwrap();
        // Effective set for the project: its grant + the global one.
        let eff = store.granted_roots(Some("p1".into())).await.unwrap();
        assert_eq!(eff.len(), 2, "{eff:?}");
        let globals = store.granted_roots(None).await.unwrap();
        assert_eq!(globals.len(), 1, "{globals:?}");
        assert_eq!(globals[0].root, root_a.to_string_lossy());
        assert_eq!(globals[0].project_id, None);
    }

    // Reopen — grants survive, no re-migration.
    let store = Store::open(&path).await.unwrap();
    let eff = store.granted_roots(Some("p1".into())).await.unwrap();
    assert_eq!(eff.len(), 2, "grants survive reopen: {eff:?}");
    assert!(eff.iter().any(|g| g.project_id.is_none()));
    assert!(eff.iter().any(|g| g.project_id.as_deref() == Some("p1")));

    // Revoking the global leaves the project grant, and vice versa.
    assert!(
        store
            .revoke_root(None, root_a.to_string_lossy().into_owned())
            .await
            .unwrap()
    );
    assert!(
        !store
            .revoke_root(None, root_a.to_string_lossy().into_owned())
            .await
            .unwrap()
    );
    assert_eq!(
        store.granted_roots(Some("p1".into())).await.unwrap().len(),
        1
    );
    assert!(
        store
            .revoke_root(Some("p1".into()), root_b.to_string_lossy().into_owned())
            .await
            .unwrap()
    );
    assert!(
        store
            .granted_roots(Some("p1".into()))
            .await
            .unwrap()
            .is_empty()
    );

    let _ = std::fs::remove_dir_all(&root_a);
    let _ = std::fs::remove_dir_all(&root_b);
    let _ = std::fs::remove_file(&path);
}

/// A re-asked plan chains onto the same planId as revision+1 while the
/// previous review is still open, supersedes it, and a decided review
/// starts a fresh chain.
#[tokio::test]
async fn plan_revisions_chain_while_open_and_restart_when_decided() {
    let store = Store::in_memory().await.unwrap();
    let first = store
        .record_plan("s", "req-1", "ExitPlanMode", "draft one", true)
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(first.status, "awaiting_review");

    // Re-ask while open → same planId, revision 2, rev 1 superseded.
    let second = store
        .record_plan("s", "req-2", "ExitPlanMode", "draft two", true)
        .await
        .unwrap();
    assert_eq!(second.plan_id, first.plan_id);
    assert_eq!(second.revision, 2);
    let rev1 = store.plan_row(&first.plan_id, 1).await.unwrap().unwrap();
    assert_eq!(rev1.status, "superseded");
    assert_eq!(rev1.superseded_by, Some(2));
    // Content hash distinguishes the revisions' text.
    assert_ne!(rev1.content_hash, second.content_hash);

    // Decide revision 2 (terminal) → the next ask starts a NEW chain.
    store
        .submit_plan_decision(&second.plan_id, 2, "approve", None)
        .await
        .unwrap();
    store
        .settle_plan(&second.plan_id, 2, "approve")
        .await
        .unwrap();
    let third = store
        .record_plan("s", "req-3", "ExitPlanMode", "another plan", true)
        .await
        .unwrap();
    assert_ne!(third.plan_id, first.plan_id);
    assert_eq!(third.revision, 1);
}

/// The CAS gate: two submitters with the same expectedRevision —
/// exactly one claims, the other conflicts with the current row; a
/// decided review conflicts too.
#[tokio::test]
async fn plan_decision_claims_once_then_conflicts() {
    let store = Store::in_memory().await.unwrap();
    let row = store
        .record_plan("s", "req-1", "ExitPlanMode", "the plan", true)
        .await
        .unwrap();

    let win = store
        .submit_plan_decision(&row.plan_id, 1, "approve", None)
        .await
        .unwrap();
    let PlanSubmit::Claimed { previous, .. } = win else {
        panic!("first submit must claim: {win:?}");
    };
    assert_eq!(previous, "awaiting_review");

    // Double-click / second surface: conflict with the live row.
    let loser = store
        .submit_plan_decision(&row.plan_id, 1, "approve", None)
        .await
        .unwrap();
    let PlanSubmit::Conflict(current) = loser else {
        panic!("second submit must conflict: {loser:?}");
    };
    assert_eq!(current.status, "submitting");

    // Ack settles the winner; anything further conflicts.
    store.settle_plan(&row.plan_id, 1, "approve").await.unwrap();
    let late = store
        .submit_plan_decision(&row.plan_id, 1, "request_changes", Some("late"))
        .await
        .unwrap();
    assert!(matches!(late, PlanSubmit::Conflict(_)));

    // An unknown revision is a typed NotFound, not a conflict.
    assert!(matches!(
        store
            .submit_plan_decision(&row.plan_id, 99, "approve", None)
            .await
            .unwrap(),
        PlanSubmit::NotFound
    ));
}

/// Approval of an incomplete plan is refused, request_changes demands
/// non-empty feedback, and defer is allowed even on an incomplete plan
/// (nothing natively approved).
#[tokio::test]
async fn plan_submit_refuses_incomplete_approvals_and_empty_feedback() {
    let store = Store::in_memory().await.unwrap();
    let row = store
        .record_plan("s", "req-1", "ExitPlanMode", "", false)
        .await
        .unwrap();
    assert!(!row.complete);

    assert!(matches!(
        store
            .submit_plan_decision(&row.plan_id, 1, "approve", None)
            .await
            .unwrap(),
        PlanSubmit::Incomplete
    ));
    assert!(matches!(
        store
            .submit_plan_decision(&row.plan_id, 1, "request_changes", Some("  "))
            .await
            .unwrap(),
        PlanSubmit::FeedbackRequired
    ));
    assert!(matches!(
        store
            .submit_plan_decision(&row.plan_id, 1, "request_changes", None)
            .await
            .unwrap(),
        PlanSubmit::FeedbackRequired
    ));

    // Defer on an incomplete plan is fine — nothing was approved.
    assert!(matches!(
        store
            .submit_plan_decision(&row.plan_id, 1, "defer", None)
            .await
            .unwrap(),
        PlanSubmit::Claimed { .. }
    ));
}

/// A failed native relay keeps the decision intent on file and reopens
/// the row — an approval never reads as delivered when the write was
/// lost. The intent (decision + feedback) survives the revert.
#[tokio::test]
async fn plan_relay_failure_keeps_intent_and_reopens_row() {
    let store = Store::in_memory().await.unwrap();
    let row = store
        .record_plan("s", "req-1", "ExitPlanMode", "the plan", true)
        .await
        .unwrap();

    let claimed = store
        .submit_plan_decision(
            &row.plan_id,
            1,
            "request_changes",
            Some("add a rollback step"),
        )
        .await
        .unwrap();
    let PlanSubmit::Claimed { previous, .. } = claimed else {
        panic!("must claim: {claimed:?}");
    };
    assert_eq!(previous, "awaiting_review");

    // The intent is durably recorded in the submitting state.
    let mid = store.plan_row(&row.plan_id, 1).await.unwrap().unwrap();
    assert_eq!(mid.status, "submitting");
    assert!(mid.decision_intent_at.is_some());
    assert_eq!(
        mid.decision.as_ref().map(|d| d["decision"].as_str()),
        Some(Some("request_changes"))
    );
    assert_eq!(
        mid.decision.as_ref().map(|d| d["feedback"].as_str()),
        Some(Some("add a rollback step"))
    );
    assert_eq!(mid.applied_at, None);

    // The write was lost: revert reopens the row, the intent stays.
    store.revert_plan(&row.plan_id, 1, &previous).await.unwrap();
    let reverted = store.plan_row(&row.plan_id, 1).await.unwrap().unwrap();
    assert_eq!(reverted.status, "awaiting_review");
    assert!(reverted.decision_intent_at.is_some());
    assert_eq!(reverted.applied_at, None);
    assert!(reverted.decision.is_some());

    // A deferred claim that never got a native write can still be
    // answered later — deferred is non-terminal.
    store
        .submit_plan_decision(&row.plan_id, 1, "defer", None)
        .await
        .unwrap();
    store.settle_plan(&row.plan_id, 1, "defer").await.unwrap();
    let deferred = store.plan_row(&row.plan_id, 1).await.unwrap().unwrap();
    assert_eq!(deferred.status, "deferred");
    let later = store
        .submit_plan_decision(&row.plan_id, 1, "approve", None)
        .await
        .unwrap();
    assert!(matches!(later, PlanSubmit::Claimed { .. }));
}

/// Turn end expires the session's still-open reviews (ghost plans must
/// not stay answerable), and deleting the session removes every row.
#[tokio::test]
async fn plan_rows_expire_on_turn_end_and_delete_with_the_session() {
    let path = temp_db("plan-lifecycle");
    let store = Store::open(&path).await.unwrap();
    store.create_session("s", "/tmp", None).await.unwrap();
    store
        .record_plan("s", "req-1", "ExitPlanMode", "p1", true)
        .await
        .unwrap();
    store
        .record_plan("s", "req-2", "ExitPlanMode", "p2", true)
        .await
        .unwrap();
    // rev2 open (awaiting_review), rev1 superseded.

    assert_eq!(store.expire_open_plans("s").await.unwrap(), 1);
    let rows = store.plan_reviews("s").await.unwrap();
    assert_eq!(rows[0].status, "superseded", "already-closed stays");
    assert_eq!(rows[1].status, "expired");

    store.delete_session("s").await.unwrap();
    assert!(store.plan_reviews("s").await.unwrap().is_empty());
    let _ = std::fs::remove_file(&path);
}

/// The pairing key is single-use with rotation in one locked step: the
/// same key mints until spent, a wrong key spends nothing, the spend
/// rotates a DIFFERENT fresh key, and neither the spent key nor any
/// replay works again.
#[tokio::test]
async fn pairing_key_spends_once_and_rotates() {
    let store = Store::in_memory().await.unwrap();

    let key = store.pairing_key_ensure().await.unwrap();
    assert_eq!(
        store.pairing_key_ensure().await.unwrap(),
        key,
        "stable until spent"
    );

    // Wrong key: nothing spent, the real key survives.
    assert_eq!(store.consume_pairing_key("WRONG").await.unwrap(), None);
    assert_eq!(store.pairing_key_ensure().await.unwrap(), key);

    // Correct key: spent, fresh one rotated in, old key dead.
    let fresh = store
        .consume_pairing_key(&format!("  {key}  "))
        .await
        .unwrap()
        .expect("correct key spends");
    assert_ne!(fresh, key);
    assert_eq!(
        store.pairing_key_ensure().await.unwrap(),
        fresh,
        "the rotated key is active"
    );
    assert_eq!(
        store.consume_pairing_key(&key).await.unwrap(),
        None,
        "replay dead"
    );

    // The fresh key spends too — every key is exactly one pairing.
    assert!(store.consume_pairing_key(&fresh).await.unwrap().is_some());
}

/// Device rows: pending mints nothing, approval unlocks the token
/// hash, revoke kills it immediately, rename validates, and the
/// last-seen write is throttled to once per minute.
#[tokio::test]
async fn device_rows_gate_on_approval_and_revoke_immediately() {
    let store = Store::in_memory().await.unwrap();
    let hash = damon_core::cli_lifecycle::hash_device_token("device-secret");
    let id = store.create_device("pixel", &hash).await.unwrap();

    let rows = store.devices().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "pending");
    assert_eq!(rows[0].name, "pixel");
    assert_eq!(rows[0].approved_at, None);

    // Pending: the token mints nothing.
    assert!(!store.device_token_ok(&hash).await.unwrap());

    // Approve: the hash unlocks.
    assert!(store.approve_device(&id).await.unwrap());
    assert!(store.device_token_ok(&hash).await.unwrap());
    assert!(
        !store.approve_device(&id).await.unwrap(),
        "approve is pending-only"
    );

    // A DIFFERENT token never matches (hash discipline).
    assert!(
        !store
            .device_token_ok(&damon_core::cli_lifecycle::hash_device_token("other"))
            .await
            .unwrap()
    );

    // Rename: blank refused, real name sticks.
    assert!(!store.rename_device(&id, "   ").await.unwrap());
    assert!(store.rename_device(&id, "pixel pro").await.unwrap());
    assert_eq!(store.devices().await.unwrap()[0].name, "pixel pro");

    // Throttled last-seen: the first touch writes, a second touch
    // within the minute is a no-op.
    store.touch_device(&hash).await.unwrap();
    let seen = store.devices().await.unwrap()[0].last_seen.clone();
    assert!(seen.is_some());
    store.touch_device(&hash).await.unwrap();
    assert_eq!(store.devices().await.unwrap()[0].last_seen, seen);

    // Revoke: the row (and its hash) is gone — dead immediately.
    assert!(store.revoke_device(&id).await.unwrap());
    assert!(!store.device_token_ok(&hash).await.unwrap());
    assert!(store.devices().await.unwrap().is_empty());
    assert!(!store.revoke_device(&id).await.unwrap());

    // Unnamed devices get a readable fallback label.
    let id = store.create_device("  ", &hash).await.unwrap();
    assert_eq!(store.devices().await.unwrap()[0].name, "unnamed device");
    assert!(store.revoke_device(&id).await.unwrap());
}
