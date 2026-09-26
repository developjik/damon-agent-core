use damon_core::store::Store;
use serde_json::json;

fn temp_db(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "damon-store-test-{name}-{}.db",
        uuid::Uuid::new_v4()
    ))
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
        .list_sessions_paged(u32::MAX, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|(id, ..)| id)
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
            .search_filtered("old msg", 10, None, None, None)
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
        .search_filtered("error timeout", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);

    // Explicit operators pass through.
    let hits = store
        .search_filtered("error AND timeout", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    let hits = store
        .search_filtered("timeout OR unrelated", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    let hits = store
        .search_filtered("error NOT timeout", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 0);

    // Quoted phrase matches the exact sequence only.
    let hits = store
        .search_filtered("\"timeout in relay\"", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let hits = store
        .search_filtered("\"in timeout\"", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 0);

    // FTS5 metacharacters in terms stay literal — no parse error. Inside
    // quotes FTS5 tokenizes `*`/`:`/`,` away, so these degrade to the
    // contained terms rather than matching operators.
    assert_eq!(
        store
            .search_filtered("error*", 10, None, None, None)
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(
        store
            .search_filtered("content:error", 10, None, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("NEAR(error, timeout)", 10, None, None, None)
            .await
            .unwrap()
            .is_empty()
    );

    // Operator-only or empty queries return nothing instead of erroring.
    assert!(
        store
            .search_filtered("AND OR", 10, None, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("NOT", 10, None, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .search_filtered("\"\"", 10, None, None, None)
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

    let all = store.list_sessions_paged(u32::MAX, 0).await.unwrap();
    assert_eq!(all.len(), 5);

    let page = store.list_sessions_paged(2, 0).await.unwrap();
    assert_eq!(page, all[..2]);

    let page = store.list_sessions_paged(2, 2).await.unwrap();
    assert_eq!(page, all[2..4]);

    let page = store.list_sessions_paged(2, 4).await.unwrap();
    assert_eq!(page, all[4..]);

    assert!(store.list_sessions_paged(2, 10).await.unwrap().is_empty());
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
        .search_filtered("rust AND NOT gc", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    let all = store.messages_paged("s", u32::MAX, 0).await.unwrap();
    let hit = all.iter().find(|m| m.id == hits[0].1).unwrap();
    assert_eq!(hit.data["content"], "rust memory safe");

    // Leading NOT: FTS5 has no unary NOT, so the query degrades to the
    // bare term — a parseable MATCH, not an error.
    let hits = store
        .search_filtered("NOT safe", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);

    // Trailing NOT dies without an operand; the term still matches.
    let hits = store
        .search_filtered("gc NOT", 10, None, None, None)
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
    let sessions = store.list_sessions_paged(u32::MAX, 0).await.unwrap();
    assert_eq!(sessions[0].3, "first");

    // Explicit rename overwrites.
    assert!(store.rename_session("s1", "renamed").await.unwrap());
    assert!(!store.rename_session("ghost", "x").await.unwrap());
    assert_eq!(
        store.list_sessions_paged(u32::MAX, 0).await.unwrap()[0].3,
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
            .search_filtered("flaky-widget", 10, None, None, None)
            .await
            .unwrap()
            .len(),
        1,
        "tool arguments indexed"
    );
    assert_eq!(
        store
            .search_filtered("run_tests", 10, None, None, None)
            .await
            .unwrap()
            .len(),
        1,
        "tool name indexed"
    );
    let tool_hits = store
        .search_filtered("quux-42", 10, None, None, None)
        .await
        .unwrap();
    assert_eq!(tool_hits.len(), 1, "tool result text indexed");
    assert_eq!(tool_hits[0].0, "s", "hit belongs to the prompting session");
    assert_eq!(
        store
            .search_filtered("zeta-9", 10, None, None, None)
            .await
            .unwrap()
            .len(),
        1,
        "array text part indexed"
    );
    assert!(
        store
            .search_filtered("QUFB", 10, None, None, None)
            .await
            .unwrap()
            .is_empty(),
        "image data not indexed"
    );
}

#[tokio::test]
async fn search_filtered_by_session_and_time_bounds() {
    let path = temp_db("search-filter");
    let store = Store::open(&path).await.unwrap();
    store.create_session("old", "/tmp", None).await.unwrap();
    store.create_session("new", "/tmp", None).await.unwrap();
    store
        .append("old", "user", &json!({"content": "needle in old session"}))
        .await
        .unwrap();
    store
        .append("new", "user", &json!({"content": "needle in new session"}))
        .await
        .unwrap();
    backdate(&path, "old", 30);
    let created: std::collections::HashMap<String, String> = store
        .list_sessions_paged(u32::MAX, 0)
        .await
        .unwrap()
        .into_iter()
        .map(|(id, created, _, _)| (id, created))
        .collect();
    let old_at = &created["old"];
    let new_at = &created["new"];

    // No filters → all matches.
    assert_eq!(
        store
            .search_filtered("needle", 10, None, None, None)
            .await
            .unwrap()
            .len(),
        2
    );

    // sessionId narrows to one session.
    let hits = store
        .search_filtered("needle", 10, Some("new"), None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "new");

    // before= old's creation (inclusive): only the old session.
    let hits = store
        .search_filtered("needle", 10, None, Some(old_at.clone()), None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "old");

    // after= old's creation (inclusive): both sessions.
    assert_eq!(
        store
            .search_filtered("needle", 10, None, None, Some(old_at.clone()))
            .await
            .unwrap()
            .len(),
        2
    );

    // after= new's creation: only the new session. ISO `T` separator
    // normalizes to the stored space form.
    let hits = store
        .search_filtered("needle", 10, None, None, Some(new_at.replace(' ', "T")))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, "new");

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
        .search_filtered("fork-marker-1", 10, None, None, None)
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
