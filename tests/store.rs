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

    assert!(!store.session_exists("old").await.unwrap());
    assert!(store.session_exists("new").await.unwrap());
    assert!(store.messages_full("old").await.unwrap().is_empty());
    assert_eq!(store.messages_full("new").await.unwrap().len(), 1);
    // FTS rows for the removed session are gone too.
    assert!(store.search("old msg", 10).await.unwrap().is_empty());

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
    let hits = store.search("error timeout", 10).await.unwrap();
    assert_eq!(hits.len(), 2);

    // Explicit operators pass through.
    let hits = store.search("error AND timeout", 10).await.unwrap();
    assert_eq!(hits.len(), 2);
    let hits = store.search("timeout OR unrelated", 10).await.unwrap();
    assert_eq!(hits.len(), 3);
    let hits = store.search("error NOT timeout", 10).await.unwrap();
    assert_eq!(hits.len(), 0);

    // Quoted phrase matches the exact sequence only.
    let hits = store.search("\"timeout in relay\"", 10).await.unwrap();
    assert_eq!(hits.len(), 1);
    let hits = store.search("\"in timeout\"", 10).await.unwrap();
    assert_eq!(hits.len(), 0);

    // FTS5 metacharacters in terms stay literal — no parse error. Inside
    // quotes FTS5 tokenizes `*`/`:`/`,` away, so these degrade to the
    // contained terms rather than matching operators.
    assert_eq!(store.search("error*", 10).await.unwrap().len(), 2);
    assert!(store.search("content:error", 10).await.unwrap().is_empty());
    assert!(
        store
            .search("NEAR(error, timeout)", 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Operator-only or empty queries return nothing instead of erroring.
    assert!(store.search("AND OR", 10).await.unwrap().is_empty());
    assert!(store.search("NOT", 10).await.unwrap().is_empty());
    assert!(store.search("\"\"", 10).await.unwrap().is_empty());
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

    let all = store.list_sessions().await.unwrap();
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

    let all = store.messages_full("s").await.unwrap();
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
    let hits = store.search("rust AND NOT gc", 10).await.unwrap();
    assert_eq!(hits.len(), 1);
    let all = store.messages_full("s").await.unwrap();
    let hit = all.iter().find(|m| m.id == hits[0].1).unwrap();
    assert_eq!(hit.data["content"], "rust memory safe");

    // Leading NOT: FTS5 has no unary NOT, so the query degrades to the
    // bare term — a parseable MATCH, not an error.
    let hits = store.search("NOT safe", 10).await.unwrap();
    assert_eq!(hits.len(), 1);

    // Trailing NOT dies without an operand; the term still matches.
    let hits = store.search("gc NOT", 10).await.unwrap();
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
    let sessions = store.list_sessions().await.unwrap();
    assert_eq!(sessions[0].3, "first");

    // Explicit rename overwrites.
    assert!(store.rename_session("s1", "renamed").await.unwrap());
    assert!(!store.rename_session("ghost", "x").await.unwrap());
    assert_eq!(store.list_sessions().await.unwrap()[0].3, "renamed");

    // Model override set/clear round-trips through session_model.
    assert!(store.set_session_model("s1", Some("gpt-4o")).await.unwrap());
    assert_eq!(store.session_model("s1").await.unwrap().as_deref(), Some("gpt-4o"));
    assert!(store.set_session_model("s1", None).await.unwrap());
    assert_eq!(store.session_model("s1").await.unwrap(), None);
    assert!(!store.set_session_model("ghost", Some("m")).await.unwrap());

    let _ = std::fs::remove_file(&path);
}
