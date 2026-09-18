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
