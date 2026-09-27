//! Worktree workspace integration tests: the full PR flow (fetch
//! refs/pull/N/head → worktree add → project registration), branch
//! flow, resume, typed failure kinds, removal semantics, the
//! allowed_dirs gate, and PR resolution — against real git with a
//! local bare origin, no network.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use damon_core::api::{self, AppState};
use damon_core::config::SharedConfig;
use damon_core::store::Store;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn test_config() -> SharedConfig {
    Arc::new(parking_lot::RwLock::new(common::mock_config(None)))
}

async fn serve(config: SharedConfig) -> (Arc<AppState>, Store, String) {
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(config, store.clone()).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let app = api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (state, store, addr.to_string())
}

async fn read_json(ws: &mut Ws) -> Value {
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(60), ws.next())
            .await
            .expect("timed out waiting for a daemon frame")
            .unwrap()
            .unwrap();
        match frame {
            tungstenite::Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            _ => continue,
        }
    }
}

async fn rpc_send(ws: &mut Ws, msg: Value) {
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

async fn ws_connect(url: &str) -> (Ws, Value) {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = read_json(&mut ws).await;
    assert!(
        hello["hello"].is_object(),
        "first frame must be hello: {hello}"
    );
    (ws, hello)
}

/// Read until the reply with `id`, collecting every worktree.progress
/// frame seen on the way (stage names, in arrival order).
async fn read_reply_and_progress(ws: &mut Ws, id: u64) -> (Value, Vec<String>) {
    let mut stages = Vec::new();
    loop {
        let v = read_json(ws).await;
        if v.get("id").and_then(|i| i.as_u64()) == Some(id) {
            return (v, stages);
        }
        if v["event"] == "worktree.progress" {
            stages.push(v["data"]["stage"].as_str().unwrap_or("?").to_string());
        }
    }
}

async fn call(ws: &mut Ws, id: u64, method: &str, params: Value) -> (Value, Vec<String>) {
    rpc_send(ws, json!({"id": id, "method": method, "params": params})).await;
    read_reply_and_progress(ws, id).await
}

// ---------------------------------------------------------------------------
// git fixture

/// Hermetic git: no user/system config bleeds in; identity comes per
/// command via `-c`.
fn git(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("git spawn");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn commit_all(dir: &Path, msg: &str) {
    let ok = git(dir, &["add", "-A"]).0;
    assert!(ok, "git add failed");
    let (ok, text) = git(
        dir,
        &[
            "-c",
            "user.name=tester",
            "-c",
            "user.email=tester@example.com",
            "commit",
            "-m",
            msg,
        ],
    );
    assert!(ok, "commit failed: {text}");
}

/// A clone whose `origin` is a local bare repo carrying main + a
/// simulated PR (refs/pull/7/head at a second commit).
fn pr_fixture(tag: &str) -> PathBuf {
    let root =
        std::env::temp_dir().join(format!("damon-wt-{tag}-{}", uuid::Uuid::new_v4().simple()));
    let origin = root.join("origin.git");
    let seed = root.join("seed");
    let repo = root.join("repo");
    std::fs::create_dir_all(&root).unwrap();

    let (ok, text) = git(&root, &["init", "-q", "--bare", &origin.to_string_lossy()]);
    assert!(ok, "bare init failed: {text}");
    let (ok, text) = git(
        &root,
        &["init", "-q", "-b", "main", &seed.to_string_lossy()],
    );
    assert!(ok, "seed init failed: {text}");
    std::fs::write(seed.join("README.md"), "base\n").unwrap();
    commit_all(&seed, "base");
    let (ok, text) = git(
        &seed,
        &["push", "-q", &origin.to_string_lossy(), "main:main"],
    );
    assert!(ok, "seed push failed: {text}");

    let (ok, text) = git(
        &root,
        &[
            "clone",
            "-q",
            &origin.to_string_lossy(),
            &repo.to_string_lossy(),
        ],
    );
    assert!(ok, "clone failed: {text}");

    // The "PR": a commit not on main, published under refs/pull/7/head.
    let (ok, text) = git(&repo, &["checkout", "-q", "-b", "prwork"]);
    assert!(ok, "branch failed: {text}");
    std::fs::write(repo.join("fix.txt"), "pr change\n").unwrap();
    commit_all(&repo, "pr fix");
    let (ok, text) = git(
        &repo,
        &[
            "push",
            "-q",
            &origin.to_string_lossy(),
            "HEAD:refs/pull/7/head",
        ],
    );
    assert!(ok, "pr push failed: {text}");
    git(&repo, &["checkout", "-q", "main"]);
    git(&repo, &["branch", "-D", "prwork"]);
    repo
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_pr_worktree_registers_project_and_resumes() {
    let repo = pr_fixture("pr");
    let (_state, _store, addr) = serve(test_config()).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    let (r, stages) = call(
        &mut ws,
        1,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "prNumber": 7}),
    )
    .await;
    let result = r["result"].clone();
    assert!(r.get("error").is_none(), "create failed: {r}");
    let path = PathBuf::from(result["path"].as_str().unwrap());
    assert_eq!(result["branch"], "pr-7");
    assert_eq!(result["meta"]["prNumber"], 7);
    assert_eq!(result["resumed"], false);
    assert!(path.exists(), "worktree checked out");
    assert!(path.join("fix.txt").exists(), "PR head content present");
    assert_eq!(
        stages,
        vec!["validate", "fetch", "add", "register", "done"],
        "progress frames for every stage"
    );
    let project_id = result["projectId"].as_str().unwrap().to_string();
    let parent_id = result["parentProjectId"].as_str().unwrap().to_string();

    // Registration: a worktree row nested under the repo's project.
    let (list, _) = call(&mut ws, 2, "project.list", json!({})).await;
    let projects = list["result"]["projects"].as_array().unwrap().clone();
    let wt = projects
        .iter()
        .find(|p| p["projectId"] == project_id.as_str())
        .expect("worktree project row");
    assert_eq!(wt["kind"], "worktree");
    assert_eq!(wt["parentId"], parent_id.as_str());
    let canon_repo = std::fs::canonicalize(&repo).unwrap();
    assert!(
        projects.iter().any(|p| p["projectId"] == parent_id.as_str()
            && p["kind"] == "project"
            && p["root"]
                .as_str()
                .and_then(|r| std::fs::canonicalize(r).ok())
                .is_some_and(|root| root == canon_repo)),
        "parent project auto-created for the repo root"
    );

    // Git's own view joined with the registration.
    let (wl, _) = call(
        &mut ws,
        3,
        "worktree.list",
        json!({"repo": repo.to_string_lossy()}),
    )
    .await;
    let entry = wl["result"]["worktrees"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["branch"] == "pr-7")
        .expect("pr-7 in porcelain list");
    assert_eq!(entry["projectId"], project_id.as_str());

    // Retrying the same create resumes instead of bouncing off
    // branch_exists.
    let (again, _) = call(
        &mut ws,
        4,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "prNumber": 7}),
    )
    .await;
    assert!(again.get("error").is_none(), "resume failed: {again}");
    assert_eq!(again["result"]["resumed"], true);
    assert_eq!(again["result"]["projectId"], project_id.as_str());

    std::fs::remove_dir_all(repo.parent().unwrap()).ok();
}

#[tokio::test]
async fn branch_flow_creates_and_branch_exists_refuses() {
    let repo = pr_fixture("branch");
    let (_state, _store, addr) = serve(test_config()).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    let (r, stages) = call(
        &mut ws,
        1,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "branch": "experiment"}),
    )
    .await;
    assert!(r.get("error").is_none(), "branch create failed: {r}");
    assert_eq!(stages, vec!["validate", "fetch", "add", "register", "done"]);
    let path = PathBuf::from(r["result"]["path"].as_str().unwrap());
    assert!(path.exists());
    let project_id = r["result"]["projectId"].as_str().unwrap().to_string();

    // Remove the worktree but keep the branch, then recreate without
    // existingBranch — the refusal is the typed branch_exists.
    let (rm, _) = call(
        &mut ws,
        2,
        "worktree.remove",
        json!({"projectId": project_id, "deleteBranch": false}),
    )
    .await;
    assert!(rm.get("error").is_none(), "remove failed: {rm}");
    assert!(!path.exists());

    let (dup, _) = call(
        &mut ws,
        3,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "branch": "experiment"}),
    )
    .await;
    let err = dup.get("error").unwrap();
    assert_eq!(err["code"], -32007, "WORKTREE_FAILED: {dup}");
    assert_eq!(err["data"]["kind"], "branch_exists");

    std::fs::remove_dir_all(repo.parent().unwrap()).ok();
}

#[tokio::test]
async fn remove_deletes_branch_and_refuses_bound_sessions() {
    let repo = pr_fixture("rm");
    let (_state, store, addr) = serve(test_config()).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    let (r, _) = call(
        &mut ws,
        1,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "branch": "todelete"}),
    )
    .await;
    let project_id = r["result"]["projectId"].as_str().unwrap().to_string();
    let path = PathBuf::from(r["result"]["path"].as_str().unwrap());

    // A session bound to the worktree project blocks removal.
    let (s, _) = call(
        &mut ws,
        2,
        "session.create",
        json!({"projectId": project_id}),
    )
    .await;
    assert!(s.get("error").is_none(), "session in worktree: {s}");
    let session_id = s["result"]["sessionId"].as_str().unwrap().to_string();
    // session.create does not echo cwd — read it back from the list.
    let (sl, _) = call(&mut ws, 21, "session.list", json!({})).await;
    let listed = sl["result"]["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["sessionId"] == session_id.as_str())
        .expect("session listed");
    let cwd = listed["cwd"].as_str().unwrap_or("");
    assert!(
        Path::new(cwd).starts_with(&path),
        "session cwd is the worktree: {cwd}"
    );

    let (blocked, _) = call(
        &mut ws,
        3,
        "worktree.remove",
        json!({"projectId": project_id}),
    )
    .await;
    assert_eq!(blocked["error"]["code"], -32602, "refusal: {blocked}");
    assert!(path.exists(), "nothing removed on refusal");
    assert!(store.get_project(&project_id).await.unwrap().is_some());

    // Free the session, then remove with branch deletion.
    let _ = call(
        &mut ws,
        4,
        "session.delete",
        json!({"sessionId": session_id}),
    )
    .await;
    let (rm, _) = call(
        &mut ws,
        5,
        "worktree.remove",
        json!({"projectId": project_id, "deleteBranch": true}),
    )
    .await;
    assert!(rm.get("error").is_none(), "remove failed: {rm}");
    assert_eq!(rm["result"]["branchDeleted"], true);
    assert!(!path.exists());
    assert!(store.get_project(&project_id).await.unwrap().is_none());
    let (exists, _) = git(&repo, &["rev-parse", "--verify", "refs/heads/todelete"]);
    assert!(!exists, "branch deleted");

    std::fs::remove_dir_all(repo.parent().unwrap()).ok();
}

#[tokio::test]
async fn dir_exists_and_cancel_unknown() {
    let repo = pr_fixture("dirs");
    let root = repo.parent().unwrap().to_path_buf();
    let (_state, _store, addr) = serve(test_config()).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    // Pre-create the default worktree dir with foreign content.
    let target = root.join("repo-worktrees").join("pr-7");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("keep.txt"), "user data").unwrap();
    let (r, _) = call(
        &mut ws,
        1,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "prNumber": 7}),
    )
    .await;
    let err = r.get("error").unwrap();
    assert_eq!(err["code"], -32007);
    assert_eq!(err["data"]["kind"], "dir_exists");
    assert!(target.join("keep.txt").exists(), "user directory untouched");

    // Cancelling an unknown creation is a soft false, not an error.
    let (c, _) = call(&mut ws, 2, "worktree.cancel", json!({"creationId": "nope"})).await;
    assert!(c.get("error").is_none());
    assert_eq!(c["result"]["cancelled"], false);

    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn allowed_dirs_gate_and_catalog() {
    let repo = pr_fixture("gate");
    let mut cfg = common::mock_config(None);
    cfg.allowed_dirs = vec![repo.clone()];
    let (_state, _store, addr) = serve(Arc::new(parking_lot::RwLock::new(cfg))).await;
    let (mut ws, _hello) = ws_connect(&format!("ws://{addr}/ws")).await;

    // The default sibling layout lands outside the jail that holds
    // only the repo itself.
    let (r, _) = call(
        &mut ws,
        1,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "prNumber": 7}),
    )
    .await;
    let err = r.get("error").unwrap();
    assert_eq!(err["code"], -32007);
    assert_eq!(err["data"]["kind"], "not_allowed");

    // An explicit path inside the jail passes.
    let inside = repo.join(".damon-test-wt");
    let (r2, _) = call(
        &mut ws,
        2,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "prNumber": 7, "path": inside.to_string_lossy()}),
    )
    .await;
    assert!(r2.get("error").is_none(), "in-jail create failed: {r2}");

    // The self-describing schema carries the new surface (the connect
    // push is minimal; the hello method returns the catalog).
    let (h, _) = call(&mut ws, 3, "hello", json!({})).await;
    let methods: Vec<&str> = h["result"]["methods"]
        .as_array()
        .map(|a| a.iter().filter_map(|m| m["name"].as_str()).collect())
        .unwrap_or_default();
    for expected in [
        "worktree.create",
        "worktree.cancel",
        "worktree.remove",
        "worktree.list",
        "worktree.resolve_pr",
        "worktree.merged",
    ] {
        assert!(
            methods.contains(&expected),
            "{expected} missing from schema"
        );
    }

    std::fs::remove_dir_all(repo.parent().unwrap()).ok();
}

#[tokio::test]
async fn resolve_pr_shapes_and_not_github() {
    let repo = pr_fixture("resolve");
    let (_state, _store, addr) = serve(test_config()).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    // A file-path origin is not a GitHub remote — typed refusal.
    let (r, _) = call(
        &mut ws,
        1,
        "worktree.resolve_pr",
        json!({"repo": repo.to_string_lossy(), "input": "#7"}),
    )
    .await;
    let err = r.get("error").unwrap();
    assert_eq!(err["code"], -32007);
    assert_eq!(err["data"]["kind"], "not_github");

    // A github-shaped origin resolves: number and slug are confirmed
    // without gh; details degrade rather than fail.
    git(
        &repo,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/acme/widget.git",
        ],
    );
    for input in ["7", "#7", "https://github.com/acme/widget/pull/7/files"] {
        let (p, _) = call(
            &mut ws,
            2,
            "worktree.resolve_pr",
            json!({"repo": repo.to_string_lossy(), "input": input}),
        )
        .await;
        assert!(p.get("error").is_none(), "{input} failed: {p}");
        assert_eq!(p["result"]["number"], 7);
        assert_eq!(p["result"]["repo"], "acme/widget");
        assert_eq!(p["result"]["suggestedBranch"], "pr-7");
    }

    // Bogus input is rejected up front.
    let (bad, _) = call(
        &mut ws,
        3,
        "worktree.resolve_pr",
        json!({"repo": repo.to_string_lossy(), "input": "not a pr"}),
    )
    .await;
    assert_eq!(bad["error"]["data"]["kind"], "invalid_pr_input");

    std::fs::remove_dir_all(repo.parent().unwrap()).ok();
}

#[tokio::test]
async fn merged_reports_ancestry() {
    let repo = pr_fixture("merged");
    let (_state, _store, addr) = serve(test_config()).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    // main is merged into itself; a PR-only ref is not on main yet.
    let (yes, _) = call(
        &mut ws,
        1,
        "worktree.merged",
        json!({"repo": repo.to_string_lossy(), "branch": "main", "base": "main"}),
    )
    .await;
    assert_eq!(yes["result"]["merged"], true);

    let (pr, _) = call(
        &mut ws,
        2,
        "worktree.create",
        json!({"repo": repo.to_string_lossy(), "prNumber": 7}),
    )
    .await;
    assert!(pr.get("error").is_none());
    let (no, _) = call(
        &mut ws,
        3,
        "worktree.merged",
        json!({"repo": repo.to_string_lossy(), "branch": "pr-7", "base": "main"}),
    )
    .await;
    assert_eq!(
        no["result"]["merged"], false,
        "refs/pull/7 is ahead of main"
    );

    std::fs::remove_dir_all(repo.parent().unwrap()).ok();
}
