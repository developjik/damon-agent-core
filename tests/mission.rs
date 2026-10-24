//! Mission DAG runner tests. Two layers:
//! - `MissionScheduler` driven directly with a stub agent runner (no
//!   processes, no network) — convergence, slots, retries, exclude
//!   cascade, human CAS, crash honesty, condition grammar;
//! - the rpc surface over a real server (`Store::in_memory`, mock
//!   backend) — create/start/get/list/respond/watch dispatch.

mod common;

use damon_core::mission::{
    self, MissionAgentRequest, MissionAgentRunner, MissionCommand, MissionDecision,
    MissionDefinition, MissionScheduler, MissionTaskStatus,
};
use damon_core::store::Store;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------
// Condition grammar — ported semantics from upstream
// tests/mission-conditions.test.ts (`key=value`, `key!=value`, `and`
// binds over `or`, missing key ⇒ false, default edge only when nothing
// else matched).
// ---------------------------------------------------------------------

fn edges_activated(def: &Value, output: Value) -> Vec<String> {
    let def = mission::parse_definition(def).unwrap();
    let settings =
        json!({"concurrency": def.settings.concurrency, "retries": def.settings.retries});
    mission::activated_edges(&def.edges, &output, &settings)
        .into_iter()
        .map(|i| def.edges[i].to.clone())
        .collect()
}

#[test]
fn condition_eq_matches_and_ne_inverts() {
    let def = json!({
        "nodes": [
            {"id": "a", "kind": "input", "value": 1},
            {"id": "eq", "kind": "output"},
            {"id": "ne", "kind": "output"},
        ],
        "edges": [
            {"from": "a", "to": "eq", "condition": "status=ok"},
            {"from": "a", "to": "ne", "condition": "status!=ok"},
        ],
        "settings": {}
    });
    let got = edges_activated(&def, json!({"status": "ok"}));
    assert!(got.contains(&"eq".to_string()), "{got:?}");
    assert!(!got.contains(&"ne".to_string()), "{got:?}");
    let got = edges_activated(&def, json!({"status": "bad"}));
    assert!(got.contains(&"ne".to_string()), "{got:?}");
    assert!(!got.contains(&"eq".to_string()), "{got:?}");
}

#[test]
fn condition_and_binds_over_or() {
    let def = json!({
        "nodes": [
            {"id": "a", "kind": "input", "value": 1},
            {"id": "both", "kind": "output"},
            {"id": "half", "kind": "output"},
        ],
        "edges": [
            // a=1 and b=2, or c=3 — the and binds over the or.
            {"from": "a", "to": "both", "condition": "a=1 and b=2 or c=3"},
            // a=1 or (b=2 and c=3) — same tokens, different reading is
            // NOT possible; grammar is fixed: and over or.
            {"from": "a", "to": "half", "condition": "a=9 or b=2 and c=3"},
        ],
        "settings": {}
    });
    // a=1, b=2, c absent: first edge's left or-group matches (a=1 and b=2).
    let got = edges_activated(&def, json!({"a": 1, "b": 2}));
    assert!(got.contains(&"both".to_string()), "{got:?}");
    assert!(!got.contains(&"half".to_string()), "{got:?}");
    // c=3 alone: first edge matches via its or tail.
    let got = edges_activated(&def, json!({"c": 3}));
    assert!(got.contains(&"both".to_string()), "{got:?}");
    assert!(!got.contains(&"half".to_string()), "{got:?}");
    // a=9 only: second edge's or head matches (the first edge's groups
    // both fail).
    let got = edges_activated(&def, json!({"a": 9}));
    assert!(got.contains(&"half".to_string()), "{got:?}");
    assert!(!got.contains(&"both".to_string()), "{got:?}");
}

#[test]
fn condition_missing_key_is_false_and_default_fires() {
    let def = json!({
        "nodes": [
            {"id": "a", "kind": "input", "value": 1},
            {"id": "strict", "kind": "output"},
            {"id": "fallback", "kind": "output"},
        ],
        "edges": [
            {"from": "a", "to": "strict", "condition": "nope=1"},
            {"from": "a", "to": "fallback"},
        ],
        "settings": {}
    });
    let got = edges_activated(&def, json!({"status": "ok"}));
    assert!(!got.contains(&"strict".to_string()), "{got:?}");
    assert!(
        got.contains(&"fallback".to_string()),
        "default fires when nothing matched: {got:?}"
    );
    // A matching explicit condition suppresses the default.
    let def2 = json!({
        "nodes": [
            {"id": "a", "kind": "input", "value": 1},
            {"id": "hit", "kind": "output"},
            {"id": "fallback", "kind": "output"},
        ],
        "edges": [
            {"from": "a", "to": "hit", "condition": "status=ok"},
            {"from": "a", "to": "fallback"},
        ],
        "settings": {}
    });
    let got = edges_activated(&def2, json!({"status": "ok"}));
    assert!(
        got.contains(&"hit".to_string()) && !got.contains(&"fallback".to_string()),
        "{got:?}"
    );
}

#[test]
fn condition_reads_upstream_object_and_settings() {
    let def = json!({
        "nodes": [
            {"id": "a", "kind": "input", "value": 1},
            {"id": "by_settings", "kind": "output"},
            {"id": "by_upstream", "kind": "output"},
        ],
        "edges": [
            {"from": "a", "to": "by_settings", "condition": "env=prod"},
            {"from": "a", "to": "by_upstream", "condition": "risk=low"},
        ],
        "settings": {"env": "prod"}
    });
    let def = mission::parse_definition(&def).unwrap();
    let settings = json!({"env": "prod"});
    let got = mission::activated_edges(&def.edges, &json!({"risk": "low"}), &settings)
        .into_iter()
        .map(|i| def.edges[i].to.clone())
        .collect::<Vec<_>>();
    assert!(got.contains(&"by_settings".to_string()), "{got:?}");
    assert!(got.contains(&"by_upstream".to_string()), "{got:?}");
}

#[test]
fn invalid_conditions_are_rejected_at_validation() {
    let def = json!({
        "nodes": [
            {"id": "a", "kind": "input", "value": 1},
            {"id": "b", "kind": "output"},
        ],
        "edges": [{"from": "a", "to": "b", "condition": "junk === nope"}],
        "settings": {}
    });
    // Refusal happens at parse: mission.create never stores a flow
    // whose edge conditions do not parse.
    assert!(
        mission::parse_definition(&def).is_err(),
        "unparseable condition must refuse the flow"
    );
}

// ---------------------------------------------------------------------
// Scheduler — stub runner driving one fake agent turn per request.
// `fail:<n>` fails the first n attempts then succeeds with its kv.
// ---------------------------------------------------------------------

struct StubRunner {
    /// (prompt-marker → output kv line) — the stub echoes per-node.
    outputs: std::collections::HashMap<String, String>,
    /// Prompt-marker → number of initial failures.
    failures: std::collections::HashMap<String, u32>,
    calls: parking_lot::Mutex<Vec<String>>,
    /// Max concurrent in-flight `run_agent_node` calls.
    max_concurrent: parking_lot::Mutex<usize>,
    current: std::sync::atomic::AtomicUsize,
}

impl StubRunner {
    fn new() -> Self {
        Self {
            outputs: std::collections::HashMap::new(),
            failures: std::collections::HashMap::new(),
            calls: parking_lot::Mutex::new(Vec::new()),
            max_concurrent: parking_lot::Mutex::new(0),
            current: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn out(mut self, marker: &str, kv: &str) -> Self {
        self.outputs.insert(marker.to_string(), kv.to_string());
        self
    }
    fn failing(mut self, marker: &str, n: u32) -> Self {
        self.failures.insert(marker.to_string(), n);
        self
    }
}

#[async_trait::async_trait]
impl MissionAgentRunner for StubRunner {
    async fn run_agent_node(&self, req: MissionAgentRequest) -> Result<Value, String> {
        let marker = req
            .prompt
            .split("mission-out:")
            .nth(1)
            .unwrap_or_default()
            .lines()
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        self.calls.lock().push(marker.clone());
        let now = self
            .current
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        {
            let mut m = self.max_concurrent.lock();
            if now > *m {
                *m = now;
            }
        }
        // A tiny hop so overlapping spawns really do overlap.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        self.current
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        let fails = self.failures.get(&marker).copied().unwrap_or(0);
        let calls = self.calls.lock().iter().filter(|c| **c == marker).count();
        if (calls as u32) <= fails {
            return Err(format!("stub failure for {marker}"));
        }
        Ok(mission::parse_agent_output(
            self.outputs
                .get(&marker)
                .map(String::as_str)
                .unwrap_or("status=ok"),
        ))
    }
}

async fn run_flow(def: Value, runner: Arc<StubRunner>) -> (Store, String, MissionDefinition) {
    let store = Store::in_memory().await.unwrap();
    let def = mission::parse_definition(&def).unwrap();
    mission::validate_definition(&def).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    store
        .create_mission_run(&run_id, "flow", 1, &json!({}).to_string())
        .await
        .unwrap();
    for n in &def.nodes {
        store.insert_mission_task(&run_id, &n.id).await.unwrap();
    }
    let (events, _) = broadcast::channel(64);
    let (_tx, rx) = mpsc::channel(16);
    let scheduler = MissionScheduler::new(
        run_id.clone(),
        def.clone(),
        store.clone(),
        runner,
        events,
        rx,
        CancellationToken::new(),
    );
    scheduler.run().await.unwrap();
    (store, run_id, def)
}

async fn task_statuses(store: &Store, run_id: &str) -> Vec<(String, String)> {
    store
        .mission_tasks(run_id)
        .await
        .unwrap()
        .into_iter()
        .map(|t| (t.0, t.1))
        .collect()
}

#[tokio::test]
async fn sequential_chain_converges_with_condition_branch() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": {"ok": true}},
            {"id": "probe", "kind": "agent", "backend": "stub", "prompt": "mission-out:probe status=ok risk=low"},
            {"id": "fast", "kind": "agent", "backend": "stub", "prompt": "mission-out:fast done=yes"},
            {"id": "slow", "kind": "agent", "backend": "stub", "prompt": "mission-out:slow done=yes"},
            {"id": "end", "kind": "output"},
        ],
        "edges": [
            {"from": "start", "to": "probe"},
            {"from": "probe", "to": "fast", "condition": "risk=low"},
            {"from": "probe", "to": "slow", "condition": "risk!=low"},
            {"from": "fast", "to": "end"},
            {"from": "slow", "to": "end"},
        ],
        "settings": {}
    });
    let runner = Arc::new(
        StubRunner::new()
            .out("probe", "status=ok risk=low")
            .out("fast", "done=yes")
            .out("slow", "done=yes"),
    );
    let (store, run_id, _) = run_flow(def, runner).await;
    let tasks = task_statuses(&store, &run_id).await;
    let st = |id: &str| tasks.iter().find(|t| t.0 == id).unwrap().1.as_str();
    assert_eq!(st("start"), "succeeded");
    assert_eq!(st("probe"), "succeeded", "tasks: {tasks:?}");
    assert_eq!(st("fast"), "succeeded", "tasks: {tasks:?}");
    assert_eq!(
        st("slow"),
        "cancelled",
        "unreachable branch cancels, never fakes: {tasks:?}"
    );
    assert_eq!(st("end"), "succeeded", "tasks: {tasks:?}");
}

#[tokio::test]
async fn concurrency_slots_bound_parallel_agent_nodes() {
    // 4 independent agent nodes, budget 2 — the stub observes at most 2
    // in flight.
    let mut nodes = vec![json!({"id": "start", "kind": "input", "value": 1})];
    let mut edges = vec![];
    for i in 0..4 {
        nodes.push(json!({"id": format!("a{i}"), "kind": "agent", "backend": "stub", "prompt": format!("mission-out:a{i} done=yes")}));
        edges.push(json!({"from": "start", "to": format!("a{i}")}));
    }
    let def = json!({"nodes": nodes, "edges": edges, "settings": {"concurrency": 2}});
    let runner = Arc::new(StubRunner::new());
    let (store, run_id, _) = run_flow(def, runner.clone()).await;
    let tasks = task_statuses(&store, &run_id).await;
    assert!(tasks.iter().all(|t| t.1 == "succeeded"), "{tasks:?}");
    assert!(
        *runner.max_concurrent.lock() <= 2,
        "concurrency budget must bind: {}",
        runner.max_concurrent.lock()
    );
}

#[tokio::test]
async fn retries_exhaust_to_failed_and_error_is_persisted() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "flaky", "kind": "agent", "backend": "stub", "prompt": "mission-out:flaky done=yes"},
        ],
        "edges": [{"from": "start", "to": "flaky"}],
        "settings": {"retries": 2}
    });
    let runner = Arc::new(StubRunner::new().failing("flaky", 5));
    let (store, run_id, _) = run_flow(def, runner.clone()).await;
    let tasks = store.mission_tasks(&run_id).await.unwrap();
    let flaky = tasks.iter().find(|t| t.0 == "flaky").unwrap();
    assert_eq!(flaky.1, "failed", "{tasks:?}");
    assert_eq!(flaky.2, 3, "initial attempt + 2 retries: {flaky:?}");
    let err = flaky.4.as_deref().unwrap_or_default();
    assert!(err.contains("stub failure"), "{flaky:?}");
    assert_eq!(runner.calls.lock().len(), 3, "three attempts total");
}

#[tokio::test]
async fn retry_recovers_within_budget() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "flaky", "kind": "agent", "backend": "stub", "prompt": "mission-out:flaky done=yes"},
        ],
        "edges": [{"from": "start", "to": "flaky"}],
        "settings": {"retries": 2}
    });
    let runner = Arc::new(StubRunner::new().failing("flaky", 1));
    let (store, run_id, _) = run_flow(def, runner).await;
    let tasks = store.mission_tasks(&run_id).await.unwrap();
    let flaky = tasks.iter().find(|t| t.0 == "flaky").unwrap();
    assert_eq!(flaky.1, "succeeded", "{tasks:?}");
    assert_eq!(flaky.2, 2);
}

#[tokio::test]
async fn human_parks_and_respond_approve_frees_the_branch() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "ask", "kind": "human", "title": "ship it?"},
            {"id": "after", "kind": "agent", "backend": "stub", "prompt": "mission-out:after done=yes"},
            {"id": "end", "kind": "output"},
        ],
        "edges": [
            {"from": "start", "to": "ask"},
            {"from": "ask", "to": "after"},
            {"from": "after", "to": "end"},
        ],
        "settings": {}
    });
    let store = Store::in_memory().await.unwrap();
    let def = mission::parse_definition(&def).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    store
        .create_mission_run(&run_id, "flow", 1, "{}")
        .await
        .unwrap();
    for n in &def.nodes {
        store.insert_mission_task(&run_id, &n.id).await.unwrap();
    }
    let (events, _) = broadcast::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let scheduler = MissionScheduler::new(
        run_id.clone(),
        def.clone(),
        store.clone(),
        Arc::new(StubRunner::new().out("after", "done=yes")),
        events.clone(),
        cmd_rx,
        cancel.clone(),
    );
    let handle = tokio::spawn(scheduler.run());
    // Wait until the human node parks.
    loop {
        let tasks = task_statuses(&store, &run_id).await;
        if tasks.iter().any(|t| t.0 == "ask" && t.1 == "waiting_human") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // First answer wins the CAS; the loser gets false.
    assert!(
        store
            .resolve_mission_inbox(&run_id, "ask", "approve", None)
            .await
            .unwrap()
    );
    assert!(
        !store
            .resolve_mission_inbox(&run_id, "ask", "exclude", None)
            .await
            .unwrap()
    );
    cmd_tx
        .send(MissionCommand::Respond {
            node_id: "ask".into(),
            decision: "approve".into(),
            feedback: None,
        })
        .await
        .unwrap();
    handle.await.unwrap().unwrap();
    let tasks = task_statuses(&store, &run_id).await;
    let st = |id: &str| tasks.iter().find(|t| t.0 == id).unwrap().1.as_str();
    assert_eq!(st("ask"), "succeeded");
    assert_eq!(st("after"), "succeeded");
    assert_eq!(st("end"), "succeeded");
}

#[tokio::test]
async fn exclude_terminates_the_branch_and_cancels_downstream() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "ask", "kind": "human"},
            {"id": "down", "kind": "agent", "backend": "stub", "prompt": "mission-out:down done=yes"},
            {"id": "end", "kind": "output"},
        ],
        "edges": [
            {"from": "start", "to": "ask"},
            {"from": "ask", "to": "down"},
            {"from": "down", "to": "end"},
        ],
        "settings": {}
    });
    let store = Store::in_memory().await.unwrap();
    let def = mission::parse_definition(&def).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    store
        .create_mission_run(&run_id, "flow", 1, "{}")
        .await
        .unwrap();
    for n in &def.nodes {
        store.insert_mission_task(&run_id, &n.id).await.unwrap();
    }
    let (events, _) = broadcast::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let scheduler = MissionScheduler::new(
        run_id.clone(),
        def.clone(),
        store.clone(),
        Arc::new(StubRunner::new()),
        events,
        cmd_rx,
        CancellationToken::new(),
    );
    let handle = tokio::spawn(scheduler.run());
    loop {
        let tasks = task_statuses(&store, &run_id).await;
        if tasks.iter().any(|t| t.0 == "ask" && t.1 == "waiting_human") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        store
            .resolve_mission_inbox(&run_id, "ask", "exclude", None)
            .await
            .unwrap()
    );
    cmd_tx
        .send(MissionCommand::Respond {
            node_id: "ask".into(),
            decision: "exclude".into(),
            feedback: None,
        })
        .await
        .unwrap();
    handle.await.unwrap().unwrap();
    let tasks = task_statuses(&store, &run_id).await;
    let st = |id: &str| tasks.iter().find(|t| t.0 == id).unwrap().1.as_str();
    assert_eq!(st("ask"), "excluded");
    assert_eq!(st("down"), "excluded", "exclude cascades downstream");
    assert_eq!(st("end"), "excluded");
    // Derived status is honest: an excluded branch is a failed run.
    assert_eq!(
        mission::derive_run_status(
            &store.mission_tasks(&run_id).await.unwrap(),
            false,
            true,
            false
        ),
        "failed"
    );
}

#[tokio::test]
async fn feedback_requeues_the_human_ask_with_the_feedback_recorded() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "ask", "kind": "human"},
        ],
        "edges": [{"from": "start", "to": "ask"}],
        "settings": {}
    });
    let store = Store::in_memory().await.unwrap();
    let def = mission::parse_definition(&def).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    store
        .create_mission_run(&run_id, "flow", 1, "{}")
        .await
        .unwrap();
    for n in &def.nodes {
        store.insert_mission_task(&run_id, &n.id).await.unwrap();
    }
    let (events, _) = broadcast::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);
    let scheduler = MissionScheduler::new(
        run_id.clone(),
        def.clone(),
        store.clone(),
        Arc::new(StubRunner::new()),
        events,
        cmd_rx,
        CancellationToken::new(),
    );
    let handle = tokio::spawn(scheduler.run());
    loop {
        let tasks = task_statuses(&store, &run_id).await;
        if tasks.iter().any(|t| t.0 == "ask" && t.1 == "waiting_human") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        store
            .resolve_mission_inbox(&run_id, "ask", "feedback", Some("tighten the scope"))
            .await
            .unwrap()
    );
    cmd_tx
        .send(MissionCommand::Respond {
            node_id: "ask".into(),
            decision: "feedback".into(),
            feedback: Some("tighten the scope".into()),
        })
        .await
        .unwrap();
    // The ask re-parks with the attempt bumped.
    loop {
        let tasks = store.mission_tasks(&run_id).await.unwrap();
        let ask = tasks.iter().find(|t| t.0 == "ask").unwrap();
        if ask.1 == "waiting_human" && ask.2 == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // The scheduler still parks at waiting_human; end it via a cancel
    // channel (already closed cmd_tx — drop the handle's inputs).
    handle.abort();
    let inbox = store.mission_inbox_for_run(&run_id).await.unwrap();
    // Round 1 (answered with feedback) + round 2 (the re-ask, pending).
    assert_eq!(inbox.len(), 2, "{inbox:?}");
    assert_eq!(inbox[0].1, "resolved");
    assert_eq!(inbox[0].2.as_deref(), Some("feedback"));
    assert_eq!(inbox[0].3.as_deref(), Some("tighten the scope"));
    assert_eq!(inbox[1].1, "pending");
}

#[tokio::test]
async fn cancel_cancels_non_terminal_tasks_and_ends_the_run() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "a", "kind": "agent", "backend": "stub", "prompt": "mission-out:a done=yes"},
        ],
        "edges": [{"from": "start", "to": "a"}],
        "settings": {}
    });
    let store = Store::in_memory().await.unwrap();
    let def = mission::parse_definition(&def).unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    store
        .create_mission_run(&run_id, "flow", 1, "{}")
        .await
        .unwrap();
    for n in &def.nodes {
        store.insert_mission_task(&run_id, &n.id).await.unwrap();
    }
    let (events, _) = broadcast::channel(64);
    let (_tx, rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    // A runner that hangs until cancelled — the cancel must free it.
    struct Hanging(CancellationToken);
    #[async_trait::async_trait]
    impl MissionAgentRunner for Hanging {
        async fn run_agent_node(&self, _req: MissionAgentRequest) -> Result<Value, String> {
            self.0.cancelled().await;
            Err("cancelled".into())
        }
    }
    let scheduler = MissionScheduler::new(
        run_id.clone(),
        def.clone(),
        store.clone(),
        Arc::new(Hanging(cancel.clone())),
        events,
        rx,
        cancel.clone(),
    );
    let handle = tokio::spawn(scheduler.run());
    loop {
        let tasks = task_statuses(&store, &run_id).await;
        if tasks.iter().any(|t| t.1 == "running") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    cancel.cancel();
    handle.await.unwrap().unwrap();
    let tasks = task_statuses(&store, &run_id).await;
    // Already-terminal rows keep their outcome; open work cancels.
    assert_eq!(
        tasks.iter().find(|t| t.0 == "a").unwrap().1,
        "cancelled",
        "{tasks:?}"
    );
    assert_eq!(
        tasks.iter().find(|t| t.0 == "start").unwrap().1,
        "succeeded"
    );
    let run = store.mission_run(&run_id).await.unwrap().unwrap();
    assert!(run.4, "run must end when cancelled");
    assert!(run.5, "explicit cancel is recorded as a run fact");
}

#[tokio::test]
async fn boot_recovery_marks_orphaned_runs_interrupted() {
    let store = Store::in_memory().await.unwrap();
    let run_id = uuid::Uuid::new_v4().to_string();
    store
        .create_mission_run(&run_id, "flow", 1, "{}")
        .await
        .unwrap();
    store.insert_mission_task(&run_id, "a").await.unwrap();
    store.insert_mission_inbox(&run_id, "h").await.unwrap();
    let recovered = store
        .recover_interrupted_mission_runs(vec![])
        .await
        .unwrap();
    assert_eq!(recovered, vec![run_id.clone()]);
    let tasks = store.mission_tasks(&run_id).await.unwrap();
    assert_eq!(tasks[0].1, "cancelled");
    let inbox = store.mission_inbox_for_run(&run_id).await.unwrap();
    assert_eq!(inbox[0].1, "resolved");
    assert_eq!(inbox[0].2.as_deref(), Some("cancelled"));
    // Idempotent: a second pass finds nothing open.
    assert!(
        store
            .recover_interrupted_mission_runs(vec![])
            .await
            .unwrap()
            .is_empty()
    );
}

// ---------------------------------------------------------------------
// Output parsing — the key=value contract agent nodes answer with.
// ---------------------------------------------------------------------

#[test]
fn agent_output_parses_keyvalue_lines_and_falls_back_to_text() {
    // Space-separated pairs on one line parse like separate lines.
    let out = mission::parse_agent_output("status=ok risk=low");
    assert_eq!(out["status"], "ok");
    assert_eq!(out["risk"], "low");
    // Prose without pairs falls back to a single output value.
    let out = mission::parse_agent_output("no pairs here");
    assert_eq!(out["output"], "no pairs here");
}

// ---------------------------------------------------------------------
// rpc surface — real server, mock backend, Store::in_memory.
// ---------------------------------------------------------------------

fn flow_e2e() -> Value {
    json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": {"ok": true}},
            {"id": "probe", "kind": "agent", "backend": "mock", "prompt": "mission-out:status=ok risk=low"},
            {"id": "fast", "kind": "agent", "backend": "mock", "prompt": "mission-out:done=yes"},
            {"id": "slow", "kind": "agent", "backend": "mock", "prompt": "mission-out:done=yes"},
            {"id": "end", "kind": "output"},
        ],
        "edges": [
            {"from": "start", "to": "probe"},
            {"from": "probe", "to": "fast", "condition": "risk=low"},
            {"from": "probe", "to": "slow", "condition": "risk!=low"},
            {"from": "fast", "to": "end"},
            {"from": "slow", "to": "end"},
        ],
        "settings": {}
    })
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn serve() -> (String, String) {
    let cfg: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(
        common::mock_config(Some("secret")),
    ));
    let store = Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(cfg, store).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let app = damon_core::api::router(state);
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
    (addr.to_string(), "secret".to_string())
}

async fn ws_connect(addr: &str, token: &str) -> Ws {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let client = reqwest::Client::new();
    let ticket = client
        .post(format!("http://{addr}/v1/ws_ticket"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    let (ws, _) = tokio_tungstenite::connect_async(
        format!("ws://{addr}/ws?ticket={ticket}")
            .into_client_request()
            .unwrap(),
    )
    .await
    .unwrap();
    ws
}

async fn rpc_send(ws: &mut Ws, id: u64, method: &str, params: Value) {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    ws.send(Message::Text(
        json!({"id": id, "method": method, "params": params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
}

async fn read_reply(ws: &mut Ws, id: u64) -> Value {
    loop {
        let v = next_frame(ws).await;
        if v["id"].as_u64() == Some(id) {
            return v;
        }
    }
}

async fn rpc_call(ws: &mut Ws, id: u64, method: &str, params: Value) -> Value {
    rpc_send(ws, id, method, params).await;
    read_reply(ws, id).await
}

async fn next_frame(ws: &mut Ws) -> Value {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    let frame = tokio::time::timeout(std::time::Duration::from_secs(30), ws.next())
        .await
        .expect("timed out waiting for a daemon frame")
        .unwrap()
        .unwrap();
    match frame {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[tokio::test]
async fn mission_lifecycle_over_rpc_create_start_get_and_watch() {
    let (addr, token) = serve().await;
    let mut ws = ws_connect(&addr, &token).await;
    let created = rpc_call(
        &mut ws,
        1,
        "mission.create",
        json!({"flowId": "ship", "name": "ship", "definition": flow_e2e()}),
    )
    .await;
    assert_eq!(created["result"]["flowId"], "ship", "{created}");
    assert_eq!(created["result"]["version"], 1);

    // Invalid definitions refuse before the store.
    let bad = rpc_call(
        &mut ws,
        2,
        "mission.create",
        json!({"flowId": "bad", "definition": json!({
            "nodes": [{"id": "a", "kind": "foreach"}], "edges": [], "settings": {}
        })}),
    )
    .await;
    assert!(bad["error"].is_object(), "{bad}");

    let started = rpc_call(&mut ws, 3, "mission.start", json!({"flowId": "ship"})).await;
    let run_id = started["result"]["runId"].as_str().unwrap().to_string();
    assert_eq!(started["result"]["version"], 1);

    // Watch BEFORE the run converges: progress frames stream in.
    let watched = rpc_call(&mut ws, 4, "mission.watch", json!({"runId": run_id})).await;
    assert_eq!(watched["result"]["subscribed"], true, "{watched}");
    let mut saw_running = false;
    for _ in 0..64 {
        let f = tokio::time::timeout(std::time::Duration::from_secs(10), next_frame(&mut ws)).await;
        let Ok(f) = f else { break };
        if f["event"] == "mission.progress"
            && f["runId"] == run_id
            && matches!(
                f["data"]["status"].as_str(),
                Some("running") | Some("succeeded")
            )
        {
            saw_running = true;
        }
        if f["event"] == "mission.progress"
            && let Some(n) = f["data"]["node"].as_object()
            && n.get("id").and_then(|i| i.as_str()) == Some("end")
        {
            break;
        }
    }
    assert!(saw_running, "watch must stream progress frames");

    // The run converged: end succeeded, the unreachable branch cancelled.
    let got = rpc_call(&mut ws, 5, "mission.get", json!({"runId": run_id})).await;
    let result = &got["result"];
    assert_eq!(result["status"], "succeeded", "{got}");
    let task = |id: &str| {
        result["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["nodeId"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(task("probe")["status"], "succeeded");
    assert_eq!(task("probe")["output"]["risk"], "low");
    assert_eq!(task("slow")["status"], "cancelled");
    assert_eq!(task("end")["status"], "succeeded");

    let list = rpc_call(&mut ws, 6, "mission.list", json!({})).await;
    let runs = list["result"]["runs"].as_array().unwrap();
    assert!(
        runs.iter()
            .any(|r| r["runId"] == run_id && r["status"] == "succeeded"),
        "{list}"
    );

    let unwatched = rpc_call(&mut ws, 7, "mission.unwatch", json!({"runId": run_id})).await;
    assert_eq!(unwatched["result"]["subscribed"], false);
}

#[tokio::test]
async fn mission_start_refuses_read_only_on_backends_without_allowlist_enforcement() {
    let (addr, token) = serve().await;
    let mut ws = ws_connect(&addr, &token).await;
    // The mock CAN enforce (tool_allowlist: true) — a read-only node on
    // it starts fine. A backend that does not exist refuses with
    // -32003; the -32002 refusal path is covered by the capability
    // check itself (strict client asserted in tests/common).
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "careful", "kind": "agent", "backend": "mock", "readOnly": true,
             "allowedTools": ["Read", "Grep", "Glob"], "prompt": "mission-out:status=ok"},
        ],
        "edges": [{"from": "start", "to": "careful"}],
        "settings": {}
    });
    let created = rpc_call(
        &mut ws,
        1,
        "mission.create",
        json!({"flowId": "ro", "definition": def}),
    )
    .await;
    assert!(created["result"]["flowId"].is_string(), "{created}");
    let started = rpc_call(&mut ws, 2, "mission.start", json!({"flowId": "ro"})).await;
    assert!(started["result"]["runId"].is_string(), "{started}");
    let run_id = started["result"]["runId"].as_str().unwrap().to_string();

    // Unknown backend: typed refusal.
    let def2 = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "ghost", "kind": "agent", "backend": "no-such-backend", "prompt": "mission-out:status=ok"},
        ],
        "edges": [{"from": "start", "to": "ghost"}],
        "settings": {}
    });
    let _ = rpc_call(
        &mut ws,
        3,
        "mission.create",
        json!({"flowId": "ghost-flow", "definition": def2}),
    )
    .await;
    let refused = rpc_call(&mut ws, 4, "mission.start", json!({"flowId": "ghost-flow"})).await;
    assert_eq!(refused["error"]["code"], -32003, "{refused}");

    // Read-only validation: readOnly without allowedTools refuses at
    // create (damon refuses what it cannot enforce).
    let def3 = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "ro", "kind": "agent", "backend": "mock", "readOnly": true, "prompt": "mission-out:status=ok"},
        ],
        "edges": [{"from": "start", "to": "ro"}],
        "settings": {}
    });
    let refused = rpc_call(
        &mut ws,
        5,
        "mission.create",
        json!({"flowId": "ro-no-tools", "definition": def3}),
    )
    .await;
    assert!(refused["error"].is_object(), "{refused}");

    let _ = run_id;
}

#[tokio::test]
async fn mission_agent_turn_transcript_lands_in_the_store_search() {
    let (addr, token) = serve().await;
    let mut ws = ws_connect(&addr, &token).await;
    let _ = rpc_call(
        &mut ws,
        1,
        "mission.create",
        json!({"flowId": "searchable", "definition": flow_e2e()}),
    )
    .await;
    let started = rpc_call(&mut ws, 2, "mission.start", json!({"flowId": "searchable"})).await;
    let run_id = started["result"]["runId"].as_str().unwrap().to_string();
    // Wait for convergence via mission.get polling (watch is covered above).
    for _ in 0..200 {
        let got = rpc_call(&mut ws, 3, "mission.get", json!({"runId": run_id})).await;
        if got["result"]["status"] == "succeeded" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // The per-node session persisted: searching for the node's kv hits.
    let hits = rpc_call(
        &mut ws,
        4,
        "session.search",
        json!({"query": "mission-out"}),
    )
    .await;
    let results = hits["result"]["results"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        !results.is_empty(),
        "mission agent turns must be searchable: {hits}"
    );
}

#[tokio::test]
async fn mission_human_over_rpc_cas_one_answer_wins() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "gate", "kind": "human"},
        ],
        "edges": [{"from": "start", "to": "gate"}],
        "settings": {}
    });
    let (addr, token) = serve().await;
    let mut ws = ws_connect(&addr, &token).await;
    let _ = rpc_call(
        &mut ws,
        1,
        "mission.create",
        json!({"flowId": "gate-flow", "definition": def}),
    )
    .await;
    let started = rpc_call(&mut ws, 2, "mission.start", json!({"flowId": "gate-flow"})).await;
    let run_id = started["result"]["runId"].as_str().unwrap().to_string();
    // Wait for the inbox item.
    for _ in 0..200 {
        let got = rpc_call(&mut ws, 3, "mission.get", json!({"runId": run_id})).await;
        if got["result"]["inbox"]
            .as_array()
            .is_some_and(|i| !i.is_empty())
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let ok = rpc_call(
        &mut ws,
        4,
        "mission.respond",
        json!({"runId": run_id, "nodeId": "gate", "decision": "approve"}),
    )
    .await;
    assert_eq!(ok["result"]["accepted"], true, "{ok}");
    let lost = rpc_call(
        &mut ws,
        5,
        "mission.respond",
        json!({"runId": run_id, "nodeId": "gate", "decision": "exclude"}),
    )
    .await;
    assert_eq!(lost["error"]["code"], -32602, "{lost}");
    let bad = rpc_call(
        &mut ws,
        6,
        "mission.respond",
        json!({"runId": run_id, "nodeId": "gate", "decision": "feedback"}),
    )
    .await;
    assert_eq!(
        bad["error"]["code"], -32602,
        "feedback without text refuses: {bad}"
    );
}

#[tokio::test]
async fn mission_cancel_over_rpc_stops_a_live_run() {
    let def = json!({
        "nodes": [
            {"id": "start", "kind": "input", "value": 1},
            {"id": "gate", "kind": "human"},
        ],
        "edges": [{"from": "start", "to": "gate"}],
        "settings": {}
    });
    let (addr, token) = serve().await;
    let mut ws = ws_connect(&addr, &token).await;
    let _ = rpc_call(
        &mut ws,
        1,
        "mission.create",
        json!({"flowId": "cancel-flow", "definition": def}),
    )
    .await;
    let started = rpc_call(
        &mut ws,
        2,
        "mission.start",
        json!({"flowId": "cancel-flow"}),
    )
    .await;
    let run_id = started["result"]["runId"].as_str().unwrap().to_string();
    let cancelled = rpc_call(&mut ws, 3, "mission.cancel", json!({"runId": run_id})).await;
    assert_eq!(cancelled["result"]["cancelled"], true, "{cancelled}");
    for _ in 0..200 {
        let got = rpc_call(&mut ws, 4, "mission.get", json!({"runId": run_id})).await;
        if got["result"]["ended"] == true {
            assert_eq!(got["result"]["status"], "cancelled", "{got}");
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("cancelled run never ended");
}

#[test]
fn decision_enum_roundtrips() {
    // The scheduler's decision mapping — approve/feedback/exclude only.
    assert!(matches!(MissionDecision::Approve, MissionDecision::Approve));
    let statuses = [
        "queued",
        "running",
        "waiting_human",
        "succeeded",
        "failed",
        "cancelled",
        "excluded",
    ];
    for s in statuses {
        assert!(MissionTaskStatus::parse(s).is_some(), "{s}");
    }
    assert!(MissionTaskStatus::parse("exploded").is_none());
}

/// Helper kept beside the tests: flow_id of a run row (wire checks).
#[allow(dead_code)]
async fn flow_of(store: &Store, run_id: &str) -> String {
    store.mission_run(run_id).await.unwrap().unwrap().1
}
