use damon_core::builtin::BuiltinTools;
use damon_core::config::BuiltinConfig;
use serde_json::json;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "damon-builtin-test-{name}-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn tools(cfg: BuiltinConfig) -> BuiltinTools {
    BuiltinTools::from_config(&cfg)
}

#[tokio::test]
async fn write_then_read_roundtrip() {
    let dir = temp_dir("roundtrip");
    let t = tools(BuiltinConfig::default());
    let cwd = dir.to_str().unwrap();

    let w = t
        .call(
            "fs.write",
            json!({"path": "sub/notes.txt", "content": "line1\nline2\nline3\n"}),
            cwd,
        )
        .await
        .unwrap();
    assert_eq!(w["bytes"], 18);
    // Parent dirs were created.
    assert!(dir.join("sub").is_dir());

    let r = t
        .call("fs.read", json!({"path": "sub/notes.txt"}), cwd)
        .await
        .unwrap();
    assert_eq!(r["content"], "line1\nline2\nline3\n");
    assert_eq!(r["truncated"], false);

    // Line window: offset is 1-based.
    let r = t
        .call(
            "fs.read",
            json!({"path": "sub/notes.txt", "offset": 2, "limit": 1}),
            cwd,
        )
        .await
        .unwrap();
    assert_eq!(r["content"], "line2\n");
    assert_eq!(r["truncated"], true);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn edit_requires_exactly_one_match() {
    let dir = temp_dir("edit");
    let t = tools(BuiltinConfig::default());
    let cwd = dir.to_str().unwrap();
    let file = dir.join("f.txt");
    std::fs::write(&file, "foo bar foo\n").unwrap();

    // Zero matches → error.
    assert!(
        t.call(
            "fs.edit",
            json!({"path": "f.txt", "old_string": "nope", "new_string": "x"}),
            cwd,
        )
        .await
        .is_err()
    );
    // Two matches → error (uniqueness is the safety story).
    assert!(
        t.call(
            "fs.edit",
            json!({"path": "f.txt", "old_string": "foo", "new_string": "x"}),
            cwd,
        )
        .await
        .is_err()
    );
    // Unique match applies.
    let r = t
        .call(
            "fs.edit",
            json!({"path": "f.txt", "old_string": "bar", "new_string": "baz"}),
            cwd,
        )
        .await
        .unwrap();
    assert_eq!(r["replacements"], 1);
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "foo baz foo\n");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_and_search() {
    let dir = temp_dir("search");
    let t = tools(BuiltinConfig::default());
    let cwd = dir.to_str().unwrap();
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("a.txt"), "alpha\nbeta\n").unwrap();
    std::fs::write(dir.join("sub").join("b.txt"), "gamma\nbeta2\n").unwrap();

    // Flat listing: immediate children only, dirs suffixed.
    let r = t.call("fs.list", json!({"path": "."}), cwd).await.unwrap();
    let entries: Vec<&str> = r["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e.as_str().unwrap())
        .collect();
    assert_eq!(entries, ["a.txt", "sub/"]);

    // Glob pattern → recursive walk over relative paths.
    let r = t
        .call("fs.list", json!({"path": ".", "pattern": "*.txt"}), cwd)
        .await
        .unwrap();
    let entries: Vec<&str> = r["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e.as_str().unwrap())
        .collect();
    assert_eq!(entries, ["a.txt", "sub/b.txt"]);

    // Regex search finds the line, in both files.
    let r = t
        .call("fs.search", json!({"path": ".", "pattern": "bet."}), cwd)
        .await
        .unwrap();
    let matches = r["matches"].as_array().unwrap();
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["file"], "a.txt");
    assert_eq!(matches[0]["line"], 2);
    assert_eq!(matches[0]["text"], "beta");
    assert!(matches[1]["file"].as_str().unwrap().ends_with("b.txt"));
    assert_eq!(matches[1]["line"], 2);

    // Invalid regex is a clean error, not a panic.
    assert!(
        t.call("fs.search", json!({"path": ".", "pattern": "["}), cwd)
            .await
            .is_err()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn shell_exec_captures_output_and_exit_code() {
    let t = tools(BuiltinConfig::default());
    let r = t
        .call("shell.exec", json!({"command": "echo hi && exit 3"}), "")
        .await
        .unwrap();
    assert!(r["stdout"].as_str().unwrap().contains("hi"));
    assert_eq!(r["exit_code"], 3);
    assert_eq!(r["timed_out"], false);
}

#[cfg(unix)]
#[tokio::test]
async fn shell_exec_timeout_kills() {
    let t = tools(BuiltinConfig::default());
    let start = std::time::Instant::now();
    let r = t
        .call(
            "shell.exec",
            json!({"command": "sleep 60", "timeout_secs": 1}),
            "",
        )
        .await
        .unwrap();
    assert_eq!(r["timed_out"], true);
    assert!(r["exit_code"].is_null());
    // Killed, not awaited to completion.
    assert!(start.elapsed() < std::time::Duration::from_secs(30));
}

#[tokio::test]
async fn allowed_paths_confines_fs_tools() {
    let dir = temp_dir("sandbox");
    let inside = dir.join("inside");
    std::fs::create_dir_all(&inside).unwrap();
    std::fs::write(inside.join("ok.txt"), "hi").unwrap();
    let cfg = BuiltinConfig {
        allowed_paths: vec![inside.to_string_lossy().into_owned()],
        ..Default::default()
    };
    let t = tools(cfg);
    let cwd = inside.to_str().unwrap();

    // In-sandbox relative path resolves against cwd and works.
    t.call("fs.read", json!({"path": "ok.txt"}), cwd)
        .await
        .unwrap();

    // Absolute escape → rejected.
    assert!(
        t.call("fs.read", json!({"path": "/etc/hosts"}), cwd)
            .await
            .is_err()
    );
    // `..` escape → rejected (the non-existent tail can't launder it).
    assert!(
        t.call("fs.read", json!({"path": "../outside.txt"}), cwd)
            .await
            .is_err()
    );
    assert!(
        t.call(
            "fs.write",
            json!({"path": "../evil.txt", "content": "x"}),
            cwd,
        )
        .await
        .is_err()
    );
    // Writes inside the sandbox still work.
    t.call("fs.write", json!({"path": "new.txt", "content": "x"}), cwd)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(inside.join("new.txt")).unwrap(),
        "x"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn config_flags_gate_the_toolset() {
    // enabled = false → empty toolset.
    let t = tools(BuiltinConfig {
        enabled: Some(false),
        ..Default::default()
    });
    assert!(t.openai_tools().is_empty());
    assert!(!t.has_tool("fs.read"));
    assert!(t.call("fs.read", json!({"path": "x"}), "").await.is_err());

    // shell = false → shell.exec absent, fs.* still present.
    let t = tools(BuiltinConfig {
        shell: Some(false),
        ..Default::default()
    });
    assert!(t.has_tool("fs.read"));
    assert!(!t.has_tool("shell.exec"));
    let decls = t.openai_tools();
    let names: Vec<&str> = decls
        .iter()
        .filter_map(|d| d["function"]["name"].as_str())
        .collect();
    assert_eq!(names.len(), 5);
    assert!(!names.contains(&"shell.exec"));

    // auto_approve applies uniformly to builtin tools only.
    let t = tools(BuiltinConfig {
        auto_approve: Some(true),
        ..Default::default()
    });
    assert!(t.auto_approve("fs.read"));
    assert!(t.auto_approve("shell.exec"));
    assert!(!t.auto_approve("other.tool"));

    // Defaults: enabled, shell on, no auto-approve.
    let t = tools(BuiltinConfig::default());
    assert_eq!(t.openai_tools().len(), 6);
    assert!(!t.auto_approve("fs.read"));
}
