//! MCP inventory + probe — what will each agent actually load, and does
//! it even connect (adapted from upstream desktop-cc-gui's mcp module).
//!
//! Inventory reads every backend's NATIVE MCP config where the CLI
//! actually keeps it (paths verified against this machine's real files
//! and/or each CLI's own docs — see `sources_for`). REDACTION is the
//! invariant: environment keys may appear, values never; URLs lose
//! userinfo, query and fragment; stdio shows the program and an arg
//! COUNT only (args can carry `--header "Authorization: …"`); a URL
//! that cannot be parsed is dropped entirely. The raw server config
//! (with real commands/args/env) is an internal-only type — it never
//! serializes.
//!
//! The probe is explicit-request only (`mcp.probe`) — never scheduled,
//! never auto-run. It performs the MCP handshake for real (stdio:
//! spawn in its own process group, killed on exit; HTTP: POST) under
//! hard timeouts, and returns tool NAMES only.
//!
//! Writes (`mcp.setEnabled`) are CLAUDE-only this round: claude's own
//! toggle (`projects[cwd].disabledMcpServers` in ~/.claude.json — the
//! switch its TUI uses), CAS'd on the entry's content version, written
//! atomically, parse-fail never writes. Every other backend is
//! read-only with the reason: agents own their config.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

/// Total probe deadline (stdio handshake + tools/list).
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(25);
/// HTTP probe deadline.
pub const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(12);
/// Max MCP protocol version Damon speaks.
const PROTOCOL_VERSION: &str = "2024-11-05";
/// Tool names returned per probe — a listing, not an enumeration.
const PROBE_TOOL_CAP: usize = 40;
/// Max bytes per protocol line — a runaway server line is dropped
/// (the handshake matches on JSON-RPC id, not on full line content).
const LINE_CAP_BYTES: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

/// One inventoried server, fully redacted — this IS the wire shape.
#[derive(Debug, Clone)]
pub struct McpEntry {
    pub backend: String,
    pub name: String,
    /// stdio | http | sse | unknown
    pub transport: String,
    /// stdio: `<program> (<n> args redacted)` — args can carry secrets.
    pub command: Option<String>,
    /// http/sse: scheme+host+port+path with userinfo masked, query and
    /// fragment dropped; None when the URL could not be parsed.
    pub url: Option<String>,
    /// Environment KEY names — values never leave the file.
    pub env_keys: Vec<String>,
    /// Config file the entry came from.
    pub source: String,
    /// claude-json | codex-toml | qwen-json | opencode-json | droid-json
    pub format: String,
    /// Content hash — CAS handle for mcp.setEnabled.
    pub version: String,
    /// `enabled` / `disabled` when the backend tracks it (claude toggle,
    /// codex `enabled` flag); absent otherwise.
    pub enabled: Option<bool>,
    /// Why `mcp.setEnabled` is read-only for this entry (non-claude).
    pub readonly_reason: Option<String>,
}

impl McpEntry {
    pub fn to_json(&self) -> Value {
        let mut v = json!({
            "backend": self.backend,
            "name": self.name,
            "transport": self.transport,
            "source": self.source,
            "format": self.format,
            "version": self.version,
        });
        if let Some(c) = &self.command {
            v["command"] = json!(c);
        }
        if let Some(u) = &self.url {
            v["url"] = json!(u);
        }
        if !self.env_keys.is_empty() {
            v["envKeys"] = json!(self.env_keys);
        }
        if let Some(e) = self.enabled {
            v["enabled"] = json!(e);
        }
        if let Some(r) = &self.readonly_reason {
            v["readonlyReason"] = json!(r);
        }
        v
    }
}

/// One config file's read outcome — a broken source is reported, never
/// allowed to abort the inventory.
#[derive(Debug, Clone)]
pub struct SourceReport {
    pub backend: String,
    pub source: String,
    pub format: String,
    /// None = parsed fine.
    pub error: Option<String>,
}

/// Probe outcome (tool NAMES only, ≤40).
#[derive(Debug, Clone)]
pub struct ProbeResult {
    /// connected | needs_auth | failed | unsupported
    pub status: String,
    pub tools: Vec<String>,
    pub server_name: Option<String>,
    pub protocol_version: Option<String>,
    pub elapsed_ms: u64,
    pub error: Option<String>,
}

impl ProbeResult {
    pub fn of(status: &str, elapsed_ms: u64) -> Self {
        Self {
            status: status.to_string(),
            tools: vec![],
            server_name: None,
            protocol_version: None,
            elapsed_ms,
            error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Redaction (the invariant — pure and unit-tested)
// ---------------------------------------------------------------------------

/// stdio command, redacted: the program stays (users need it to
/// recognize the server), argument VALUES go — args routinely carry
/// `--header "Authorization: …"` and `-e KEY=SECRET`.
pub fn redact_command(program: &str, args_len: usize) -> String {
    if args_len == 0 {
        program.to_string()
    } else {
        format!("{program} <{args_len} args redacted>")
    }
}

/// URL, redacted: userinfo masked, query and fragment dropped entirely
/// (queries carry `?key=…`), scheme/host/port/path kept. `None` when
/// the text has no recognizable `scheme://` shape — nothing guessed.
pub fn redact_url(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    if scheme.is_empty()
        || !scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
    {
        return None;
    }
    // Everything up to the first path separator (or end) is authority.
    let slash = rest.find('/').unwrap_or(rest.len());
    let (authority, path_and_after) = rest.split_at(slash);
    // Userinfo sits before the LAST '@' inside the authority.
    let authority = match authority.rfind('@') {
        Some(at) => format!("***@{}", &authority[at + 1..]),
        None => authority.to_string(),
    };
    // Query and fragment are dropped wholesale.
    let path = path_and_after.split(['?', '#']).next().unwrap_or("");
    Some(format!("{scheme}://{authority}{path}"))
}

// ---------------------------------------------------------------------------
// Config sources (paths verified on this machine and/or per CLI docs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    ClaudeJson,
    CodexToml,
    QwenJson,
    OpencodeJson,
    DroidJson,
}

impl Format {
    fn as_str(self) -> &'static str {
        match self {
            Format::ClaudeJson => "claude-json",
            Format::CodexToml => "codex-toml",
            Format::QwenJson => "qwen-json",
            Format::OpencodeJson => "opencode-json",
            Format::DroidJson => "droid-json",
        }
    }
}

/// One backend's native MCP config location. Paths verified against
/// this machine's real files and/or each CLI's own documentation;
/// backends with neither get NO source (the honest gap).
fn sources_for(backend: &str) -> Vec<(PathBuf, Format)> {
    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_default();
    match backend {
        // ~/.claude.json (top-level mcpServers + projects[cwd] scopes);
        // CLAUDE_CONFIG_DIR relocates the whole config root.
        "claude" => vec![(
            match std::env::var("CLAUDE_CONFIG_DIR") {
                Ok(dir) if !dir.is_empty() => PathBuf::from(dir).join(".claude.json"),
                _ => home.join(".claude.json"),
            },
            Format::ClaudeJson,
        )],
        // ~/.codex/config.toml — [mcp_servers.<name>]; CODEX_HOME
        // relocates the root (verified in codex's docs).
        "codex" => vec![(
            match std::env::var("CODEX_HOME") {
                Ok(dir) if !dir.is_empty() => PathBuf::from(dir).join("config.toml"),
                _ => home.join(".codex").join("config.toml"),
            },
            Format::CodexToml,
        )],
        // ~/.qwen/settings.json — mcpServers (gemini-cli shape; the
        // CLI's own docs: docs/users/features/mcp.md).
        "qwen" => vec![(home.join(".qwen").join("settings.json"), Format::QwenJson)],
        // ~/.config/opencode/opencode.json — mcp.<name> local/remote
        // (verified on this machine: mcp.pencil).
        "opencode" => {
            let base = std::env::var("XDG_CONFIG_HOME")
                .ok()
                .filter(|d| !d.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"));
            vec![(
                base.join("opencode").join("opencode.json"),
                Format::OpencodeJson,
            )]
        }
        // ~/.factory/mcp.json — mcpServers (HTTP + stdio; Factory's MCP
        // docs; verified present on this machine, empty today).
        "droid" => vec![(
            match std::env::var("FACTORY_HOME_OVERRIDE") {
                Ok(dir) if !dir.is_empty() => PathBuf::from(dir).join(".factory").join("mcp.json"),
                _ => home.join(".factory").join("mcp.json"),
            },
            Format::DroidJson,
        )],
        // omp/pi manage MCP through their extension system — no
        // mcpServers file format verified. zcode/mimo: no verified
        // channel. Honest gaps, never guessed paths.
        _ => vec![],
    }
}

/// Every backend with a verified source.
pub fn inventoried_backends() -> Vec<&'static str> {
    vec!["claude", "codex", "qwen", "opencode", "droid"]
}

// ---------------------------------------------------------------------------
// Parsing → entries (raw stays internal; wire output is pre-redacted)
// ---------------------------------------------------------------------------

/// Internal view of one configured server. NEVER serializes — `raw`
/// carries real commands/args/env.
#[derive(Debug, Clone)]
pub struct RawServer {
    pub name: String,
    pub transport: String,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub env_keys: Vec<String>,
    /// Real env values — probe-only. Never serialized, never logged.
    pub env: serde_json::Map<String, Value>,
    pub url: Option<String>,
    pub raw: Value,
    /// claude toggle / codex `enabled`, when the format tracks it.
    pub enabled: Option<bool>,
    pub source: PathBuf,
    pub format: Format,
}

impl RawServer {
    /// The wire view — redaction happens HERE and nowhere else.
    pub fn to_entry(&self, backend: &str, readonly_reason: Option<String>) -> McpEntry {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        self.raw.to_string().hash(&mut hasher);
        self.name.hash(&mut hasher);
        if let Some(e) = self.enabled {
            e.hash(&mut hasher);
        }
        McpEntry {
            backend: backend.to_string(),
            name: self.name.clone(),
            transport: self.transport.clone(),
            command: self
                .command
                .as_deref()
                .map(|c| redact_command(c, self.args.len())),
            url: self.url.as_deref().and_then(redact_url),
            env_keys: self.env_keys.clone(),
            source: self.source.to_string_lossy().into_owned(),
            format: self.format.as_str().to_string(),
            version: format!("{:016x}", hasher.finish()),
            enabled: self.enabled,
            readonly_reason,
        }
    }
}

fn transport_of(entry: &Value) -> String {
    match entry["type"].as_str() {
        Some("http") | Some("streamable-http") => "http".to_string(),
        Some("sse") => "sse".to_string(),
        Some("local") | Some("stdio") => "stdio".to_string(),
        _ => {
            if entry.get("url").is_some() {
                "http".to_string()
            } else if entry.get("command").is_some() {
                "stdio".to_string()
            } else {
                "unknown".to_string()
            }
        }
    }
}

fn parse_json_entries(
    path: &Path,
    format: Format,
    servers: &serde_json::Map<String, Value>,
) -> Vec<RawServer> {
    servers
        .iter()
        .map(|(name, entry)| RawServer {
            name: name.clone(),
            transport: transport_of(entry),
            command: entry["command"].as_str().map(String::from),
            args: entry["args"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            env_keys: entry["env"]
                .as_object()
                .or_else(|| entry["environment"].as_object())
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default(),
            env: entry["env"]
                .as_object()
                .or_else(|| entry["environment"].as_object())
                .cloned()
                .unwrap_or_default(),
            url: entry["url"]
                .as_str()
                .or_else(|| entry["httpUrl"].as_str())
                .map(String::from),
            raw: entry.clone(),
            enabled: entry["enabled"].as_bool().or(Some(true)),
            source: path.to_path_buf(),
            format,
        })
        .collect()
}

/// Parse one config file into raw servers.
fn parse_source(path: &Path, format: Format) -> anyhow::Result<Vec<RawServer>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    match format {
        Format::ClaudeJson => {
            let v: Value = serde_json::from_str(&text).context("claude.json is not valid JSON")?;
            let mut out = parse_json_entries(path, format, empty_or_map(&v["mcpServers"]));
            // Claude has two parallel disable keys across versions and
            // scopes — a server is disabled when EITHER list carries
            // its name.
            fn disabled_in(body: &Value, name: &str) -> bool {
                ["disabledMcpServers", "disabledMcpjsonServers"]
                    .iter()
                    .any(|key| {
                        body.get(*key)
                            .and_then(|v| v.as_array())
                            .map(|a| a.iter().filter_map(|n| n.as_str()).any(|n| n == name))
                            .unwrap_or(false)
                    })
            }
            if let Some(projects) = v["projects"].as_object() {
                // Global entries: enabled unless SOME project disables
                // the same name (the inventory is a global view;
                // re-enable via mcp.setEnabled on that project's cwd).
                for srv in out.iter_mut() {
                    let disabled = projects.values().any(|body| disabled_in(body, &srv.name));
                    if srv.enabled == Some(true) && disabled {
                        srv.enabled = Some(false);
                    }
                }
                // Project scopes: each project's mcpServers, with that
                // scope's own toggle state.
                for (project, body) in projects {
                    for mut srv in
                        parse_json_entries(path, format, empty_or_map(&body["mcpServers"]))
                    {
                        srv.enabled = Some(!disabled_in(body, &srv.name));
                        srv.raw["project"] = json!(project);
                        out.push(srv);
                    }
                }
            }
            Ok(out)
        }
        Format::CodexToml => {
            let v: toml::Value =
                toml::from_str(&text).context("codex config.toml is not valid TOML")?;
            let servers = v
                .get("mcp_servers")
                .and_then(|m| m.as_table())
                .cloned()
                .unwrap_or_default();
            let mut out = vec![];
            for (name, entry) in &servers {
                let entry_json = toml_to_json(entry);
                let mut srv = RawServer {
                    name: name.clone(),
                    transport: transport_of(&entry_json),
                    command: entry_json["command"].as_str().map(String::from),
                    args: entry_json["args"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default(),
                    env_keys: entry_json["env"]
                        .as_object()
                        .map(|m| m.keys().cloned().collect())
                        .unwrap_or_default(),
                    env: entry_json["env"].as_object().cloned().unwrap_or_default(),
                    url: entry_json["url"].as_str().map(String::from),
                    raw: entry_json,
                    enabled: entry
                        .get("enabled")
                        .and_then(|e| e.as_bool())
                        .or(Some(true)),
                    source: path.to_path_buf(),
                    format,
                };
                srv.name = name.clone();
                out.push(srv);
            }
            Ok(out)
        }
        Format::QwenJson | Format::DroidJson => {
            let v: Value = serde_json::from_str(&text)
                .with_context(|| format!("{path:?} is not valid JSON"))?;
            Ok(parse_json_entries(
                path,
                format,
                empty_or_map(&v["mcpServers"]),
            ))
        }
        Format::OpencodeJson => {
            let v: Value =
                serde_json::from_str(&text).context("opencode.json is not valid JSON")?;
            Ok(parse_json_entries(path, format, empty_or_map(&v["mcp"])))
        }
    }
}

fn empty_or_map(v: &Value) -> &serde_json::Map<String, Value> {
    static EMPTY: std::sync::OnceLock<serde_json::Map<String, Value>> = std::sync::OnceLock::new();
    v.as_object()
        .unwrap_or_else(|| EMPTY.get_or_init(serde_json::Map::new))
}

/// toml::Value → serde_json::Value (lossy on exotic types — dates do
/// not appear in mcp servers tables).
fn toml_to_json(v: &toml::Value) -> Value {
    match v {
        toml::Value::String(s) => json!(s),
        toml::Value::Integer(i) => json!(i),
        toml::Value::Float(f) => json!(f),
        toml::Value::Boolean(b) => json!(b),
        toml::Value::Array(a) => Value::Array(a.iter().map(toml_to_json).collect()),
        toml::Value::Table(t) => {
            let mut m = serde_json::Map::new();
            for (k, val) in t {
                m.insert(k.clone(), toml_to_json(val));
            }
            Value::Object(m)
        }
        toml::Value::Datetime(d) => json!(d.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Inventory orchestration
// ---------------------------------------------------------------------------

/// Read-only default — inventory runs through spawn_blocking per source
/// (fs reads); a broken source reports an error and the rest continues.
pub async fn inventory() -> Result<(Vec<McpEntry>, Vec<SourceReport>)> {
    let mut entries = Vec::new();
    let mut reports = Vec::new();
    for backend in inventoried_backends() {
        for (path, format) in sources_for(backend) {
            let job_path = path.clone();
            let parsed = tokio::task::spawn_blocking(move || parse_source(&job_path, format))
                .await
                .context("inventory read join failed")?;
            match parsed {
                Ok(servers) => {
                    reports.push(SourceReport {
                        backend: backend.to_string(),
                        source: path.to_string_lossy().into_owned(),
                        format: format.as_str().to_string(),
                        error: None,
                    });
                    let reason = readonly_reason(backend);
                    for srv in servers {
                        entries.push(srv.to_entry(backend, reason.clone()));
                    }
                }
                Err(e) => {
                    reports.push(SourceReport {
                        backend: backend.to_string(),
                        source: path.to_string_lossy().into_owned(),
                        format: format.as_str().to_string(),
                        error: Some(format!("{e:#}")),
                    });
                }
            }
        }
    }
    Ok((entries, reports))
}

/// `mcp.setEnabled` is claude-only this round — every other backend
/// carries the reason on its entries.
pub fn readonly_reason(backend: &str) -> Option<String> {
    match backend {
        "claude" => None,
        other => Some(format!(
            "{other} has no verified config toggle — agents own their config; edit its native MCP config directly"
        )),
    }
}

/// Look up one raw server (fresh from the config — probing uses the
/// real command/args/env, which never reach the wire).
pub fn find_raw_server(backend: &str, name: &str) -> Result<Option<RawServer>> {
    for (path, format) in sources_for(backend) {
        if let Some(srv) = parse_source(&path, format)
            .unwrap_or_default()
            .into_iter()
            .find(|s| s.name == name)
        {
            return Ok(Some(srv));
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// setEnabled — claude toggle, CAS'd, atomic, serialized
// ---------------------------------------------------------------------------

/// Flip claude's own per-project toggle for one server. CAS on
/// `expected_version` (external edits reject); the file must parse
/// (parse-fail never writes); same-path writes serialize on a module
/// lock; the replace is atomic (temp file + rename in the same dir).
pub fn set_enabled_blocking(
    name: &str,
    enabled: bool,
    expected_version: &str,
    cwd: &Path,
) -> Result<(), SetEnabledError> {
    // Same-path writes serialize — two surfaces flipping the toggle
    // line up behind the lock instead of last-write-wins.
    static CLAUDE_JSON_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    let Some((path, format)) = sources_for("claude").into_iter().next() else {
        return Err(SetEnabledError::Readonly);
    };
    if format != Format::ClaudeJson {
        return Err(SetEnabledError::Readonly);
    }
    let _guard = CLAUDE_JSON_LOCK.lock();

    let text = std::fs::read_to_string(&path).map_err(|e| {
        tracing::debug!(path = %path.display(), error = %e, "mcp setEnabled: config not readable");
        SetEnabledError::MissingConfig
    })?;
    let mut doc: Value = serde_json::from_str(&text).map_err(|_| SetEnabledError::ParseFailed)?;

    // Locate the server: global scope first, then any project scope.
    let mut found_version: Option<String> = None;
    let mut found_where: Option<(Value, bool)> = None; // (raw, disabled)
    if let Some(srv) = doc["mcpServers"].get_mut(name) {
        let raw = srv.clone();
        let disabled = doc["projects"][cwd.to_string_lossy().as_ref()]["disabledMcpServers"]
            .as_array()
            .map(|a| a.iter().filter_map(|n| n.as_str()).any(|n| n == name))
            .unwrap_or(false);
        found_version = Some(entry_version(&raw, name, !disabled));
        found_where = Some((raw, disabled));
    }
    if found_where.is_none()
        && let Some(projects) = doc["projects"].as_object()
    {
        for body in projects.values() {
            if let Some(srv) = body["mcpServers"].get(name) {
                let disabled = body["disabledMcpServers"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|n| n.as_str()).any(|n| n == name))
                    .unwrap_or(false);
                found_version = Some(entry_version(srv, name, !disabled));
                found_where = Some((srv.clone(), disabled));
                break;
            }
        }
    }
    let Some((_raw, _current_disabled)) = found_where else {
        tracing::debug!(name, "mcp setEnabled: no such server in claude config");
        return Err(SetEnabledError::UnknownServer);
    };
    tracing::debug!(
        name,
        found = ?found_version.as_deref(),
        expected = ?expected_version,
        "mcp setEnabled CAS"
    );
    if found_version.as_deref() != Some(expected_version) {
        tracing::debug!(name, "mcp setEnabled: version conflict (external edit)");
        return Err(SetEnabledError::Conflict);
    }

    // The toggle: claude's own switch — projects[cwd].disabledMcpServers.
    let project_key = cwd.to_string_lossy().into_owned();
    {
        // A config without a projects map gains one (claude's own file
        // always has it, but a minimal config may not).
        if !doc["projects"].is_object() {
            doc["projects"] = json!({});
        }
        let projects = doc["projects"]
            .as_object_mut()
            .ok_or(SetEnabledError::ParseFailed)?;
        let project = projects
            .entry(project_key.clone())
            .or_insert_with(|| json!({}));
        if !project.is_object() {
            return Err(SetEnabledError::ParseFailed);
        }
        // First toggle for this project: create the switch array.
        if !project["disabledMcpServers"].is_array() {
            project["disabledMcpServers"] = json!([]);
        }
        let list = project["disabledMcpServers"]
            .as_array_mut()
            .ok_or(SetEnabledError::ParseFailed)?;
        list.retain(|n| n.as_str() != Some(name));
        if !enabled {
            list.push(json!(name));
        }
        tracing::debug!(name, enabled, path = %path.display(), "mcp setEnabled toggle applied");
    }

    // Atomic replace: temp file in the same dir, rename over. The
    // document parsed — we only ever write parsed documents.
    let serialized =
        serde_json::to_string_pretty(&doc).map_err(|_| SetEnabledError::ParseFailed)?;
    let tmp = path.with_extension("json.damon-tmp");
    {
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| SetEnabledError::WriteFailed(path.clone(), e))?;
        use std::io::Write;
        f.write_all(serialized.as_bytes())
            .and_then(|_| f.sync_all())
            .map_err(|e| SetEnabledError::WriteFailed(path.clone(), e))?;
    }
    std::fs::rename(&tmp, &path).map_err(|e| SetEnabledError::WriteFailed(path.clone(), e))?;
    Ok(())
}

fn entry_version(raw: &Value, name: &str, enabled: bool) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    use std::hash::{Hash, Hasher};
    raw.to_string().hash(&mut hasher);
    name.hash(&mut hasher);
    enabled.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Version of a server entry as the inventory reports it (claude only
/// — the setEnabled CAS target).
pub fn entry_version_for(name: &str, cwd: &Path) -> Result<Option<String>> {
    let Some((path, format)) = sources_for("claude").into_iter().next() else {
        return Ok(None);
    };
    if format != Format::ClaudeJson {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path).map_err(|e| {
        tracing::debug!(path = %path.display(), error = %e, "mcp entry_version: config not readable");
        SetEnabledError::MissingConfig
    })?;
    let doc: Value = serde_json::from_str(&text).map_err(|_| SetEnabledError::ParseFailed)?;
    let mut version = None;
    if let Some(srv) = doc["mcpServers"].get(name) {
        let disabled = doc["projects"][cwd.to_string_lossy().as_ref()]["disabledMcpServers"]
            .as_array()
            .map(|a| a.iter().filter_map(|n| n.as_str()).any(|n| n == name))
            .unwrap_or(false);
        version = Some(entry_version(srv, name, !disabled));
    }
    if version.is_none()
        && let Some(projects) = doc["projects"].as_object()
    {
        for body in projects.values() {
            if let Some(srv) = body["mcpServers"].get(name) {
                let disabled = body["disabledMcpServers"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|n| n.as_str()).any(|n| n == name))
                    .unwrap_or(false);
                version = Some(entry_version(srv, name, !disabled));
                break;
            }
        }
    }
    Ok(version)
}

#[derive(Debug)]
pub enum SetEnabledError {
    /// The backend has no verified toggle — agents own their config.
    Readonly,
    MissingConfig,
    /// The config file does not parse — never write over it.
    ParseFailed,
    UnknownServer,
    /// The entry changed since the caller's inventory snapshot.
    Conflict,
    WriteFailed(PathBuf, std::io::Error),
}

impl std::fmt::Display for SetEnabledError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetEnabledError::Readonly => {
                write!(f, "this backend has no verified config toggle")
            }
            SetEnabledError::MissingConfig => write!(f, "claude config file not found"),
            SetEnabledError::ParseFailed => {
                write!(f, "claude config does not parse — refusing to write")
            }
            SetEnabledError::UnknownServer => write!(f, "no such server in claude config"),
            SetEnabledError::Conflict => write!(f, "server changed since the inventory snapshot"),
            SetEnabledError::WriteFailed(p, e) => {
                write!(f, "atomic write failed: {} — {e}", p.display())
            }
        }
    }
}

impl std::error::Error for SetEnabledError {}

// ---------------------------------------------------------------------------
// Probe — explicit request only, real handshake, hard timeouts
// ---------------------------------------------------------------------------

/// The probe surface — injectable so dispatch tests never spawn a
/// process. The real impl speaks MCP over stdio and streamable HTTP.
#[async_trait]
pub trait ProbeRunner: Send + Sync {
    async fn probe_stdio(
        &self,
        program: &str,
        args: &[String],
        env: &serde_json::Map<String, Value>,
    ) -> ProbeResult;
    async fn probe_http(&self, url: &str) -> ProbeResult;
}

/// Probe one configured server. Unsupported (no verified source for
/// the backend / unknown name) → status "unsupported".
pub async fn probe(runner: &dyn ProbeRunner, backend: &str, name: &str) -> Result<ProbeResult> {
    if sources_for(backend).is_empty() {
        let mut r = ProbeResult::of("unsupported", 0);
        r.error = Some(format!(
            "{backend} has no verified native MCP config source"
        ));
        return Ok(r);
    }
    let Some(srv) = find_raw_server(backend, name)? else {
        let mut r = ProbeResult::of("unsupported", 0);
        r.error = Some(format!(
            "no server named {name:?} in {backend}'s MCP config"
        ));
        return Ok(r);
    };
    let started = Instant::now();
    let result = match srv.transport.as_str() {
        "stdio" => {
            runner
                .probe_stdio(
                    &srv.command.clone().unwrap_or_default(),
                    &srv.args,
                    &srv.env,
                )
                .await
        }
        "http" | "sse" => match &srv.url {
            Some(url) => runner.probe_http(url).await,
            None => ProbeResult::of("failed", 0),
        },
        _ => ProbeResult::of("unsupported", 0),
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(ProbeResult {
        elapsed_ms,
        ..result
    })
}

/// Real handshake. stdio: spawn (own process group, killed on exit),
/// `initialize` → skip everything until the matching JSON-RPC id →
/// `notifications/initialized` → `tools/list`, per-line 1 MiB cap.
/// HTTP: streamable-HTTP POSTs with the echoed `Mcp-Session-Id`.
pub struct RealProbeRunner;

impl RealProbeRunner {
    fn parse_handshake(line: &str, id: u64) -> Option<(String, String)> {
        let v: Value = serde_json::from_str(line).ok()?;
        if v["id"].as_u64() != Some(id) {
            return None; // banner/noise line — skipped until our id
        }
        if v["error"].is_object() {
            return None;
        }
        let result = &v["result"];
        Some((
            result["serverInfo"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            result["protocolVersion"]
                .as_str()
                .unwrap_or(PROTOCOL_VERSION)
                .to_string(),
        ))
    }
}

#[async_trait]
impl ProbeRunner for RealProbeRunner {
    async fn probe_stdio(
        &self,
        program: &str,
        args: &[String],
        env: &serde_json::Map<String, Value>,
    ) -> ProbeResult {
        let started = Instant::now();
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        cmd.process_group(0);
        for (k, v) in env {
            if let Some(v) = v.as_str() {
                cmd.env(k, v);
            }
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let mut r = ProbeResult::of("failed", started.elapsed().as_millis() as u64);
                r.error = Some(format!("spawn failed: {e:#}"));
                return r;
            }
        };
        let mut stdin = child.stdin.take().expect("stdin piped");
        let mut stdout = child.stdout.take().expect("stdout piped");
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut result;
        let _ = stdin
            .write_all(
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{{\"protocolVersion\":\"{PROTOCOL_VERSION}\",\"capabilities\":{{}},\"clientInfo\":{{\"name\":\"damon\",\"version\":\"{}\"}}}}}}\n",
                    env!("CARGO_PKG_VERSION")
                )
                .as_bytes(),
            )
            .await;

        // Read lines under the cap, skipping noise until id 1 answers.
        let deadline = Instant::now() + PROBE_TIMEOUT;
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        let mut handshake: Option<(String, String)> = None;
        while handshake.is_none() {
            if Instant::now() >= deadline {
                break;
            }
            buf.clear();
            loop {
                match tokio::time::timeout(
                    deadline.saturating_duration_since(Instant::now()),
                    stdout.read(&mut byte),
                )
                .await
                {
                    Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                    Ok(Ok(_)) => {
                        if byte[0] == b'\n' {
                            break;
                        }
                        if buf.len() < LINE_CAP_BYTES {
                            buf.push(byte[0]);
                        }
                        // Past the cap: keep draining to the newline.
                    }
                }
            }
            if buf.is_empty() {
                break;
            }
            handshake = Self::parse_handshake(&String::from_utf8_lossy(&buf), 1);
        }

        if let Some((server_name, protocol_version)) = handshake {
            let _ = stdin
                .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
                .await;
            let _ = stdin
                .write_all(
                    b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n",
                )
                .await;
            let mut tools = Vec::new();
            let mut saw_tools = false;
            while !saw_tools && Instant::now() < deadline {
                buf.clear();
                let mut over = false;
                loop {
                    match tokio::time::timeout(
                        deadline.saturating_duration_since(Instant::now()),
                        stdout.read(&mut byte),
                    )
                    .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                        Ok(Ok(_)) => {
                            if byte[0] == b'\n' {
                                break;
                            }
                            if buf.len() < LINE_CAP_BYTES {
                                buf.push(byte[0]);
                            } else {
                                over = true;
                            }
                        }
                    }
                }
                if over || buf.is_empty() {
                    // A line over the cap cannot be the tools reply we
                    // asked for within budget — keep draining; a server
                    // exceeding 1 MiB for its tool list is dropped.
                    if buf.is_empty() {
                        break;
                    }
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<Value>(&String::from_utf8_lossy(&buf))
                    && v["id"].as_u64() == Some(2)
                    && v["result"].is_object()
                {
                    for t in v["result"]["tools"].as_array().into_iter().flatten() {
                        if let Some(name) = t["name"].as_str() {
                            tools.push(name.to_string());
                            if tools.len() >= PROBE_TOOL_CAP {
                                break;
                            }
                        }
                    }
                    saw_tools = true;
                }
            }
            result = ProbeResult {
                status: "connected".to_string(),
                tools,
                server_name: Some(server_name),
                protocol_version: Some(protocol_version),
                elapsed_ms: started.elapsed().as_millis() as u64,
                error: None,
            };
        } else {
            result = ProbeResult::of("failed", started.elapsed().as_millis() as u64);
            result.error = Some("no initialize response within the probe window".to_string());
        }

        // Own process group — the kill takes the server's children down
        // too; kill_on_drop is the backstop.
        if let Some(pid) = child.id() {
            #[cfg(unix)]
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
            #[cfg(not(unix))]
            let _ = pid;
        }
        let _ = child.start_kill();
        let _ = child.wait().await;
        result.elapsed_ms = started.elapsed().as_millis() as u64;
        result
    }

    async fn probe_http(&self, url: &str) -> ProbeResult {
        let started = Instant::now();
        let client = match reqwest::Client::builder()
            .timeout(HTTP_PROBE_TIMEOUT)
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                let mut r = ProbeResult::of("failed", 0);
                r.error = Some(format!("client build failed: {e:#}"));
                return r;
            }
        };
        let initialize = json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "damon", "version": env!("CARGO_PKG_VERSION")},
            }
        });
        let resp = match client
            .post(url)
            .header("Accept", "application/json, text/event-stream")
            .json(&initialize)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let mut r = ProbeResult::of("failed", started.elapsed().as_millis() as u64);
                r.error = Some(format!("request failed: {e:#}"));
                return r;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return ProbeResult::of("needs_auth", started.elapsed().as_millis() as u64);
        }
        if !status.is_success() {
            let mut r = ProbeResult::of("failed", started.elapsed().as_millis() as u64);
            r.error = Some(format!("HTTP {status}"));
            return r;
        }
        let session = resp
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let body = resp.text().await.unwrap_or_default();
        let parsed = parse_http_body(&body);
        let Some(init) = parsed else {
            let mut r = ProbeResult::of("failed", started.elapsed().as_millis() as u64);
            r.error = Some("no JSON-RPC result in the response".to_string());
            return r;
        };
        let server_name = init["result"]["serverInfo"]["name"]
            .as_str()
            .map(String::from);
        let protocol_version = init["result"]["protocolVersion"]
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| PROTOCOL_VERSION.to_string());

        // initialized notification, then tools/list — echoing the
        // server's session id when streamable HTTP assigned one.
        let mut req = client
            .post(url)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        if let Some(sid) = &session {
            req = req.header("Mcp-Session-Id", sid);
        }
        let _ = req.send().await;
        let mut req = client
            .post(url)
            .header("Accept", "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}
            }));
        if let Some(sid) = &session {
            req = req.header("Mcp-Session-Id", sid);
        }
        let mut result = ProbeResult {
            status: "connected".to_string(),
            tools: vec![],
            server_name,
            protocol_version: Some(protocol_version),
            elapsed_ms: started.elapsed().as_millis() as u64,
            error: None,
        };
        if let Ok(resp) = req.send().await
            && let Ok(body) = resp.text().await
            && let Some(tools_reply) = parse_http_body(&body)
        {
            for t in tools_reply["result"]["tools"]
                .as_array()
                .into_iter()
                .flatten()
            {
                if let Some(name) = t["name"].as_str() {
                    result.tools.push(name.to_string());
                    if result.tools.len() >= PROBE_TOOL_CAP {
                        break;
                    }
                }
            }
        }
        result.elapsed_ms = started.elapsed().as_millis() as u64;
        result
    }
}

/// Streamable-HTTP responses are either JSON or an SSE stream whose
/// `data:` lines carry the JSON — handle both.
fn parse_http_body(body: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        return Some(v);
    }
    for line in body.lines() {
        if let Some(data) = line.strip_prefix("data:")
            && let Ok(v) = serde_json::from_str::<Value>(data.trim())
            && (v.get("id").is_some() || v.get("result").is_some())
        {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Redaction (the invariant) --------------------------------

    #[test]
    fn redact_command_shows_program_and_arg_count_only() {
        assert_eq!(redact_command("npx", 0), "npx");
        assert_eq!(
            redact_command("node", 3),
            "node <3 args redacted>",
            "args can carry --header Authorization and -e KEY=SECRET"
        );
    }

    #[test]
    fn redact_url_masks_userinfo_drops_query_and_fragment() {
        assert_eq!(
            redact_url("https://user:secret@example.com/mcp?api-key=kfj3#frag"),
            Some("https://***@example.com/mcp".to_string()),
            "userinfo masked, query + fragment dropped"
        );
        assert_eq!(
            redact_url("http://example.com/sse"),
            Some("http://example.com/sse".to_string())
        );
        // No recognizable scheme → nothing guessed.
        assert_eq!(redact_url("not a url"), None);
        assert_eq!(redact_url(""), None);
    }

    // ---- Parsers over fixture files --------------------------------

    /// Fixture dirs for every format + the env vars the CLIs actually
    /// honor, pointed at them.
    struct Fixtures {
        _keep: tempfile_guard::Guard,
        claude: PathBuf,
        codex: PathBuf,
        qwen: PathBuf,
        opencode: PathBuf,
        droid: PathBuf,
    }

    mod tempfile_guard {
        use std::path::PathBuf;
        pub struct Guard(pub PathBuf);
        impl Drop for Guard {
            fn drop(&mut self) {
                std::fs::remove_dir_all(&self.0).ok();
            }
        }
    }

    fn fixtures() -> Fixtures {
        let root = std::env::temp_dir().join(format!(
            "damon-mcp-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(root.join("opencode")).unwrap();
        std::fs::create_dir_all(root.join("factory")).unwrap();
        let claude = root.join("claude.json");
        std::fs::write(
            &claude,
            r#"{
  "mcpServers": {
    "global-server": {"type": "stdio", "command": "npx", "args": ["-y", "@x/serve"], "env": {"API_KEY": "sk-secret-value"}}
  },
  "projects": {
    "/tmp/work": {
      "mcpServers": {"project-server": {"command": "./serve.sh", "args": ["--token", "abc123"]}},
      "disabledMcpServers": ["project-server"]
    }
  }
}"#,
        )
        .unwrap();
        let codex = root.join("config.toml");
        std::fs::write(
            &codex,
            r#"[mcp_servers.docs]
url = "https://docs.example.com/mcp?token=abc"

[mcp_servers.localone]
command = "uvx"
args = ["some-mcp"]
env = { TOKEN = "toml-secret" }
enabled = false
"#,
        )
        .unwrap();
        let qwen = root.join("settings.json");
        std::fs::write(
            &qwen,
            r#"{"mcpServers": {"fetch": {"command": "qwen-fetch", "args": [], "env": {}}}}"#,
        )
        .unwrap();
        // XDG shape: $XDG_CONFIG_HOME/opencode/opencode.json.
        let opencode = root.join("opencode").join("opencode.json");
        std::fs::write(
            &opencode,
            r#"{"mcp": {"localone": {"type": "local", "command": ["bun", "x", "mcp"], "environment": {"K": "V"}}}}"#,
        )
        .unwrap();
        let droid = root.join("mcp.json");
        std::fs::write(
            &droid,
            r#"{"mcpServers": {"remote-one": {"url": "https://mcp.example.com/sse?session=xyz"}}}"#,
        )
        .unwrap();
        Fixtures {
            _keep: tempfile_guard::Guard(root.clone()),
            claude,
            codex,
            qwen,
            opencode,
            droid,
        }
    }

    #[test]
    fn claude_json_yields_global_and_project_entries_with_toggle_state() {
        let fx = fixtures();
        let entries = parse_source(&fx.claude, Format::ClaudeJson).unwrap();
        assert_eq!(entries.len(), 2, "global + project: {entries:?}");

        let global = entries.iter().find(|e| e.name == "global-server").unwrap();
        assert_eq!(global.transport, "stdio");
        // RAW keeps the program (args too); the WIRE is redacted.
        assert_eq!(global.command.as_deref(), Some("npx"));
        assert_eq!(global.args.len(), 2);
        let wire = global.to_entry("claude", None).to_json();
        assert_eq!(wire["command"], "npx <2 args redacted>", "{wire}");
        assert_eq!(global.env_keys, ["API_KEY"]);
        assert_eq!(global.enabled, Some(true));

        let project = entries.iter().find(|e| e.name == "project-server").unwrap();
        assert_eq!(project.enabled, Some(false), "disabledMcpServers toggle");
        assert_eq!(project.command.as_deref(), Some("./serve.sh"));
        let wire = project.to_entry("claude", None).to_json();
        assert_eq!(wire["command"], "./serve.sh <2 args redacted>", "{wire}");
    }

    #[test]
    fn claude_parse_failure_is_reported_not_fatal() {
        let dir = std::env::temp_dir().join(format!("damon-mcp-bad-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("claude.json");
        std::fs::write(&path, "{ not json").unwrap();
        assert!(parse_source(&path, Format::ClaudeJson).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn codex_toml_yields_http_and_stdio_with_enabled_flag() {
        let fx = fixtures();
        let entries = parse_source(&fx.codex, Format::CodexToml).unwrap();
        assert_eq!(entries.len(), 2, "{entries:?}");
        let http = entries.iter().find(|e| e.name == "docs").unwrap();
        assert_eq!(http.transport, "http");
        // The query string carried a token — dropped by redaction at
        // the wire boundary (the raw keeps it for the probe).
        let wire = http.to_entry("codex", None).to_json();
        assert_eq!(wire["url"], "https://docs.example.com/mcp", "{wire}");
        let raw_url = http.url.as_deref().unwrap();
        assert!(
            raw_url.contains("?token=abc"),
            "raw keeps the URL for the probe"
        );
        let local = entries.iter().find(|e| e.name == "localone").unwrap();
        assert_eq!(local.enabled, Some(false));
        assert_eq!(local.env_keys, ["TOKEN"]);
    }

    #[test]
    fn qwen_opencode_droid_parse_their_shapes() {
        let fx = fixtures();
        let qwen = parse_source(&fx.qwen, Format::QwenJson).unwrap();
        assert_eq!(qwen.len(), 1);
        assert_eq!(qwen[0].name, "fetch");
        assert_eq!(qwen[0].transport, "stdio");

        let opencode = parse_source(&fx.opencode, Format::OpencodeJson).unwrap();
        assert_eq!(opencode[0].name, "localone");
        assert_eq!(opencode[0].transport, "stdio");
        assert_eq!(opencode[0].env_keys, ["K"]);

        let droid = parse_source(&fx.droid, Format::DroidJson).unwrap();
        assert_eq!(droid[0].name, "remote-one");
        assert_eq!(droid[0].transport, "http");
        // Session query dropped — it carried a session secret. The
        // RAW keeps the full URL for the probe; the WIRE is redacted.
        assert!(droid[0].url.as_deref().unwrap().contains("?session=xyz"));
        let wire = droid[0].to_entry("droid", None).to_json();
        assert_eq!(wire["url"], "https://mcp.example.com/sse", "{wire}");
    }

    /// The wire view is the redaction boundary: the serialized entry
    /// never contains a secret VALUE, only the key names.
    #[test]
    fn wire_entries_never_carry_secret_values() {
        let fx = fixtures();
        for (path, format) in [
            (&fx.claude, Format::ClaudeJson),
            (&fx.codex, Format::CodexToml),
            (&fx.qwen, Format::QwenJson),
            (&fx.opencode, Format::OpencodeJson),
            (&fx.droid, Format::DroidJson),
        ] {
            for srv in parse_source(path, format).unwrap() {
                let entry = srv.to_entry("test", None).to_json().to_string();
                assert!(!entry.contains("sk-secret-value"), "{entry}");
                assert!(!entry.contains("toml-secret"), "{entry}");
                assert!(!entry.contains("abc123"), "{entry}");
                assert!(!entry.contains("kfj3"), "{entry}");
                assert!(!entry.contains("session=xyz"), "{entry}");
                // Key names ARE allowed through.
                if !srv.env_keys.is_empty() {
                    assert!(entry.contains("envKeys"), "{entry}");
                }
            }
        }
    }

    /// Inventory orchestration with the env vars the CLIs actually
    /// honor, pointed at fixture dirs — sources resolve, entries come
    /// back redacted, and the env-var reads are the documented ones.
    #[test]
    fn inventory_resolves_sources_through_cli_env_overrides() {
        let fx = fixtures();
        // The env vars are process-global — serialize against other
        // tests that read them.
        let _lock = crate::tests_env_lock();
        // SAFETY: process-global env mutation, serialized by the lock
        // above (same pattern as the skills-hub tests).
        unsafe {
            std::env::set_var("CLAUDE_CONFIG_DIR", fx._keep.0.to_str().unwrap());
            std::env::set_var("CODEX_HOME", fx._keep.0.to_str().unwrap());
            std::env::set_var("XDG_CONFIG_HOME", fx._keep.0.to_str().unwrap());
            std::env::set_var("FACTORY_HOME_OVERRIDE", fx._keep.0.to_str().unwrap());
        }

        // claude: $CLAUDE_CONFIG_DIR/.claude.json — copy the fixture.
        std::fs::copy(&fx.claude, fx._keep.0.join(".claude.json")).unwrap();
        // codex: $CODEX_HOME/config.toml ✓ (fixture is already that path).
        // opencode: $XDG_CONFIG_HOME/opencode/opencode.json ✓.
        // droid: $FACTORY_HOME_OVERRIDE/.factory/mcp.json — copy.
        let factory = fx._keep.0.join(".factory");
        std::fs::create_dir_all(&factory).unwrap();
        std::fs::copy(&fx.droid, factory.join("mcp.json")).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (entries, sources) = rt.block_on(inventory()).unwrap();

        let by_backend: std::collections::HashMap<&str, usize> =
            entries
                .iter()
                .fold(std::collections::HashMap::new(), |mut m, e| {
                    *m.entry(e.backend.as_str()).or_insert(0) += 1;
                    m
                });
        assert_eq!(by_backend.get("claude"), Some(&2), "{by_backend:?}");
        assert_eq!(by_backend.get("codex"), Some(&2), "{by_backend:?}");
        let oc_source = sources
            .iter()
            .find(|s| s.backend == "opencode")
            .expect("opencode source always reported");
        assert!(
            oc_source.error.is_none(),
            "opencode source: {oc_source:?} — entries: {by_backend:?}"
        );
        assert_eq!(
            by_backend.get("droid"),
            Some(&1),
            "droid entries missing — sources: {sources:?} entries: {entries:?}"
        );
        assert!(
            !by_backend.contains_key("qwen"),
            "qwen's home is not overridden — the real ~/.qwen decides"
        );

        // Sources all parsed — EXCEPT qwen, whose real ~/.qwen/settings.json
        // does not exist on the test machine (the reported error is the
        // documented degradation, not a bug).
        assert!(
            sources
                .iter()
                .filter(|s| s.backend != "qwen")
                .all(|s| s.error.is_none()),
            "{sources:?}"
        );
        // And the wire is still clean.
        let wire = entries
            .iter()
            .map(|e| e.to_json().to_string())
            .collect::<Vec<_>>()
            .join("");
        assert!(!wire.contains("sk-secret-value"));
        assert!(!wire.contains("toml-secret"));
    }

    // ---- setEnabled (claude toggle, CAS, atomic) -------------------

    #[test]
    fn set_enabled_flips_toggles_with_cas_and_fails_closed() {
        // The env lock is held for the WHOLE body: parallel tests also
        // mutate CLAUDE_CONFIG_DIR, and the parse+assert below reads it.
        let _env = crate::tests_env_lock();
        let fx = fixtures();
        // SAFETY: serialized by the lock above.
        unsafe {
            std::env::set_var("CLAUDE_CONFIG_DIR", fx._keep.0.to_str().unwrap());
        }
        std::fs::copy(&fx.claude, fx._keep.0.join(".claude.json")).unwrap();
        let cwd = Path::new("/tmp/work");
        let project_server = "project-server";

        // The inventory's version is the CAS handle.
        let version = entry_version_for(project_server, cwd)
            .unwrap()
            .expect("entry exists");

        // Stale version → conflict, file untouched.
        match set_enabled_blocking(project_server, true, "stale", cwd) {
            Err(SetEnabledError::Conflict) => {}
            other => panic!("expected conflict: {other:?}"),
        }

        // Fresh version → the toggle flips. The fixture ships
        // project-server DISABLED, so enabling REMOVES it from the
        // disabled list (claude's own semantics: enabled = not on it).
        set_enabled_blocking(project_server, true, &version, cwd).unwrap();
        // And flipping back off with a fresh snapshot re-adds it.
        let version2 = entry_version_for(project_server, cwd)
            .unwrap()
            .expect("entry exists after the first flip");
        set_enabled_blocking(project_server, false, &version2, cwd).unwrap();
        let doc: Value = serde_json::from_str(
            &std::fs::read_to_string(fx._keep.0.join(".claude.json")).unwrap(),
        )
        .unwrap();
        let disabled = doc["projects"]["/tmp/work"]["disabledMcpServers"]
            .as_array()
            .unwrap();
        assert!(
            disabled.iter().any(|n| n == project_server),
            "re-disabled: {doc}"
        );

        // Global-scope server toggles too (disabled list gains it).
        let version = entry_version_for("global-server", cwd).unwrap().unwrap();
        set_enabled_blocking("global-server", false, &version, cwd).unwrap();
        let doc: Value = serde_json::from_str(
            &std::fs::read_to_string(fx._keep.0.join(".claude.json")).unwrap(),
        )
        .unwrap();
        let disabled = doc["projects"]["/tmp/work"]["disabledMcpServers"]
            .as_array()
            .unwrap();
        assert!(disabled.iter().any(|n| n == "global-server"), "{doc}");

        // Unknown server → typed error.
        match set_enabled_blocking("ghost", true, "whatever", cwd) {
            Err(SetEnabledError::UnknownServer) => {}
            other => panic!("expected unknown server: {other:?}"),
        }

        // Parse-fail never writes.
        std::fs::write(fx._keep.0.join(".claude.json"), "{ broken").unwrap();
        match set_enabled_blocking(project_server, true, "whatever", cwd) {
            Err(SetEnabledError::ParseFailed) => {}
            other => panic!("expected parse refusal: {other:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(fx._keep.0.join(".claude.json")).unwrap(),
            "{ broken",
            "the broken file was never overwritten"
        );
    }
}
