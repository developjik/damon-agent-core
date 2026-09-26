# Damon v2 redesign — native protocol adapters

**English** | [한국어](redesign.ko.md)

> Status: **implemented** (2026-09-24). ACP removed; three agents
> (Claude Code / Codex / Oh My Pi) driven directly over their native
> protocols. Client API v2 finalized — see `docs/protocol-v2.md`.

## 1. Goals and scope

**What changes**

- Agent integration: ACP v1 → per-agent native stdio protocols
- Supported agents: catalog of 19 → 3 (claude, codex, omp)
- Process model: one process per agent (ACP multiplexing) → one process per session
- Client API: ACP-shaped JSON-RPC → Damon's own protocol

**What stays**

- SQLite + FTS5 session store, search
- Channel bridges (Telegram/Discord/Slack)
- Relay tunnel (X25519 + AES-256-GCM)
- Auth / rate limiting / metrics / SSE fanout
- Distribution channels (npm, crates.io, Homebrew, service registration)

## 2. Native protocol per agent

| | Claude Code | Codex | Oh My Pi |
|---|---|---|---|
| Launch | `claude -p --output-format stream-json --input-format stream-json --verbose` | `codex app-server` | `omp --mode rpc` |
| Wire | stdio NDJSON (non-RPC frames) | stdio NDJSON, JSON-RPC 2.0 | stdio NDJSON, typed frames |
| Handshake | first `system.init` frame (obtains session_id) | `initialize` | `ready` frame → `negotiate_protocol` v2 |
| Session resume | respawn with `--resume <session_id>` | `thread/resume` | `switch_session` |
| Prompt | stdin user message | `turn/start` | `prompt` command |
| Permissions | `control_request.can_use_tool` → `control_response` | approval request/response | `extension_ui_request` → `extension_ui_response` |
| Interrupt | `control_request.interrupt` | `turn/interrupt` | `abort` |
| History | replay of the session file | thread history | `get_messages_page` |
| Native session file | `~/.claude/projects/{cwd}/*.jsonl` | `~/.codex/sessions/**/rollout-*.jsonl` | session file (jsonl) |

## 3. New architecture

```
[clients] ──WS──▶ damond
                    │  SessionManager: damon_session_id → Box<dyn AgentSession>
                    │
              AgentClient (brand-facing)          AgentSession (one conversation)
              - is_available                    - start_turn / steer
              - fetch_catalog (models+modes)    - subscribe → StreamEvent
              - create_session                  - respond_to_permission
              - resume_session                  - interrupt / close
              - list_importable_sessions        - history / persistence_handle
                    │
        ┌───────────┼───────────┐
   ClaudeBackend CodexBackend OmpBackend
        └───────────┴───────────┘
              NdjsonTransport (shared)
         spawn / send / subscribe / stderr ring / exit
```

### Normalized model (borrowed from Paseo)

- `StreamEvent { turn_id, kind }` — ThreadStarted, TurnStarted/Completed/Failed/Canceled,
  UsageUpdated, ModeChanged, ModelChanged, Timeline(item), PermissionRequested/Resolved,
  AttentionRequired
- `TimelineItem` — UserMessage, AssistantMessage, Reasoning, ToolCall, Todo, Error,
  Compaction, **Unknown{raw}** (original preserved when translation fails)
- `ToolCallDetail` — Shell/Read/Edit/Write/Search/Fetch/SubAgent/Plan/Unknown
- `PermissionRequest` — kind(tool/plan/question/mode/other) + actions[](the agent's own
  buttons) + suggestions (always-allow family); `PermissionResult.follow_up_prompt`
  expresses the follow-up turn after approving a plan
- `Capabilities` — streaming, session_persistence, session_listing, dynamic_modes,
  mcp_servers, reasoning_stream, steer, rewind, subagent_events
- `PersistenceHandle { provider, native_handle, metadata }` — the provider's session
  file is the source of truth; the Damon DB is a bookmark + search index

## 4. Per-file work plan

### Delete

| File | Reason |
|---|---|
| `src/acp.rs` | the whole ACP host → replaced by `backend/transport.rs` + backends |
| `src/agents.rs` | the 19-agent ACP catalog → `backend/registry.rs` (3) |
| `src/bin/damon-acp-mock.rs` | ACP mock → `src/bin/damon-mock.rs` (native mock) |
| `tests/rpc.rs` | ACP-shaped API tests → rewritten against the new API |
| `tests/channels.rs` | rewritten for the new event model |
| `tests/client.rs`, `tests/relay.rs`, `tests/service.rs`, `tests/api.rs`, `tests/boot.rs`, `tests/cli.rs` | rewritten for the new API/backends (store.rs tests mostly kept) |
| `docs/audit/` | old-design audit records — meaningless after the redesign; delete or move to `docs/audit-v1/` |
| `.gjc/` | external tool artifacts — gitignore and remove |

### Rewrite

| File | Change |
|---|---|
| `src/rpc.rs` | dispatch rewritten for the new API; session/permission routing moves into SessionManager |
| `src/client.rs` | reference client for the new wire protocol |
| `src/ui.html` | rewritten for the new event model/methods (UI skeleton reused) |
| `src/config.rs` | `[agents.X]` → `[backends.X]` (command/args/env overrides only). `mcp_servers` kept |
| `src/channel.rs` | `ClientEvent` → consuming `StreamEvent`; the permission card renders actions[] |
| `src/bin/damon.rs` | CLI onto the new API (chat/search/backup kept) |
| `npm/client.mjs` + `client.test.mjs` | client for the new protocol |
| `python/src/` + tests | client for the new protocol |
| `README.md` / `README.ko.md` | drop ACP mentions; 3 agents; new architecture |
| `docs/integration.md` (+ko) | new wire protocol doc |
| `config.example.toml` | new config schema |
| `CHANGELOG.md` | v0.3.0 (breaking) entry |

### Keep (minimal changes)

| File | Change |
| `src/store.rs` | untouched — the existing `agent`/`agent_session` columns already serve as provider/native_handle (no migration needed, confirmed) |
| `src/api.rs` | router/auth/rate-limit/metrics kept. `acp_sessions`/`permission_routes` maps removed |
| `src/relay.rs` | untouched (the transport layer is protocol-agnostic) |
| `src/telegram.rs` / `discord.rs` / `slack.rs` | untouched (ChannelApi boundary kept) |
| `src/service.rs` / `discovery.rs` / `ui.rs` | untouched |
| `src/bin/damond.rs` | doctor updated to detect the 3 backends |
| `src/bin/damon-{telegram,discord,slack}.rs` | untouched |
| `src/bin/damon-relay.rs` | untouched |
| `Formula/damon.rb`, `npm/install.js`, `scripts/` | untouched |

### New

```
src/backend/
  mod.rs        — trait + re-exports
  types.rs      — StreamEvent, TimelineItem, ToolCallDetail, Permission*, Usage, Capabilities
  transport.rs  — NdjsonTransport (generalized process/line management from acp.rs)
  registry.rs   — 3 backends registered, PATH detection, config overrides
  claude.rs     — the stream-json backend
  codex.rs      — the app-server backend
  omp.rs        — the omp --mode rpc backend
src/session.rs  — SessionManager (session id → AgentSession, permission routing, idle reaping)
tests/common/mod.rs — in-process mock backend (confirmed over a damon-mock binary)
```

## 5. Client API redesign direction

Away from ACP vocabulary. One WS connection, JSON text frames.

- Handshake: `hello { client, protocolVersion, capabilities }` → `server_info { version, backends[], capabilities }`
  - append-only schema; new events/fields gated by capability (Paseo's rule)
- Request/response: `{ id, type: "request", method, params }` → `{ id, type: "response", result|error }`
- Events: `{ type: "event", sessionId, turnId?, event: StreamEventKind }` — the normalized model exposed as-is
- Methods (draft): `session.create/resume/list/messages/search/export/fork/delete/rename/usage`,
  `turn.start/steer/cancel`, `permission.respond`, `backend.list/status`
- Auth: the existing bearer + ws_ticket kept

## 6. Migration order

1. **Foundation**: `backend/types.rs` + `transport.rs` + `registry.rs` + `session.rs`
   (coexisting with acp.rs — the old path still live)
2. **Claude backend** — the stream-json implementation, manually verified against the real CLI instead of a mock
3. **Codex backend** — app-server
4. **OMP backend** — rpc mode
5. **Daemon integration** — rpc.rs rewrite, store migration, new WS API
6. **Clients** — client.rs, damon CLI, ui.html, channel.rs, npm, python
7. **Cleanup** — delete acp.rs/agents.rs/mock/old tests, rewrite docs, CHANGELOG

Every step keeps the tree compiling. Step 5 removes the ACP path (clean cutover).

## 7. Open questions

1. The full list of Claude stream-json control_requests — the official docs sit behind the SDK, so real-world observation is needed
   (`claude -p --input-format stream-json` to observe the init frame and control flow)
2. Codex app-server's exact methods/approval payloads — verify against the `codex-rs/app-server` README
3. OMP `switch_session` arguments (session file path) and resume semantics
4. Handling in-flight permission requests when an idle session process is killed
5. Resource ceilings for the one-process-per-session model — is a concurrent-session cap needed?
