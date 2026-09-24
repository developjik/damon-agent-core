# Changelog

## Unreleased

### Changed — native-CLI backend architecture (breaking)

- **ACP dropped; backends speak their native CLI protocols.** The agent
  registry and ACP host layer are replaced by a `backend` module:
  `claude` drives `claude -p --output-format stream-json`, `codex`
  drives `codex app-server`, `omp` drives `omp --mode rpc`. Each backend
  owns its process, protocol translation, and resume token — Damon's
  normalized `StreamEvent`/`TimelineItem`/`PermissionRequest` model is
  the single contract every surface consumes.
- **Wire protocol v2.** `initialize`/`session/new`/`session/prompt`/
  `session/update` are replaced by `hello`, `session.create`,
  `turn.start`, and `session.event` pushes. One connection multiplexes
  every session; requests dispatch concurrently so a long turn never
  serializes the socket. `turn.start` resolves at turn end with
  `{turnId, stopReason, usage}`; events stream as
  `{event:"session.event", sessionId, data}`.
- **Config renames.** `[agents.X]` → `[backends.X]`,
  `default_agent` → `default_backend`. Old keys are rejected
  (`deny_unknown_fields`) — rename on upgrade.
- **Permission flow.** `session/request_permission` is now a
  `permission_requested` event answered by `permission.respond
  {sessionId, requestId, response:{behavior:"allow"|"deny",…}}`.
- **`damon-acp-mock` removed** — tests use an in-process mock backend.

### Added — wider zero-config agent catalog

- Eight more CLIs self-register from PATH, each speaking ACP natively
  (no Node needed): `gemini` (`gemini --acp`), `copilot`
  (`copilot --acp`), `cursor` (`cursor-agent acp`), `qwen` (`qwen --acp`),
  `kimi` (`kimi acp`), `grok` (`grok agent stdio`), `droid`
  (`droid exec --output-format acp-daemon`), and `auggie` (`auggie --acp`).
  Droid and Auggie launch with self-update disabled — Damon supervises
  the process, so the binary must not replace itself mid-run.

- Eight more CLIs self-register from PATH, all native ACP: `goose`
  (`goose acp`), `vibe` (`vibe-acp`), `cline` (`cline --acp`), `kilo`
  (`kilo acp`), `kiro` (`kiro-cli acp`), `openhands` (`openhands acp`),
  `qoder` (`qoder --acp`), and `glm` (`glm-acp-agent`, Z.AI Coding Plan).

- Catalog entries can now carry default env for the launch line
  (`[agents.X].env` still wins per key).

### Added — agent supervision

- **Crash backoff & give-up state** — a crash-looping agent process no
  longer respawns on every request: each discovered death or failed
  spawn doubles the retry window (1→2→4→…s, capped 60s) and five
  consecutive failures park the agent for ten minutes.
  `agents/list` entries gain `status` (`"ok"`/`"cooling"`/`"crashed"`),
  `crashes`, and `lastError` fields (additive; older clients ignore
  them).
- **`agent/status {id}` RPC** — per-agent observability: supervision
  state plus the live process's `pid`, `uptimeSecs`, and a bounded
  `stderrTail` (newest 256 lines; stderr is now piped instead of
  discarded, drained by a dedicated task so chatty adapters can never
  stall agent I/O). Unknown agent ids return `-32601`.
- **Idle subprocess shutdown** — new `agent_idle_secs` config (default
  1800, `0` disables): agent processes unused for that long are killed
  by a periodic sweep (agents with a live prompt are skipped) and the
  next prompt respawns them cleanly via the resume chain — no crash is
  recorded.
- **Registry hot reload** — config edits now rebuild the resolved
  agent set on the fly: new `[agents.X]` blocks appear in
  `agents/list` without a restart, changed launch lines respawn on next
  use, and vanished agents' processes are shut down.
- **`damond service start|stop|restart`** — controls the registered
  OS service (launchd `kickstart -k`/`bootout`, `systemctl --user`,
  `schtasks /Run`/`/End`) and prints the resulting status line;
  best-effort, matching `uninstall`.
- **`damon-acp-mock` knobs** — `MOCK_DIE=1` (exit right after
  initialize) and `MOCK_STDERR_MSG=<text>` (one stderr line at
  startup) for supervisor and stderr-ring testing.

### Added — session export, backups, richer search

- **`session/export {sessionId, format}` RPC + `damon export <id> [--md]`** —
  the full session as a Markdown transcript (`# title`, `**role**` blocks;
  text-less tool payloads render as fenced JSON) or a pretty JSON
  `{session, messages}` object. Oversized exports fail loudly with
  `-32602` like other frame-capped responses.
- **`damon backup [--out <path>]`** — snapshots the store with SQLite
  `VACUUM INTO`: consistent while `damond` is running (WAL), refuses to
  overwrite an existing file, and defaults to
  `<data_dir>/damon-backup-YYYYMMDD-HHMMSS.db`.
- **`session/search` filters** — `sessionId`, `before`, `after`
  (inclusive ISO-8601 bounds on the session's creation) narrow results;
  no filters behaves exactly as before.
- **Tool-text indexing** — tool-call names/inputs and tool-result text
  are now full-text searchable, as are text parts of array-form
  content. Applies to messages written from this version on; existing
  rows keep their original index entries.
- **Rust client connection state** — `ConnState` and
  `DamonClient::conn_state()` expose the reconnect supervisor's
  liveness watch, and `ClientEvent::Connected`/`Disconnected` mirror
  link drops/redials on the event stream (best-effort; the watch is
  authoritative).

### Added — session fork, SSE events, richer metrics

- **`session/fork {sessionId, uptoMessageId?}` RPC + `damon fork <id> [--upto <msgId>]`** —
  copies the session row and its messages into a new session, optionally
  only up to and including `uptoMessageId`. The agent's own session id is
  NOT copied — the fork attaches a fresh agent session on first prompt
  (`fresh: true`). Web UI gains a ⑂ fork button on session rows; chat
  bridges gain `!fork`.
- **`GET /v1/events` SSE** — Server-Sent Events fan-out of daemon lifecycle
  events: `session.started`, `session.deleted`, `session.forked`,
  `turn.started`, `turn.finished`, `agent.status`. Token-gated with the
  rest of `/v1`; a lagging subscriber sees a `lagged` comment frame.
- **`GET /metrics` expansion** — new counters `damon_live_prompts`,
  `damon_permission_waits_total`, `damon_permission_wait_ms_total`, plus
  per-agent `damon_turn_duration_ms_sum`/`_count`/`damon_turn_errors_total`.
- **`#[tracing::instrument]`** on `handle_socket`, `session_new`,
  `session_resume`, `run_turn` — structured spans for RPC dispatch and
  turn lifecycle.

### Added — chat channel UX

- **Thread-scoped sessions** — Slack `thread_ts` and Telegram
  `message_thread_id` now scope a conversation to its own session lane:
  each thread gets its own damon session, turn slot, and permission
  lane. Discord threads are channels, so they already scoped correctly.
- **Typing indicators** — Telegram (`sendChatAction`) and Discord
  (`/channels/{id}/typing`) show a working indicator while a turn runs;
  Slack bots have no typing API and skip it.
- **Non-image attachments** — text-ish payloads (`text/*`,
  `application/json`) go inline as ACP `resource` blocks; binary
  payloads go as `resource` blobs. Images keep the `image` block.
- **`always` permission reply** — when the agent offers an
  `allow-always` option, replying `always` selects it (falls back to
  `allow-once` when not offered).
- **`!delete` command** — removes the chat's session and its history
  from the daemon.

### Added — CLI polish

- **`damon --json`** — machine-readable JSON output for `sessions`,
  `usage`, `search`, `rename`, `delete`, `fork`, and `doctor`.
- **`damon doctor`** — connectivity + auth check: reports daemon
  version, agent count, and RPC method count.
- **`damon completions <shell>`** — generates shell completions
  (bash/zsh/fish/powershell/elvish) via `clap_complete`.
- **`damond --print-rpc-schema`** — prints the JSON-RPC method schema
  (same list `initialize` returns) for client codegen and docs.

### Fixed — usage accounting

- **Orphaned usage rows** — `session/delete` and the retention sweep
  now remove a session's usage rows; deleted sessions no longer bleed
  phantom costs into `session/usage` rollups.
- **Cumulative-cost over-counting** — ACP `usage_update` reports a
  CUMULATIVE per-session cost, but each turn's row stored it raw, so
  `SUM(cost_usd)` counted every intermediate value (a 0.50 + 0.80
  session reported 1.30). Rows now store per-turn deltas (raw
  cumulative kept in a new `cumulative_cost` baseline column; a
  counter reset clamps to the reported value instead of going
  negative), and pre-existing rows are migrated once on daemon start.

### Fixed — web UI + permission relay

- **Connection indicator never went green** — `setConn("on", …)` was
  never called, so the status dot stayed "connecting" forever and the
  settings drawer reported "connecting…" on a healthy link. The dot now
  flips on a successful `initialize` handshake and shows
  "handshake failed" when it doesn't.
- **Permission asks ignored `permission_timeout_secs`** — an unanswered
  card was denied only after the configured budget PLUS a hardcoded 30s
  transport slack (a 15s config denied at 45s). `run_turn` now races the
  client answer against the configured timeout and turn cancellation,
  so a cancel also interrupts a pending ask instead of waiting it out.
- **Permission card outlived its ask** — a card stayed visible after
  the ask timed out, the turn ended, the session was deleted, or the
  connection dropped. The card is now tracked per session: hidden when
  its turn ends or its session is left, and re-shown when the session
  is re-selected while the ask is still live.
- **Mid-turn session switch lost the busy state** — switching away and
  back during a live turn showed the session idle; a sent message then
  rendered its bubble and failed with "session already has a prompt in
  progress". Turns are now tracked per session (`liveTurns`), the busy
  state is restored on return, mid-turn input queues as designed, and
  chunks dropped while viewing another session are re-rendered from the
  store when the turn completes.
- **"0 turns" after a real turn** — `session/usage` counted usage rows,
  so agents that never emit `usage_update` reported 0 turns. Turns now
  count persisted prompt messages (MAX of prompts vs usage rows).
- **Sessions started with an attachment got no title** — the
  first-prompt title was only read from plain-string content; a
  text+image prompt (parts array) left the session named by its id.
  Text parts now contribute the title.
- **Search snippets false-highlighted literal brackets** — FTS5's `[`/`]`
  match markers collided with real `[`/`]` in message text. Markers are
  now control characters (U+0001/U+0002) mapped to `<mark>` in the UI.

### Changed — usage schema cleanup

- The `input_tokens`/`output_tokens` columns are dropped from the
  usage table: agents never sent token counts, so both were always 0.
  The migration is automatic on the next `damond` start (bundled
  SQLite supports `DROP COLUMN`).

### Changed — Damon is now an ACP host

Damon no longer runs its own agent runtime. It drives coding agents as
ACP (Agent Client Protocol) subprocesses and owns sessions, permissions,
history, channels, and remote access instead:

- **Zero-config agents** — `claude`, `codex`, and `opencode` binaries on
  PATH register themselves. Claude Code runs through
  `@agentclientprotocol/claude-agent-acp`, Codex through
  `@agentclientprotocol/codex-acp`, OpenCode natively (`opencode acp`).
  Any other ACP agent plugs in via `[agents.X]`. Models, tools,
  subscription auth, and context management belong to the agents.
- **`session/new` takes `agent`** — per-session agent selection; the
  legacy `model` field is interpreted as an agent id for compatibility.
  `session/resume` reattaches agent sessions across daemon restarts via
  ACP `session/resume`.
- **Permission relay** — the agent's `session/request_permission` flows
  to the web UI / CLI / chat channels (`allow`/`deny` replies) unchanged.
- **`[mcp_servers]` is forwarded to agents** at session setup; Damon no
  longer spawns MCP servers itself.
- **Web UI agent control** — the composer's free-text model box is now an
  agent picker (available agents selectable, not-installed ones shown with
  install hints), the sidebar gains an agents status panel (live dots), and
  session rows show their backing agent. Backed by a new `agents/list` RPC.
- **`damon-acp-mock`** ships as a minimal ACP agent for tests and
  protocol debugging (echo, permission ask, hang modes).
- Sessions persist their backing agent (`sessions.agent` /
  `agent_session` columns, auto-migrated); ACP `usage_update` telemetry
  feeds `session/usage`.

### Fixed — Web UI and daemon correctness

- **Assistant replies lost their last block** — the markdown renderer
  never flushed the trailing paragraph/list, so single-paragraph replies
  rendered as empty bubbles. Streaming re-renders also wiped the copy
  button and timestamp; both fixed.
- **Send button sent "[object PointerEvent]"** — the click handler passed
  the event through as the message text.
- **Image attachments never reached the agent** — the daemon flattened
  prompt blocks to their text, silently dropping `image` parts; the raw
  ACP blocks are now forwarded verbatim.
- **Sessions died after a daemon restart** — `session/resume` now falls
  back `session/resume` → `session/load` → a fresh `session/new`, and
  `session/prompt` auto-reattaches a detached session instead of failing
  with "no agent backend". The response gains a `fresh` flag so clients
  can note the context reset.
- **Permission prompts showed "Allow ?"** — the relay dropped the
  agent's `toolCall` object; it is forwarded now, so the card shows the
  tool title, kind, and input.
- **`session/usage` reported context occupancy as input tokens** — ACP
  `usage_update` (`used`/`size`/`cost`) is recorded once per turn into
  new `context_used`/`context_size`/`cost_usd` columns (auto-migrated);
  responses now return `contextUsed`, `contextSize`, `costUsd`, `turns`,
  and the UI/CLI/channels show "used/size ctx · $cost · turns".

### Changed — Web UI rework

- **Markdown-lite for agent replies** — headings, lists, task lists,
  blockquotes, links (auto-linked bare URLs too), emphasis, strikethrough,
  and inline code, all escape-first; fenced code blocks gain copy buttons.
- **Session chrome** — a header bar with the active agent badge, session
  title, and a live connection dot; a typing indicator with elapsed time
  while a turn runs; a jump-to-latest button that appears only when you
  have scrolled up (autoscroll no longer fights reading position).
- **Responsive** — under 760px the sidebar becomes an off-canvas drawer
  with a scrim; a light theme follows `prefers-color-scheme`; composer
  respects iOS safe-area insets.
- **Sidebar sessions** — rows show title, backing agent, and relative
  age, sorted newest-first; sessions are renameable (✎ action, backed by
  the existing `session/rename` RPC) with inline delete/rename buttons
  always visible on touch devices.
- **Composer** — Enter no longer fires mid-IME-composition (Korean/
  Japanese/Chinese input commits the candidate instead of sending);
  typing stays enabled while a turn runs so the next message can be
  drafted; the streaming bubble shows a blinking caret.
- **Honest states** — empty log gets a "start a session" call to action,
  empty search says so inline, `Ctrl/Cmd+K` focuses search, and the auth
  card only appears when the daemon actually rejects an unauthenticated
  ticket request (it used to pop for any unreachable daemon). Closing the
  tab mid-turn warns before unloading.

### Fixed

- `session/new` with an empty or relative `cwd` — the web UI's case — was
  rejected by ACP agents ("must be an absolute path"), making session
  creation from the browser impossible; the daemon now falls back to its
  own working directory, and `session/resume` sends the session's stored
  cwd instead of an empty one, fixing reattach after a daemon restart.
- The web UI's message log and session list could never scroll (flex
  items default to `min-height: auto`): long conversations silently
  pushed the composer below the viewport. Both now scroll in place.
- The Stop button never appeared while a turn ran: `setBusy` reset the
  inline `display` while the stylesheet kept `#cancelBtn { display: none }`.
- A missing `}` in the sidebar CSS swallowed the agents-panel block and
  the `#sidebar h1` bottom border.

### Removed

- The built-in agent runtime and provider layer: `runtime.rs` (tool loop,
  compaction), `provider/*` (OpenAI/Anthropic/Gemini transports), OAuth
  login/refresh, credential harvest, builtin fs/shell tools, and the
  `/v1` OpenAI-compatible endpoints (`/v1/chat/completions`, `/v1/models`,
  `/v1/responses`). `POST /v1/ws_ticket` remains for browser auth.
- `damond login` / `damond logout` (agents own their auth), the
  `session/compact` / `session/set_model` RPCs (agents compact and select
  models themselves), and the `[providers]` / `[models]` /
  `[builtin_tools]` config tables.

### Added

- **Token usage accounting** — `session/usage` RPC, `damon usage
  [--session ID]`, and `!usage` in chat channels report per-model
  input/output token totals and turn counts, persisted in a new `usage`
  table. Usage is recorded on every turn exit path — including errors
  and cancellations, which still cost money.
- **Session titles + rename** — `session/rename` RPC, `damon rename`,
  and auto-derived titles from the first user message; `session/list`
  and the web UI sidebar show titles instead of bare ids.
- **`/v1/responses` endpoint** — OpenAI Responses API passthrough:
  requests translate to chat-completions internally and the response
  (JSON or SSE) is re-shaped to Responses format, so Responses-only
  clients work against any configured provider.
- **Image prompts** — `session/prompt` accepts `image` content blocks;
  the web UI has an attach button with thumbnails, and Telegram photos
  are fetched via getFile and sent as image blocks.
- **Channel `!` commands** — `!new`, `!model`, `!compact`, `!usage`,
  `!help` in Telegram/Slack/Discord chats; unknown `!` commands get a
  hint instead of silently reaching the model.
- **Web UI search** — sidebar search box runs `session/search` (FTS5)
  and jumps to the matching session.

- **omp-parity provider presets** — `damond presets` lists ~26 hosted and
  local providers (Groq, OpenRouter, Mistral, xAI, DeepSeek, Fireworks,
  Together, Cerebras, NVIDIA, Moonshot/Kimi, Z.AI, BigModel, MiniMax,
  SiliconFlow, Venice, Hugging Face, Vercel AI Gateway, LiteLLM, Azure
  OpenAI, Vertex AI, Bedrock-mantle, LM Studio, llama.cpp) that
  auto-register when their key environment variable is set — env-only,
  zero Damon config; an explicit provider block of the same id wins. New
  compat wire shapes: `azure_deployment_urls` (deployment-in-path +
  `api-version` + `api-key` header), `vertex` (Vertex AI paths),
  `bearer_auth` (Anthropic-compatible Bearer). Local engines
  `lm-studio`/`llama.cpp` discover keylessly out of the box.
- **Subscription OAuth flavors: kimi-code, github-copilot, xai-oauth** —
  RFC 8628 device flows via `damond login` (`kimi-code`/`github-copilot`
  pair with `openai-completions`, `xai-oauth` with `openai-responses`);
  tokens live in the OS keychain and auto-refresh (Copilot's long-lived
  GitHub token needs no rotation). Presets register each provider as soon
  as its login exists, and `qwen-portal` joins as an env-key preset
  (`QWEN_OAUTH_TOKEN` / `QWEN_PORTAL_API_KEY`). The oauth sentinel now
  also names the flavor explicitly: `api_key = "oauth:kimi-code"` —
  required where one api kind carries several flavors, with api-kind
  validation in `Provider::new` and `damond doctor`.
- **ChatGPT 구독제 OAuth (`damond login openai`)** — Codex PKCE 플로우로
  ChatGPT Plus/Pro 구독을 그대로 사용한다. 토큰 + `chatgpt-account-id`는 OS
  키체인에 저장되고 자동 갱신되며, `api = "openai-responses"` +
  `api_key = "oauth"` 프로바이더는 ChatGPT 백엔드
  (`chatgpt.com/backend-api/codex`)로 요청을 번역해 보낸다(SSE 전용이므로
  비스트리밍 요청은 내부에서 SSE로 스트리밍 후 JSON으로 접어 돌려준다).
  401 재시도, `store:false` 강제, `chatgpt-account-id` 헤더가 포함된다.
  `damond doctor`도 OAuth 프로바이더를 올바르게 검증한다(기존에는
  `api_key = "oauth"`를 시크릿 참조로 파싱해 실패로 보고했다).
- **`~/.damon/daemon.json` discovery file** — the daemon writes
  `{port, pid, version, tls, configPath}` (0700/0600) at boot and removes
  it on shutdown, deleting only while the file still names its own pid so
  a successor daemon's file is never clobbered. Deliberately contains no
  token or secret — local clients find the port; the token still comes
  from the user or config. A stale file after SIGKILL is overwritten by
  the next boot.
- **`session/prompt` result gains `model`** — the upstream model string
  actually sent to the provider for the turn. With default-provider
  fallback the requested name passes through; with `provider/model` or
  glob routing it is the remapped upstream id, so clients can badge the
  real route taken. Absent only when the turn was cancelled before the
  first model resolution.
- **`session/compact` RPC + `damon compact <id>`** — force a context
  compaction on a session regardless of the 85% estimate threshold: the
  escape hatch for a session wedged against the real context window while
  the estimate still reads under it. Takes the session's live-prompt slot
  (rejects while a turn runs, cancellable via session/cancel or
  session/delete) and returns `{compacted, compactedThrough, reason?}`.
- **Per-session MCP servers (ACP `mcpServers`)** — `session/new` now
  accepts `{name, command, args?, env?, auto_approve?}` entries instead
  of rejecting non-empty lists. Servers spawn as a session overlay:
  their tools merge into the turn's tool list (session names shadow
  globals), permission grants stay scoped to the owning registry, and
  the overlay's stdio children are torn down on session/delete, the
  retention sweep, and daemon shutdown. Caps: 8 servers per session,
  64 live overlays. Names colliding with a configured `[mcp_servers]`
  entry are rejected rather than shadowing it.
- **Embedded web UI at `/ui`** — a zero-dependency single-file chat
  client served by the daemon: session sidebar, streaming replies,
  tool-call status, permission prompts (allow/deny), cancel, reconnect
  with backoff, and the ws_ticket auth flow when `auth_token` is set.
- **Deeper `/metrics`** — `damon_mcp_tool_calls_total`,
  `damon_mcp_tool_errors_total`, and per-provider
  `damon_provider_{requests,errors}_total` +
  `damon_provider_latency_ms_sum` (TTFB) labeled by provider name.
- **`ConnectOptions::event_capacity` + `dropped_events()`** — the Rust
  client's event channel is still bounded and lossy for Update
  notifications (a slow consumer must never stall RPC responses), but
  capacity is now configurable and every drop is counted and logged
  (once per power of two) instead of vanishing silently.
- **`damon-relay --help`/`--version`** — the relay used to ignore argv
  entirely and start the server on `--help`; unknown args now exit 2.

## 0.2.0 — unreleased

### Breaking

- **Relay wire protocol**: the E2E handshake now authenticates the
  CLIENT FIRST — the daemon answers its `sha256(token || daemon_pub ||
  client_pub)` proof only after verifying the client's, so an
  unauthenticated peer can no longer harvest proof samples to brute the
  auth_token offline. The registration secret moves from the `?secret=`
  query into the first post-upgrade frame (`{"auth": secret}`) — queries
  leak into access logs — and a duplicate registration is refused with
  an explicit `name_taken` error instead of a silent close. Pre-0.2.0
  clients cannot connect to a 0.2.0 daemon over a relay — upgrade both.
- **`?token=` WS auth removed**: query-string tokens leak into logs and
  browser history. Use the `Authorization: Bearer` header or a single-use
  `?ticket=` from `POST /v1/ws_ticket` (60s TTL). `DamonClient::connect`
  and the npm `DamonClient` do the ticket exchange automatically.

### Added

- `POST /v1/ws_ticket` — one-shot WS auth tickets.
- `GET /metrics` — Prometheus counters (requests, prompts, tokens,
  active sessions).
- `session/messages {sessionId, limit?, offset?}` — paged history.
- `session/list {limit?, offset?}` — paged session list.
- `session/request_permission` gains an `allow-always` option — approves
  the tool for the rest of the session (in-memory only).
- `stopReason: max_turn_requests` when the tool-loop iteration cap hits.
- Config: `permission_timeout_secs` (default 300), `max_tool_output`,
  `summary_model`, `session_retention_days`.
- `DamonClient` (Rust + npm) auto-reconnects with exponential backoff;
  `request()` waits up to 10s for the link. Relay connections included.
- `/v1` rate limiting on non-loopback binds (60 req/min, burst 20, per IP).
- Graceful shutdown: SIGTERM cancels live turns and drains connections
  (both plain and TLS listeners).
- CI: `cargo fmt --check`, `clippy -D warnings`, `cargo audit`.

### Fixed

- Sparse tool-call indices (Anthropic text-before-tool_use, Responses
  reasoning-before-function_call) no longer persist phantom empty calls.
- Mid-stream provider errors (Anthropic `error` events, Responses
  `failed`/`incomplete`, Gemini error payloads) now surface as errors
  instead of silently ending the turn.
- Non-streaming `finish_reason` reflects the real stop reason
  (`tool_calls`, `length`) instead of a hardcoded `"stop"`.
- Compaction failure no longer records a placeholder that permanently
  dropped half the session's context.
- `session/delete` cancels a live turn first and waits for it to wind
  down before deleting (refusing with `session busy` only if the turn
  is still wedged after 10s) instead of orphaning rows.
- MCP tool calls no longer hold the connection lock across the 300s
  timeout; a crashed server backs off exponentially instead of
  respawning on every call.
- `!cmd`/`keychain` secrets re-resolve after a 60s cache TTL instead of
  going stale until the next config reload.
- Cyclic `context_promotion_target` configs can no longer recurse
  forever — promotion retries are depth-capped.
- Client requests pending on a dead connection now fail fast instead of
  hanging.
- `set-version.sh` works on BSD/macOS sed.
- Full-audit remediation (2026-09-19, 67 defects fixed):
  - Channels: slow platform sends no longer silently drop streamed reply
    chunks (per-chat event queues are unbounded now); permission prompt
    timeout follows the configured `permission_timeout_secs` instead of a
    hardcoded 300s; chat→session mappings evict after 24h idle; the bridge
    loop backs off when an adapter returns no events.
  - Providers: tool-name mangling is collision-safe — `a.b` and a literal
    `a__b` can coexist and each call restores to the right tool (OpenAI,
    Anthropic, Gemini); unknown tool names are no longer guessed back
    (visible error instead of silent wrong-tool execution); string `stop`
    wraps to `stop_sequences`; mid-stream Anthropic `overloaded_error`
    maps to `RateLimited` backoff; negative/NaN `Retry-After` no longer
    panics; 429 backoff honors the upstream `Retry-After` on the OpenAI
    path; Gemini safety blocks report `content_filter`/the block reason
    instead of an empty reply; multimodal (image) parts translate to the
    Responses API instead of vanishing; array-form system messages survive
    in-band tool rendering and compat coalescing; streamed text keeps its
    original ordering relative to thinking events.
  - API: context-overflow promotion no longer triggers on generic
    "request too large"/"too many tokens" bodies; oversized or unreadable
    upstream error bodies surface a real message instead of an empty one;
    upstream error details (including internal URLs) are logged, not
    echoed to clients.
  - RPC: `live_prompts` bookkeeping no longer holds the session map
    across store awaits (a slow SQLite delete stalled every client);
    a single send-timeout no longer permanently disables notifications
    for a connection; non-canonical numeric JSON-RPC ids resolve.
  - Runtime: CJK-heavy sessions no longer compact ~2x too early
    (non-ASCII token estimate corrected); cancelled tool calls report
    `status: cancelled` instead of `failed`.
  - Store: search rejects leading `NOT` and handles `x AND NOT y`
    without FTS5 syntax errors or silently dropped exclusions; the data
    directory and database are created 0700/0600.
  - Relay: replacing a daemon no longer strands attached clients on a
    dead tunnel (sessions close immediately; the new generation is
    immune); over-capacity connects get an explicit `over_capacity`
    error frame; duplicate client ids can no longer corrupt slot
    bookkeeping.
  - MCP: a config reload can no longer resurrect a removed server's
    tools; transport retries are limited to send failures (side-effectful
    tools can't double-execute); concurrent reconnects spawn one child,
    not N; children are closed explicitly at daemon shutdown.
  - Client (Rust): a stalled event consumer can no longer deadlock
    `prompt()` via the pending map; the ws_ticket exchange has a
    timeout; local WS connections cap frame size like the relay path.
  - CLI/bins: `damon` exits 141 quietly on a closed pipe instead of
    panicking; `RUST_LOG` in the config-dir `.env` works; platform
    binaries warn when `--token` (visible in process lists) is used.
  - npm: reconnect dials time out (black-hole hosts no longer wedge the
    client); ticket URLs handle query strings; downloads have timeouts
    and clean up partial tarballs; Windows gets `.cmd` launchers.
  - Config/OAuth: config-file `models` globs treat `?` as one character
    (not one byte); token comparison is length-leak-free; unknown keys
    under `[models.*]` are rejected like every other section; the starter
    config is created 0600 atomically; OAuth errors keep the HTTP status
    when the error body isn't JSON.
  - CI: workflow-level `permissions: contents: read`; actions pinned to
    commit SHAs; toolchain pinned to 1.95.0; Homebrew formula sha256s
    update automatically on release.
- Re-audit remediation (2026-09-19, 12 defects fixed):
  - Runtime: the CJK token estimate is weighted to ~1 token/char —
    Korean-heavy sessions compact at the 85% threshold instead of
    wedging with context-length 400s past the window.
  - Providers: a string `reasoning` paired with a `:low|:medium|:high`
    model suffix no longer panics the Responses translator; array-form
    system messages coalesce again for single-system endpoints
    (multimodal parts preserved losslessly).
  - RPC: oversized `session/messages` / `session/list` responses fail
    loud with a paging hint instead of sending a frame the client's
    4 MiB receive cap drops (which killed the link and left the session
    permanently unreadable).
  - Relay: a saturated daemon tunnel no longer pins client-session
    slots — pump sends are bounded and attach/disconnect notices are
    best-effort, so cleanup can never wait on the tunnel draining.
  - MCP: daemon shutdown bounds the per-slot lock wait — a reload that
    is dialing a server can no longer stall exit past the 5s budget and
    force SIGKILL (which orphaned the children the hook exists to reap).
  - npm: ticket URLs preserve the path prefix before `/ws` (prefixed
    proxy deployments work again, matching the Rust client);
    handshake-timeout sockets are closed instead of abandoned; calls
    after `close()` fail fast instead of hanging 10s; the reconnect
    wait and backoff timers no longer keep the process alive.
  - CI: Windows release tarballs exclude `.pdb` debug symbols (~35% of
    the payload).
- **Relay hardening**: per-IP connection cap on unauthenticated
  `/connect` (8), per-tunnel session cap on the daemon (32), a 10s
  registration-auth deadline on the relay, and `session/delete`-style
  loud logging when a name is held by another tunnel.
- **Provider HTTP bounds**: all provider clients now carry a 15s connect
  timeout and a 120s read-idle timeout, and the initial `chat_stream`
  await participates in cancellation with a 120s TTFB deadline — a
  wedged upstream can no longer park a turn forever and permanently
  brick the session ("busy" until restart).
- **OAuth test hooks compiled out of release builds**: `DAMON_TEST_TOKEN_URL`
  /`DIR` no longer redirect token exchange/storage in release binaries,
  and the cwd `.env` auto-load is debug-only (config-dir `.env` always
  loads). Test token files are written 0600. A hostile `.env` in a
  cloned repo can no longer exfiltrate OAuth refresh tokens.
- `/v1` passthrough forwards the RESOLVED upstream model — prefix
  routing (`provider/model`) and thinking-suffix selection (`:high`)
  previously reached openai-completions upstreams verbatim and were
  rejected as unknown models.
- Translated `/v1` SSE now emits a `finish_reason` chunk before
  `[DONE]` — OpenAI SDK tool loops key on `finish_reason == "tool_calls"`
  and previously saw only null.
- Anthropic thinking blocks that cannot be replayed (OpenAI-wire tool
  history without thinking) now disable thinking for that request
  instead of hard-400ing every follow-up turn of the tool loop.
- Truncated tool-call argument streams salvage complete key/value pairs
  instead of executing the tool with empty arguments.
- WS client read pump skips Ping/Pong/Binary frames instead of tearing
  down the connection — keepalive intermediaries no longer cause
  permanent redial churn.
- Relay registration secret re-resolves per reconnect attempt; an
  unresolvable secret is logged loudly and skips the attempt instead of
  silently registering without it.
- Compaction summarizer calls are bounded by a 120s timeout (best-effort
  failure keeps full history).
- Channel bridges (telegram/slack/discord) now require `--allow` /
  `DAMON_ALLOW` — a public bot without an allowlist was unauthenticated
  agent access. Unlisted senders are dropped before any session work.
- FTS search escapes user input — queries like `c++` or `"` no longer 500.
- Queued permission prompts now respect cancellation instead of waiting
  out the current prompt's remaining timeout.
- **Review hardening pass** (multi-reviewer audit):
  - `permission_timeout_secs` actually takes effect — the WS request
    round-trip no longer hard-caps at 120s (the configured budget + slack
    is the bound now).
  - A panic inside a prompt turn can no longer brick the session —
    `live_prompts` is released via catch_unwind.
  - `session/prompt` + `session/delete` TOCTOU closed: the exists-check
    and the live-prompt registration run under one lock, and the
    retention sweep skips sessions with a live turn (and clears their
    MCP approvals). `PRAGMA foreign_keys` is on.
  - `model:low|:medium|:high` on a default-provider fallthrough no
    longer reaches the upstream verbatim (`gpt-4o:high` → 400).
  - `summary_model` routes through provider resolution — a
    `provider/model` value no longer hits the turn's provider verbatim.
  - MCP: a backoff-window refusal no longer permanently strips the
    server's tools; "always allow" grants are dropped when a server's
    config changes; non-object tool arguments now fail instead of
    invoking the tool with empty args.
  - Relay tunnel: a flooding client can no longer stall every session
    (try_send + abort instead of a blocking send); session slots are
    freed when the task exits (not only on relay disconnect); the E2E
    handshake is bounded by a 15s timeout on both sides; relay-path
    websockets cap frames at 4 MiB; `damon-relay` applies the per-IP
    cap to `/register` too and no longer leaks a slot when an upgrade
    never completes.
  - Channels: an anonymous/senderless message can no longer approve a
    known user's permission prompt; Telegram skips bot-authored
    messages (a bot could trigger AND approve a tool call); a deleted
    session no longer bricks the chat — the bridge retries once on a
    fresh session; Telegram bot tokens are stripped from logged errors;
    a zero `heartbeat_interval` from a Discord gateway can't panic the
    heartbeat task.
  - `/v1`: a mid-stream provider error now terminates the SSE stream
    with `[DONE]`; the 503 auth-resolution error no longer echoes the
    `!cmd`/keychain reference to unauthenticated callers; discovery
    probes resolve `env:`/`keychain:`/`!cmd` header values instead of
    sending them literally.
  - `session/cancel` sent with an id now gets a response; client error
    replies to server requests surface as errors instead of `null`.
  - `ToolCallAccumulator` bounds the streamed tool-call index (256) — a
    hostile upstream can't OOM the daemon with `index: 1e9`.
  - The model-glob matcher is linear-time — `*`-heavy patterns can't
    pin a worker.
  - npm client: `respond()`/`cancel()` wait for the reconnect gate like
    every other outbound frame; a malformed frame no longer crashes the
  - npm client: `respond()`/`cancel()` wait for the reconnect gate like
    every other outbound frame; a malformed frame no longer crashes the
    process; the unconsumed event queue is bounded.
- **Second review pass** (cross-verified audit):
  - Non-streaming `chat()` on every provider now reports the upstream
    status + raw body on failure — a gateway 502/504 with an HTML body
    no longer collapses into `error decoding response body`.
  - `openai-responses` `list_models` checks the HTTP status — a 401/500
    no longer masquerades as an empty model list.
  - Gemini and compat adapters read array-form `content` for system
    messages — an OpenAI-standard `[{type:"text",…}]` system prompt is
    no longer silently dropped (or merged into `"\n"`).
  - Anthropic `message_delta` usage now emits only the output-token
    increment over `message_start`'s baseline — metrics and `/v1` SSE
    no longer double-count output tokens.
  - Anthropic/Gemini SSE parsers drain a trailing un-terminated line at
    EOF — a proxy that cuts the last `data:` line's newline no longer
    loses the finish reason and final usage.
  - Providers return a typed `RateLimited` error on 429/529 honoring
    `Retry-After`; `run_prompt` backs off before its single retry.
  - `session/prompt` exists-check and `live_prompts` insert now run
    under ONE lock, and `session/delete` re-checks under the lock —
    a prompt can no longer register into a session being deleted.
  - Server→client requests cancelled mid-flight free their pending
    entry via a drop-guard instead of leaking until disconnect.
  - A stalled WS client is marked after the first send timeout — later
    notifications drop immediately instead of paying 10s per chunk.
  - A cancelled turn no longer rewrites a parallel tool call's real
    error (e.g. `permission denied`) as `cancelled` in history.
  - `maybe_compact` no longer clones the whole session tail per turn
    just to estimate tokens.
  - `damon prompt`/`chat` share ONE buffered stdin between the REPL and
    permission answers — typing during a permission prompt no longer
    corrupts input in both directions.
  - `damon health` exits non-zero on a non-2xx daemon response;
    `--relay` now requires `--relay-name` (clap).
  - Discord/Slack/Telegram sends honor `Retry-After`/`retry_after` on
    429 and retry the failed chunk once — a long split reply no longer
    loses its tail. Discord's heartbeat task is aborted on reconnect
    instead of detaching until its next tick.
  - Telegram's poll offset only advances on a parsed `update_id` — a
    malformed entry can no longer skip a real update.
  - Client `prompt()` frees its pending entry on send failure; inbound
    `session/update` notifications use `try_send` so a slow consumer
    can't backpressure the pump into stalling RPC responses.
  - Channel permission replies: an anonymous requester's prompt can only
    be approved by an anonymous replier — a named group member can no
    longer authorize an unidentified user's tool run.
  - Relay docs now state the real threat model (the relay sees the
    client proof — use a high-entropy token + wss://); `run_tunnel`
    warns on non-loopback `ws://` URLs.
  - `Config::load` warns when an existing config.toml is group/world
    readable (it may hold literal secrets).
  - Release tarballs ship `.sha256` sidecars and `npm install` verifies
    the checksum before extraction.
