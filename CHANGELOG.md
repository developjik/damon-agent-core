# Changelog

## 0.5.0 — 2026-09-27

### Added — the web UI through the relay (anywhere, no VPN)

- **`damon-relay` serves the chat UI** — the bundled single-page UI is
  now available at the relay's own root (`/`, `/ui`) plus its
  `/relay-client.js` and PWA assets. Open the relay host in any phone
  or laptop browser, enter the daemon name + auth token, and the page
  connects back through the same relay: no inbound port on the daemon,
  no VPN, nothing to host.
- **Browser E2E relay client (`DamonRelay`)** — a dependency-free
  WebSocket shim (`src/relay-client.js`, loaded by the UI wherever it
  is served) that runs the exact Rust relay handshake: X25519 ephemeral
  key exchange, the client-first `sha256(token ‖ …)` proof with the
  `s256` stretched negotiation, and direction-separated AES-256-GCM
  frames with strict sequence numbers. Crypto is pure JS on purpose —
  `crypto.subtle` only exists in secure contexts, and self-hosted
  relays are typically plain `ws://` — and self-tests against
  published vectors (RFC 7748, the Rust proof vectors) on first
  connect. A 30s encrypted `hello` keepalive replaces WebSocket pings
  (browsers can't send them) to keep the relay's 120s idle reaper fed.
- **UI remote mode** — the auth card gains a direct/relay switch
  (auto-defaulted by where the page is served from), persisted per
  tab; reconnect and auth-failure handling work through the relay like
  the local path. The daemon also serves `/relay-client.js` so a
  locally-opened page can switch to relay mode without redeploying.
- **Verified byte-for-byte against the Rust implementation**: npm
  `relay.test.mjs` cross-checks sha256/X25519/AES-256-GCM against
  Node's native crypto and the Rust proof vectors, and — with the
  debug binaries built — runs a full E2E: JS handshake ↔ real
  `damon-relay` + `damond`, RPC round trips, and wrong-token rejection.

### Added — cross-surface session pickup and notifications

- **`!sessions` / `!resume <id|title>`** — continue desk work from the
  phone. `!sessions` lists recent sessions from every surface (the
  current one marked), `!resume` adopts one into the chat: it is
  brought live through the persisted resume handle, the chat→session
  mapping is rebound (persisted as before), and the session is
  auto-watched. Resolution accepts an exact id, a unique id prefix, or
  a unique case-insensitive title fragment — whatever a phone keyboard
  can produce.
- **`!watch` / `!unwatch <id|prefix|all>`** — follow a live session
  without adopting it. When a watched session finishes a turn, fails,
  is canceled, or asks for permission — on ANY surface (web UI, CLI,
  another chat) — the chat is pinged; a permission ask delivered this
  way is answerable from the chat with the same `allow`/`deny`/`always`
  replies (routed straight to `permission.respond`). Watched sessions
  that this bridge is currently streaming to a chat never double-notify.
- **`session.watch` / `session.unwatch`** — the wire counterpart:
  subscribe/unsubscribe one connection to a live session's events
  without creating/resuming/turning on it. Watches are per-connection —
  the bridge re-issues them after every reconnect and restores the
  registry (one daemon-side `channel_state` row) on restart.

### Added — detached turns (mobile-friendly)

- **`turn.start {detach: true}`** — a turn that outlives its client.
  The response returns immediately (`{turnId, detached: true}`) and a
  background collector finishes the turn on a task no connection owns:
  disconnecting never cancels it. This is the mobile case — a locked
  phone or a wifi→cellular handoff drops the socket mid-turn, which
  used to cancel the running work. `turn.cancel`, a deny-with-
  `interrupt`, and `timeoutSecs` still end a detached turn early.
  Persistence, broadcast events, replay, the `busy` claim, idle-reap
  protection, and the 64-prompt concurrency cap are identical to
  blocking turns, so a reconnecting client (or a second client) picks
  the turn up through the normal replay path — in-flight journal while
  it runs, store rows once it ends. Blocking `turn.start` is unchanged:
  disconnect still cancels unowned turns.
- **Web UI** sends detached turns and drives its busy state from turn
  events instead of the `turn.start` await; on reconnect it restores
  the running indicator from `session.status` when a detached turn
  survived the drop.
- **Node/Python SDKs** expose `prompt(..., { detach: true })` /
  `prompt(..., detach=True)`.

### Added — phase-5 SDK resilience and projects

- **npm TypeScript types** — `client.d.mts` ships with the package
  (`exports["."].types`), covering the full public surface: typed
  `ClientEvent` union, `StreamEvent`, permission/response shapes,
  session/project rows, and the `autoResume` option. `npm run
  typecheck` (tsc --strict, new typescript devDependency) compiles a
  `types-smoke.ts` exercising every declaration — the documented
  Electron/Tauri embedders finally get IDE support.
- **Auto-resubscribe on reconnect (all three SDKs)** — the Rust
  (`ConnectOptions { auto_resume }`, default on), npm
  (`{ autoResume }` on connect/connectRelay), and Python
  (`auto_resume=True`) clients now remember every session they
  created/resumed/prompted and re-issue `session.resume` for each
  after a redial lands. The daemon re-attaches the backend and replays
  missed events (tagged `replay: true`), so subscriptions survive
  daemon restarts transparently — the core headless-runner flow that
  used to be every caller's manual chore. Fire-and-forget: a dropped
  session surfaces on the next real call, never as a reconnect
  failure. The Rust supervisor shares the request id space with the
  caller (a second counter would collide in the pending map).
- **Projects / workspace abstraction** — `project.create/list/get/
  set_defaults/delete` over a new `projects` table; sessions gain a
  `project_id` (column-existence migration). `session.create
  {projectId}` scopes the session: the project root becomes the cwd
  default and its defaults (`backend`/`model`/`mode`/`mcpServers`)
  fill whatever the request left unset — explicit params always win,
  and the daemon-resolved model is now persisted on the session row
  (`set_session_model`, revived with a producer). `session.list`,
  `session.search`, and the global `session.usage` accept a
  `projectId` filter; `project.delete` refuses while sessions are
  still bound. Unknown defaults keys fail at `project.create` time,
  not at the first session that consumes them.
- **`turn.start {detach}`** (concurrent work landed mid-phase) — a
  detached turn resolves immediately `{turnId, detached: true}` and
  outlives its connection; disconnect no longer cancels it. Completing
  the wiring here: `start_turn` now races the disconnect token (a
  backend that parks inside its start path — the mock's hang knob, a
  slow agent boot — could otherwise hold the busy claim with no
  connection left to answer for it), and the mock grew a
  detached-friendly hang so the detached reply arrives before the
  park.

### Added — phase-4 file API and channel persistence

- **`file.read` / `file.write` / `file.list`** — client-facing file
  access jailed to the session's stored cwd. Paths resolve relative
  (or absolute-inside); the jail is enforced after best-effort
  canonicalization (existing symlink hops resolve, a write target
  resolves through its deepest existing ancestor so a symlinked cwd
  still admits new files, lexical `..` folding covers the rest) and
  every escape fails closed with `-32602`. Caps: 512 KiB reads
  (base64 body fits the 1 MiB frame budget, `-32005` over), 1 MiB
  decoded writes, 1000 listing entries. Unlocks channel attachments
  and UI file browsing on the same surface.
- **`channel.get/set/delete_state` + bridge persistence** — a
  daemon-side `channel_state` table (conv_id, key) that the chat
  bridges now back their in-memory maps with: the chat→session
  mapping and the `!cwd`/`!agent` prefs survive a bridge restart (a
  fresh Bridge reattaches the same session instead of silently
  resetting the conversation to defaults), the 24h cache eviction
  re-loads from state instead of dropping context, and `!new`/
  `!delete`/backend switches clear the persisted mapping so a restart
  never resurrects a replaced session. Persistence failures warn and
  degrade to the old in-memory behavior.

### Added — phase-3 lifecycle and ops

- **`session.status`** — live-session introspection over RPC:
  `{sessionId, backend, busy, idleSecs}` for every resident backend
  process. Complements the `/metrics` gauges with per-session detail.
- **`session.restart`** — kill the live backend (crashed, wedged, or
  healthy) and reattach through the persisted handle in one call;
  returns the resume shape. Mid-turn restarts refuse with `-32006`
  instead of killing the turn's process underneath the collector.
- **`session.set_pinned` / `session.set_archived`** — pinned rows
  float to the front of `session.list` (which now reports `pinned`/
  `archived` per row and accepts `includeArchived`); both flags are
  exempt from the `session_retention_days` sweep. Schema columns are
  added by the existing column-existence migration.
- **`logs.tail` / `logs.follow` + `damon logs [-f] [--lines N]`** —
  the daemon tees its formatted log output (ANSI off) into a bounded
  in-process ring (2000 lines). `logs.tail` serves the recent window;
  `logs.follow` opts a connection into a live `log.line` push per new
  line (same envelope family as `session.event`, so it works through
  the relay). `damon logs -f` prints the backlog then streams; Ctrl-C
  stops. Console output is unchanged.


- **Slack native permission buttons** — the old "interactive blocks
  need a public request URL" rationale only holds for HTTP apps:
  with Socket Mode, `block_actions` presses arrive on the WebSocket
  the bridge already holds. `send_permission` now posts a Block Kit
  card (allow / scoped-always / deny with primary/danger styles, the
  title mrkdwn-escaped), a press acks the envelope, strips the
  buttons via `chat.update` (so a resolved card can't be re-pressed
  into a literal "allow" prompt), and synthesizes the typed-reply
  Incoming — same word, same presser, so the bridge's pending-lane
  and identity checks apply unchanged. One-time app config: enable
  Interactivity with Socket Mode. Blocks shapes pinned by a mock-
  server test; UNVERIFIED against a live workspace, like the upload
  flow.
- **Discord inbound attachments** — `MESSAGE_CREATE` attachments map
  to lazy `Url` attachments (CDN URLs are public and signed; fetched
  at prompt time like Slack's `url_private`), replacing the
  "attachments aren't supported on this channel yet" notice. A
  mention + file with no text is now a real prompt.
- **OMP compaction events** — `auto_compaction_start`/`_end` frames
  map to the same normalized `Compaction` timeline item codex emits
  (previously `{}`-dropped), so context-compaction boundaries render
  and persist uniformly across backends.
- **FTS prefix search** — a trailing `term*` becomes an FTS5 prefix
  query (`error*` matches `errors`); quote-wrapping used to swallow
  the star so prefix searches silently degraded to exact matches. A
  bare `*` searches nothing.
- **Claude catalog + import titles** — the model picker offers the
  CLI's stable alias names (`sonnet`/`opus`/`opusplan`; full model
  ids still pass through `set_model`), and imported sessions carry a
  title extracted from the transcript's first user message
  (whitespace-collapsed, 80-byte cap) instead of `None`.
- **Gemini mode honesty** — mode descriptions now say what the ids
  actually do in headless mode: `default` and `bypassPermissions`
  both auto-approve (no permission wire), `acceptEdits` maps to
  `auto_edit`, `plan` to `plan`. A picker no longer implies
  "default" will ask first.

### Fixed — phase-1 defect sweep

- **Concurrent `turn.start` on one session is rejected** with a new
  typed error (`-32006`, "a turn is already running on this session")
  instead of running two collectors on the same broadcast — which
  double-persisted every event and let disconnect cancel the wrong
  turn. The busy claim is now taken atomically in the dispatcher and
  released by a guard on every exit path (a rejected turn can no
  longer wedge the session).
- **Failed turns leave a durable marker**: a backend `turn_failed` now
  persists an `error` row (role `error`, reason in `data.content`).
  `session.messages`/`session.export` show why a prompt has no answer,
  and a late subscriber's replay re-raises it as a timeline
  `error {message}` item — reconnecting clients no longer lose the
  failure reason entirely. New `TimelineItem::Error` carries it on the
  wire; the web UI renders the stored row as an error bubble.
- **Backend compaction events are persisted** as `compaction` rows
  (codex already emitted the timeline item; the store dropped it).
  Compaction boundaries are now part of the searchable transcript and
  replay; the web UI renders the stored row with the same collapsed
  card as the live event.
- **`session.fork` copies the backend resume handle** — the fork is
  resumable. Both rows share the native handle only until each runs
  its next turn (run_turn rebinds a session to its own fresh native
  handle afterwards); `upto` still bounds the copied store history
  only, the native transcript replays in full on resume.
- **`session.close` RPC added** — kills the live backend process,
  keeps the row and history, frees a `max_sessions` slot, idempotent.
  The `SESSION_LIMIT` error message always told clients to "close,
  delete, or let idle sessions reap first"; close now actually exists
  on the wire. Documented in `docs/protocol-v2.md` and the
  self-describing `rpc_methods()` schema.
- **`allowed_dirs` config** — optional cwd allowlist gating
  `session.create`, both `session.resume` paths, and
  `session.import`. Component-wise prefix after best-effort
  canonicalization (`/a/b` admits `/a/b/c`, not `/a/bc`); unset keeps
  the zero-config default of unrestricted. Fail-closed for
  remote/TLS deployments where the shared token would otherwise let
  an agent process run anywhere. Hot-reloaded.
- **Store v3 migration re-indexes legacy FTS rows**: the v1 backfill
  only indexed plain-string `content`, so older array-form messages
  (text parts, tool payloads — exactly what the current extractor
  handles) were silently unsearchable. Reopening the store re-runs
  `fts_text` over every message missing from the index (idempotent)
  and stamps `user_version = 3`.
- **npm client surfaces the `replay` flag** on `events()` — JS
  consumers can finally tell catch-up history from live frames after
  a reconnect, matching the Rust and Python clients (one-field fix,
  pinned by a new test).
- **Schema test completeness**: the expected-method list in
  `tests/rpc.rs` now includes `session.fork` (it was missing — a
  schema regression on fork would have passed) and `session.close`.

### Added — P3 infrastructure sweep


- **Typed JSON-RPC error codes** — parse/invalid-request/method/param
  failures answer with their reserved codes
  (`-32700`/`-32600`/`-32601`/`-32602`) instead of every error being
  `-32000`; an unparseable frame or an id-carrying method-less frame
  now gets an error reply instead of silence. Damon server codes let
  clients degrade gracefully instead of parsing messages: `-32001`
  session-not-live (auto-resume), `-32002` capability-unsupported
  (hide model/mode pickers, queue steer text as the next prompt),
  `-32003` backend-unavailable, `-32004` session-limit, `-32005`
  response-over-budget — a response over the 1 MiB relay frame budget
  fails loudly with a paging hint instead of vanishing into the
  encrypter. Messages are unchanged (string-matching clients keep
  working); codes are documented in `docs/protocol-v2.md`, and the
  Rust client carries them on the error
  (`downcast_ref::<rpc::RpcError>()`); the npm client's existing
  `RpcError.code` now sees real values.
- **`/metrics` overhaul** — `damon_live_sessions` reports the real
  live-session count (it used to count busy turns), with
  `damon_busy_sessions` alongside; new `damon_rpc_requests_total`,
  `damon_rpc_errors_total{code}`, per-backend
  `damon_turns_total{backend,status}`, and a `damon_turn_seconds_*`
  duration histogram (fixed buckets plus sum/count).
- **CI release gating** — the release job now needs `audit`, `deny`,
  and `coverage` in addition to `test`, so a vulnerable or
  license-banned build cannot ship; coverage enforces a 60% line floor
  (`--fail-under-lines`, measured 63.5% at introduction); MSRV is
  declared (`rust-version = "1.95"`) with a CI drift check against the
  pinned toolchain.
- **Release signing (SHA256SUMS + minisign)** — every tag publishes a
  `SHA256SUMS` manifest covering all five tarballs (assembled only
  after each was verified against its build-computed `.sha256`
  sidecar) and, once the `MINISIGN_SECRET_KEY` repo secret is
  registered, a minisign-format signature. `scripts/verify-release.sh`
  checks checksums and signature (signed/tampered/unsigned-advisory/
  unsigned-strict paths all exercised), and `npm install` prefers the
  manifest over per-target sidecars. `docs/release.md` documents the
  one-time key setup.
- **Changelog accuracy** — the missing `0.3.0` section heading is
  restored (its content sat unattributed inside 0.4.0), the post-0.3.0
  additions (chat surface parity, interrupt classification) moved into
  0.4.0 where they belong, and a `0.1.0` section was reconstructed for
  the initial release (no changelog existed at that tag).


### Added — P1 reliability sweep

- **`max_sessions` config** caps live in-memory backend sessions
  (create and resume); going past it fails with a named error until
  sessions are closed, deleted, or reaped. Hot-reloaded.
- **Fresh-session reap grace**: a session that has never run a turn is
  not idle-reaped for its first 60s — the client's
  create→turn.start gap survives even a tiny `agent_idle_secs`.
- **Event replay for late subscribers**: a connection first touching a
  session now receives everything it missed before live streaming —
  persisted history from the store, then the in-flight turn's journal,
  each frame tagged `"replay": true`. Reconnecting mid-turn no longer
  loses the earlier stream; create/resume results report the
  `replayed` count.
- **Discord gateway RESUME (op 6)**: reconnects resume the gateway
  session when it survives (messages missed while disconnected are
  replayed by Discord) and re-identify only on an invalidated session
  (op 9 `d:false`), under the existing exponential backoff. READY's
  `session_id` is tracked; the heartbeat seq resets on fresh identify.
- **Rate-limit handling for channel sends** (shared
  `ratelimit::send_with_rate_limit`): Telegram/Discord/Slack now retry
  429s up to 3 extra attempts honoring `Retry-After` (header or JSON
  body — Discord/Telegram shapes) with exponential fallback, and pace
  the next chunk when an `X-RateLimit-Remaining: 0` bucket is
  exhausted. A second consecutive 429 used to fail the whole turn.
- **Channel bridge delivery failures are logged** (`Bridge::deliver`)
  — a lost final reply, permission prompt, or error notice leaves a
  warn trace instead of vanishing (`let _` everywhere before).
- **Relay link liveness, both ends**: the Rust client pings every 30s
  and self-closes after 120s of inbound silence (channels close,
  callers see it instead of hanging on a half-open socket);
  damon-relay drops client sockets silent for 120s and notifies the
  daemon so its session slot is freed.
- **Opt-in stretched relay proof (`kdf: "s256"`)**: handshakes between
  current clients and daemons iterate sha256 2^16 extra times per
  proof, making offline brute-force of a captured handshake cost 2^16
  compressions per token guess instead of one. Older peers keep the
  single-hash proof (additive negotiation, byte-compatible — pinned by
  cross-implementation test vectors). npm client mirrors it.
- **`permission.respond` deny with `interrupt: true` now actually
  interrupts the turn** — the daemon drives the backend interrupt
  after delivering the denial (previously every backend ignored the
  flag).

### Fixed — P0 defect sweep

- **Persistent sessions no longer hang when the backend process dies.**
  The transport now exposes an exit signal (stdout EOF, read error, or
  shutdown); claude/amp/codex/omp dispatch pumps select on it and fail
  the in-flight turn with `TurnFailed` instead of blocking forever on
  the still-open broadcast channel. One-shot dialects (cursor/kimi/
  qwen) get the guarantee their exit pump was written for — it had the
  same latent hang — and the omp handshake fails fast when the process
  dies before `ready`.
- **Backend stderr is logged, not discarded.** Every capped stderr line
  lands in the daemon log at WARN with the backend command — CLI
  auth/login failures used to look like silent hangs.
- **Codex permission bookkeeping**: `serverRequest/resolved` now also
  matches numeric JSON-RPC ids, so answered asks are actually removed
  from the pending map.
- **RPC self-description matches the dispatch**: `session.messages`
  documents `offset` (it said `before`, which no code reads);
  `permission.respond` now lists its required `requestId`.
- **Capabilities tell the truth**: `rewind` is false everywhere (no
  backend implements it), `subagent_events` false for claude/amp (only
  omp emits `Subagent` events), `mcp_servers` false for codex/omp
  (only claude forwards `session.create`'s `mcpServers` today).
- **amp steering marks the frame** with `steer: true` as its input
  protocol documents; other stream-json dialects steer with a plain
  user frame as before.
- **config.example.toml** no longer ships a commented `[mcp_servers.*]`
  block — uncommenting it would fail startup since 0.4.0 stopped
  parsing the section. It now points at `session.create`'s
  `mcpServers`.

### Added — P2 feature sweep

- **Native permission buttons**: Telegram permission prompts render as an
  inline keyboard and Discord's as component buttons (allow / scoped-always /
  deny derived from the agent's offered actions). A press resolves the ask as
  if the presser typed the reply — the requester-identity check and the
  single-pending-per-conversation lane are unchanged. Slack stays text-only:
  interactive blocks require a public request URL the Socket-Mode-only
  daemon does not host.
- **Per-conversation `!cwd` / `!agent`**: a chat can pin its project
  directory (validated, absolute) and backend (`!agent <id>` validated
  against `backend.list`, resetting the session since backends are fixed at
  create time). Preferences are in-memory like the chat→session map — a
  daemon restart falls back to the configured defaults.
- **Bounded prompt queueing**: a second prompt arriving mid-turn is queued
  (cap 3) and runs when the live turn ends, instead of being rejected;
  `!cancel` drains the queue too.
- **Channel media**: Slack `file_share` attachments now reach the agent
  (`url_private` fetched with the bot token — previously "attachments
  aren't supported"); `ChannelApi::send_media` delivers outbound images on
  Telegram (sendPhoto/sendDocument), Discord (multipart), and Slack
  (files upload v2), ready for backends that emit `images` on assistant
  timeline items.
- **Markdown over channels**: replies render as Telegram HTML (parse-error
  fallback to plain text), and stream flushing is fence-safe — chunks never
  split inside a ``` block (oversized fences split and reopen).
- **Store/RPC**: `session.list` and `session.search` filter by
  `backend`/`cwd`/`tag` (search also `since`/`until`, RFC3339 or date);
  messages carry real `ts` (unix ms — legacy rows clamp to the session's
  creation time for time-filtered search); `session.set_tags`; `session.export`
  (full transcript superset of the CLI export); `session.usage {daily:true,
  days:N}` day-bucketed cost/turn rollup; `session.fork` now copies usage
  history, tags, and title.
- **Dashboard**: steer bar while a turn runs (unavailable backends queue the
  text as the next prompt, per the SteerResult contract); search hits
  deep-link to the exact message (paged walk, compaction-aware); session
  list pages 50-at-a-time with Load more instead of a hardcoded 500.
- **Rust client + CLI parity**: `DamonClient::turn_steer/set_model/set_mode`
  and `damon steer|model|mode` subcommands.
- **Python client relay transport**: the npm client's relay link (X25519 +
  AES-256-GCM, stretched-proof `kdf:"s256"` negotiation) is ported to
  stdlib-only Python, pinned by cross-implementation vectors against the npm
  reference and relay.rs goldens.
- **gemini backend**: `gemini` CLI via headless `--output-format stream-json`
  (init/message/tool_use/tool_result/error/result frames grounded in the
  upstream formatter sources; UNVERIFIED dialect until run against a real
  install — same convention as cursor/amp/kimi/qwen).
- **Claude subagent events**: Task tool_use/results now raise omp-shaped
  `Subagent` stream events (capability flipped true), so consumers render
  one consistent shape across backends.
- **Codex**: `turn/completed` token usage is mapped (totalTokenUsage /
  lastTokenUsage fallback) instead of dropped; Question approvals can carry
  the user's free-text answer (`PermissionResponse::Allow.answer`, optional
  and backward-compatible).
- **cursor delta streaming**: `--stream-partial-output` enabled — only the
  delta kind (`timestamp_ms` set, `model_call_id` absent) is emitted per the
  documented three-kind rule; every consumer already coalesces consecutive
  assistant messages, so transcripts don't duplicate.
- **Shared Claude-frame parser**: amp/kimi/qwen's copy-pasted frame
  translation collapsed into one `streamjson::ClaudeFrameParser` with
  per-dialect quirks (tool-detail mappers, OpenAI tool_calls, usage shape,
  interrupt detection) — one place to fix dialect bugs now.

## 0.4.0 — 2026-09-26

### Removed — dead-code sweep (breaking)

- **Rust client:** `DamonClient::{notify, dropped_events, backend_list,
  turn_steer, set_model, set_mode, catalog_models}` deleted — no in-repo
  caller, and the wire methods stay available via `request()` / the npm
  and python clients.
- **Store:** `session_row`/`SessionRow`, `search`, `messages`,
  `messages_full`, `list_sessions`, `session_exists` deleted — the RPC
  surface uses `search_filtered`/`messages_paged`/`list_sessions_paged`.
- **Backend traits/types:** `AgentSession::{pending_permissions, history}`
  (implemented but never called), `ResumePurpose` (all resume paths were
  interactive), `PermissionResult` (callers discarded it),
  `SessionConfig.{system_prompt, env}` (never populated),
  `TimelineItem::Error`, `StreamEventKind::UsageUpdated`,
  `AttentionReason::Error`, `ToolCallStatus::Canceled`,
  `PermissionKind::{plan, mode, other}` (no producer — the UI branches
  and docs describing them are gone too), `NdjsonTransport::{pending,
  exited, pid, uptime, stderr_tail}` plus the unread stderr ring.
- **Config:** the `[mcp_servers.*]` section is no longer parsed — it was
  deserialized but never forwarded (only `session.create`'s
  `mcpServers` reaches an agent). Existing config files carrying the
  section now fail with an unknown-field error instead of silently
  ignoring it.
- **HTTP:** `GET /v1/events` (SSE) removed — nothing ever produced a
  daemon event, so the stream only sent keepalives. `GET /metrics` now
  emits `damon_requests_total` and `damon_live_sessions` only; the other
  counters were always 0.
- **npm:** `__test.detectX25519` export removed (test hook never used by
  the tests).
- **Dependencies:** `tokio-stream` (prod) and `sysinfo`, `tempfile`,
  `http-body-util`, `rcgen` (dev) had no usage and are dropped.

### Fixed — cursor permission escalation

- **`cursor` no longer maps `acceptEdits` to `--force`.** `--force`
  auto-approves every command not explicitly denied — far beyond the
  edit-only auto-approval `acceptEdits` promises. Only
  `bypassPermissions` maps to `--force` now, and the mode catalog
  advertises `default`/`plan`/`bypassPermissions` instead of the
  non-existent cursor `acceptEdits`.

### Added — omp permission modes

- **`omp` honors `session.create {mode}`** — `bypassPermissions` →
  `--approval-mode yolo`, `acceptEdits` → `--approval-mode write`
  (omp 18.2.6's native flag). omp's RPC protocol has no runtime
  approval-mode command, so the mode is fixed at spawn
  (`dynamic_modes` stays false) and unknown modes defer to omp's own
  `tools.approvalMode` setting.

### Added — chat surface parity

- **Subagent events render everywhere** — `subagent` stream events
  (OMP today) were silently dropped by every chat surface. The web UI
  shows a collapsible 🤖 card per subagent (name/status plus the raw
  frame), chat channels post `🤖 name status`, and the CLI prints
  `[subagent name status]`.
- **Model/mode pickers in the web UI** — the composer gains model and
  mode selects fed by `catalog.models`; a change applies to the live
  session via `session.set_model`/`session.set_mode`, or rides along on
  the next `session.create`.
- **Native session import** — `session.resume` now accepts
  `{handle:{provider,native_handle}, cwd?, title?}`: the daemon mints a
  session row bound to the handle (deduped — re-importing the same
  native session returns the same session) and resumes it. The web UI
  sidebar has an Import button listing `session.import` results, the
  CLI has `damon import <backend> [--attach N]`, and the Rust/Node/
  Python clients expose `resume_by_handle`/`resumeByHandle`.
- **Cancel from every surface** — chat channels gain `!cancel`, and
  the CLI REPL accepts `/cancel` (or `cancel`/`/stop`) mid-turn. Other
  input typed mid-turn is carried over as the next prompt instead of
  being swallowed.

### Fixed — interrupt classification

- **Claude cancels reported as failures** — an interrupted turn's
  result frame arrives `is_error:true` with subtype
  `error_during_execution`, so `turn.cancel` surfaced as
  `[turn error] unknown error`. The dialect now maps
  `terminal_reason:"aborted_streaming"` (and `subtype:"interrupted"`)
  to `TurnCanceled`.
- **`mode_changed` fell through to `model_changed`** in the web UI
  event switch, printing a bogus `model: undefined` line on every mode
  change.

## 0.3.0 — 2026-09-24

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

## 0.2.0 — 2026-09-19

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

## 0.1.0 — 2026-09-16

Initial release — Damon as a local, always-on agent core (no changelog
existed at this tag; section reconstructed from the v0.1.0 tree):

- **Core daemon (`damond`)** with a built-in agent runtime: sessions,
  streaming, tool loop, interactive permission prompts, cancellation,
  context compaction (85% threshold), exposed as ACP-style JSON-RPC
  over WebSocket.
- **Provider layer**: OpenAI, Anthropic, Gemini plus compat presets
  (OpenRouter, Groq, DeepSeek, vLLM, Ollama… auto-discovered), model
  glob routing, `model:low/:medium/:high` thinking suffixes, and
  OpenAI-compatible `/v1/chat/completions` + `/v1/responses`
  endpoints for drop-in clients.
- **MCP stdio servers** declared in config; tools namespaced
  `server.tool` with per-server `auto_approve` or interactive
  permission prompts.
- **Surfaces**: `damon` CLI, embedded web UI at `/ui`, Rust + npm
  clients, and Telegram/Slack/Discord channel binaries with per-chat
  session mapping and `allow`/`deny` permission replies.
- **`damon-relay`** public-host tunnel — the daemon dials out, the
  link is end-to-end encrypted (X25519 + AES-256-GCM), the relay sees
  only ciphertext.
- **Secrets**: `env:`/`keychain:`/`!cmd` references only (literal keys
  rejected), OAuth tokens in the OS keychain with auto-refresh.
