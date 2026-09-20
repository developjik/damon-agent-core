# Changelog

## Unreleased

### Added

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
