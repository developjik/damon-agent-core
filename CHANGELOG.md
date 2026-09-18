# Changelog

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
