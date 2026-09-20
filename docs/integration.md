# Damon Integration Guide

**English** | [한국어](integration.ko.md)

`damond` is a local agent daemon. Any program attaches through two surfaces:

| Surface | Purpose |
|---|---|
| `GET /health` | Health check |
| `POST /v1/chat/completions`, `GET /v1/models` | OpenAI-compatible passthrough (drop-in for existing clients) |
| `GET /ws` | ACP-style JSON-RPC over WebSocket — agent runtime (sessions, tool loop, permissions) |

## Providers

`api` selects the wire transport:

| api | Target | Notes |
|---|---|---|
| `openai-completions` | OpenAI, Groq, OpenRouter, DeepSeek, vLLM, Ollama, … | Default. `/chat/completions` passthrough + compat shaping |
| `openai-responses` | o-series, GPT-5, Codex, xAI | `/responses` — request/response translation |
| `anthropic-messages` | Claude | `/v1/messages` translation |
| `gemini` | Gemini | `generateContent` translation |

Subscription auth: set `api_key = "oauth"` and log in once —
`anthropic-messages` then speaks for Claude Pro/Max (`damond login
anthropic`) and `openai-responses` for ChatGPT Plus/Pro (`damond login
openai`). Tokens live in the OS keychain and refresh automatically; the
openai flavor targets the ChatGPT backend unless `base_url` is set.
Further subscription flavors: `kimi-code`, `github-copilot`, `xai-oauth`
(RFC 8628 device flows — `damond login <flavor>`); `qwen-portal` joins
as an env-key preset instead of a login.

Provider presets: `damond presets` lists the omp-parity catalog — hosted
backends (Groq, OpenRouter, Mistral, xAI, DeepSeek, Fireworks, Together,
Cerebras, NVIDIA, Moonshot/Kimi, Z.AI, BigModel, MiniMax, SiliconFlow,
Venice, Hugging Face, Vercel AI Gateway, LiteLLM, …) plus Azure OpenAI,
Vertex AI, Bedrock-mantle and the keyless local engines `lm-studio` /
`llama.cpp`. Any preset whose key env var is set registers itself at boot;
an explicit `[providers.<id>]` block overrides it. Wire shapes beyond the
four transports are compat flags: `compat.azure_deployment_urls` (+
`azure_api_version`, key in the `api-key` header), `compat.vertex`
(Vertex paths on a project/location base), and `compat.bearer_auth`
(Anthropic-compatible `Authorization: Bearer` — Bedrock mantle).

Model routing: the `model` field resolves by `provider/model` prefix →
config `models` glob → default, in that order. `/v1/chat/completions`
always answers in the OpenAI schema regardless of provider — clients never
care about translation.

### compat flags

When an endpoint deviates from the standard, tune `[providers.X.compat]`:

| Flag | Effect |
|---|---|
| `supports_store` | Sends `store: false` |
| `supports_developer_role` | Converts `system` → `developer` role |
| `supports_multiple_system_messages` | `false` merges consecutive system messages |
| `max_tokens_field` | Renames the field, e.g. `"max_completion_tokens"` |
| `requires_tool_result_name` | Injects `name` into tool results (Mistral) |
| `requires_mistral_tool_ids` | Normalizes tool_call ids to 9-char alphanumeric (Mistral) |
| `supports_usage_in_streaming` | Whether to send `stream_options.include_usage` |
| `extra_body` | Top-level fields merged into every request |

Custom headers can be added via `[providers.X.headers]`.

### Secrets

`api_key` and header values accept three reference forms (literals are rejected):

| Form | Example |
|---|---|
| `env:VAR` | `env:OPENAI_API_KEY` |
| `keychain:svc/acct` | `keychain:damon/openai` |
| `!cmd` | `"!op read op://dev/openai"` — stdout, 10s timeout |

`.env` files load from the config dir first, then cwd (already-set env wins).

### Model discovery

`discovery = "openai-models-list"` → `GET {base}/models`; `"ollama"` →
`GET {base}/api/tags`. Discovered model ids route without a `models` glob —
a request for `model: "local-model-7b"` goes to the provider that
discovered it. If `[providers.ollama]` is absent, damond probes
`$OLLAMA_HOST` (default `http://127.0.0.1:11434`) automatically.
`/v1/models` merges discovered ids as `provider/id`.

### Context promotion

`context_promotion_target = "model-id"` (same provider) or
`"provider/model-id"`. On a context-overflow error
(`context_length_exceeded`, …) the request retries once against the
target model. Works on streaming/non-streaming and passthrough/translated
paths alike.

### In-band tools (local models)

`compat.inband_tools = true` — for models without a native tool API.
`tools` are rendered into the system prompt and `<tool_call>{...}</tool_call>`
blocks in the response text are parsed into `tool_calls`. Streaming buffers
the full response, then re-emits events.

### Thinking levels

A `:low` / `:medium` / `:high` suffix on the model name maps to each
provider's control: OpenAI `reasoning_effort`, Anthropic
`thinking.budget_tokens` (1024/8192/32768), Gemini
`thinkingConfig.thinkingBudget`, Responses `reasoning.effort`.

### Prompt caching (Anthropic)

`anthropic-messages` automatically attaches `cache_control: ephemeral` to
the system block and the last content block of the last message — cuts
input-token cost on long sessions.

### Context compaction

When a session's estimated tokens exceed 85% of the model's
`context_window`, the oldest half is summarized by the provider
(`summary_model` config overrides which model summarizes) and
`compacted_through` is recorded. `messages()` then returns the summary
plus the remainder. A failed summarization records nothing — the full
history is kept and the turn proceeds.

### Discovery

At boot the daemon writes `~/.damon/daemon.json` (0700 dir / 0600 file)
and removes it on shutdown (graceful or unwound — only while the file
still names this daemon's pid, so a successor's file is never clobbered):

```json
{"port": 9470, "pid": 1234, "version": "0.2.0", "tls": false,
 "configPath": "/…/config.toml"}
```

Local clients read this to find the port without configuration. It
deliberately contains **no token or secret** — secrets never touch disk
in plaintext; the token still comes from the user or the config. A stale
file (SIGKILL) is simply overwritten by the next boot; clients can
liveness-check `pid`.

### Authentication

Required only when `auth_token` is configured. `/v1` takes
`Authorization: Bearer <token>`; `/ws` takes the same header or a
single-use `?ticket=<ticket>` issued by `POST /v1/ws_ticket` (60s TTL).
Query-string tokens (`?token=`) are not accepted — they leak into logs
and browser history. Unset → open on localhost.
`GET /metrics` (behind the token gate) exposes Prometheus counters:
`damon_requests_total`, `damon_prompts_total`, `damon_tokens_input_total`,
`damon_tokens_output_total`, `damon_active_sessions`,
`damon_mcp_tool_calls_total`, `damon_mcp_tool_errors_total`, and
per-provider `damon_provider_requests_total` / `damon_provider_errors_total`
/ `damon_provider_latency_ms_sum` labeled `provider="…"`.

Browser origins: when no `auth_token` is configured, requests carrying an
`Origin` header are only accepted from loopback origins (`localhost`,
`*.localhost`, `127.0.0.0/8`, `[::1]`) — on both `/ws` and `/v1`. Clients
without an `Origin` header (curl, Node, native apps) are unaffected.
With `auth_token` set, any origin may connect — the token is the gate.

## WS protocol (JSON-RPC 2.0)

Client → daemon requests:

- `initialize` → `{protocolVersion, agentCapabilities, agentInfo}`
- `session/new {cwd, model?}` → `{sessionId}` — `model` sets the session's
  default model (`provider/model`, glob-routed or discovered id, or a
  `model:low|medium|high` thinking suffix). `mcpServers` declares
  per-session stdio MCP servers — `[{name, command, args?, env?,
  auto_approve?}]` (env as `[{name,value}]` or an object). Their tools
  overlay the daemon's `[mcp_servers]` for that session only, are torn
  down on session/delete and daemon shutdown, and cap at 8 per session /
  64 live overlays; a name colliding with a configured server is
  rejected.
- `session/list {limit?, offset?}` → `{sessions: [{sessionId, createdAt, model, title}]}`
- `session/resume {sessionId}` → `{sessionId}` (error if unknown)
- `session/rename {sessionId, title}` → `{renamed: true}` — empty titles
  rejected with `-32602`; unknown session → `-32602`
- `session/usage {sessionId?}` → `{models: [{model, inputTokens, outputTokens, turns}]}`
  when `sessionId` is omitted (per-model rollup across all sessions), or
  `{inputTokens, outputTokens, turns}` for one session
- `session/delete {sessionId}` → `{deleted: true}` — cancels a live turn
  first; `-32603 "session busy"` if it is still winding down after 10s
- `session/messages {sessionId, limit?, offset?}` → `{messages: [...]}`
- `session/search {query, limit}` → `{results: [{sessionId, messageId, snippet}]}`
- `session/prompt {sessionId, prompt: [{type:"text", text}], model?}` → `{stopReason, model?}` — `model` overrides the session default for this turn; the response arrives when the turn ends. The result's `model` is the upstream model string actually sent to the provider — with default-provider fallback the request name passes through, with `provider/model` or glob routing it is the remapped upstream id, so clients can badge the real route taken. Absent only when the turn was cancelled before the first model resolution. Unknown `sessionId` → `-32602`.
- `session/prompt` content blocks: `{type:"text", text}` and
  `{type:"image", data: <base64>, mimeType}` — images are forwarded to
  vision-capable providers as `image_url` data URLs.
- `session/cancel {sessionId}` — notification; if sent with an `id` it gets an empty `{}` result
- `session/compact {sessionId}` → `{compacted, compactedThrough?, reason?}` —
  force a context compaction now, ignoring the 85% estimate threshold.
  Rejected while a turn runs on the session (`-32602`); the summarization
  itself is bounded at 120s and cancellable via `session/cancel`.

Daemon → client:
- `session/request_permission` request — the client must answer `{outcome: {outcome:"selected", optionId:"allow-once"|"reject-once"|"allow-always"}}` for tool execution to proceed. `allow-always` approves that tool for the rest of the session (in-memory only). Unanswered prompts are denied after `permission_timeout_secs` (default 300s). `auto_approve` servers never send this.

`session/prompt` response `stopReason`: `end_turn` | `max_tokens` | `tool_use` | `cancelled` | `max_turn_requests` (tool-loop iteration cap hit).

Concurrency rules:

- One live prompt per session, across all connections — a second
  `session/prompt` for the same session is rejected with `-32602` until
  the running turn finishes.
- If a connection drops, every prompt it started is cancelled and wound
  down before the session accepts a new prompt.
- `session/cancel` works from any connection, not just the one that
  started the turn.

## Minimal client flow

```
connect → initialize → session/new → session/prompt
  ├─ render session/update notifications as they arrive
  ├─ answer session/request_permission when it arrives
  └─ the response whose id matches the prompt request ends the turn
```

## Minimal clients

Copy-paste examples that run as-is. Assumes the daemon is on `127.0.0.1:9470`.

### curl (OpenAI-compatible passthrough)

```sh
curl -N http://127.0.0.1:9470/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"default","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

### Node.js — the packaged client (recommended)

`npm i damon-agent` ships a zero-dependency client (Node ≥ 22, or pass a
`webSocket` constructor):

```js
import { DamonClient } from "damon-agent";

const client = await DamonClient.connect("ws://127.0.0.1:9470/ws");
await client.initialize();
const sessionId = await client.newSession(process.cwd());
client.prompt(sessionId, "hi");

for await (const ev of client.events()) {
  if (ev.type === "update" && ev.update?.sessionUpdate === "agent_message_chunk")
    process.stdout.write(ev.update.content.text);
  if (ev.type === "request")  // permission prompt
    await client.respond(ev.id, { outcome: { outcome: "selected", optionId: "allow-once" } });
  if (ev.type === "promptDone") break;
}
client.close();
```

### Node.js — raw WebSocket (no dependency)

```js
// node client.mjs — Node ≥ 22 (global WebSocket)
const ws = new WebSocket("ws://127.0.0.1:9470/ws");
let id = 0;
const pending = new Map();
const call = (method, params) =>
  new Promise((res) => (pending.set(++id, res), ws.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }))));

ws.onmessage = async (e) => {
  const m = JSON.parse(e.data);
  if (m.id !== undefined && pending.has(m.id)) return pending.get(m.id)(m.result ?? m.error);
  const u = m.params?.update;
  if (u?.sessionUpdate === "agent_message_chunk") process.stdout.write(u.content.text);
};
ws.onopen = async () => {
  await call("initialize", { protocolVersion: 1, clientCapabilities: {} });
  const { sessionId } = await call("session/new", { cwd: "/tmp", mcpServers: [] });
  await call("session/prompt", { sessionId, prompt: [{ type: "text", text: "hi" }] });
  ws.close();
};
```

### Python (WS, `pip install websockets`)

```python
# python client.py
import asyncio, json, websockets

async def call(ws, id, method, params):
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
    while True:  # skip notifications until this request's response arrives
        m = json.loads(await ws.recv())
        if m.get("id") == id:
            return m.get("result")
        u = m.get("params", {}).get("update", {})
        if u.get("sessionUpdate") == "agent_message_chunk":
            print(u["content"]["text"], end="", flush=True)

async def main():
    async with websockets.connect("ws://127.0.0.1:9470/ws") as ws:
        await call(ws, 1, "initialize", {"protocolVersion": 1, "clientCapabilities": {}})
        sid = (await call(ws, 2, "session/new", {"cwd": "/tmp", "mcpServers": []}))["sessionId"]
        await call(ws, 3, "session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "hi"}]})

asyncio.run(main())
```

The Rust example is `examples/client.rs` (`cargo run --example client`).

## From Rust

`damon_core::client::DamonClient` is the reference implementation (used by
`src/bin/damon.rs`):

```rust
let client = DamonClient::connect("ws://127.0.0.1:9470/ws", None).await?;
client.initialize().await?;
let session = client.new_session("/tmp").await?;
// consume ClientEvent::Update / Request from events() while awaiting prompt()
```

## Desktop apps (Electron/Tauri)

The daemon is already a resident process. Don't spawn it from the app —
attach to `ws://127.0.0.1:9470/ws`. If the daemon isn't running, use the
sidecar pattern: spawn `damond`, but keep the app's lifecycle separate
from the daemon's.

## Remote access

- Recommended: Tailscale — WireGuard E2E, attach to `ws://<tailscale-ip>:9470/ws` with zero daemon config.
- Direct: set `tls_cert` + `tls_key` to serve `wss`. Non-loopback binds refuse to start without `auth_token`.
- Self-hosted relay: `damon-relay` on a public host + `[relay]` in the daemon config — the daemon dials out, no inbound port. Clients: `damon --relay ws://relay:8080 --relay-name <name> --token <auth_token>`.

## Channel adapters

All three channels attach with the same pattern — each channel chat maps
to its own daemon session, replies stream in, and permission requests are
approved by replying `allow`/`deny`.

| Adapter | Run | Receive | Notes |
|---|---|---|---|
| Telegram | `damon-telegram --bot-token <token>` | Bot API long-poll | `TELEGRAM_BOT_TOKEN` env works |
| Discord | `damon-discord --bot-token <token>` | Gateway WebSocket | `DISCORD_BOT_TOKEN` env. Guild channels need @bot mention; DMs work directly. Enable the MESSAGE_CONTENT privileged intent in the dev portal |
| Slack | `damon-slack --app-token xapp-… --bot-token xoxb-…` | Socket Mode | `SLACK_APP_TOKEN`/`SLACK_BOT_TOKEN` env. Channels need @bot mention; DMs work directly. Scopes: `connections:write` (app), `chat:write`+`im:history`+`channels:history`+`app_mentions:read` (bot) |

Adding a channel: implement `damon_core::channel::ChannelApi`
(`ready`/`recv`/`send`) and hand it to `channel::Bridge::new(channel,
client).run()` — the bridge owns session mapping, event demux, the
permission flow, and the streaming loop. `src/telegram.rs` is the minimal
example.
