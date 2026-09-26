# Damon integration guide

**English** | [한국어](integration.ko.md)

`damond` is a local agent-control daemon. Any program attaches through two surfaces:

| Surface | Purpose |
|---|---|
| `GET /health` | health check (includes the installed backend list) |
| `GET /ws` | JSON-RPC over WebSocket — conversations with agents (protocol v2) |

`POST /v1/ws_ticket` issues a one-shot ticket for `?ticket=` auth (clients that cannot set headers, e.g. browsers).

## Backends

Damon drives coding agents through their native CLIs as stdio subprocesses. Model choice, tools, subscription auth, and context management all belong to the agent.

| id | Agent | Launch | Auth |
|---|---|---|---|
| `claude` | Claude Code (Claude Pro/Max) | `claude -p --output-format stream-json --input-format stream-json --verbose` | `claude` CLI login |
| `codex` | Codex CLI (ChatGPT Plus/Pro) | `codex app-server` | `codex` CLI login |
| `omp` | Oh My Pi (any provider keys it manages) | `omp --mode rpc` | OMP's own auth store |
| `cursor` | Cursor Agent (Cursor subscription) | `cursor-agent -p --output-format stream-json --trust` | `cursor-agent login` or `CURSOR_API_KEY` |
| `amp` | Amp (Sourcegraph) | `amp --execute --stream-json --stream-json-input` | `amp` CLI login |
| `kimi` | Kimi Code (Moonshot) | `kimi -p --output-format stream-json` | `kimi login` |
| `qwen` | Qwen Code (Alibaba) | `qwen -p --output-format stream-json` | `qwen` CLI login |
| `gemini` | Gemini CLI (Google) | `gemini --output-format stream-json --skip-trust` | `gemini` CLI login or `GEMINI_API_KEY` |

Detection: the binary on PATH registers the backend automatically.

Session shapes differ per backend: `claude` and `amp` keep one process per
session (bidirectional stream-json — permission relay, steering, interrupts
work). `cursor`, `kimi`, `qwen`, and `gemini` are one-shot per turn — each
prompt respawns the CLI with a resume flag (`--resume`/`--session`), so
mid-turn steering and permission relay are unavailable; headless runs use
the CLI's own auto-approval policy.

Overrides — replace a launch line or point at a local build:

```toml
[backends.claude]
command = "/opt/claude"
args = ["-p", "--output-format", "stream-json", "--input-format", "stream-json", "--verbose"]
[backends.claude.env]
ANTHROPIC_MODEL = "claude-sonnet-4-5"
```

`session.create`'s `backend` parameter picks the backend per session. Omitted → the `default_backend` setting → the first available backend.

MCP servers — `session.create`'s `mcpServers` — are **forwarded to the agent**, which spawns and permissions them itself. Damon never launches MCP servers directly.

## Auth

Only needed when `auth_token` is set. `/ws` takes the same bearer token via the Authorization header or a single-use `?ticket=` (60s TTL) from `POST /v1/ws_ticket`. Query-string tokens (`?token=`) are not supported — they leak into logs and browser history. Without a token, localhost is open.

Browser Origin: without `auth_token`, requests carrying an `Origin` header must be loopback origins. With a token, all origins pass — the token is the gate.

`GET /metrics` (behind the token gate) exposes Prometheus counters: `damon_requests_total` and `damon_live_sessions`.

## WS protocol (v2)

Full contract: [protocol-v2.md](protocol-v2.md). Summary:

Client → daemon requests:

- `hello` → `{protocol, backends, methods}` — also pushed on connect
- `backend.list` → `{backends: [{id, available, capabilities}]}`
- `session.create {backend?, cwd?, model?, mode?, mcpServers?}` → `{sessionId, backend}`
- `session.resume {sessionId}` → `{sessionId, backend}` — reattaches via the backend's native resume token. Call with `{handle:{provider,native_handle}, cwd?, title?}` to import a native session made outside the daemon (same handle dedups to the same session)
- `session.list {limit?, offset?}` → `{sessions: [...]}`
- `session.messages {sessionId, limit?, offset?}` → `{messages: [...]}`
- `session.import {backend, cwd?}` → `{sessions: [...]}` — native sessions created outside the daemon
- `session.delete {sessionId}` → `{deleted: true}`
- `session.rename {sessionId, title}` → `{renamed: bool}`
- `session.fork {sessionId, upto?}` → `{sessionId}`
- `session.usage {sessionId?}` → `{contextUsed, contextSize, costUsd, turns}` or a per-session rollup
- `session.search {query, limit?}` → `{results: [...]}` — FTS5 over message text and tool I/O
- `turn.start {sessionId, prompt, timeoutSecs?, detach?}` → `{turnId, stopReason, usage?}` — resolves when the turn ends; events stream first. With `detach: true` the response returns immediately (`{turnId, detached: true}`) and the turn outlives the connection — disconnecting never cancels it (`turn.cancel` and `timeoutSecs` still apply); a reconnecting client picks the outcome up through replay
- `turn.steer {sessionId, prompt, expectedTurn?}` → `{result}` — mid-turn steering where supported
- `turn.cancel {sessionId}` → `{cancelled: true}`
- `permission.respond {sessionId, requestId, response}` → `{}` — answer a permission ask
- `session.set_model {sessionId, model}` / `session.set_mode {sessionId, mode}` → `{}`
- `catalog.models {backend}` → `{models, modes, commands}`
- `session.watch {sessionId}` / `session.unwatch {sessionId}` → subscribe this connection to a live session's events without touching it — the cross-surface notification path (a chat bridge pings its channel when a web-UI session finishes or asks for permission)

Daemon → client pushes:

- `{"event":"session.event","sessionId","data":<StreamEvent>}` — every event for sessions this connection has touched (create/resume/turn). StreamEvent kinds: `turn_started`, `timeline` (assistant_message/reasoning/tool_call/todo/…), `permission_requested`, `turn_completed`, `turn_failed`, `turn_canceled`, `attention_required`, `model_changed`, `mode_changed`, `thread_started`, `subagent`.

`turn.start` `stopReason`: `completed` | `failed` | `canceled` | `timeout`.

## Minimal client flow

```
connect → (hello arrives) → session.create {backend:"claude", cwd:"/repo"}
  ├─ render session.event pushes as they arrive
  ├─ answer permission_requested via permission.respond
  └─ the turn.start response ends the turn
```

## Minimal clients

Three copy-paste examples. Assumes the daemon at `127.0.0.1:9470`.

### Node.js (Node ≥ 22 — global WebSocket)

```js
// node client.mjs
const ws = new WebSocket("ws://127.0.0.1:9470/ws");
let id = 0;
const pending = new Map();
const call = (method, params) =>
  new Promise((res) => (pending.set(++id, res), ws.send(JSON.stringify({ id, method, params }))));

ws.onmessage = async (e) => {
  const m = JSON.parse(e.data);
  if (m.id !== undefined && pending.has(m.id)) return pending.get(m.id)(m.result ?? m.error);
  if (m.event === "session.event") {
    const ev = m.data;
    if (ev.type === "timeline" && ev.kind === "assistant_message")
      process.stdout.write(ev.text);
    if (ev.type === "permission_requested")
      call("permission.respond", {
        sessionId: m.sessionId, requestId: ev.id,
        response: { behavior: "allow" },
      });
  }
};
ws.onopen = async () => {
  const { sessionId } = await call("session.create", { backend: "claude", cwd: "/tmp" });
  await call("turn.start", { sessionId, prompt: "hi" });
  ws.close();
};
```

### Python (WS, `pip install websockets`)

```python
# python client.py
import asyncio, json, websockets

async def call(ws, id, method, params):
    await ws.send(json.dumps({"id": id, "method": method, "params": params}))
    while True:  # skip events, wait for this request's response
        m = json.loads(await ws.recv())
        if m.get("id") == id:
            return m.get("result")
        if m.get("event") == "session.event" and m["data"].get("type") == "permission_requested":
            await ws.send(json.dumps({"id": 999, "method": "permission.respond",
                "params": {"sessionId": m["sessionId"], "requestId": m["data"]["id"],
                           "response": {"behavior": "allow"}}}))

async def main():
    async with websockets.connect("ws://127.0.0.1:9470/ws") as ws:
        sid = (await call(ws, 1, "session.create", {"backend": "claude", "cwd": "/tmp"}))["sessionId"]
        await call(ws, 2, "turn.start", {"sessionId": sid, "prompt": "hi"})

asyncio.run(main())
```

Rust example: `examples/backend_probe.rs` (`cargo run --example backend_probe`).

## Attaching from Rust

`damon_core::client::DamonClient` is the reference implementation:

```rust
let client = DamonClient::connect("ws://127.0.0.1:9470/ws", None).await?;
let session = client.create_session(Some("claude"), "/tmp", None).await?;
// consume ClientEvent::Event pushes; the turn result arrives as TurnDone.
```

## Desktop apps (Electron/Tauri)

The daemon is already resident. Attach at `ws://127.0.0.1:9470/ws` instead of spawning it; if it isn't running, use the sidecar pattern — app exit and daemon lifecycle stay separate.

## Daemon discovery

Local clients (CLI, TUI, IDE plugins) can find the running daemon without
parsing its config: at boot `damond` writes `~/.damon/daemon.json` and
removes it on clean shutdown.

```json
{"port": 9470, "pid": 1234, "version": "0.3.0", "tls": false, "configPath": "/path/to/config.toml"}
```

The file carries no secrets — `port`, `pid`, `version`, `tls`, and the
`configPath` the daemon booted from. On Unix the file is `0600` and
`~/.damon` is `0700` regardless of umask.

A SIGKILLed daemon leaves the file behind, so readers must treat it as a
hint, not proof: check that `pid` names a live process (and that `port`
answers) before trusting it. A successor daemon overwrites the file at
boot, and the shutdown guard only removes it when the recorded pid is
still its own.

## Remote access

- Recommended: Tailscale — WireGuard E2E, attach at `ws://<tailscale-ip>:9470/ws` with no daemon changes
- Direct: `tls_cert` + `tls_key` serve `wss`. Non-loopback binds refuse to start without `auth_token`.
- Relay: run `damon-relay` on a public host and add `[relay]` — the daemon dials out, so no inbound port; X25519 + AES-256-GCM end-to-end encrypted, the relay sees only ciphertext. **The relay also serves the bundled web UI at its root** — open the relay host in any browser, enter daemon name + token, and the page connects back through the relay end-to-end encrypted (no VPN, no port forwarding; the browser runs its own crypto, so plain `ws://` hosting works).

## Channel adapters

Three channels attach the same way — per-chat session mapping, streamed replies, permissions approved by replying "allow"/"deny".

Cross-surface pickup: `!sessions` lists recent sessions from every surface, `!resume <id|title>` continues one in the chat (auto-watched), and `!watch`/`!unwatch <id|title>` follow a session so completions and permission asks ping the chat — answerable with the same `allow`/`deny` replies — even when the turn runs on the web UI or CLI.

| Adapter | Run | Receive | Notes |
|---|---|---|---|
| Telegram | `damon-telegram --bot-token <token>` | Bot API long-poll | `TELEGRAM_BOT_TOKEN` env works |
| Discord | `damon-discord --bot-token <token>` | Gateway WebSocket | `DISCORD_BOT_TOKEN` env works; MESSAGE_CONTENT privileged intent required |
| Slack | `damon-slack --app-token xapp-… --bot-token xoxb-…` | Socket Mode | `SLACK_APP_TOKEN`/`SLACK_BOT_TOKEN` env works |

New channel: implement `damon_core::channel::ChannelApi` (`ready`/`recv`/`send`) and hand it to `channel::Bridge::new(channel, client).run()` — session mapping, event demux, the permission flow, and the streaming loop belong to the bridge.
