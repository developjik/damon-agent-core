# damon-agent

Prebuilt binaries + zero-dependency Node client for **Damon**, a local
agent-control daemon. Damon drives coding agents (Claude Code, Codex
CLI, Cursor, Amp, Kimi, Qwen, Oh My Pi) through their native CLIs —
the model, tools, credentials, and context management belong to the
agents; Damon owns sessions, permission relaying, searchable history,
chat channels, and remote access. Your app attaches as a thin client.

- Docs, config reference, protocol: <https://github.com/developjik/damon-agent-core>
- Install: `npm install -g damon-agent` (postinstall downloads the
  platform binaries from GitHub Releases)

```js
import { DamonClient } from "damon-agent";

const client = await DamonClient.connect("ws://127.0.0.1:9470/ws");
await client.hello();
const sessionId = await client.newSession(process.cwd(), "claude"); // backend optional
const turn = client.prompt(sessionId, "hi"); // resolves when the turn ends

for await (const ev of client.events()) {
  // StreamEvents arrive as {type:"event", sessionId, event}; timeline
  // items ride as event {type:"timeline", kind:…}.
  if (ev.type === "event" && ev.event?.kind === "assistant_message")
    process.stdout.write(ev.event.text);
  if (ev.type === "event" && ev.event?.type === "permission_requested")
    await client.respondPermission(ev.sessionId, ev.event.id, { behavior: "allow" });
  if (ev.type === "promptDone") break;
}
await turn;
client.close();
```

## Through a relay (E2E-encrypted)

Connect to a remote daemon through a `damon-relay` — the relay pipes
frames but never sees plaintext: keys come from an X25519 handshake and
every frame is AES-256-GCM with direction-separated keys and strict
sequence numbers ([src/relay.rs] in the repo). Same client surface:

```js
import { connectRelay } from "damon-agent";

const client = await connectRelay({
  url: "wss://relay.example.com", // the relay's ws(s)://host:port
  name: "home",                   // the daemon's registered relay name
  token: process.env.DAMON_TOKEN, // the daemon's auth_token
});
await client.hello();            // identical API from here on
```

A dropped link redials with backoff and re-runs the full handshake (a
fresh X25519 keypair each attempt), matching the Rust `connect_relay`.
Requires WebCrypto X25519: Node ≥ 22, Chrome 133+, or Safari 17+.

Direct connections require Node ≥ 22 (global `WebSocket`), or pass a
WHATWG-style constructor via `DamonClient.connect(url, { webSocket })`.

License: MIT OR Apache-2.0.

