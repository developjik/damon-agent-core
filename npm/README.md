# damon-agent

Prebuilt binaries + zero-dependency Node client for **Damon**, a local
multi-provider agent daemon. One resident daemon owns the tool loop,
sessions, memory, provider quirks, and secrets; your app attaches as a
thin client.

- Docs, config reference, protocol: <https://github.com/developjik/damon-agent-core>
- Install: `npm install -g damon-agent` (postinstall downloads the
  platform binaries from GitHub Releases)

```js
import { DamonClient } from "damon-agent";

const client = await DamonClient.connect("ws://127.0.0.1:9470/ws");
await client.initialize();
const sessionId = await client.newSession(process.cwd(), "gpt-4o"); // model optional
client.prompt(sessionId, "hi");

for await (const ev of client.events()) {
  if (ev.type === "update" && ev.update?.sessionUpdate === "agent_message_chunk")
    process.stdout.write(ev.update.content.text);
  if (ev.type === "request") // permission prompt
    await client.respond(ev.id, { outcome: { outcome: "selected", optionId: "allow-once" } });
  if (ev.type === "promptDone") break;
}
client.close();
```

Requires Node ≥ 22 (global `WebSocket`), or pass a WHATWG-style
constructor via `DamonClient.connect(url, { webSocket })`.

License: MIT OR Apache-2.0.
