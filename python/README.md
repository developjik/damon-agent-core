# damon-agent (Python)

Asyncio client for **Damon**, a local agent-control daemon. Damon is an
ACP host: it drives coding agents (Claude Code, Codex, Gemini CLI,
Copilot, …) over the Agent Client Protocol — the model, tools,
credentials, and context management belong to the agents; Damon owns
sessions, permission relaying, searchable history, and remote access.
Your app attaches as a thin client. Same API shape as the Node
(`damon-agent`) and Rust (`damon::client`) SDKs.

- Docs, config reference, protocol: <https://github.com/developjik/damon-agent-core>
- Install: `pip install damon-agent` (Python ≥ 3.11, single runtime
  dependency: `websockets`)

```python
import asyncio
from damon_agent import DamonClient

async def main():
    client = await DamonClient.connect("ws://127.0.0.1:9470/ws")
    await client.hello()
    session_id = await client.new_session("/repo", backend="claude")  # backend optional
    turn = asyncio.create_task(client.prompt(session_id, "hi"))

    async for ev in client.events():
        # StreamEvents arrive as {"type": "event", "sessionId", "event"};
        # timeline items ride as event {"type": "timeline", "kind": …}.
        if ev["type"] == "event" and ev["event"].get("kind") == "assistant_message":
            print(ev["event"]["text"], end="", flush=True)
        if ev["type"] == "event" and ev["event"].get("type") == "permission_requested":
            await client.respond_permission(
                ev["sessionId"], ev["event"]["id"], {"behavior": "allow"}
            )
        if ev["type"] == "prompt_done":
            break

    print((await turn)["stopReason"])
    client.close()

asyncio.run(main())
```

## Auth

Without `auth_token` the daemon is open on localhost. With one, pass the
token — the client exchanges it for a single-use ticket at
`POST /v1/ws_ticket` (Bearer auth) and connects with `?ticket=`, so the
token never appears in a URL:

```python
client = await DamonClient.connect("ws://127.0.0.1:9470/ws", token=os.environ["DAMON_TOKEN"])
```

## Prompting

`prompt()` resolves with the turn result (`{turnId, stopReason, usage?}`)
and accepts a string or raw content blocks for multimodal turns:

```python
result = await client.prompt(session_id, [
    {"type": "text", "text": "what is in this image?"},
    {"type": "image", "data": base64_data, "mimeType": "image/png"},
])
```

The same outcome is also delivered as a `prompt_done` event, so callers
that only iterate `events()` never miss a turn end.

## Permissions

The daemon asks before an agent runs something sensitive — it arrives
as an `event` whose StreamEvent `type` is `permission_requested`
(carrying `id`, `kind`, `name`, `title`, `input`, `detail`, and the
`actions` the agent offered). Answer it with `respond_permission()` —
a PermissionResponse like `{"behavior": "allow", "action_id": …}` or
`{"behavior": "deny", "message": …}`:

```python
async for ev in client.events():
    if ev["type"] == "event" and ev["event"].get("type") == "permission_requested":
        await client.respond_permission(
            ev["sessionId"], ev["event"]["id"], {"behavior": "deny"}
        )
```

Unanswered prompts are treated as denied by the daemon after a timeout
(`permissionTimeoutSecs` in the `hello` payload / `client.server_hello`).

## Events

`async for ev in client.events()` yields dicts with a `type` of:

| type            | fields                     | meaning                          |
|-----------------|----------------------------|----------------------------------|
| `connected`     | —                          | the initial link came up         |
| `event`         | `sessionId`, `event`       | StreamEvent push (`session.event`) |
| `prompt_done`   | `sessionId`, `result`\|`error` | a prompt turn ended           |
| `disconnected`  | —                          | link dropped, redialing          |
| `reconnected`   | —                          | link is back (daemon restarted)  |

The stream survives reconnects and ends only after `close()`. One
consumer at a time (the queue is consumed, not broadcast); a slow
consumer drops the oldest event that isn't a permission ask — check
`client.dropped_events()`.

## Reconnects

A background supervisor owns the link. When it drops, pending calls
fail with `ConnectionError` (prompts also report a `prompt_done` error
event), and the client redials with backoff — 100ms doubling to a 5s
cap — until the daemon returns. Calls made while down wait up to 10s
for the link, then raise. Kill and restart `damond` mid-conversation:
your app sees `disconnected` → `reconnected` and keeps going.

Relay links (`connect_relay` in the Node/Rust SDKs) are not part of
this milestone.

License: MIT OR Apache-2.0.
