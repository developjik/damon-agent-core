# Damon wire protocol v2

JSON messages over WebSocket (`GET /ws`) or the relay tunnel. One
connection multiplexes every session; requests dispatch concurrently —
a long `turn.start` never serializes the socket.

## Envelopes

```json
// request
{"id": 1, "method": "session.create", "params": {"backend": "claude"}}
// response
{"id": 1, "result": {"sessionId": "…", "backend": "claude"}}
{"id": 1, "error": {"code": -32000, "message": "…"}}
// server push (no id)
{"event": "session.event", "sessionId": "…", "data": {…StreamEvent…}}
// sent on connect, and as the response to a "hello" request
{"hello": {"protocol": 2, "daemon": "damond", "version": "0.3.0",
           "permissionTimeoutSecs": 300}}
```

Auth: `Authorization: Bearer <token>` header, or a single-use
`?ticket=` from `POST /v1/ws_ticket` (query-string tokens are refused —
they leak into logs). Loopback + no configured token → open.

## Methods

| method | params | result |
|---|---|---|
| `hello` | — | `{protocol, daemon, version, backends[], methods[]}` |
| `backend.list` | — | `{backends: [{id, available, capabilities}]}` |
| `session.create` | `{backend?, cwd?, model?, mode?, mcpServers?}` | `{sessionId, backend}` |
| `session.resume` | `{sessionId}` — or `{handle:{provider,native_handle}, cwd?, title?}` to import a native session | `{sessionId, backend}` |
| `session.list` | `{limit?, offset?}` | `{sessions: [{sessionId, createdAt, backend, title}]}` |
| `session.messages` | `{sessionId, limit?, offset?}` | `{messages: [StoredMessage]}` |
| `session.import` | `{backend, cwd?}` | `{sessions: [ImportableSession]}` |
| `session.delete` | `{sessionId}` | `{deleted: bool}` |
| `session.rename` | `{sessionId, title}` | `{renamed: bool}` |
| `session.fork` | `{sessionId, upto?}` | `{sessionId}` |
| `session.usage` | `{sessionId?}` | `{contextUsed, contextSize, costUsd, turns}` or `{sessions}` |
| `session.search` | `{query, limit?}` | `{results: [{sessionId, messageId, snippet}]}` |
| `turn.start` | `{sessionId, prompt, timeoutSecs?}` | `{turnId, stopReason, usage?}` |
| `turn.steer` | `{sessionId, prompt, expectedTurn?}` | `{result}` |
| `turn.cancel` | `{sessionId}` | `{cancelled: true}` |
| `permission.respond` | `{sessionId, requestId, response}` | `{}` |
| `session.set_model` | `{sessionId, model}` | `{}` |
| `session.set_mode` | `{sessionId, mode}` | `{}` |
| `catalog.models` | `{backend}` | `{models[], modes[], commands[]}` |

`prompt` is a string or an array of blocks `[{type:"text",text},…]`.

`turn.start` resolves when the turn ends — events stream first, then
the response. Disconnect cancels the turns that connection started.

## StreamEvent

`session.event.data` is a `StreamEvent`: `{turn_id?, type, …}` where
`type` is the `StreamEventKind` tag (snake_case):

- `thread_started {native_handle}` — resume token arrived
- `turn_started`, `turn_completed {usage?}`, `turn_failed {error, code?}`,
  `turn_canceled {reason}`
- `mode_changed {mode?}`, `model_changed {model}`
- `timeline` + flattened `TimelineItem` (`kind` tag):
  `user_message`/`assistant_message`/`reasoning {text}`,
  `tool_call {call_id,name,status,detail}`, `todo {items}`,
  `compaction {summary}`, `unknown {raw}`
- `permission_requested {id,kind,name,title?,input?,detail?,actions[]}`
- `permission_resolved {request_id}`
- `attention_required {reason: finished|permission}`
- `subagent {event}`

`ToolCall.detail` is `{type: shell|read|edit|web|task|other, …}`.
`PermissionResponse` is `{behavior:"allow",action_id?,updated_input?}`
or `{behavior:"deny",action_id?,message?,interrupt}`.

## Usage

`{input_tokens?, cached_input_tokens?, output_tokens?, cost_usd?,
context_window?, context_used?}` — all optional; backends report what
they know.
