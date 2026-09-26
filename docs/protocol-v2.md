# Damon wire protocol v2

**English** | [한국어](protocol-v2.ko.md)

JSON messages over WebSocket (`GET /ws`) or the relay tunnel. One
connection multiplexes every session; requests dispatch concurrently —
a long `turn.start` never serializes the socket.

## Envelopes

```json
// request
{"id": 1, "method": "session.create", "params": {"backend": "claude"}}
// response
{"id": 1, "result": {"sessionId": "…", "backend": "claude"}}
{"id": 1, "error": {"code": -32602, "message": "sessionId required"}}
// server push (no id)
{"event": "session.event", "sessionId": "…", "data": {…StreamEvent…}}
// sent on connect, and as the response to a "hello" request
{"hello": {"protocol": 2, "daemon": "damond", "version": "0.3.0",
           "permissionTimeoutSecs": 300}}
```

Auth: `Authorization: Bearer <token>` header, or a single-use
`?ticket=` from `POST /v1/ws_ticket` (query-string tokens are refused —
they leak into logs). Loopback + no configured token → open.

## Error codes

Errors carry a typed code. The JSON-RPC 2.0 reserved range covers
frame and request failures; Damon's own codes (`-32000..-32099`) are
stable wire contract so clients can degrade gracefully instead of
parsing messages:

| code | meaning | typical client action |
|---|---|---|
| `-32700` | frame was not valid JSON (id is null) | fix the serializer |
| `-32600` | an id to answer, but no `method` | fix the request |
| `-32601` | unknown method | check `hello` → `methods` |
| `-32602` | missing/malformed param | fix params |
| `-32000` | untyped server error | surface the message |
| `-32001` | session not live | `session.resume` and retry |
| `-32002` | backend lacks the capability (model/mode switch, steering) | hide the affordance / queue the text as the next prompt |
| `-32003` | backend not detected/registered | pick another backend from `backend.list` |
| `-32004` | `max_sessions` cap reached | close/delete a session first |
| `-32005` | response over the 1 MiB frame budget | page via `limit`/`offset` |
| `-32006` | a turn is already running on the session | wait for `turn.*` events, or `turn.cancel` first |

Pre-0.5.0 daemons answered every error with `-32000`; messages are
unchanged, so string-matching clients keep working.

## Methods

| method | params | result |
|---|---|---|
| `hello` | — | `{protocol, daemon, version, backends[], methods[]}` |
| `backend.list` | — | `{backends: [{id, available, capabilities}]}` |
| `session.create` | `{backend?, cwd?, model?, mode?, mcpServers?, projectId?}` — the project scopes the session: its root becomes the cwd default and its defaults fill unset params (explicit always wins) | `{sessionId, backend, replayed}` |
| `session.resume` | `{sessionId}` — or `{handle:{provider,native_handle}, cwd?, title?}` to import a native session | `{sessionId, backend, replayed}` |
| `session.list` | `{limit?, offset?, backend?, cwd?, tag?}` | `{sessions: [{sessionId, createdAt, backend, title, cwd, tags[]}]}` |
| `session.messages` | `{sessionId, limit?, offset?}` | `{messages: [{id, session_id, role, ts, data}]}` |
| `session.export` | `{sessionId}` | `{session: {sessionId, createdAt, backend, title, tags, cwd}, messages: [{id, role, ts, data}], usage: {contextUsed, contextSize, costUsd, turns}}` |
| `session.import` | `{backend, cwd?}` | `{sessions: [ImportableSession]}` |
| `session.delete` | `{sessionId}` | `{deleted: bool}` |
| `session.close` | `{sessionId}` | `{closed: bool}` — kills the live backend process, keeps the row/history; frees a `max_sessions` slot; idempotent |
| `session.restart` | `{sessionId}` | `{sessionId, backend, replayed}` — kills the live backend (mid-turn refuses `-32006`) and reattaches through the persisted handle |
| `session.status` | `{}` | `{sessions: [{sessionId, backend, busy, idleSecs}]}` — live backend sessions only |
| `session.watch` | `{sessionId}` — must be live | `{sessionId, subscribed, replayed}` — subscribes this connection to the session's events without creating/resuming/turning on it; the cross-surface notification path (a chat bridge pings its channel when a web-UI session finishes or asks for permission) |
| `session.unwatch` | `{sessionId}` | `{sessionId, subscribed}` — drops this connection's subscription; the session and other connections are untouched |
| `session.set_pinned` | `{sessionId, pinned}` | `{updated: bool}` — pinned rows float first in `session.list` and survive the retention sweep |
| `session.set_archived` | `{sessionId, archived}` | `{updated: bool}` — archived rows are hidden from `session.list` unless `includeArchived`, and survive the retention sweep |
| `session.set_tags` | `{sessionId, tags[]}` — replaces the whole list | `{updated: bool}` |
| `session.rename` | `{sessionId, title}` | `{renamed: bool}` |
| `session.fork` | `{sessionId, upto?}` | `{sessionId}` — messages, tags, usage history, and the backend resume handle are copied; the title gains `" (fork)"` |
| `session.usage` | `{sessionId?}` — or `{daily: true, days?}` for the global day rollup (default 7 days) | per-session `{sessionId, contextUsed, contextSize, costUsd, turns}`, per-model `{sessions: [...]}`, or `{daily: [{date, turns, costUsd, contextUsed}]}` |
| `session.list` | `{limit?, offset?, backend?, cwd?, tag?, includeArchived?}` | `{sessions: [{sessionId, createdAt, backend, title, cwd, tags, pinned, archived}]}` — pinned first; archived hidden unless requested |
| `session.search` | `{query, limit?, sessionId?, backend?, cwd?, since?, until?}` | `{results: [{sessionId, messageId, snippet}]}` |
| `turn.start` | `{sessionId, prompt, timeoutSecs?, detach?}` | `{turnId, stopReason, usage?}` — or `{turnId, detached: true}` immediately when `detach` is true |
| `turn.steer` | `{sessionId, prompt, expectedTurn?}` | `{result}` |
| `turn.cancel` | `{sessionId}` | `{cancelled: true}` |
| `permission.respond` | `{sessionId, requestId, response}` | `{}` |
| `session.set_model` | `{sessionId, model}` | `{}` |
| `session.set_mode` | `{sessionId, mode}` | `{}` |
| `catalog.models` | `{backend}` | `{models[], modes[], commands[]}` |
| `logs.tail` | `{lines? — default 200, max 2000}` | `{lines: string[]}` — the daemon's recent log lines, oldest first |
| `logs.follow` | `{follow? — default true}` | `{following: bool}` — every new log line arrives as a `{"event":"log.line","data":{"line"}}` push on this connection; `follow:false` stops it |
| `file.read` | `{sessionId, path}` | `{content: base64, bytes, path}` — jailed to the session cwd; 512 KiB read cap (`-32005` over) |
| `file.write` | `{sessionId, path, content: base64}` | `{written, path}` — jailed to the session cwd; 1 MiB decoded cap |
| `file.list` | `{sessionId, path? — default "."}` | `{entries: [{name, dir, bytes}], path}` — jailed to the session cwd, 1000 entries |
| `channel.get_state` | `{convId, key}` | `{value: string?}` |
| `channel.set_state` | `{convId, key, value}` | `{set: true}` |
| `channel.delete_state` | `{convId, key}` | `{deleted: true}` |
| `project.create` | `{name?, root, defaults?}` | `{projectId, name, root, defaults}` — root must be absolute; defaults keys: backend/model/mode/mcpServers |
| `project.list` | `{}` | `{projects: [{projectId, name, root, defaults}]}` |
| `project.get` | `{projectId}` | `{projectId, name, root, defaults}` |
| `project.set_defaults` | `{projectId, defaults}` | `{updated: bool}` — replaces the whole set |
| `project.delete` | `{projectId}` | `{deleted: true}` — refuses `-32602` while sessions reference the project |

`session.search` queries are tokenized, never passed to the FTS5
parser raw: quoted spans are phrases, `AND`/`OR`/`NOT` pass through,
and a **trailing `*` on a term makes it a prefix query** — `error*`
matches `error`/`errors`/`erroring` (a bare `*` is nothing
searchable).

### Projects

A project groups sessions by workspace root: `project.create` mints
`{projectId, root, defaults}`; `session.create {projectId}` scopes the
new session (root → cwd default; backend/model/mode/mcpServers
defaults fill whatever the request left unset — explicit params win).
`session.list`, `session.search`, and the global `session.usage` view
all accept a `projectId` filter; deleting a project refuses while
sessions still reference it.

`file.*` paths resolve relative to the session's stored cwd (or
absolute, but still inside it). The jail is enforced after
best-effort canonicalization — symlink hops resolve where the path
exists, a not-yet-existing write target resolves through its deepest
existing ancestor, and lexical `..` folding covers the rest — so an
escape fails closed with `-32602`. Combine with `allowed_dirs` to
bound which cwds sessions may have in the first place.

`prompt` is a string or an array of blocks `[{type:"text",text},…]`.


### Listing filters, timestamps, tags

`session.list` filters AND together: `backend` (agent id) and `cwd`
match the stored values exactly; `tag` matches membership in the
session's tag list. Tags are managed whole-list by `session.set_tags`
(`[]` clears) and ride along on forks.

Every stored message carries `ts`, the unix-ms persistence stamp;
rows written before the column existed read `ts: 0` (unknown).
`session.search` bounds are message-time: `since`/`until` accept
RFC3339 (`2026-09-01T13:30:00Z`, optional offset) or date-only
`YYYY-MM-DD`, which reads as that day's midnight UTC — an explicit
offset shifts the instant, a naive timestamp reads as UTC (the store
stamps UTC). Both bounds are inclusive. A `ts: 0` row compares as its
session's `created_at` — the coarsest bound the old schema could
express — so pre-migration history stays searchable. Malformed bounds
error instead of matching nothing.

`session.usage` with `daily: true` (global view only; `days` defaults
to 7 and covers today plus the preceding days) groups usage rows by
UTC day: `{date, turns, costUsd, contextUsed}` where cost sums the
per-turn deltas and context is the day's latest snapshot. Days with
no rows are absent from the array.
`turn.start` resolves when the turn ends — events stream first, then
the response. Disconnect cancels the turns that connection started.

### Detached turns (`detach: true`)

A turn that must outlive its client — the mobile case: a locked phone
drops its socket mid-turn — passes `detach: true`. The response returns
immediately as `{turnId, detached: true}` and the turn keeps running on
a task no connection owns: disconnecting never cancels it.
`turn.cancel`, a deny-with-`interrupt`, and `timeoutSecs` still end it
early. Persistence, events, and replay are identical to a blocking
turn, so a client that reconnects (or a second client) picks the turn
up through the normal replay path — in-flight journal while it runs,
store rows once it ends. `session.status` reports the session as busy
until the detached turn finishes.

### Event replay for late subscribers

A connection that first touches a session (`session.create`,
`session.resume`, `turn.start`) receives everything it missed BEFORE
live streaming starts, as `session.event` frames tagged
`"replay": true`: persisted history from the store (user, assistant,
reasoning, tool, compaction, and error rows as timeline events, newest
last, capped at 200),
then the in-flight turn's journal. The `replayed` count in the
create/resume result says how many frames were sent. Live frames carry
no `replay` field. A turn that overflowed the daemon's in-flight
journal is not replayed (the live remainder continues uninterrupted;
the assembled result is still persisted at turn end).

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
  `compaction {summary}`, `error {message}`, `unknown {raw}`.
  The daemon persists `compaction` and `error` rows when a backend
  reports a context compaction or a `turn_failed` — they appear in
  `session.messages` (roles `compaction`/`error`, content in
  `data.content`) and in replay; live consumers see the corresponding
  `timeline`/`turn_failed` events as they happen.
- `permission_requested {id,kind,name,title?,input?,detail?,actions[]}`
- `permission_resolved {request_id}`
- `attention_required {reason: finished|permission}`
- `subagent {event}`

`ToolCall.detail` is `{type: shell|read|edit|web|task|other, …}`.
`PermissionResponse` is `{behavior:"allow",action_id?,updated_input?}`
or `{behavior:"deny",action_id?,message?,interrupt}` — a deny with
`interrupt: true` makes the daemon interrupt the whole turn right
after delivering the denial, not just refuse the one call.

## Relay handshake

The E2E handshake's first frame may carry `"kdf": "s256"`; a daemon
that understands it echoes the field and both sides compute the proof
as sha256 iterated 2^16 extra times over a domain-separated seed
(brute-force hardening for low-entropy tokens). Peers that don't
negotiate keep the original single-hash proof — the marker is
additive, never breaking. Relay clients ping every 30s; a link silent
for 120s (either side) is closed as half-open instead of waiting out
TCP, and the relay frees the daemon-side session slot.

## Usage

`{input_tokens?, cached_input_tokens?, output_tokens?, cost_usd?,
context_window?, context_used?}` — all optional; backends report what
they know.
