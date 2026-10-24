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
{"hello": {"protocol": 2, "daemon": "damond", "version": "0.5.0",
           "permissionTimeoutSecs": 300}}
```

Auth: `Authorization: Bearer <token>` header, or a single-use
`?ticket=` from `POST /v1/ws_ticket` (query-string tokens are refused —
they leak into logs). Loopback + no configured token → open. The
ticket endpoint also accepts an approved web device's own token in the
body (`{"deviceToken": …}`) — the per-device revokable credential from
pairing (see [Web devices](#web-devices)); the master bearer path is
unchanged.

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
| `-32007` | worktree operation failed — `error.data.kind` carries the taxonomy | branch on `data.kind` (`pr_not_found`, `branch_exists`, …) |
| `-32008` | no pending ask for that `requestId` (`ASK_NOT_FOUND`) — already answered, settled by the agent, or its turn ended | stop retrying that card |
| `-32009` | plan-review conflict (`PLAN_CONFLICT`) — another surface claimed the decision, or the call targeted a stale revision / closed plan | refetch via `plan.list` and decide on the current revision |

Pre-0.5.0 daemons answered every error with `-32000`; messages are
unchanged, so string-matching clients keep working.

## Methods

| method | params | result |
|---|---|---|
| `hello` | — | `{protocol, daemon, version, backends[], methods[]}` |
| `backend.list` | — | `{backends: [{id, available, capabilities}]}` |
| `backend.versions` | `{backend?}` — one backend, else the whole catalog | `{versions: [{backend, installed, version?, latest?, updateAvailable, updateKind: "npm"\|"manual"\|"unknown"}]}` — `latest` only where an npm package is verified; probes run on this call only, never scheduled |
| `backend.update_plan` | `{backend}` | `{backend, action: "install"\|"update", kind, command[], manualCommand, canRun, blockers[], platform}` — the exact argv `backend.update` runs plus the copy-paste fallback; no side effects |
| `backend.update` | `{backend, creationId?}` | `{creationId, ok: true, version?}` — runs the plan (npm global install or the CLI's official installer); progress streams to the requesting connection as `{"event":"backend.update_progress","creationId","data":{phase, line?, exitOk?}}` with per-line caps; `-32602` when the backend has no verified channel (see the plan's blockers), `-32000` on failure or the 420s timeout (output tail rides the message) |
| `backend.update_cancel` | `{creationId}` | `{cancelled: bool}` — false when no live run holds the id |
| `session.create` | `{backend?, cwd?, model?, mode?, mcpServers?, projectId?}` — the project scopes the session: its root becomes the cwd default and its defaults fill unset params (explicit always wins) | `{sessionId, backend, replayed}` |
| `session.resume` | `{sessionId}` — or `{handle:{provider,native_handle}, cwd?, title?}` to import a native session | `{sessionId, backend, replayed}` |
| `session.messages` | `{sessionId, limit?, offset?}` | `{messages: [{id, session_id, role, ts, data}]}` |
| `session.export` | `{sessionId}` | `{session: {sessionId, createdAt, backend, title, tags, cwd}, messages: [{id, role, ts, data}], usage: {contextUsed, contextSize, costUsd, turns}}` |
| `session.import` | `{backend, cwd?}` | `{sessions: [ImportableSession]}` |
| `session.delete` | `{sessionId}` | `{deleted: bool}` |
| `session.rescan` | `{}` | `{scanId, alreadyRunning, report: {scanned, reparsed, reused, archived, tombstoned, skippedBackends[]}}` — sweeps every backend's native session storage and creates damon bookmark rows for sessions the daemon has never seen; single-flight (a second call while one runs returns the in-flight `scanId`); progress streams to the requesting connection as `{"event":"history.scan_progress","scanId","data":{stage, …}}`; see [The history scanner](#the-history-scanner) |
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
| `usage.summary` | `{days?, tzOffsetMinutes?}` — the last `days` local days (default 7, today inclusive); `tzOffsetMinutes` shifts the UTC stamps into the caller's calendar (default 0 = UTC) | `{days, rows: [{date, backend, model, turns, contextUsed, contextSize, costUsd}], total: {turns, contextUsed, contextSize, costUsd}}` — one row per local day × backend × model; context columns sum the per-turn snapshots, cost sums the per-turn deltas; days with no rows are absent |
| `usage.clear` | `{}` | `{cleared: true}` — deletes every usage row; sessions and messages are untouched |
| `mission.create` | `{flowId?, name?, definition}` — `flowId` defaults to a fresh uuid; re-creating a `flowId` inserts the next version (runs keep the snapshot they started with) | `{flowId, version}` — `-32602` for an invalid definition (unknown node kind, agent node without prompt/backend, read-only without `allowedTools`, root rule, unparseable condition) |
| `mission.start` | `{flowId}` — starts the LATEST version | `{runId, version}` — `-32602` unknown flow; `-32003` backend not registered; `-32002` read-only agent node refused: the backend cannot enforce a tool allowlist and damon never downgrades a read-only stage |
| `mission.cancel` | `{runId}` | `{runId, cancelled}` — `false` when no live scheduler holds the id |
| `mission.respond` | `{runId, nodeId, decision, feedback?}` — `decision` is `approve \| feedback \| exclude`; `feedback` requires non-empty text | `{accepted: true, live}` — the store CAS is the one-winner gate: `-32602` when the inbox item is not pending (another answer won) |
| `mission.list` | `{}` | `{runs: [{runId, flowId, version, status, interrupted, ended, createdAt, tasks: [{nodeId, status, attempt}]}]}` — status is always derived from the task states (running / succeeded / failed / interrupted / cancelled), never stored |
| `mission.get` | `{runId}` | `{runId, flowId, version, status, interrupted, ended, definition, tasks: [{nodeId, status, attempt, output?, error?}], inbox: [{nodeId, status, decision?, feedback?}]}` — `-32602` unknown run |
| `mission.watch` | `{runId}` — must be live | `{runId, subscribed}` — streams `{"event":"mission.progress","runId","data":{node, status, attempt}}` frames |
| `mission.unwatch` | `{runId}` | `{runId, subscribed}` |
| `turn.start` | `{sessionId, prompt, timeoutSecs?, detach?}` | `{turnId, stopReason, usage?}` — or `{turnId, detached: true}` immediately when `detach` is true |
| `turn.steer` | `{sessionId, prompt, expectedTurn?}` | `{result}` |
| `turn.cancel` | `{sessionId}` | `{cancelled: true}` |
| `permission.respond` | `{sessionId, requestId, response}` — for `kind: question` asks, an `answers` map (`{question → label | [labels]}`) submits picks; an allow without answers skips | `{}` — unknown `requestId` fails with `-32008`; a *delivery* failure leaves the ask answerable; on `kind: plan` asks the decision also lands in the durable plan-review row |
| `plan.list` | `{sessionId}` | `{plans: [{planId, sessionId, requestId, revision, title, content, contentHash, complete, status, decision?, decisionIntentAt?, appliedAt?, createdAt, updatedAt, supersededBy?}]}` — every revision of every plan, chain-ordered |
| `plan.respond` | `{planId, expectedRevision, decision, feedback?}` — `decision` is `approve` \| `request_changes` \| `defer`; feedback is required non-empty for request_changes | `{recorded: true, status, applied, planId, revision}` — CAS claim against `expectedRevision`: `-32009` on a lost race or a stale/closed revision; approval of an incomplete plan and feedback-less request_changes refuse `-32602`; `defer` records non-terminally without touching the live ask |
| `session.set_model` | `{sessionId, model}` | `{}` |
| `session.set_mode` | `{sessionId, mode}` | `{}` |
| `catalog.models` | `{backend}` | `{models[], modes[], commands[]}` |
| `catalog.commands` | `{sessionId?, cwd?}` — the composer's `/` picker: command and skill files the CLIs expand themselves | `{commands: [{name, description?, argumentHint?, source, kind}]}` — workspace entries shadow same-named global ones; sorted commands then skills |
| `logs.tail` | `{lines? — default 200, max 2000}` | `{lines: string[]}` — the daemon's recent log lines, oldest first |
| `logs.follow` | `{follow? — default true}` | `{following: bool}` — every new log line arrives as a `{"event":"log.line","data":{"line"}}` push on this connection; `follow:false` stops it |
| `file.read` | `{sessionId, path}` | `{content: base64, bytes, path}` — jailed to the session cwd; 512 KiB read cap (`-32005` over) |
| `file.write` | `{sessionId, path, content: base64}` | `{written, path}` — jailed to the session cwd; 1 MiB decoded cap |
| `file.list` | `{sessionId, path? — default "."}` | `{entries: [{name, dir, bytes}], path}` — jailed to the session cwd, 1000 entries |
| `file.index` | `{sessionId?, cwd?}` — the composer's `@path` picker: one gitignore-aware walk of the root | `{entries: [{rel, isDir}], truncated}` — `node_modules`/`target` pruned, hidden entries skipped, symlinks never followed; bounded at 10 000 entries / 512 KiB with `truncated: true` on an early stop |
| `file.grant` | `{projectId?, path}` — `path` absolute, must exist and be a directory; symlinks/`..` resolve to the canonical form | `{granted: true, root, projectId?}` — persists an extra directory the file jail admits (duplicate grants are idempotent); claude sessions pick it up as `--add-dir` on their next launch |
| `file.grants` | `{projectId?}` — effective grants for the scope (project + global); omitted lists globals only | `{grants: [{root, projectId?, createdAt}]}` |
| `file.ungrant` | `{projectId?, path}` — matched against the stored canonical root | `{revoked: bool}` — false when no such grant |
| `channel.get_state` | `{convId, key}` | `{value: string?}` |
| `channel.set_state` | `{convId, key, value}` | `{set: true}` |
| `channel.delete_state` | `{convId, key}` | `{deleted: true}` |
| `device.pairing_key` | `{}` | `{key: string}` — the one-time pairing key (8 unambiguous chars), minted on first ask; the next `POST /v1/pair` spends it and a fresh key rotates in, under one lock |
| `device.list` | `{}` | `{devices: [{deviceId, name, status: "pending"\|"approved", createdAt, approvedAt?, lastSeen?}]}` — lastSeen updates at most once a minute (throttled); the token hash never leaves the store |
| `device.approve` | `{deviceId}` | `{approved: bool}` — only from pending; false when unknown or already approved |
| `device.rename` | `{deviceId, name}` | `{renamed: bool}` — blank names refuse |
| `device.revoke` | `{deviceId}` | `{revoked: bool}` — the row goes with the token hash, so the credential dies immediately; doubles as deny for pending requests |
| `prompt.add` | `{text, sessionId?, cwd?}` — one sent prompt, trimmed and capped at 300 chars | `{recorded: true}` — upsert: a resend bumps the count and moves the row to newest; the newest 200 per cwd survive |
| `prompt.recent` | `{limit? — default 50, max 200, sessionId?, cwd?}` | `{prompts: [{text, count}]}` — newest first, per cwd |
| `skills.targets` | `{}` | `{targets: [{id, label, path, available}]}` — the agent CLIs' skill directories the hub can sync into; `available` means the engine home exists (the hub never creates one) |
| `skills.installed` | `{}` | `{skills: [{id, name, description?, directory, managed, repo?, targets[], targetStates, sourceKind?, readonly?, targetPaths?}], generatedAt}` — registry-managed plus unmanaged finds, name-sorted |
| `skills.repos` | `{}` | `{repos: [{owner, name, branch, enabled}]}` — registry plus `[skills] extra_repos` |
| `skills.install` | `{owner, name, branch?, directory?, installName?, targets?, force?}` — `directory` defaults `.`; `targets` defaults `[skills].default_targets` else all available; an explicit `[]` installs without syncing | `{skill: SkillEntry, targetResults: [{target, ok, error?}]}` |
| `skills.uninstall` | `{id}` | `{trashed, restoreId, ttlMs: 300000, targetResults}` |
| `skills.restore` | `{id}` — within the 5-minute trash window | `{skill, targetResults}` |
| `skills.setTargets` | `{id, targets[]}` — unknown ids fail `-32602`; newly selected sync, deselected remove | `{targetResults}` |
| `skills.importLocal` | `{directory, targets?}` — copies a target-root skill into the managed store, preserving the source | `{skill, targetResults}` |
| `skills.deleteLocal` | `{directory, targets?}` | `{targetResults}` — the preserved source copy is kept and reported |
| `skills.addRepo` | `{owner, name, branch?}` | `{added: true}` |
| `skills.removeRepo` | `{owner, name}` | `{removed: true}` — installed skills from the repo stay managed |
| `skills.activity` | `{limit? — default 50, max 500}` | `{activity: [{ts, kind, …}]}` — newest first |
| `skills.discover` | `{force?}` — GitHub trees of the enabled repos; 200 skills/repo, 1h cache | `{skills: [{key, name, description?, directory, readmeUrl?, repoOwner, repoName, repoBranch}], cached, generatedAt}` |
| `skills.search` | `{q, limit? — 1-50, offset?}` — skills.sh | `{query, totalCount, skills: [{key, name, description?, directory, installs?, repoOwner, repoName, repoBranch}]}` |
| `skills.popular` | `{limit? — 1-200, force?}` — seeded skills.sh queries, 6h cache | `{skills, cached, generatedAt}` |
| `skills.updates` | `{force?}` — re-hashes the repos installed skills came from, 1h cache | `{updates: {skillId: has-update}, checkedAt, cached}` |
| `skills.content` | `{directory}` — local read: managed copy, then target roots, then read-only sources | `{directory, path, markdown, truncated}` — 512 KiB cap |
| `skills.remoteContent` | `{owner, name, branch?, directory}` — directory-name score match against the repo tree | `{name, description?, directory, markdown, truncated, branch}` |
| `prompts.list` | `{sessionId?, cwd?}` — the prompt library (`!` picker) | `{prompts: [{name, description?, argumentHint?, scope, path}]}` — workspace first, name-sorted; `path` is the mutation handle |
| `prompts.get` | `{promptPath, sessionId?, cwd?}` | `{name, description?, argumentHint?, scope, content}` — the body |
| `prompts.create` | `{scope, name, description?, argumentHint?, content, sessionId?, cwd?}` — name: no whitespace/separators; body capped 64 KiB | `{created: true, path}` |
| `prompts.update` | `{promptPath, name?, description?, argumentHint?, content?, sessionId?, cwd?}` — missing keeps, `""` clears a meta field | `{updated: true, path}` |
| `prompts.delete` | `{promptPath, sessionId?, cwd?}` | `{deleted: bool}` — false when already gone |
| `prompts.move` | `{promptPath, scope, sessionId?, cwd?}` — must differ from the current scope | `{moved: true, path}` |
| `project.create` | `{name?, root, defaults?}` | `{projectId, name, root, defaults}` — root must be absolute; defaults keys: backend/model/mode/mcpServers |
| `project.list` | `{}` | `{projects: [{projectId, name, root, defaults, kind: "project"\|"worktree", parentId?, meta}]}` — worktree rows carry their git facts in `meta` |
| `project.get` | `{projectId}` | `{projectId, name, root, defaults, kind, parentId?, meta}` |
| `project.set_defaults` | `{projectId, defaults}` | `{updated: bool}` — replaces the whole set |
| `project.delete` | `{projectId}` | `{deleted: true}` — refuses `-32602` while sessions reference the project |
| `worktree.create` | `{repo, branch?, prNumber?, path?, baseRef?, existingBranch?, creationId?}` — `repo` resolves to its top level; PR flow fetches `+refs/pull/N/head` into `branch` (default name `pr-N`); default path `<repo parent>/<repo>-worktrees/<branch>`; `existingBranch` expects the branch to exist | `{creationId, projectId, parentProjectId?, path, branch, meta: {branch, repo, prNumber?, prUrl?, baseRef?}, resumed}` — the worktree registers as a `kind:"worktree"` project; `session.create {projectId}` lands an agent in it; `resumed` adopted an existing worktree-on-branch |
| `worktree.cancel` | `{creationId}` | `{cancelled: bool}` — false when no live creation holds the id; kills the in-flight git process group |
| `worktree.remove` | `{projectId?, path?, branch?, deleteBranch?}` — row removal happens first, so a bound-sessions refusal (`-32602`) aborts before any git effect | `{removed: true, orphanDirectory, branchDeleted, branchKeptReason?}` — git's own remove is always `--force`; a refusal falls back to a direct delete + `worktree prune` |
| `worktree.list` | `{repo? \| projectId?}` | `{repo, worktrees: [{path, branch?, head, isMain, locked, lockReason?, prunable, projectId?}], registered: [{projectId, name, path, parentId?, meta}]}` — git's porcelain view joined with damon's rows |
| `worktree.resolve_pr` | `{repo, input, branch?}` — `input` is `1842`, `#1842`, or a GitHub PR URL; details come from `gh pr view` (3s deadline) | `{number, repo: "owner/repo", title?, state?, author?, degraded, suggestedBranch, suggestedPath, branchConflict, dirConflict}` — `degraded` means gh was missing/hung; number and slug are still confirmed |
| `worktree.merged` | `{repo, branch, base? — default HEAD}` | `{merged: bool}` — ancestry check; squash merges report false |
| `mcp.inventory` | `{}` | `{entries: [{backend, name, transport, command?, url?, envKeys?, source, format, version, enabled?, readonlyReason?}], sources: [{backend, source, format, error?}]}` — every backend's NATIVE MCP config, REDACTED (env values never leave the file; URLs lose userinfo/query; stdio shows the program + an arg count); a broken config file reports its error per source and the inventory continues |
| `mcp.probe` | `{backend, name}` | `{backend, name, result: "connected"\|"needs_auth"\|"failed"\|"unsupported", tools: string[] (names, ≤40), serverName?, protocolVersion?, elapsedMs, error?}` — REAL handshake (stdio: own process group, 25s cap; HTTP: 12s cap); explicit request only, never scheduled; the server's command/args/env never reach the wire |
| `mcp.setEnabled` | `{backend, name, enabled, expectedVersion, cwd?}` — claude only | `{set: true}` — flips claude's own per-project toggle in ~/.claude.json, CAS'd on the inventory `version` (external edits reject `-32010`); parse-failed configs are never written; readonly backends refuse `-32602` with the reason |

`worktree.create` errors are `-32007` with `data.kind`:
`not_a_repo`, `invalid_branch`, `branch_not_found`, `branch_exists`,
`branch_checked_out`, `dir_exists`, `not_allowed` (the resolved path
is outside `allowed_dirs`), `no_origin`, `pr_not_found`,
`fetch_failed`, `base_not_found`, `add_failed`,
`sparse_checkout_empty`, `register_failed`, `canceled`. Creation
progress arrives on the requesting connection as
`{"event":"worktree.progress","creationId","data":{"stage","message"}}`
with stages `validate → fetch → add → register → done` — one frame per
stage, so a long fetch is not silent.

`session.search` queries are tokenized, never passed to the FTS5
parser raw: quoted spans are phrases, `AND`/`OR`/`NOT` pass through,
and a **trailing `*` on a term makes it a prefix query** — `error*`
matches `error`/`errors`/`erroring` (a bare `*` is nothing
searchable). The index uses the `trigram` tokenizer, so a match is a
substring match — including inside CJK runs (`커밋` finds
`커밋 메시지`), which the previous unicode61 index could not do.
Terms shorter than 3 characters (the trigram minimum) would be
silently dropped by MATCH, so they take an exact-substring LIKE path
instead: short terms are mandatory AND conjuncts (an `OR` touching
one narrows to AND), a NOT-operand exclusion is honored, and a query
built only of them returns newest-first hits with a Rust-built
snippet instead of ranked ones.

### CLI lifecycle

`backend.versions` probes every cataloged backend's `<bin> --version`
(10s cap) and — where an npm package is verified — the registry's
latest (`npm view`, 15s cap), concurrently. Probes run on explicit
request only: never scheduled, never on connect. The npm map is
verified against each CLI's own install docs (claude
`@anthropic-ai/claude-code`, codex `@openai/codex`, omp
`@oh-my-pi/pi-coding-agent`, pi `@earendil-works/pi-coding-agent`,
qwen `@qwen-code/qwen-code`, opencode `opencode-ai`); backends
without a verified npm distribution (droid, mimo — official
installers; zcode — no verified channel) probe the installed version
only and report `updateKind: "manual"` (official one-liner is the
update path) or `"unknown"` (blockers only) rather than guessing a
package.

`backend.update` executes the plan `backend.update_plan` previewed —
one argv construction serves both, so the confirm dialog can never
drift from what runs. Updates NEVER run automatically or on a
schedule; they are user-initiated, stream their output (per-line
capped) to the requesting connection, and can be cancelled. `damond
doctor` reuses the same 10s probe: detected backends report their
version inline, degrading to "version unknown" on a timeout.

### Web devices

A phone on the LAN no longer needs the master `auth_token`. The
desktop reveals a one-time pairing key (`device.pairing_key`, also in
the settings drawer); the device spends it at `POST /v1/pair` with
`{pairingKey, name}` — the key is consumed AND rotated in a single
locked step, so a code opens exactly one pairing — and receives a
device token plus a `pending` row (`{deviceId, token, status}`; the
token is returned exactly once and stored SHA-256-hashed
daemon-side). The device polls `POST /v1/pair/status`
(`{deviceId, token}`) → `pending` / `approved` / `revoked` (a missing
row reads revoked — fail closed). A trusted surface approves via
`device.approve`; until then the device mints nothing.

Once approved, the device exchanges its token at
`POST /v1/ws_ticket` (`{"deviceToken": …}`) for the same single-use
WS tickets the master token mints. `device.revoke` deletes the row —
the credential is dead immediately, no grace period. Non-loopback
binds still refuse to start without a resolvable master `auth_token`;
relay-mode devices keep the master-token login (the E2E handshake
proves the master secret — device tokens are a direct-reach surface).
`hello` carries `remoteClients`, the live count of non-loopback `/ws`
connections, so surfaces can show a "remote control active" badge.

### The history scanner

The daemon knows the sessions its CLIs created outside Damon:
`session.rescan` walks every backend's own native session storage —
through each backend's discovery, honoring env overrides
(`CLAUDE_CONFIG_DIR`, `FACTORY_HOME_OVERRIDE`, …) — and creates damon
bookmark rows (id, backend, native handle, cwd, title) for entries the
daemon has never seen. Per the PersistenceHandle philosophy, the
provider's session file stays the source of truth; the row is a
bookmark, and resuming it works exactly like any native-session
resume.

Scope, stated precisely: the scan delivers **visibility and
resumability** — discovered sessions appear in `session.list` (with
derived titles and cwd) and their native handles resume like any
native session. It does **not** extract transcript messages: Damon has
no message-extraction path for native handles today (`session.import`
is a listing call; a resume binds the handle and streams only new
turns), so `session.search` indexes messages from damon-run turns
only. Per-backend transcript extractors that feed scanned sessions
into the search index are future work.

The sweep is incremental: each discovered entry carries a stat
signature (size, mtime) — unchanged signature, no work. Titles derived
from transcript heads are noise-filtered (pasted `<file …>` envelopes,
`[Image #N, WxH]` placeholders, `<command-name>` shells and their kin
never name a session), and head reads are byte-capped at 1 MiB in
16 KiB chunks — Codex embeds its full base_instructions prompt in the
first line, so unbounded reads explode.

Cadence: one boot scan (a few seconds after start) plus explicit
`session.rescan`; `[history] scan_interval_secs` adds a timer (0, the
default, = off) and `[history] enabled = false` turns the automatic
sweeps off entirely. Single-flight globally — a second `session.rescan`
while one runs returns the in-flight `scanId`; progress events ride the
requesting connection.

Failures are contained: a backend whose discovery fails is reported in
`skippedBackends` and the rest of the scan proceeds. A native file that
vanishes means the provider's truth is gone — its damon session is
**archived** (hidden from `session.list`, history kept), never deleted.
`session.delete` on a discovered session tombstones its native handle
so no sweep re-imports it; resuming the same native handle clears the
tombstone (the user's explicit new intent wins). The scanner never
writes or deletes native files.

### MCP inventory and probe

Each agent CLI keeps its MCP config where its own docs say: claude in
`~/.claude.json` (`mcpServers` plus per-project scopes under
`projects[cwd]`, honoring `CLAUDE_CONFIG_DIR`), codex in
`~/.codex/config.toml` (`[mcp_servers.*]`, honoring `CODEX_HOME`),
qwen in `~/.qwen/settings.json`, opencode in
`$XDG_CONFIG_HOME/opencode/opencode.json`, droid in
`~/.factory/mcp.json`. `mcp.inventory` reads them all and reports, per
entry, the transport, a REDACTED command/URL (env keys yes, values
never; userinfo/query dropped; stdio shows the program plus an arg
count), the source file, a content-hash `version`, and — where the
backend tracks it — the enabled state. Sources that fail to parse are
reported per file (`sources[].error`) and never abort the inventory.
Backends without a verified config location report no source rather
than a guessed path (omp/pi manage MCP through extensions; zcode and
mimo are unverified).

`mcp.probe {backend, name}` performs a REAL MCP handshake against the
configured server — stdio spawns it in its own process group (killed
on exit, 25s cap), HTTP POSTs streamable-HTTP (12s cap) — walking
`initialize` → `notifications/initialized` → `tools/list` and
returning tool NAMES (≤40) plus the server's name and protocol
version. Probes are explicit-request only: never scheduled, never
auto-run. The server's command, args, and environment never appear on
the wire.

`mcp.setEnabled` is claude-only: it flips the same per-project
`disabledMcpServers` switch the claude TUI uses, guarded by the
entry's content-hash `version` — an external edit since the inventory
snapshot rejects with `-32010`. The config must parse (a broken file
is never written), same-path writes are serialized, and the replace is
atomic. Every other backend is read-only with the reason: agents own
their config; Damon flips the CLI's documented switch, it never
authors config.

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

Granted roots widen that jail on purpose: `file.grant` persists an
extra directory (absolute, existing, canonicalized — a grant to
`/var` that resolves through a symlink to `/private/var` stores the
latter, so jail checks and revocations match exactly) scoped to a
project or global; `file.grants` lists what is effective for a scope
and `file.ungrant` revokes it. Widening is immediate for `file.*`;
backends with a native widening flag pick grants up on their next
launch (claude gets one `--add-dir` per effective grant — backends
without an equivalent are not faked). A grant is an explicit user
action and is not bounded by `allowed_dirs`, which gates where
sessions may spawn, not what an explicitly granted session may read.

### Composer support

`catalog.commands`, `file.index`, `prompt.add`, and `prompt.recent`
back the web UI's composer pickers (slash commands, `@path` mentions,
prompt-history completion). They share one root selector: a named
session's stored cwd (the same jail as `file.*`), else a client cwd
through the same `allowed_dirs` gate as `session.create`, else the
daemon's working directory — which is exactly what a cwd-less
`session.create` lands on, so a picker opened before the lazy first
session indexes the directory the session will run in.

`catalog.commands` scans the command/skill files the installed CLIs
already expand (`.claude/commands` recursively, `.claude/skills`, and
the global roots the CLIs honor via `CLAUDE_CONFIG_DIR` /
`CODEX_HOME` / `~/.agents/skills`); the daemon never expands `/name`
itself — the selected text is sent as the prompt and the CLI does the
rest. Prompt history is per-cwd, newest-200, shared by every client
surface (a phone on the relay sees the same completions as the
desktop browser).

`prompt` is a string or an array of blocks `[{type:"text",text},…]`.

### Skills hub and prompt library

The `skills.*` surface is a package manager for agent skills: install
a skill once into the daemon's managed store
(`<data_dir>/skills-hub/managed/`), and the hub syncs it into every
targeted CLI's own native skills directory (`~/.claude/skills`,
`~/.codex/skills`, pi/omp/opencode homes — env overrides like
`CLAUDE_CONFIG_DIR` honored) as a symlink, copying where symlinks are
unavailable. The CLIs keep loading skills themselves; `catalog.commands`
picks the synced ones up with no extra wiring. Targets whose engine
home does not exist report failure instead of creating it.

Uninstall moves the managed copy to a 5-minute trash (restorable via
`skills.restore`); removal from every target happens even for copies
the registry forgot. Local skills found in the target roots list as
unmanaged (`sourceKind: "local"`) and can be imported — copied into
the store with the user's original preserved forever. Codex `.system`
and plugin-cache skills list read-only (`readonly: true`,
`error.data.code = "readonly"` on import attempts).

Hub errors carry a taxonomy in `error.data.code`:
`invalid_input` (`-32602`), `not_found`, `conflict` (local changes →
`force`), `readonly`, `permission`, `network`, `http`,
`rate_limited`, `internal`. Rate limits are surfaced, never retried
blindly. Discovery (`skills.discover`) hits the GitHub Trees API of
the registered repos; `skills.search`/`skills.popular` hit
skills.sh — both behind fingerprint+TTL disk caches. The whole
surface turns away with `-32002` when `[skills] enabled = false`.

The prompt library (`prompts.*`) is separate from `prompt.*` history:
markdown files with light frontmatter at `<cwd>/.damon/prompts/`
(scope `workspace`, gated by `allowed_dirs`) and
`<data_dir>/prompts/` (scope `global`). The web UI's `!` picker
inserts a prompt's body into the composer via `prompts.get`.

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

`usage.summary` groups the same rows finer — per LOCAL day × backend
× model, with `tzOffsetMinutes` shifting the UTC stamps into the
caller's calendar so buckets match it. Each row carries turns,
summed context snapshots, and summed cost deltas, plus a window
`total`. `usage.clear` deletes every usage row (sessions and
messages are untouched).

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
reasoning, tool, todo, compaction, and error rows as timeline events,
newest last, capped at 200),
then the in-flight turn's journal. The `replayed` count in the
create/resume result says how many frames were sent. Live frames carry
no `replay` field. A turn that overflowed the daemon's in-flight
journal is not replayed (the live remainder continues uninterrupted;
the assembled result is still persisted at turn end).

### The mission runner (staged)

A resident DAG runner for staged agent workflows: `input → agent → human →
output` nodes over condition-routed edges. SQLite is the truth — every
transition persists before it fans out, run status derives from the task
states, and a daemon restart converges unfinished runs to `interrupted +
cancelled` (never faked as still-running).

- **Conditions** — `key=value` / `key!=value` clauses read the upstream
  outputs plus the flow settings (`and` binds over `or`, missing key ⇒
  false); an edge without a condition is the default: it fires only when
  no explicit sibling from the same source matched.
- **Agent nodes** run as real damon turns on per-node sessions (one per
  attempt), so transcripts land in the store and `session.search` covers
  them. The node's final assistant message parses as `key=value` lines
  into the downstream condition context.
- **Read-only stages** carry `allowedTools` onto the launch line (claude:
  `--allowedTools`); `mission.start` refuses a read-only node on any
  backend whose capabilities say it cannot enforce the list.
- **Human nodes** park the task at `waiting_human` and create an inbox
  item; `mission.respond` resolves it (approve / feedback re-ask /
  exclude — the branch terminates and downstream cancels).
- **Failures** retry up to `settings.retries`, then the task `failed` —
  never silently skipped. `settings.concurrency` bounds parallel agent
  nodes (human nodes never occupy a slot).

Staged limits, on purpose: no `foreach` nodes, no channel-based approval
(a human answers in the UI drawer or over the wire), no mission editing
after start (cancel and re-create instead).

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
  `timeline`/`turn_failed` events as they happen. A turn's final
  `todo` snapshot persists the same way (role `todo`, items in
  `data.items`) — intermediate snapshots are live-only.
- `permission_requested {id,kind,name,title?,input?,detail?,actions[],questions?}`
- `permission_resolved {request_id}`
- `attention_required {reason: finished|permission}`
- `subagent {event}`

`ToolCall.detail` is `{type: shell|read|edit|web|task|other, …}`.
`PermissionResponse` is `{behavior:"allow",action_id?,updated_input?,answer?,answers?}`
or `{behavior:"deny",action_id?,message?,interrupt}` — a deny with
`interrupt: true` makes the daemon interrupt the whole turn right
after delivering the denial, not just refuse the one call.

`kind` is `tool`, `question`, or `plan`. A `question` ask carries a structured
`questions` array — Claude Code's AskUserQuestion, opencode question
batches, omp dialogs, codex input requests — of
`{question, header?, options?: [{label, description?}], multiSelect?}`
(empty `options` = free text). Surfaces render the options and answer
through `permission.respond` with an `answers` map,
`{question text → label | [labels] | typed string}` (a bare string
answers a single-question card):

- `Allow` **with** `answers` submits the user's picks to the agent.
- `Allow` **without** `answers` skips/dismisses the card — the agent
  learns nothing was chosen.
- `Deny` refuses the ask; the agent continues without an answer.

Delivery is retryable: the daemon removes a pending ask only after the
answer reached the agent, so a failed respond (transport error, `-32000`)
leaves the card up and the same `requestId` answerable. `-32008`
(`ASK_NOT_FOUND`) means the card is genuinely gone — the ask was
answered elsewhere, the agent withdrew it (`control_cancel_request`),
or its turn ended (pending asks settle with a `permission_resolved`
event before the turn's terminal event).

### Plan review

A `kind: plan` ask (claude's ExitPlanMode) carries the plan text in
`input.plan`. The daemon records it as a durable review row, visible
via `plan.list` and keyed by sessionId + planId + revision: a re-ask
while the review is still open chains as revision+1 and supersedes
the old revision. The live ask stays the transport; the row is the
multi-surface fact-source, so a phone, a chat bot, and the desktop UI
all see — and answer — the same artifact.

Decisions go through `plan.respond` (`planId`, `expectedRevision`,
`decision`, optional `feedback`): the expectedRevision CAS claim means
two surfaces answering the same revision resolve to exactly one win
plus a typed `-32009` for the loser, and a decision on a stale or
closed revision never reaches the agent. The decision intent is
persisted BEFORE the native relay: a lost pipe leaves the record and
reopens the row (status back to `awaiting_review`, intent kept on
file) instead of ever reporting a success that did not happen.
`approve` delivers an allow to the live ask, `request_changes` a deny
whose message is the feedback, and `defer` records non-terminally
without touching the ask — the agent's wait stays parked and a later
`plan.respond` can still decide it. Approving a plan whose content is
not reviewable (`complete: false`) and a feedback-less
request_changes refuse with `-32602`.

When the turn ends, still-open reviews expire (the native ask died
with the process — history must not pretend they are answerable;
recorded intents survive). `permission.respond` on a plan ask lands
its decision in the same durable row (a deny's `message` becomes the
recorded feedback), so pre-gate clients keep working.

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
