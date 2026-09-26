# Damon 연동 가이드

`damond`는 로컬 에이전트 컨트롤 데몬이다. 어떤 프로그램이든 두 표면으로 붙는다:

| 표면 | 용도 |
|---|---|
| `GET /health` | 헬스체크 (설치된 백엔드 목록 포함) |
| `GET /ws` | JSON-RPC over WebSocket — 에이전트와의 대화 (프로토콜 v2) |

`POST /v1/ws_ticket`은 `?ticket=` 인증용 단회성 티켓을 발급한다 (브라우저 등 헤더를 못 쓰는 클라이언트).

## 백엔드

Damon은 코딩 에이전트를 각자의 네이티브 CLI로, stdio subprocess로 구동한다. 모델 선택, 툴, 구독 인증, 컨텍스트 관리는 전부 에이전트가 소유한다.

| id | 에이전트 | 구동 | 인증 |
|---|---|---|---|
| `claude` | Claude Code (Claude Pro/Max) | `claude -p --output-format stream-json --input-format stream-json --verbose` | `claude` CLI 로그인 |
| `codex` | Codex CLI (ChatGPT Plus/Pro) | `codex app-server` | `codex` CLI 로그인 |
| `omp` | Oh My Pi (자체 관리 프로바이더 키) | `omp --mode rpc` | OMP 자체 auth 저장소 |

탐지: PATH에 해당 바이너리가 있으면 백엔드가 자동 등록된다.

오버라이드 — 구동 명령을 교체하거나 로컬 빌드를 가리키기:

```toml
[backends.claude]
command = "/opt/claude"
args = ["-p", "--output-format", "stream-json", "--input-format", "stream-json", "--verbose"]
[backends.claude.env]
ANTHROPIC_MODEL = "claude-sonnet-4-5"
```

`session.create`의 `backend` 파라미터로 세션마다 백엔드를 고른다. 생략하면 `default_backend` 설정 → 첫 번째 사용 가능 백엔드.

MCP 서버는 `session.create`의 `mcpServers`가 **에이전트에게 전달**된다 — 에이전트가 직접 구동하고 권한을 승인한다. Damon은 MCP 서버를 직접 띄우지 않는다.

## 인증

`auth_token`이 설정된 경우에만 필요. `/ws`는 `Authorization` 헤더의 bearer 토큰 또는 `POST /v1/ws_ticket`이 발급한 단회성 `?ticket=`(60초 TTL)을 받는다. 쿼리 문자열 토큰(`?token=`)은 미지원 — 로그와 브라우저 히스토리에 새어나가므로. 토큰 미설정 시 localhost 오픈.

브라우저 Origin: `auth_token`이 없으면 `Origin` 헤더를 달고 오는 요청은 루프백 origin이어야 한다. 토큰이 있으면 모든 origin이 통과한다 — 토큰이 게이트다.

`GET /metrics`(토큰 게이트 뒤)는 Prometheus 카운터를 노출한다: `damon_requests_total`, `damon_live_sessions`.

## WS 프로토콜 (v2)

전체 계약: [protocol-v2.md](protocol-v2.md). 요약:

클라이언트 → 데몬 요청:

- `hello` → `{protocol, backends, methods}` — 접속 시 push로도 도착
- `backend.list` → `{backends: [{id, available, capabilities}]}`
- `session.create {backend?, cwd?, model?, mode?, mcpServers?}` → `{sessionId, backend}`
- `session.resume {sessionId}` → `{sessionId, backend}` — 백엔드의 네이티브 resume 토큰으로 재접속. `{handle:{provider,native_handle}, cwd?, title?}`로 호출하면 데몬 밖 네이티브 세션을 임포트(같은 handle은 같은 세션으로 dedup)
- `session.list {limit?, offset?}` → `{sessions: [...]}`
- `session.messages {sessionId, limit?, offset?}` → `{messages: [...]}`
- `session.import {backend, cwd?}` → `{sessions: [...]}` — 데몬 밖에서 만든 네이티브 세션
- `session.delete {sessionId}` → `{deleted: true}`
- `session.rename {sessionId, title}` → `{renamed: bool}`
- `session.fork {sessionId, upto?}` → `{sessionId}`
- `session.usage {sessionId?}` → `{contextUsed, contextSize, costUsd, turns}` 또는 세션별 롤업
- `session.search {query, limit?}` → `{results: [...]}` — 메시지 텍스트와 툴 I/O에 대한 FTS5
- `turn.start {sessionId, prompt, timeoutSecs?}` → `{turnId, stopReason, usage?}` — 턴이 끝날 때 resolve; 이벤트는 먼저 스트리밍된다
- `turn.steer {sessionId, prompt, expectedTurn?}` → `{result}` — 지원되는 경우 턴 도중 스티어링
- `turn.cancel {sessionId}` → `{cancelled: true}`
- `permission.respond {sessionId, requestId, response}` → `{}` — 권한 요청에 응답
- `session.set_model {sessionId, model}` / `session.set_mode {sessionId, mode}` → `{}`
- `catalog.models {backend}` → `{models, modes, commands}`

데몬 → 클라이언트 push:

- `{"event":"session.event","sessionId","data":<StreamEvent>}` — 이 연결이 건드린 세션(create/resume/turn)의 모든 이벤트. StreamEvent 종류: `turn_started`, `timeline`(assistant_message/reasoning/tool_call/todo/…), `permission_requested`, `turn_completed`, `turn_failed`, `turn_canceled`, `attention_required`, `model_changed`, `mode_changed`, `thread_started`, `subagent`.

`turn.start`의 `stopReason`: `completed` | `failed` | `canceled` | `timeout`.

## 최소 클라이언트 흐름

```
connect → (hello 도착) → session.create {backend:"claude", cwd:"/repo"}
  ├─ session.event push를 받는 대로 렌더
  ├─ permission_requested는 permission.respond로 응답
  └─ turn.start 응답이 오면 턴 종료
```

## 최소 클라이언트

복붙해서 바로 돌아가는 세 가지 예시. 데몬이 `127.0.0.1:9470`에 떠 있다고 가정.

### Node.js (Node ≥ 22 — 글로벌 WebSocket)

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
    while True:  # 이벤트를 건너뛰고 이 요청의 응답을 기다림
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

Rust 예시는 `examples/backend_probe.rs` (`cargo run --example backend_probe`).

## Rust에서 붙이기

`damon_core::client::DamonClient`가 레퍼런스 구현이다:

```rust
let client = DamonClient::connect("ws://127.0.0.1:9470/ws", None).await?;
let session = client.create_session("claude", "/tmp").await?;
// turn_start()를 await하면서 ClientEvent::Event push를 소비한다.
```

## 데스크톱 앱 (Electron/Tauri)

데몬은 이미 상주 프로세스다. 데몬을 spawn하지 말고 `ws://127.0.0.1:9470/ws`에 attach한다. 데몬이 안 떠 있으면 sidecar 패턴을 쓴다 — 앱 종료와 데몬 생명주기는 분리.

## 데몬 탐지

로컬 클라이언트(CLI, TUI, IDE 플러그인)는 config를 파싱하지 않고 떠 있는
데몬을 찾을 수 있다: `damond`는 기동 시 `~/.damon/daemon.json`을 쓰고
정상 종료 시 지운다.

```json
{"port": 9470, "pid": 1234, "version": "0.3.0", "tls": false, "configPath": "/path/to/config.toml"}
```

파일에는 시크릿이 없다 — `port`, `pid`, `version`, `tls`, 그리고 데몬이
기동한 `configPath`뿐. Unix에서는 umask와 무관하게 파일이 `0600`,
`~/.damon`이 `0700`이다.

SIGKILL된 데몬은 파일을 남기므로, 읽는 쪽은 이를 증거가 아닌 힌트로
다뤄야 한다: 신뢰하기 전에 `pid`가 살아 있는 프로세스를 가리키는지(그리고
`port`가 응답하는지) 확인한다. 후속 데몬은 기동 시 파일을 덮어쓰고,
종료 가드는 기록된 pid가 여전히 자기 것일 때만 파일을 지운다.

## 원격 접근

- 권장: Tailscale — WireGuard 기반 E2E, 데몬 설정 변경 없이 `ws://<tailscale-ip>:9470/ws`로 attach
- 직접: `tls_cert` + `tls_key` 설정 시 `wss`로 서빙. 비루프백 바인드는 `auth_token`이 없으면 기동을 거부한다.

## 채널 어댑터

세 채널이 같은 패턴으로 붙는다 — 채널별 chat → 에이전트 세션 자동 매핑, 응답 스트리밍, 권한 요청은 "allow"/"deny" 답장으로 승인.

| 어댑터 | 실행 | 수신 | 비고 |
|---|---|---|---|
| Telegram | `damon-telegram --bot-token <token>` | Bot API long-poll | `TELEGRAM_BOT_TOKEN` env 가능 |
| Discord | `damon-discord --bot-token <token>` | Gateway WebSocket | `DISCORD_BOT_TOKEN` env 가능. MESSAGE_CONTENT privileged intent 필요 |
| Slack | `damon-slack --app-token xapp-… --bot-token xoxb-…` | Socket Mode | `SLACK_APP_TOKEN`/`SLACK_BOT_TOKEN` env 가능 |

새 채널 추가: `damon_core::channel::ChannelApi`(`ready`/`recv`/`send`)를 구현하고 `channel::Bridge::new(channel, client).run()`에 연결하면 된다 — 세션 매핑, 이벤트 demux, 권한 흐름, 스트리밍 루프는 브리지가 소유한다.
