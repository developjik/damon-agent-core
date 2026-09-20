# Damon 연동 가이드

`damond`는 로컬 에이전트 데몬이다. 어떤 프로그램이든 두 가지 표면으로 붙는다:

| 표면 | 용도 |
|---|---|
| `GET /health` | 헬스체크 |
| `POST /v1/chat/completions`, `GET /v1/models` | OpenAI 호환 패스스루 (기존 클라이언트 드롭인) |
| `GET /ws` | ACP형 JSON-RPC over WebSocket — 에이전트 런타임 (세션, tool loop, 권한) |


## 프로바이더

`api`로 wire transport 선택:

| api | 대상 | 비고 |
|---|---|---|
| `openai-completions` | OpenAI, Groq, OpenRouter, DeepSeek, vLLM, Ollama 등 | 기본. `/chat/completions` 패스스루 + compat 셰이핑 |
| `openai-responses` | o-series, GPT-5, Codex, xAI | `/responses` — 요청/응답 번역 |
| `anthropic-messages` | Claude | `/v1/messages` 번역 |
| `gemini` | Gemini | `generateContent` 번역 |

구독제 인증: `api_key = "oauth"`를 설정하고 한 번만 로그인하면 된다 —
`anthropic-messages`는 Claude Pro/Max(`damond login anthropic`),
`openai-responses`는 ChatGPT Plus/Pro(`damond login openai`)를 사용한다.
토큰은 OS 키체인에 저장되고 자동 갱신된다. openai는 `base_url` 설정이 없으면
ChatGPT 백엔드로 요청을 보낸다.
추가 구독 플레이버: `kimi-code`, `github-copilot`, `xai-oauth` (RFC 8628
디바이스 플로우 — `damond login <flavor>`); `qwen-portal`은 로그인 대신
env 키 프리셋으로 들어온다.

프로바이더 프리셋: `damond presets`로 omp 패리티 카탈로그를 볼 수 있다 —
호스티드 백엔드(Groq, OpenRouter, Mistral, xAI, DeepSeek, Fireworks,
Together, Cerebras, NVIDIA, Moonshot/Kimi, Z.AI, BigModel, MiniMax,
SiliconFlow, Venice, Hugging Face, Vercel AI Gateway, LiteLLM, …)와 Azure
OpenAI, Vertex AI, Bedrock-mantle, 키리스 로컬 엔진 `lm-studio` /
`llama.cpp`. 키 env가 설정된 프리셋은 부팅 때 자동 등록되고, 명시적
`[providers.<id>]` 블록이 같은 id 프리셋을 대체한다. 4종 transport 밖의
와이어 형태는 compat 플래그로 흡수한다: `compat.azure_deployment_urls`
(+`azure_api_version`, 키는 `api-key` 헤더), `compat.vertex`
(project/location 기반 Vertex 경로), `compat.bearer_auth`
(Anthropic 호환 `Authorization: Bearer` — Bedrock mantle).

모델 라우팅: `model` 필드가 `provider/model` 접두사 → config `models` glob → default 순으로 해석.
`/v1/chat/completions`는 어느 프로바이더든 OpenAI 스키마로 응답한다 — 클라이언트는 번역을 신경 쓸 필요 없음.

### compat 플래그

엔드포인트가 표준에서 벗어날 때 `[providers.X.compat]`로 조정:

| 플래그 | 효과 |
|---|---|
| `supports_store` | `store: false` 전송 |
| `supports_developer_role` | `system` → `developer` 역할 변환 |
| `supports_multiple_system_messages` | `false`면 연속 system 메시지 병합 |
| `max_tokens_field` | `"max_completion_tokens"` 등 필드명 변경 |
| `requires_tool_result_name` | tool 결과에 `name` 필드 주입 (Mistral) |
| `requires_mistral_tool_ids` | tool_call id를 9자 영숫자로 정규화 (Mistral) |
| `supports_usage_in_streaming` | `stream_options.include_usage` 전송 여부 |
| `extra_body` | 모든 요청에 병합할 최상위 필드 |

`[providers.X.headers]`로 커스텀 헤더도 추가 가능.

### 시크릿

`api_key`와 `headers` 값은 세 가지 참조를 받는다 (리터럴 금지):

| 형식 | 예시 |
|---|---|
| `env:VAR` | `env:OPENAI_API_KEY` |
| `keychain:svc/acct` | `keychain:damon/openai` |
| `!cmd` | `"!op read op://dev/openai"` — stdout, 10s 타임아웃 |

`.env` 파일은 config 디렉터리 → cwd 순으로 로드된다 (이미 설정된 env는 덮지 않음).

### 모델 디스커버리

`discovery = "openai-models-list"` → `GET {base}/models`, `"ollama"` → `GET {base}/api/tags`.
발견된 모델 id는 `models` glob 없이도 라우팅된다 — `model: "local-model-7b"` 요청이
발견한 provider로 간다. `[providers.ollama]`가 없으면 `$OLLAMA_HOST`(기본
`http://127.0.0.1:11434`)를 자동 프로브한다. `/v1/models`는 발견된 모델을
`provider/id` 형태로 병합해서 반환.

### 컨텍스트 프로모션

`context_promotion_target = "model-id"` (같은 provider) 또는 `"provider/model-id"`.
컨텍스트 오버플로 에러(`context_length_exceeded` 등)가 오면 타겟 모델로 1회 재시도.
스트리밍/비스트리밍, 패스스루/번역 경로 모두에서 동작.

### 인밴드 툴 (로컬 모델)

`compat.inband_tools = true` — 툴 API가 없는 모델용. `tools`를 시스템 프롬프트에
렌더링하고, 응답 텍스트의 `<tool_call>{...}</tool_call>` 블록을 파싱해서
`tool_calls`로 변환한다. 스트리밍은 전체 버퍼링 후 이벤트 재방출.

### Thinking 레벨

모델명에 `:low` / `:medium` / `:high` 접미사 → provider별 매핑:
OpenAI `reasoning_effort`, Anthropic `thinking.budget_tokens`(1024/8192/32768),
Gemini `thinkingConfig.thinkingBudget`, Responses `reasoning.effort`.

### 프롬프트 캐싱 (Anthropic)

`anthropic-messages`는 system 블록과 마지막 메시지의 마지막 content 블록에
`cache_control: ephemeral`을 자동으로 붙인다. 장기 세션에서 입력 토큰 비용 절감.

### 컨텍스트 컴팩션

세션의 추정 토큰이 모델 `context_window`의 85%를 넘으면,
가장 오래된 절반을 provider로 요약하고(`summary_model` 설정으로 요약 모델 지정 가능)
`compacted_through`를 기록한다. 이후 `messages()`는 요약 + 나머지를 반환.
요약 실패 시 아무것도 기록하지 않음 — 전체 히스토리를 유지한 채 턴 진행.

## 인증


`auth_token`이 설정된 경우에만 필요. `/v1`은 `Authorization: Bearer <token>`,
`/ws`는 같은 헤더 또는 `POST /v1/ws_ticket`이 발급한 단회성
`?ticket=<ticket>`(60초 TTL). 쿼리 문자열 토큰(`?token=`)은 미지원 —
로그와 브라우저 히스토리에 새어나가므로. 미설정 시 localhost 오픈.

`GET /metrics`(토큰 게이트 뒤)는 Prometheus 카운터를 노출:
`damon_requests_total`, `damon_prompts_total`, `damon_tokens_input_total`,
`damon_tokens_output_total`, `damon_active_sessions`,
`damon_mcp_tool_calls_total`, `damon_mcp_tool_errors_total`, 그리고
프로바이더별 `damon_provider_requests_total` / `damon_provider_errors_total`
/ `damon_provider_latency_ms_sum`(`provider="…"` 라벨).

브라우저 Origin: `auth_token` 미설정 시 `Origin` 헤더를 보내는 요청은
루프백 origin(`localhost`, `*.localhost`, `127.0.0.0/8`, `[::1]`)만 허용
— `/ws`와 `/v1` 모두. `Origin`이 없는 클라이언트(curl, Node, 네이티브
앱)는 영향 없음. `auth_token` 설정 시 모든 origin 허용 — 토큰이 게이트.

## WS 프로토콜 (JSON-RPC 2.0)

클라이언트 → 데몬 요청:

- `initialize` → `{protocolVersion, agentCapabilities, agentInfo}`
- `session/new {cwd, model?}` → `{sessionId}` — `model`은 세션 기본 모델
  (`provider/model`, 글롭/디스커버리 id, 또는 `model:low|medium|high`
  thinking 접미사). `mcpServers`는 세션 전용 stdio MCP 서버를 선언 —
  `[{name, command, args?, env?, auto_approve?}]` (env는 `[{name,value}]`
  또는 객체). 이 서버들의 툴은 해당 세션에만 데몬의 `[mcp_servers]` 위에
  오버레이되고, session/delete·데몬 셧다운 시 정리되며, 세션당 8개 /
  전체 64개 오버레이 상한. 설정된 서버와 이름이 겹치면 거부.
- `session/list {limit?, offset?}` → `{sessions: [{sessionId, createdAt, model}]}`
- `session/resume {sessionId}` → `{sessionId}` (알 수 없으면 에러)
- `session/delete {sessionId}` → `{deleted: true}` — 실행 중인 턴을 먼저 취소;
  10초 후에도 종료 중이면 `-32603 "session busy"`
- `session/messages {sessionId, limit?, offset?}` → `{messages: [...]}`
- `session/search {query, limit}` → `{results: [{sessionId, messageId, snippet}]}`
- `session/prompt {sessionId, prompt: [{type:"text", text}], model?}` → `{stopReason}` — `model`은 이 턴에서만 세션 기본값을 덮어씀. 응답은 턴 종료 시 도착. 알 수 없는 `sessionId` → `-32602`.
- `session/cancel {sessionId}` — notification; `id`를 포함하면 빈 `{}` 결과가 옴
- `session/compact {sessionId}` → `{compacted, compactedThrough?, reason?}` —
  85% 추정 임계와 무관하게 즉시 컨텍스트 압축을 강제. 세션에 실행 중인
  턴이 있으면 `-32602`로 거부; 요약 호출은 120초 상한이며
  `session/cancel`로 취소 가능.

- `session/update` notification — `update.sessionUpdate`가 `agent_message_chunk`(텍스트 델타), `agent_thought_chunk`(추론 델타), 또는 `tool_call_update`(tool_callId, status)
- `session/request_permission` request — 클라이언트가 `{outcome: {outcome:"selected", optionId:"allow-once"|"reject-once"|"allow-always"}}`로 응답해야 tool 실행이 진행됨. `allow-always`는 해당 툴을 세션 동안 자동 승인(메모리에만 유지). 응답 없는 프롬프트는 `permission_timeout_secs`(기본 300초) 후 거부. `auto_approve` 서버는 이 요청이 오지 않는다.

`session/prompt` 응답의 `stopReason`: `end_turn` | `max_tokens` | `tool_use` | `cancelled` | `max_turn_requests` (tool-loop 반복 상한 도달).


## 최소 클라이언트 흐름

```
connect → initialize → session/new → session/prompt
  ├─ session/update 알림을 받는 대로 렌더
  ├─ session/request_permission 오면 응답 전송
  └─ id가 prompt 요청과 같은 응답이 오면 턴 종료
```

## 최소 클라이언트

복붙해서 바로 돌아가는 세 가지 예시. 데몬이 `127.0.0.1:9470`에 떠 있다고 가정.

### curl (OpenAI 호환 패스스루)

```sh
curl -N http://127.0.0.1:9470/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"default","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

### Node.js (WS, Node ≥ 22 — 글로벌 WebSocket)

```js
// node client.mjs
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
    while True:  # 알림을 건너뛰고 이 요청의 응답을 기다림
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

Rust 예시는 `examples/client.rs` (`cargo run --example client`).

## Rust에서 붙이기

`damon::client::DamonClient`가 레퍼런스 구현이다 (`src/bin/damon.rs`가 사용 예시):

```rust
let client = DamonClient::connect("ws://127.0.0.1:9470/ws", None).await?;
client.initialize().await?;
let session = client.new_session("/tmp").await?;
// events()로 ClientEvent::Update / Request를 받으며 prompt()를 await
```

## 데스크톱 앱 (Electron/Tauri)

데몬은 이미 상주 프로세스다. 앱은 데몬을 spawn하지 말고 `ws://127.0.0.1:9470/ws`에 attach한다.
데몬이 안 떠 있으면 `damond`를 sidecar로 띄우는 패턴을 쓴다 — 앱 종료와 데몬 생명주기는 분리.

## 원격 접근

- 권장: Tailscale — WireGuard 기반 E2E, 데몬 설정 변경 없이 `ws://<tailscale-ip>:9470/ws`로 attach
- 직접: `tls_cert` + `tls_key` 설정 시 wss로 서빙. 비루프백 바인드는 `auth_token`이 없으면 기동을 거부한다.

## 채널 어댑터

세 채널이 같은 패턴으로 붙는다 — 채널별 chat → damon 세션 자동 매핑, 응답 스트리밍,
권한 요청은 "allow"/"deny" 답장으로 승인.

| 어댑터 | 실행 | 수신 | 비고 |
|---|---|---|---|
| Telegram | `damon-telegram --bot-token <token>` | Bot API long-poll | `TELEGRAM_BOT_TOKEN` env 가능 |
| Discord | `damon-discord --bot-token <token>` | Gateway WebSocket | `DISCORD_BOT_TOKEN` env 가능. 길드 채널은 @봇 멘션 필요, DM은 그대로. MESSAGE_CONTENT privileged intent를 dev portal에서 켜야 함 |
| Slack | `damon-slack --app-token xapp-… --bot-token xoxb-…` | Socket Mode | `SLACK_APP_TOKEN`/`SLACK_BOT_TOKEN` env 가능. 채널은 @봇 멘션 필요, DM은 그대로. 스코프: `connections:write`(앱), `chat:write`+`im:history`+`channels:history`+`app_mentions:read`(봇) |

새 채널 추가: `damon_core::channel::ChannelApi`(`ready`/`recv`/`send`)를 구현하고
`channel::Bridge::new(channel, client).run()`에 연결하면 된다 — 세션 매핑, 이벤트
demux, 권한 흐름, 스트리밍 루프는 브리지가 소유한다. `src/telegram.rs`가 최소 예시.
