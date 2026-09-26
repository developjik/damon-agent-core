# Damon 와이어 프로토콜 v2

**한국어** | [English](protocol-v2.md)

WebSocket(`GET /ws`) 또는 릴레이 터널 위의 JSON 메시지. 하나의 연결이
모든 세션을 멀티플렉싱하고, 요청은 동시에 디스패치된다 — 긴 `turn.start`가
소켓을 직렬화하지 않는다.

## 봉투(envelope)

```json
// 요청
{"id": 1, "method": "session.create", "params": {"backend": "claude"}}
// 응답
{"id": 1, "result": {"sessionId": "…", "backend": "claude"}}
{"id": 1, "error": {"code": -32602, "message": "sessionId required"}}
// 서버 push (id 없음)
{"event": "session.event", "sessionId": "…", "data": {…StreamEvent…}}
// 접속 시 push되고, "hello" 요청의 응답으로도 발송
{"hello": {"protocol": 2, "daemon": "damond", "version": "0.3.0",
           "permissionTimeoutSecs": 300}}
```

인증: `Authorization: Bearer <token>` 헤더, 또는 `POST /v1/ws_ticket`이
발급하는 단회성 `?ticket=`(쿼리 문자열 토큰은 거부 — 로그에 새어나가므로).
루프백 + 토큰 미설정 → 오픈.

## 에러 코드

에러는 타입화된 코드를 갖는다. JSON-RPC 2.0 예약 범위가 프레임·요청 실패를
담당하고, Damon 자체 코드(`-32000..-32099`)는 안정적인 와이어 계약이라
클라이언트가 메시지를 파싱하는 대신 우아하게 성능을 저하할 수 있다:

| 코드 | 의미 | 일반적인 클라이언트 대응 |
|---|---|---|
| `-32700` | 프레임이 유효한 JSON이 아님 (id null) | 직렬화 수정 |
| `-32600` | 응답할 id는 있지만 `method`가 없음 | 요청 수정 |
| `-32601` | 알 수 없는 메서드 | `hello` → `methods` 확인 |
| `-32602` | 파라미터 누락/형식 오류 | params 수정 |
| `-32000` | 타입 없는 서버 오류 | 메시지 표면화 |
| `-32001` | 세션이 라이브가 아님 | `session.resume` 후 재시도 |
| `-32002` | 백엔드가 기능 미지원 (모델/모드 전환, 스티어링) | UI에서 숨기기 / 텍스트를 다음 프롬프트로 큐 |
| `-32003` | 백엔드 미탐지/미등록 | `backend.list`에서 다른 백엔드 선택 |
| `-32004` | `max_sessions` 상한 도달 | 먼저 세션 close/delete |
| `-32005` | 응답이 1 MiB 프레임 예산 초과 | `limit`/`offset`으로 페이징 |
| `-32006` | 세션에 턴이 이미 실행 중 | `turn.*` 이벤트 대기, 또는 먼저 `turn.cancel` |

0.5.0 이전 데몬은 모든 에러를 `-32000`으로 답했다; 메시지는 그대로라
문자열 매칭 클라이언트도 계속 동작한다.

## 메서드

| 메서드 | 파라미터 | 결과 |
|---|---|---|
| `hello` | — | `{protocol, daemon, version, backends[], methods[]}` |
| `backend.list` | — | `{backends: [{id, available, capabilities}]}` |
| `session.create` | `{backend?, cwd?, model?, mode?, mcpServers?, projectId?}` — 프로젝트가 세션의 범위를 정한다: 루트가 cwd 기본값이 되고 기본값들이 미지정 파라미터를 채운다(명시 값 항상 우선) | `{sessionId, backend, replayed}` |
| `session.resume` | `{sessionId}` — 또는 네이티브 세션 임포트는 `{handle:{provider,native_handle}, cwd?, title?}` | `{sessionId, backend, replayed}` |
| `session.list` | `{limit?, offset?, backend?, cwd?, tag?}` | `{sessions: [{sessionId, createdAt, backend, title, cwd, tags[]}]}` |
| `session.messages` | `{sessionId, limit?, offset?}` | `{messages: [{id, session_id, role, ts, data}]}` |
| `session.export` | `{sessionId}` | `{session: {sessionId, createdAt, backend, title, tags, cwd}, messages: [{id, role, ts, data}], usage: {contextUsed, contextSize, costUsd, turns}}` |
| `session.import` | `{backend, cwd?}` | `{sessions: [ImportableSession]}` |
| `session.delete` | `{sessionId}` | `{deleted: bool}` |
| `session.close` | `{sessionId}` | `{closed: bool}` — 라이브 백엔드 프로세스를 종료하고 행/히스토리는 유지; `max_sessions` 슬롯 확보; 멱등 |
| `session.restart` | `{sessionId}` | `{sessionId, backend, replayed}` — 라이브 백엔드를 죽이고(턴 도중엔 `-32006`으로 거부) 영속 핸들로 재접속 |
| `session.status` | `{}` | `{sessions: [{sessionId, backend, busy, idleSecs}]}` — 라이브 백엔드 세션만 |
| `session.watch` | `{sessionId}` — 라이브여야 함 | `{sessionId, subscribed, replayed}` — 이 연결을 세션 이벤트에 건드리지 않고(생성·재개·턴 없이) 구독; 표면을 넘나드는 알림 경로(웹 UI 세션이 끝나거나 권한을 요청하면 채팅 브리지가 채널로 알림) |
| `session.unwatch` | `{sessionId}` | `{sessionId, subscribed}` — 이 연결의 구독 해제; 세션과 다른 연결은 그대로 |
| `session.set_pinned` | `{sessionId, pinned}` | `{updated: bool}` — 고정 행은 `session.list` 최상단에 뜨고 보존 스윕에서 생존 |
| `session.set_archived` | `{sessionId, archived}` | `{updated: bool}` — 보관 행은 `includeArchived` 없이는 `session.list`에서 숨고 보존 스윕에서 생존 |
| `session.set_tags` | `{sessionId, tags[]}` — 목록 전체를 교체 | `{updated: bool}` |
| `session.rename` | `{sessionId, title}` | `{renamed: bool}` |
| `session.fork` | `{sessionId, upto?}` | `{sessionId}` — 메시지·태그·사용 히스토리·백엔드 resume 핸들이 복사; 제목에 `" (fork)"` 접미 |
| `session.usage` | `{sessionId?}` — 또는 글로벌 일별 롤업은 `{daily: true, days?}`(기본 7일) | 세션별 `{sessionId, contextUsed, contextSize, costUsd, turns}`, 모델별 `{sessions: [...]}`, 또는 `{daily: [{date, turns, costUsd, contextUsed}]}` |
| `session.list` | `{limit?, offset?, backend?, cwd?, tag?, includeArchived?}` | `{sessions: [{sessionId, createdAt, backend, title, cwd, tags, pinned, archived}]}` — 고정 우선; 보관은 요청 시에만 |
| `session.search` | `{query, limit?, sessionId?, backend?, cwd?, since?, until?}` | `{results: [{sessionId, messageId, snippet}]}` |
| `turn.start` | `{sessionId, prompt, timeoutSecs?, detach?}` | `{turnId, stopReason, usage?}` — `detach`가 true면 즉시 `{turnId, detached: true}` |
| `turn.steer` | `{sessionId, prompt, expectedTurn?}` | `{result}` |
| `turn.cancel` | `{sessionId}` | `{cancelled: true}` |
| `permission.respond` | `{sessionId, requestId, response}` | `{}` |
| `session.set_model` | `{sessionId, model}` | `{}` |
| `session.set_mode` | `{sessionId, mode}` | `{}` |
| `catalog.models` | `{backend}` | `{models[], modes[], commands[]}` |
| `logs.tail` | `{lines? — 기본 200, 최대 2000}` | `{lines: string[]}` — 데몬의 최근 로그 라인, 오래된 것부터 |
| `logs.follow` | `{follow? — 기본 true; false는 이전 follow 중지}` | `{following: bool}` — true면 새 로그 라인마다 이 연결로 `{"event":"log.line","data":{"line"}}` push |
| `file.read` | `{sessionId, path}` | `{content: base64, bytes, path}` — 세션 cwd 안에 감금; 512 KiB 읽기 상한(초과 시 `-32005`) |
| `file.write` | `{sessionId, path, content: base64}` | `{written, path}` — cwd 감금; 디코드 1 MiB 상한 |
| `file.list` | `{sessionId, path? — 기본 "."}` | `{entries: [{name, dir, bytes}], path}` — cwd 감금, 1000 엔트리 |
| `channel.get_state` | `{convId, key}` | `{value: string?}` |
| `channel.set_state` | `{convId, key, value}` | `{set: true}` |
| `channel.delete_state` | `{convId, key}` | `{deleted: true}` |
| `project.create` | `{name?, root, defaults?}` | `{projectId, name, root, defaults}` — root는 절대경로; defaults 키: backend/model/mode/mcpServers |
| `project.list` | `{}` | `{projects: [{projectId, name, root, defaults}]}` |
| `project.get` | `{projectId}` | `{projectId, name, root, defaults}` |
| `project.set_defaults` | `{projectId, defaults}` | `{updated: bool}` — 목록 전체 교체 |
| `project.delete` | `{projectId}` | `{deleted: true}` — 세션이 프로젝트를 참조하는 동안엔 `-32602`로 거부 |

`session.search` 쿼리는 토크나이즈되어 FTS5 파서에 그대로 넘겨지지
않는다: 따옴표 구간은 구문이고, `AND`/`OR`/`NOT`은 통과하며, 텀 뒤
**trailing `*`는 접두사 쿼리** — `error*`가 `error`/`errors`/`erroring`에
매치(벌거벗은 `*`는 검색 가능한 게 없다).

### 프로젝트

프로젝트는 작업공간 루트별로 세션을 묶는다: `project.create`가
`{projectId, root, defaults}`를 발행하고, `session.create {projectId}`가
새 세션의 범위를 정한다(루트 → cwd 기본값; backend/model/mode/mcpServers
기본값이 요청이 비워둔 것을 채운다 — 명시 파라미터가 이긴다).
`session.list`, `session.search`, 글로벌 `session.usage` 뷰 모두
`projectId` 필터를 받는다; 세션이 참조하는 프로젝트는 삭제가 거부된다.

`file.*` 경로는 세션의 저장된 cwd 기준으로 해석된다(절대경로도 가능하지만
그 안부). 감금은 최선의 캐노니컬라이제이션 이후 강제된다 — 존재하는
symlink 홉은 해석하고, 아직 없는 쓰기 대상은 가장 깊은 존재 조상을 통해
해석하며, 나머지는 어휘적 `..` 접기가 커버한다 — 탈출은 `-32602`로
fail-closed. `allowed_dirs`와 조합해 세션이 애초에 가질 수 있는 cwd를
제한한다.

`prompt`는 문자열 또는 블록 배열 `[{type:"text",text},…]`다.

### 목록 필터, 타임스탬프, 태그

`session.list` 필터는 AND로 결합된다: `backend`(에이전트 id)와 `cwd`는
저장값과 정확히 매치하고, `tag`는 세션 태그 목록의 멤버십. 태그는
`session.set_tags`로 목록 전체를 관리하고(`[]`로 비움) 포크에도 함께
따라간다.

저장된 모든 메시지는 `ts`(unix-ms 영속 스탬프)를 갖는다; 컬럼 생기기 전
행은 `ts: 0`(알 수 없음). `session.search` 경계는 메시지 시간 기준:
`since`/`until`은 RFC3339(`2026-09-01T13:30:00Z`, 오프셋 선택) 또는
날짜만 `YYYY-MM-DD`(그날 자정 UTC로 읽음)를 받는다 — 명시 오프셋은
시점을 이동시키고, 오프셋 없는 타임스탬프는 UTC로 읽는다(스토어는 UTC로
스탬프). 양 경계 모두 포함. `ts: 0` 행은 세션의 `created_at`으로 비교 —
구 스키마가 표현할 수 있는 가장 거친 경계 — 여행 전 히스토리도 검색
가능. 형식이 잘못된 경계는 아무것도 매치하지 않는 게 아니라 에러.

`session.usage`의 `daily: true`(글로벌 뷰 전용; `days` 기본 7, 오늘과
그 앞날들)는 사용 행을 UTC 일별로 묶는다: `{date, turns, costUsd,
contextUsed}` — cost는 턴별 델타의 합, context는 그날 마지막 스냅샷.
행이 없는 날은 배열에 없다.

`turn.start`는 턴이 끝날 때 resolve — 이벤트가 먼저 스트리밍되고 그다음
응답. 연결 끊김은 그 연결이 시작한 턴을 취소한다.

### 분리된 턴 (`detach: true`)

클라이언트보다 오래 살아야 하는 턴 — 모바일 사례: 잠긴 폰이 턴 도중
소켓을 끊는다 — 는 `detach: true`를 쓴다. 응답은 즉시
`{turnId, detached: true}`로 돌아오고 턴은 어떤 연결도 소유하지 않는
태스크에서 계속 돈다: 연결 끊김이 절대 취소하지 않는다. `turn.cancel`,
`interrupt`를 동반한 deny, `timeoutSecs`는 여전히 조기 종료한다.
영속화·이벤트·리플레이는 차단 턴과 동일해서, 재접속한 클라이언트(또는
제2 클라이언트)가 일반 리플레이 경로로 턴을 다시 받는다 — 실행 중엔
인플라이트 저널, 끝나면 스토어 행. 분리된 턴이 끝날 때까지
`session.status`는 세션을 busy로 보고한다.

### 늦게 붙은 구독자의 이벤트 리플레이

세션을 처음 건드리는 연결(`session.create`, `session.resume`,
`turn.start`)은 라이브 스트리밍 시작 전에 놓친 모든 것을 `"replay": true`
태그의 `session.event` 프레임으로 받는다: 스토어의 영속 히스토리(user,
assistant, reasoning, tool, compaction, error 행이 타임라인 이벤트로,
오래된 게 마지막, 200 상한), 그다음 인플라이트 턴의 저널.
create/resume 결과의 `replayed` 수가 보낸 프레임 수. 라이브 프레임에는
`replay` 필드가 없다. 데몬의 인플라이트 저널을 넘친 턴은 리플레이되지
않는다(라이브 나머지는 끊기지 않고 계속; 조립된 결과는 턴 끝에 영속화).

## StreamEvent

`session.event.data`는 `StreamEvent`: `{turn_id?, type, …}` — `type`은
`StreamEventKind` 태그(snake_case):

- `thread_started {native_handle}` — resume 토큰 도착
- `turn_started`, `turn_completed {usage?}`, `turn_failed {error, code?}`,
  `turn_canceled {reason}`
- `mode_changed {mode?}`, `model_changed {model}`
- `timeline` + 평탄화된 `TimelineItem` (`kind` 태그):
  `user_message`/`assistant_message`/`reasoning {text}`,
  `tool_call {call_id,name,status,detail}`, `todo {items}`,
  `compaction {summary}`, `error {message}`, `unknown {raw}`.
  백엔드가 컨텍스트 컴팩션이나 `turn_failed`를 보고하면 데몬은
  `compaction`/`error` 행을 영속화 — `session.messages`(role
  `compaction`/`error`, 내용은 `data.content`)와 리플레이에 나타나고,
  라이브 소비자는 해당 `timeline`/`turn_failed` 이벤트를 실시간으로 본다.
- `permission_requested {id,kind,name,title?,input?,detail?,actions[]}`
- `permission_resolved {request_id}`
- `attention_required {reason: finished|permission}`
- `subagent {event}`

`ToolCall.detail`은 `{type: shell|read|edit|web|task|other, …}`.
`PermissionResponse`는 `{behavior:"allow",action_id?,updated_input?}` 또는
`{behavior:"deny",action_id?,message?,interrupt}` — `interrupt: true`인
deny는 한 호출만 거부하는 게 아니라 거부 전달 직후 전체 턴을 인터럽트한다.

## 릴레이 핸드셰이크

E2E 핸드셰이크의 첫 프레임은 `"kdf": "s256"`을 실을 수 있다; 이해하는
데몬은 그 필드를 되울리고, 양쪽이 도메인 분리된 시드로 sha256을 2^16회
추가 반복해 증명을 계산한다(저엔트로피 토큰의 브루트포스 경화). 협상하지
않는 피어는 원래 단일 해시 증명을 유지 — 마커는 가산적이며 절대 깨지지
않는다. 릴레이 클라이언트는 30초마다 핑하고, 120초(어느 쪽이든) 침묵한
링크는 TCP를 기다리는 대신 half-open으로 닫히며 릴레이는 데몬 쪽 세션
슬롯을 회수한다.

## Usage

`{input_tokens?, cached_input_tokens?, output_tokens?, cost_usd?,
context_window?, context_used?}` — 모두 선택; 백엔드가 아는 것만 보고한다.
