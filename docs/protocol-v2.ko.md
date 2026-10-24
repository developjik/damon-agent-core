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
{"hello": {"protocol": 2, "daemon": "damond", "version": "0.5.0",
           "permissionTimeoutSecs": 300}}
```

인증: `Authorization: Bearer <token>` 헤더, 또는 `POST /v1/ws_ticket`이
발급하는 단회성 `?ticket=`(쿼리 문자열 토큰은 거부 — 로그에 새어나가므로).
루프백 + 토큰 미설정 → 오픈. 티켓 엔드포인트는 본문으로 승인된 웹 기기의
자체 토큰도 받는다(`{"deviceToken": …}`) — 페어링의 기기별 폐기 가능
자격증명([웹 기기](#웹-기기) 참조); 마스터 bearer 경로는 그대로다.

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
| `-32007` | worktree 작업 실패 — `error.data.kind`가 택소노미를 실어준다 | `data.kind`로 분기 (`pr_not_found`, `branch_exists`, …) |
| `-32008` | 그 `requestId`에 보류 중인 ask가 없음 (`ASK_NOT_FOUND`) — 이미 답변됨, 에이전트가 정리함, 또는 턴 종료 | 그 카드 재시도 중단 |
| `-32009` | 플랜 리뷰 충돌 (`PLAN_CONFLICT`) — 다른 표면이 결정을 선점했거나 호출이 낡은 리비전/닫힌 플랜을 겨냥 | `plan.list`로 다시 읽고 현재 리비전으로 결정 |
| `-32010` | MCP setEnabled CAS 거부 (`MCP_CONFLICT`) — 인벤토리 스냅샷 이후 항목이 변경됨(외부 편집) | `mcp.inventory`로 다시 읽고 재시도 |
| `-32011` | 메모리 쓰기가 게이트 체인에서 거부됨 (`MEMORY_REJECTED`) — `error.data.kind` = `prompt_injection` \| `secret` \| `invisible` \| `capacity` (capacity는 `limit`/`current`/`attempted` 동반); 쓰기 전체가 반사됨 — 세척·절단 없음 | 내용을 고치거나 장부를 정리한 뒤 다시 쓰기 |

0.5.0 이전 데몬은 모든 에러를 `-32000`으로 답했다; 메시지는 그대로라
문자열 매칭 클라이언트도 계속 동작한다.

## 메서드

| 메서드 | 파라미터 | 결과 |
|---|---|---|
| `hello` | — | `{protocol, daemon, version, backends[], methods[]}` |
| `backend.list` | — | `{backends: [{id, available, capabilities}]}` |
| `backend.versions` | `{backend?}` — 하나만, 아니면 전체 카탈로그 | `{versions: [{backend, installed, version?, latest?, updateAvailable, updateKind: "npm"\|"manual"\|"unknown"}]}` — `latest`는 npm 패키지가 검증된 곳에만; 프룹은 이 호출에서만 실행, 스케줄되지 않음 |
| `backend.update_plan` | `{backend}` | `{backend, action: "install"\|"update", kind, command[], manualCommand, canRun, blockers[], platform}` — `backend.update`가 실행할 정확한 argv와 복사-붙여넣기 대안; 부수 효과 없음 |
| `backend.update` | `{backend, creationId?}` | `{creationId, ok: true, version?}` — 플랜을 실행(npm 글로벌 설치 또는 CLI 공식 인스톨러); 진행은 요청 연결로 `{"event":"backend.update_progress","creationId","data":{phase, line?, exitOk?}}` 스트리밍(줄당 캡); 검증된 채널이 없으면 `-32602`(플랜 blockers 참조), 실패/420s 타임아웃은 `-32000`(출력 꼬리가 메시지에 동승) |
| `backend.update_cancel` | `{creationId}` | `{cancelled: bool}` — 해당 id의 실행 중 런이 없으면 false |
| `session.create` | `{backend?, cwd?, model?, mode?, mcpServers?, projectId?, persona?}` — 프로젝트가 세션의 범위를 정한다: 루트가 cwd 기본값이 되고 기본값들이 미지정 파라미터를 채운다(명시 값 항상 우선); `persona`는 `bot.list`의 봇 id — SOUL/AGENTS 블록이 매 턴 앞에 실리며 세션 수명 동안 고정, resume 시 파일에서 재해석(모르는 id는 `-32602`) | `{sessionId, backend, persona?, replayed}` |
| `session.resume` | `{sessionId}` — 또는 네이티브 세션 임포트는 `{handle:{provider,native_handle}, cwd?, title?}` | `{sessionId, backend, replayed}` |
| `session.messages` | `{sessionId, limit?, offset?}` | `{messages: [{id, session_id, role, ts, data}]}` |
| `session.export` | `{sessionId}` | `{session: {sessionId, createdAt, backend, title, tags, cwd}, messages: [{id, role, ts, data}], usage: {contextUsed, contextSize, costUsd, turns}}` |
| `session.import` | `{backend, cwd?}` | `{sessions: [ImportableSession]}` |
| `session.delete` | `{sessionId}` | `{deleted: bool}` |
| `session.rescan` | `{}` | `{scanId, alreadyRunning, report: {scanned, reparsed, reused, archived, tombstoned, skippedBackends[]}}` — 모든 백엔드의 네이티브 세션 저장소를 훑어 데몬이 본 적 없는 세션의 북마크 행을 만든다; 단일 비행(실행 중이면 진행 중인 `scanId` 반환); 진행은 요청 연결로 `{"event":"history.scan_progress","scanId","data":{stage, …}}` 스트리밍; [히스토리 스캐너](#히스토리-스캐너) 참조 |
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
| `usage.summary` | `{days?, tzOffsetMinutes?}` — 최근 `days`개 로컬 날(기본 7, 오늘 포함); `tzOffsetMinutes`는 UTC 스탬프를 호출자의 달력으로 옮긴다(기본 0 = UTC) | `{days, rows: [{date, backend, model, turns, contextUsed, contextSize, costUsd}], total: {turns, contextUsed, contextSize, costUsd}}` — 로컬 날 × 백엔드 × 모델당 한 행; context 컬럼은 턴별 스냅샷의 합, cost는 턴별 델타의 합; 행이 없는 날은 없다 |
| `usage.clear` | `{}` | `{cleared: true}` — 모든 사용 행을 삭제; 세션과 메시지는 그대로 |
| `mission.create` | `{flowId?, name?, definition}` — `flowId` 기본값은 새 uuid; 같은 `flowId` 재생성은 다음 버전을 삽입(실행은 시작한 스냅샷 유지) | `{flowId, version}` — 정의가 잘못되면 `-32602`(알 수 없는 노드 종류, prompt/backend 없는 agent 노드, `allowedTools` 없는 read-only, 루트 규칙, 파싱 불가 조건) |
| `mission.start` | `{flowId}` — 최신 버전을 시작 | `{runId, version}` — `-32602` unknown flow; `-32003` 미등록 백엔드; `-32002` read-only 에이전트 노드 거부: 백엔드가 도구 허용목록을 강제할 수 없으며 damon은 read-only 단계를 강등하지 않는다 |
| `mission.cancel` | `{runId}` | `{runId, cancelled}` — 실행 중인 스케줄러가 없으면 `false` |
| `mission.respond` | `{runId, nodeId, decision, feedback?}` — `decision`은 `approve \| feedback \| exclude`; `feedback`는 비어 있지 않은 텍스트 필요 | `{accepted: true, live}` — 스토어 CAS가 단일 승자 게이트: 인박스 항목이 pending이 아니면(다른 답이 이김) `-32602` |
| `mission.list` | `{}` | `{runs: [{runId, flowId, version, status, interrupted, ended, createdAt, tasks: [{nodeId, status, attempt}]}]}` — status는 항상 태스크 상태에서 도출(running / succeeded / failed / interrupted / cancelled), 저장하지 않음 |
| `mission.get` | `{runId}` | `{runId, flowId, version, status, interrupted, ended, definition, tasks: [{nodeId, status, attempt, output?, error?}], inbox: [{nodeId, status, decision?, feedback?}]}` — `-32602` unknown run |
| `mission.watch` | `{runId}` — 실행 중이어야 함 | `{runId, subscribed}` — `{"event":"mission.progress","runId","data":{node, status, attempt}}` 프레임 스트림 |
| `mission.unwatch` | `{runId}` | `{runId, subscribed}` |
| `memory.usage` | `{scope: "user"\|"bot", owner?}` — `owner`는 봇 장부 이름 | `{scope, owner, chars, limit}` — chars는 ACTIVE 항목만 집계; `[memory]` 비활성 중엔 `-32002` |
| `memory.list` | `{scope, owner?, includePending?}` | `{entries: [{id, scope, owner, content, source, status, createdAt, updatedAt}]}` |
| `memory.pending` | `{}` | `{entries: [...]}` — 모든 장부의 pending 쓰기, 오래된 순 |
| `memory.write` | `{scope, owner?, content, source?}` | `{id, status: "active"\|"pending"}` — 단일 게이트 체인: 스캔 → 한도 → 승인; 세척·절단 없음; `-32011`의 `error.data.kind` = `prompt_injection` \| `secret` \| `invisible` \| `capacity` (capacity는 `limit`/`current`/`attempted` 동반); 빈 내용이나 잘못된 scope/owner 쌍은 `-32602` |
| `memory.remove` | `{id}` | `{removed: bool}` |
| `memory.approve` | `{id}` — pending 쓰기 | `{approved: bool}` — 원샷 CAS: unknown이거나 이미 active면 `false` |
| `memory.reject` | `{id}` | `{rejected: bool}` — pending 쓰기 삭제 |
| `bot.list` | `{}` | `{bots: [{id, name, description?, hasSoul, hasAgents}], root}` — 페르소나는 `root/<id>/`의 평범한 파일들(`bot.json` + `SOUL.md` + `AGENTS.md`); 아무 에디터로 고치면 다음 세션부터 반영 |
| `bot.get` | `{id}` | `{bot: {id, name, description?, soul?, agents?}}` — 파일 전문; 모르는 id는 `-32602` |
| `bot.save` | `{id, name, description?, soul?, agents?}` — 선택 문자열 필드: 생략은 유지, null(또는 `""`)은 삭제, 문자열은 설정 | `{bot: {id, name}}` — 생성 또는 갱신; 실행 중 세션은 생성 시점의 블록을 유지 |
| `bot.delete` | `{id}` | `{deleted: bool}` — 묶여 있던 세션의 이후 resume은 조용히 페르소나 없이 |
| `turn.start` | `{sessionId, prompt, timeoutSecs?, detach?, memoryOwner?}` — `[memory]` 활성 시 `memoryOwner`가 지명한 봇 장부의 ACTIVE 항목이 user 장부와 함께 `<memory>` 블록으로 주입됨(데이터 선언 구분 블록; 저장되는 사용자 메시지는 깨끗한 원본 프롬프트 유지) | `{turnId, stopReason, usage?}` — `detach`가 true면 즉시 `{turnId, detached: true}` |
| `turn.steer` | `{sessionId, prompt, expectedTurn?}` | `{result}` |
| `turn.cancel` | `{sessionId}` | `{cancelled: true}` |
| `permission.respond` | `{sessionId, requestId, response}` | `{}` — `kind: plan` ask에서는 결정이 영속 plan-review 행에도 기록된다 |
| `plan.list` | `{sessionId}` | `{plans: [{planId, sessionId, requestId, revision, title, content, contentHash, complete, status, decision?, decisionIntentAt?, appliedAt?, createdAt, updatedAt, supersededBy?}]}` — 모든 플랜의 모든 리비전, 체인 순 |
| `plan.respond` | `{planId, expectedRevision, decision, feedback?}` — `decision`은 `approve` \| `request_changes` \| `defer`; request_changes에는 비어 있지 않은 feedback 필수 | `{recorded: true, status, applied, planId, revision}` — `expectedRevision` CAS 클레임: 경쟁에서 지거나 낡은/닫힌 리비전이면 `-32009`; 불완전 플랜 승인과 feedback 없는 request_changes는 `-32602` 거부; `defer`는 라이브 ask를 건드리지 않고 비종단적으로 기록 |
| `session.set_model` | `{sessionId, model}` | `{}` |
| `session.set_mode` | `{sessionId, mode}` | `{}` |
| `catalog.models` | `{backend}` | `{models[], modes[], commands[]}` |
| `catalog.commands` | `{sessionId?, cwd?}` — 컴포저의 `/` 피커: CLI들이 스스로 확장하는 커맨드/스킬 파일 | `{commands: [{name, description?, argumentHint?, source, kind}]}` — 워크스페이스 항목이 같은 이름의 글로벌 항목을 가림; 커맨드 → 스킬 순 정렬 |
| `logs.tail` | `{lines? — 기본 200, 최대 2000}` | `{lines: string[]}` — 데몬의 최근 로그 라인, 오래된 것부터 |
| `logs.follow` | `{follow? — 기본 true; false는 이전 follow 중지}` | `{following: bool}` — true면 새 로그 라인마다 이 연결로 `{"event":"log.line","data":{"line"}}` push |
| `file.read` | `{sessionId, path}` | `{content: base64, bytes, path}` — 세션 cwd 안에 감금; 512 KiB 읽기 상한(초과 시 `-32005`) |
| `file.write` | `{sessionId, path, content: base64}` | `{written, path}` — cwd 감금; 디코드 1 MiB 상한 |
| `file.list` | `{sessionId, path? — 기본 "."}` | `{entries: [{name, dir, bytes}], path}` — cwd 감금, 1000 엔트리 |
| `file.index` | `{sessionId?, cwd?}` — 컴포저의 `@path` 피커: 루트의 gitignore 인식 워크 1회 | `{entries: [{rel, isDir}], truncated}` — `node_modules`/`target` 프룬, 숨김 항목 스킵, symlink 미추적; 10 000 엔트리 / 512 KiB 상한, 조기 중단 시 `truncated: true` |
| `file.grant` | `{projectId?, path}` — `path`는 절대경로, 존재하는 디렉터리여야 함; symlink/`..`는 정규형으로 해석 | `{granted: true, root, projectId?}` — 파일 감금이 허용할 추가 디렉터리를 영속화(중복 그랜트는 멱등); claude 세션은 다음 실행 때 `--add-dir`로 받는다 |
| `file.grants` | `{projectId?}` — 그 범위의 실효 그랜트(프로젝트 + 글로벌); 생략 시 글로벌만 | `{grants: [{root, projectId?, createdAt}]}` |
| `file.ungrant` | `{projectId?, path}` — 저장된 정규 루트와 매치 | `{revoked: bool}` — 해당 그랜트가 없으면 false |
| `channel.get_state` | `{convId, key}` | `{value: string?}` |
| `channel.set_state` | `{convId, key, value}` | `{set: true}` |
| `channel.delete_state` | `{convId, key}` | `{deleted: true}` |
| `device.pairing_key` | `{}` | `{key: string}` — 일회용 페어링 키(8자 혼동 없는 문자), 첫 요청에서 발급; 다음 `POST /v1/pair`가 소비하면 하나의 락 안에서 새 키로 로테이션 |
| `device.list` | `{}` | `{devices: [{deviceId, name, status: "pending"\|"approved", createdAt, approvedAt?, lastSeen?}]}` — lastSeen은 분당 1회로 제한(스로틀); 토큰 해시는 스토어 밖으로 나가지 않는다 |
| `device.approve` | `{deviceId}` | `{approved: bool}` — pending에서만; 미지정/이미 승인이면 false |
| `device.rename` | `{deviceId, name}` | `{renamed: bool}` — 빈 이름은 거부 |
| `device.revoke` | `{deviceId}` | `{revoked: bool, socketsCut: number}` — 행이 토큰 해시와 함께 사라져 자격증명은 즉시 무효; 이미 열려 있던 해당 기기의 `/ws` 연결도 그 즉시 절단되고 미사용 티켓도 폐기된다; pending 요청의 deny를 겸한다 |
| `prompt.add` | `{text, sessionId?, cwd?}` — 보낸 프롬프트 하나, trim 후 300자 상한 | `{recorded: true}` — upsert: 재전송은 카운트를 올리고 최신으로 이동; cwd당 최신 200개만 남음 |
| `prompt.recent` | `{limit? — 기본 50, 최대 200, sessionId?, cwd?}` | `{prompts: [{text, count}]}` — 최신순, cwd별 |
| `skills.targets` | `{}` | `{targets: [{id, label, path, available}]}` — 허브가 싱크할 수 있는 CLI들의 스킬 디렉터리; `available`은 엔진 홈이 존재한다는 뜻(허브는 절대 만들지 않는다) |
| `skills.installed` | `{}` | `{skills: [{id, name, description?, directory, managed, repo?, targets[], targetStates, sourceKind?, readonly?, targetPaths?}], generatedAt}` — 레지스트리 관리 + 비관리 발견, 이름순 |
| `skills.repos` | `{}` | `{repos: [{owner, name, branch, enabled}]}` — 레지스트리 + `[skills] extra_repos` |
| `skills.install` | `{owner, name, branch?, directory?, installName?, targets?, force?}` — `directory` 기본 `.`; `targets` 기본 `[skills].default_targets`, 없으면 가능한 전부; 명시적 `[]`는 싱크 없이 설치 | `{skill: SkillEntry, targetResults: [{target, ok, error?}]}` |
| `skills.uninstall` | `{id}` | `{trashed, restoreId, ttlMs: 300000, targetResults}` |
| `skills.restore` | `{id}` — 5분 휴지통 창 안에서 | `{skill, targetResults}` |
| `skills.setTargets` | `{id, targets[]}` — 모르는 id는 `-32602`; 새로 선택된 것 싱크, 해제된 것 제거 | `{targetResults}` |
| `skills.importLocal` | `{directory, targets?}` — 타겟 루트의 스킬을 관리 스토어로 복사, 원본 보존 | `{skill, targetResults}` |
| `skills.deleteLocal` | `{directory, targets?}` | `{targetResults}` — 보존된 원본은 남고 보고됨 |
| `skills.addRepo` | `{owner, name, branch?}` | `{added: true}` |
| `skills.removeRepo` | `{owner, name}` | `{removed: true}` — 그 리포의 설치된 스킬은 관리 상태 유지 |
| `skills.activity` | `{limit? — 기본 50, 최대 500}` | `{activity: [{ts, kind, …}]}` — 최신순 |
| `skills.discover` | `{force?}` — 활성 리포들의 GitHub 트리; 리포당 200개, 1시간 캐시 | `{skills: [{key, name, description?, directory, readmeUrl?, repoOwner, repoName, repoBranch}], cached, generatedAt}` |
| `skills.search` | `{q, limit? — 1-50, offset?}` — skills.sh | `{query, totalCount, skills: [{key, name, description?, directory, installs?, repoOwner, repoName, repoBranch}]}` |
| `skills.popular` | `{limit? — 1-200, force?}` — 시드 쿼리, 6시간 캐시 | `{skills, cached, generatedAt}` |
| `skills.updates` | `{force?}` — 설치 출처 리포 재해싱, 1시간 캐시 | `{updates: {skillId: 업데이트 여부}, checkedAt, cached}` |
| `skills.content` | `{directory}` — 로컬 읽기: 관리 사본 → 타겟 루트 → 읽기전용 소스 순 | `{directory, path, markdown, truncated}` — 512 KiB 상한 |
| `skills.remoteContent` | `{owner, name, branch?, directory}` — 디렉터리 이름 점수 매칭 | `{name, description?, directory, markdown, truncated, branch}` |
| `prompts.list` | `{sessionId?, cwd?}` — 프롬프트 라이브러리(`!` 피커) | `{prompts: [{name, description?, argumentHint?, scope, path}]}` — 워크스페이스 먼저, 이름순; `path`가 변경 핸들 |
| `prompts.get` | `{promptPath, sessionId?, cwd?}` | `{name, description?, argumentHint?, scope, content}` — 본문 |
| `prompts.create` | `{scope, name, description?, argumentHint?, content, sessionId?, cwd?}` — 이름: 공백/구분자 금지; 본문 64 KiB 상한 | `{created: true, path}` |
| `prompts.update` | `{promptPath, name?, description?, argumentHint?, content?, sessionId?, cwd?}` — 생략은 유지, `""`는 메타 삭제 | `{updated: true, path}` |
| `prompts.delete` | `{promptPath, sessionId?, cwd?}` | `{deleted: bool}` — 이미 없으면 false |
| `prompts.move` | `{promptPath, scope, sessionId?, cwd?}` — 현재 스코프와 달라야 함 | `{moved: true, path}` |
| `project.create` | `{name?, root, defaults?}` | `{projectId, name, root, defaults}` — root는 절대경로; defaults 키: backend/model/mode/mcpServers |
| `project.list` | `{}` | `{projects: [{projectId, name, root, defaults, kind: "project"\|"worktree", parentId?, meta}]}` — worktree 행은 `meta`에 git 팩트를 실어준다 |
| `project.get` | `{projectId}` | `{projectId, name, root, defaults, kind, parentId?, meta}` |
| `project.set_defaults` | `{projectId, defaults}` | `{updated: bool}` — 목록 전체 교체 |
| `project.delete` | `{projectId}` | `{deleted: true}` — 세션이 프로젝트를 참조하는 동안엔 `-32602`로 거부 |
| `worktree.create` | `{repo, branch?, prNumber?, path?, baseRef?, existingBranch?, creationId?}` — `repo`는 리포 최상위로 해석; PR 흐름은 `+refs/pull/N/head`를 `branch`(기본 `pr-N`)로 fetch; 기본 경로 `<repo 부모>/<repo>-worktrees/<branch>`; `existingBranch`는 브랜치가 이미 있을 때 | `{creationId, projectId, parentProjectId?, path, branch, meta: {branch, repo, prNumber?, prUrl?, baseRef?}, resumed}` — worktree는 `kind:"worktree"` 프로젝트로 등록; `session.create {projectId}`로 에이전트가 그 안에 착륙; `resumed`는 기존 worktree-on-branch를 입양했다는 뜻 |
| `worktree.cancel` | `{creationId}` | `{cancelled: bool}` — 그 id를 가진 진행 중 생성이 없으면 false; 실행 중 git 프로세스 그룹을 죽인다 |
| `worktree.remove` | `{projectId?, path?, branch?, deleteBranch?}` — 행 제거가 먼저라 세션 바인딩 거부(`-32602`)는 git 효과 전에 중단 | `{removed: true, orphanDirectory, branchDeleted, branchKeptReason?}` — git 제거는 항상 `--force`; 거부 시 직접 삭제 + `worktree prune`로 폴백 |
| `worktree.list` | `{repo? \| projectId?}` | `{repo, worktrees: [{path, branch?, head, isMain, locked, lockReason?, prunable, projectId?}], registered: [{projectId, name, path, parentId?, meta}]}` — git porcelain 뷰와 damon 행의 조인 |
| `worktree.resolve_pr` | `{repo, input, branch?}` — `input`은 `1842`, `#1842`, GitHub PR URL; 상세는 `gh pr view`(3초 데드라인) | `{number, repo: "owner/repo", title?, state?, author?, degraded, suggestedBranch, suggestedPath, branchConflict, dirConflict}` — `degraded`는 gh 부재/지연; 번호와 슬러그는 여전히 확인됨 |
| `worktree.merged` | `{repo, branch, base? — 기본 HEAD}` | `{merged: bool}` — 조상 검사; 스쿼시 머지는 false |
| `mcp.inventory` | `{}` | `{entries: [{backend, name, transport, command?, url?, envKeys?, source, format, version, enabled?, readonlyReason?}], sources: [{backend, source, format, error?}]}` — 모든 백엔드의 네이티브 MCP 설정, REDACTED(env 값은 파일을 떠나지 않음; URL의 userinfo/query 제거; stdio는 프로그램 + 인자 수만); 깨진 설정 파일은 소스별로 오류를 보고하고 인벤토리는 계속된다 |
| `mcp.probe` | `{backend, name}` | `{backend, name, result: "connected"\|"needs_auth"\|"failed"\|"unsupported", tools: string[] (이름만, ≤40), serverName?, protocolVersion?, elapsedMs, error?}` — 실제 핸드셰이크(stdio: 자체 프로세스 그룹, 25초 캡; HTTP: 12초 캡); 명시적 요청만, 스케줄 없음; 서버의 command/args/env는 와이어에 나가지 않는다 |
| `mcp.setEnabled` | `{backend, name, enabled, expectedVersion, cwd?}` — 이번 라운드는 claude만 | `{set: true}` — ~/.claude.json에서 claude 자체의 프로젝트별 토글을 뒤집는다. 인벤토리 `version`으로 CAS(외부 편집은 `-32010` 거부); 파싱 실패한 설정은 절대 쓰지 않음; 읽기 전용 백엔드는 이유와 함께 `-32602` 거부 |

`worktree.create` 실패는 `-32007`에 `data.kind`를 실어 온다:
`not_a_repo`, `invalid_branch`, `branch_not_found`, `branch_exists`,
`branch_checked_out`, `dir_exists`, `not_allowed`(해석된 경로가
`allowed_dirs` 밖), `no_origin`, `pr_not_found`, `fetch_failed`,
`base_not_found`, `add_failed`, `sparse_checkout_empty`,
`register_failed`, `canceled`. 생성 진행 상황은 요청 연결로
`{"event":"worktree.progress","creationId","data":{"stage","message"}}`
프레임이 스테이지마다 하나씩 온다(`validate → fetch → add → register →
done`) — 긴 fetch가 침묵으로 흐르지 않는다. 하나의 PR에 하나의 격리된
에이전트 워크스페이스: `session.create {projectId}`가 에이전트를
워크트리 체크아웃에 배치하고, 세션이 묶여 있는 worktree는 제거가
거부된다.

`session.search` 쿼리는 토크나이즈되어 FTS5 파서에 그대로 넘겨지지
않는다: 따옴표 구간은 구문이고, `AND`/`OR`/`NOT`은 통과하며, 텀 뒤
**trailing `*`는 접두사 쿼리** — `error*`가 `error`/`errors`/`erroring`에
매치(벌거벗은 `*`는 검색 가능한 게 없다). 인덱스는 `trigram`
토크나이저를 쓰므로 매치는 부분 문자열 매치다 — CJK 런 안까지
(`커밋`이 `커밋 메시지`를 찾는다; 이전 unicode61 인덱스로는 불가능).
3미만 문자 텀(trigram 최소값)은 MATCH가 조용히 버리므로 대신
정확한 부분 문자열 LIKE 경로를 탄다: 짧은 텀은 필수 AND 접속사다
(그걸 건드리는 `OR`는 AND로 좁아진다), NOT 피연산자의 배제는
존중되고, 전부 짧은 텀으로만 이룬 쿼리는 랭킹 대신 최신순 히트와
Rust에서 만든 스니펫을 돌려준다.

### CLI 라이프사이클

`backend.versions`는 카탈로그의 모든 백엔드에 대해 `<bin> --version`(10초
캡)과 — npm 패키지가 검증된 곳에서는 — 레지스트리 최신 버전(`npm view`,
15초 캡)을 동시에 프룹한다. 프룹은 명시적 요청에서만 실행된다: 스케줄링
안 됨, 접속 시에도 안 됨. npm 맵은 각 CLI의 자체 설치 문서로 검증됐다
(claude `@anthropic-ai/claude-code`, codex `@openai/codex`, omp
`@oh-my-pi/pi-coding-agent`, pi `@earendil-works/pi-coding-agent`, qwen
`@qwen-code/qwen-code`, opencode `opencode-ai`); 검증된 npm 배포가 없는
백엔드(droid, mimo — 공식 인스톨러; zcode — 검증된 채널 없음)는 설치된
버전만 프룹하고 `updateKind: "manual"`(공식 원라이너가 갱신 경로) 또는
`"unknown"`(blockers만)을 보고한다 — 패키지를 추측하는 대신.

`backend.update`는 `backend.update_plan`이 미리 보여준 플랜을 실행한다 —
하나의 argv 생성이 양쪽에 쓰이므로 확인 대화가 실행 내용과 어긋날 수
없다. 갱신은 절대 자동으로, 스케줄로 실행되지 않는다; 사용자가 시작하고,
출력을(줄당 캡으로) 요청 연결에 스트리밍하며, 취소할 수 있다. `damond
doctor`는 같은 10초 프룹을 재사용한다: 탐지된 백엔드는 버전을 인라인으로
보고하고, 타임아웃 시 "version unknown"으로 열화된다.

### 웹 기기

LAN 위 폰은 더 이상 마스터 `auth_token`이 필요 없다. 데스크톱이 일회용
페어링 키를 공개하고(`device.pairing_key`, 설정 서랍에서도), 기기가
`POST /v1/pair`로 `{pairingKey, name}`을 소비한다 — 키는 하나의 락 안에서
소비되고 로테이션되므로 하나의 코드는 정확히 하나의 페어링만 연다 — 그리고
기기 토큰과 `pending` 행을 받는다(`{deviceId, token, status}`; 토큰은 딱 한
번, 여기서 반환되고 데몬에는 SHA-256 해시로 저장된다). 기기는
`POST /v1/pair/status`(`{deviceId, token}`)를 폴링해 `pending` / `approved` /
`revoked`를 확인한다(행이 없으면 revoked — fail closed). 신뢰하는 표면이
`device.approve`로 승인하고; 그 전까지 기기는 아무것도 발급하지 못한다.

승인되면 기기는 `POST /v1/ws_ticket`(`{"deviceToken": …}`)에서 자신의
토큰을 마스터 토큰이 발급하는 것과 같은 일회용 WS 티켓으로 교환한다;
발급된 티켓에는 기기 id가 찍혀 `/ws` 업그레이드가 어느 기기의 소켓인지
알 수 있다. `device.revoke`는 행을 삭제한다 — 자격증명은 즉시 무효,
유예 기간 없음 — 그리고 이미 열려 있던 그 기기의 소켓도 그 자리에서
닫는다(응답의 `socketsCut`이 개수를 센다). 죽은 자격증명을 실은 연결이
살아남는 일은 없다. 비루프백 바인드는 여전히 해석 가능한 마스터
`auth_token` 없이 기동을
거부한다; 릴레이 모드 기기는 마스터 토큰 로그인을 유지한다(E2E 핸드셰이크가
마스터 시크릿을 증명 — 기기 토큰은 직접 닿는 표면이다). `hello`는
비루프백 `/ws` 연결의 실시간 수인 `remoteClients`를 실어 "원격 제어
활성" 배지를 표시할 수 있다.

### 봇 페르소나

페르소나는 `<data_dir>/bots/<id>/` 아래의 파일 디렉터리다:
`bot.json`(`{name, description?}`; id가 곧 디렉터리 이름), `SOUL.md`(성격),
`AGENTS.md`(행동 지침). 파일이 편집 표면이고 `bot.*`이 발견 표면이다 —
채팅의 `!bot`과 웹 UI가 이 메서드들을 쓴다. `session.create {persona}`는
파일들을 합성해 `<persona>`/`<persona-instructions>` 쌍으로 매 턴 앞에
실는다(정체성 → `<memory>` 블록 → 사용자 문장 순서). 메모리 블록과
달리 이것은 지시문이다 — 그게 핵심이다: 모델이 만든 데이터가 아니라
사용자가 직접 쓴 파일이라 메모리 게이트가 의도적으로 적용되지 않는다.
묶음은 세션 수명 동안 고정 — 파일 수정은 다음 세션부터 반영 — 되고
세션 행에 실려 데몬 재시작 후 resume/restart가 재해석한다(그 사이 봇이
삭제됐으면 조용히 풀린다). 채널 브리지는 `--bot <id>` 기본값을 받고
채팅별 `!bot`/`!bot off`를 제공한다.

### 히스토리 스캐너

데몬은 CLIS가 Damon 밖에서 만든 세션도 안다: `session.rescan`은 모든
백엔드의 고유 네이티브 세션 저장소를 — 각 백엔드의 디스커버리를 통해,
env 오버라이드(`CLAUDE_CONFIG_DIR`, `FACTORY_HOME_OVERRIDE`, …)를
존중하며 — 훑고, 데몬이 본 적 없는 항목의 북마크 행(id, backend, native
handle, cwd, title)을 만든다. PersistenceHandle 철학대로 프로바이더의
세션 파일이 진실의 원천이고; 행은 북마크며, resume은 다른 네이티브 세션
resume과 똑같이 동작한다.

범위를 정확히: 스캔이 주는 것은 **가시성과 재개 가능성**이다 — 발견된
세션이 `session.list`에 나타나고(파생된 제목과 cwd와 함께) 네이티브
핸들이 다른 네이티브 세션처럼 resume된다. 트랜스크립트 메시지를
추출하지는 않는다: 오늘 Damon에는 네이티브 핸들에 대한 메시지 추출
경로가 없다(`session.import`는 listing 호출이고, resume은 핸들을 묶고
새 턴만 스트리밍한다), 그래서 `session.search`는 damon이 실행한 턴의
메시지만 색인한다. 스캔된 세션을 검색 색인에 넣는 백엔드별 트랜스크립트
추출기는 향후 과제다.

스윕은 증분이다: 발견된 항목마다 스탯 시그니처(크기, mtime)가 있고 —
변함없으면 아무 작업도 없다. 트랜스크립트 헤드에서 파생된 제목은 노이즈
필터를 거친다(붙여넣은 `<file …>` 봉투, `[Image #N, WxH]` 플레이스홀더,
`<command-name>` 껍데기 등은 세션 이름이 되지 않고), 헤드 읽기는 16 KiB
청크로 1 MiB에서 바이트 캡된다 — Codex는 첫 줄에 base_instructions 전체를
싣으므로 무제한 읽기는 폭발한다.

빈도: 부트 스캔 한 번(시작 몇 초 뒤) + 명시적 `session.rescan`;
`[history] scan_interval_secs`가 타이머를 추가하고(0, 기본값 = 끔),
`[history] enabled = false`는 자동 스윕을 아예 끈다. 전역 단일 비행 —
실행 중인 스캔에 두 번째 `session.rescan`은 진행 중인 `scanId`를
반환한다; 진행 이벤트는 요청 연결로 간다.

실패는 갇힌다: 디스커버리가 실패한 백엔드는 `skippedBackends`에
보고되고 나머지 스캔은 계속된다. 사라진 네이티브 파일은 프로바이더의
진실이 사라졌다는 뜻 — 그 damon 세션은 **보관(archive)**된다
(`session.list`에서 숨겨짐, 히스토리 보존), 삭제되지 않는다. 발견된
세션의 `session.delete`는 네이티브 핸들을 툼스톤해 어떤 스윕도 다시
가져오지 않게 하고; 같은 네이티브 핸들을 resume하면 툼스톤이 지워진다
(사용자의 명시적 새 의도가 이긴다). 스캐너는 네이티브 파일을 절대
쓰거나 지우지 않는다.

### MCP 인벤토리와 프로브

각 에이전트 CLI는 자체 문서가 말하는 곳에 MCP 설정을 둔다: claude는
`~/.claude.json`(`mcpServers` + `projects[cwd]` 스코프,
`CLAUDE_CONFIG_DIR` 존중), codex는 `~/.codex/config.toml`
(`[mcp_servers.*]`, `CODEX_HOME` 존중), qwen은 `~/.qwen/settings.json`,
opencode는 `$XDG_CONFIG_HOME/opencode/opencode.json`, droid는
`~/.factory/mcp.json`. `mcp.inventory`는 이것들을 모두 읽고 항목별로
트랜스포트, REDACTED된 command/URL(env 키는 예, 값은 결코;
userinfo/query 제거; stdio는 프로그램 + 인자 수), 소스 파일,
콘텐츠 해시 `version`, 백엔드가 추적하는 곳에서는 enabled 상태를
보고한다. 파싱에 실패한 소스는 파일별로 보고되고(`sources[].error`)
인벤토리를 중단하지 않는다. 검증된 설정 위치가 없는 백엔드는 추측한
경로 대신 소스를 보고하지 않는다(omp/pi는 확장으로 MCP를 관리; zcode와
mimo는 미검증).

`mcp.probe {backend, name}`은 설정된 서버에 대한 실제 MCP 핸드셰이크를
수행한다 — stdio는 자체 프로세스 그룹으로 스폰(종료 시 kill, 25초 캡),
HTTP는 스트리머블-HTTP POST(12초 캡) — `initialize` →
`notifications/initialized` → `tools/list`를 걸어 도구 이름(≤40)과
서버 이름/프로토콜 버전을 돌려준다. 프루브는 명시적 요청에서만
실행된다: 스케줄되지 않고, 자동 실행되지도 않는다. 서버의 command,
args, 환경은 와이어에 나타나지 않는다.

`mcp.setEnabled`는 이번 라운드는 claude 전용: claude TUI가 쓰는 같은
프로젝트별 `disabledMcpServers` 스위치를 뒤집으며, 항목의 콘텐츠 해시
`version`이 CAS로 보호된다 — 인벤토리 스냅샷 이후 외부 편집은
`-32010`으로 거부된다. 설정은 파싱돼야 쓰여지고(깨진 파일은 절대 쓰지
않음), 같은 경로의 쓰기는 직렬화되며, 교체는 원자적이다. 다른 모든
백엔드는 이유와 함께 읽기 전용이다: 에이전트가 자기 설정을 소유한다;
Damon은 CLI의 문서화된 스위치를 뒤집을 뿐, 설정을 대신 작성하지 않는다.

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

그랜티드 루트는 이 감금을 의도적으로 넓힌다: `file.grant`는 추가
디렉터리를 영속화한다(절대경로, 존재, 정규화 — symlink로 `/private/var`에
귀결되는 `/var` 그랜트는 후자를 저장해 감금 검사와 철회가 정확히
일치). 프로젝트 또는 글로벌 범위로; `file.grants`는 범위의 실효
그랜트를 나열하고 `file.ungrant`가 철회한다. 넓히기는 `file.*`에 즉시
적용되고, 네이티브 확장 플래그가 있는 백엔드는 다음 실행 때 받는다
(claude는 실효 그랜트당 `--add-dir` 하나 — 동등한 것이 없는 백엔드는
가짜로 흉내 내지 않는다). 그랜트는 명시적 사용자 행위이며
`allowed_dirs`로 묶이지 않는다 — 그 게이트는 세션이 어디서 실행될 수
있는지를 제한하지, 명시적으로 그랜트된 세션이 무엇을 읽을 수 있는지가
아니다.

### 컴포저 지원

`catalog.commands`, `file.index`, `prompt.add`, `prompt.recent`는
웹 UI 컴포저의 피커들(슬래시 커맨드, `@path` 멘션, 프롬프트 히스토리
완성)을 뒷받침한다. 넷은 하나의 루트 셀렉터를 공유한다: 지정된 세션의
저장된 cwd(`file.*`와 같은 감금), 아니면 `session.create`와 같은
`allowed_dirs` 게이트를 통과한 클라이언트 cwd, 아니면 데몬 작업 디렉터리 —
cwd 없는 `session.create`가 정확히 그곳에 착륙하므로, 늦게 만들어지는
첫 세션 이전에 열린 피커도 세션이 실행될 디렉터리를 인덱싱한다.

`catalog.commands`는 설치된 CLI들이 이미 확장하는 커맨드/스킬 파일을
스캔한다(`.claude/commands` 재귀, `.claude/skills`, 그리고 CLI들이
`CLAUDE_CONFIG_DIR` / `CODEX_HOME` / `~/.agents/skills`로 존중하는
글로벌 루트); 데몬은 절대 `/name`을 스스로 확장하지 않는다 — 선택된
텍스트는 프롬프트로 전송되고 나머지는 CLI가 한다. 프롬프트 히스토리는
cwd별·최신 200개이며 모든 클라이언트 표면이 공유한다(릴레이의 폰이
데스크톱 브라우저와 같은 완성을 본다).

`prompt`는 문자열 또는 블록 배열 `[{type:"text",text},…]`다.

### 스킬 허브와 프롬프트 라이브러리

`skills.*` 표면은 에이전트 스킬의 패키지 매니저다: 스킬을 데몬의
관리 스토어(`<data_dir>/skills-hub/managed/`)에 한 번 설치하면
허브가 대상 CLI 각자의 네이티브 스킬 디렉터리(`~/.claude/skills`,
`~/.codex/skills`, pi/omp/opencode 홈 — `CLAUDE_CONFIG_DIR` 같은 env
오버라이드 존중)로 심볼릭링크 싱크를 하고, 심볼릭링크를 쓸 수 없는
환경에선 복사한다. 스킬 로딩은 여전히 CLI 자신이 한다 — `catalog.commands`가
싱크된 스킬을 별도 배선 없이 잡는다. 엔진 홈이 없는 타겟은 만들지
않고 실패로 보고한다.

언인스톨은 관리 사본을 5분짜리 휴지통으로 옮긴다(`skills.restore`로
복원 가능); 레지스트리가 잊은 사본까지 모든 타겟에서 제거된다. 타겟
루트에서 발견한 로컬 스킬은 비관리로 목록에 나타나고(`sourceKind:
"local"`) 임포트할 수 있다 — 스토어로 복사되며 사용자의 원본은 영구히
보존된다. Codex `.system`과 플러그인 캐시 스킬은 읽기전용으로 나열된다
(`readonly: true`, 임포트 시도 시 `error.data.code = "readonly"`).

허브 에러는 `error.data.code`에 택소노미를 싣는다: `invalid_input`
(`-32602`), `not_found`, `conflict`(로컬 변경 → `force`), `readonly`,
`permission`, `network`, `http`, `rate_limited`, `internal`.
레이트 리밋은 표면화되지 맹목 재시도되지 않는다. 디스커버리
(`skills.discover`)는 등록된 리포들의 GitHub Trees API를,
`skills.search`/`skills.popular`는 skills.sh를 hit 한다 — 둘 다
fingerprint+TTL 디스크 캐시 뒤에서. `[skills] enabled = false`면
표면 전체가 `-32002`로 거부된다.

프롬프트 라이브러리(`prompts.*`)는 `prompt.*` 히스토리와 별개다:
가벼운 frontmatter를 가진 마크다운 파일이 `<cwd>/.damon/prompts/`
(스코프 `workspace`, `allowed_dirs` 게이트)와 `<data_dir>/prompts/`
(스코프 `global`)에 저장된다. 웹 UI의 `!` 피커가 `prompts.get`으로
프롬프트 본문을 컴포저에 삽입한다.

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

`usage.summary`는 같은 행을 더 잘게 묶는다 — 로컬 날 × 백엔드 ×
모델별로, `tzOffsetMinutes`가 UTC 스탬프를 호출자의 달력으로 옮겨
버킷이 그 달력을 따르게 한다. 각 행은 턴 수와 합산 context 스냅샷,
합산 cost 델타를 담고 창 전체 `total`이 함께 돌아온다.
`usage.clear`는 모든 사용 행을 삭제한다(세션과 메시지는 그대로).

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

### 미션 러너 (스테이지 1)

단계형 에이전트 워크플로를 위한 상주 DAG 러너: 조건 라우팅 엣지 위의
`input → agent → human → output` 노드. SQLite가 진실의 원천 — 모든 전이는
팬아웃 전에 저장되고, 실행 상태는 태스크 상태에서 도출되며, 데몬 재시작 시
끝나지 않은 실행은 `interrupted + cancelled`로 수렴한다(실행 중인 것처럼
가장하지 않음).

- **조건** — `key=value` / `key!=value` 절이 업스트림 출력과 플로우 설정을
  읽는다(`and`가 `or`보다 우선, 없는 키 ⇒ false); 조건 없는 엣지는
  기본값: 같은 소스의 명시적 형제가 하나도 안 맞았을 때만 발화.
- **에이전트 노드**는 노드별 세션(시도마다 하나)에서 실제 damon 턴으로
  실행 — 전사본이 스토어에 쌓여 `session.search`가 뒤따른다. 노드의 마지막
  어시스턴트 메시지를 `key=value` 라인으로 파싱해 다운스트림 조건 컨텍스트로
  쓴다.
- **읽기 전용 단계**는 `allowedTools`를 런치 라인에 싣는다(claude:
  `--allowedTools`); 강제할 수 없는 백엔드에서 `mission.start`는 해당 노드를
  거부한다.
- **휴먼 노드**는 태스크를 `waiting_human`에 세우고 인박스 항목을 만든다;
  `mission.respond`가 해결한다(approve / feedback 재질문 / exclude — 브랜치
  종료, 다운스트림 취소).
- **실패**는 `settings.retries`까지 재시도 후 `failed` — 조용히 건너뛰지
  않는다. `settings.concurrency`가 병렬 에이전트 노드를 제한한다(휴먼 노드는
  슬롯을 점유하지 않음).

의도된 스테이지 한계: `foreach` 노드 없음, 채널 기반 승인 없음(휴먼은 UI
드로어나 와이어로 답한다), 시작 후 미션 편집 없음(취소 후 재생성).

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

### 플랜 리뷰

`kind: plan` ask(claude의 ExitPlanMode)는 플랜 텍스트를 `input.plan`에
싣는다. 데몬은 이를 영속 리뷰 행으로 기록한다(`plan.list`에서 확인;
sessionId + planId + revision 키): 리뷰가 열려 있는 동안 다시 ask되면
revision+1로 체인되고 이전 리비전은 대체된다. 라이브 ask가 전송
통로로 남고, 행이 다중 표면 팩트소스다 — 폰, 챗 봇, 데스크톱 UI가 같은
아티팩트를 보고 답한다.

결정은 `plan.respond`(`planId`, `expectedRevision`, `decision`, 선택
`feedback`)으로 간다: expectedRevision CAS 클레임 때문에 같은 리비전에
답하는 두 표면은 정확히 하나의 승자와 패자의 타입화된 `-32009`로
귀결되고, 낡거나 닫힌 리비전에 대한 결정은 에이전트에 절대 전달되지
않는다. 결정 의도는 네이티브 릴레이 **전에** 영속화된다: 끊긴 파이프는
기록을 남기고 행을 다시 연다(상태를 `awaiting_review`로 복귀, 의도는
파일에 보존) — 일어나지 않은 성공을 보고하는 일은 없다. `approve`는
라이브 ask에 allow를 전달하고, `request_changes`는 feedback을 message로
싣는 deny를, `defer`는 ask를 건드리지 않고 비종단적으로 기록한다 —
에이전트의 대기는 그대로 멈춰 있고 나중의 `plan.respond`가 still
결정할 수 있다. 리뷰 불가능한 내용(`complete: false`)의 플랜 승인과
feedback 없는 request_changes는 `-32602`로 거부된다.

턴이 끝나면 아직 열린 리뷰는 만료된다(네이티브 ask가 프로세스와 함께
죽었다 — 히스토리가 답변 가능한 척해선 안 된다; 기록된 의도는
보존된다). 플랜 ask에 대한 `permission.respond`도 같은 영속 행에
결정을 기록한다(deny의 `message`가 기록되는 feedback이 된다) — 게이트
이전 클라이언트도 계속 동작한다.

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
