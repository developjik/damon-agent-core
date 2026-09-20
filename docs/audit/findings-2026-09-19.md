# Audit Findings

## 2026-09-19 — Full Audit (New Findings)

- 대상: damon-agent-core 저장소 전체 (Rust 크레이트 `damon-core` + npm 클라이언트 + 테스트 + 빌드/CI 설정)
- 방식: 읽기 전용 전수 감사 — 조사 26 에이전트(모듈 × 관점 분해) → 적대적 검증 12 에이전트 → 오케스트레이터 스팟체크
- 원시 근거: `audit-scratch-2026-09-19/` (Phase 1/2 JSON payload 전문)
- 기존 findings 없음(첫 감사) → 기존 중복 폐기 0건

---

### 1. 결함 목록 (심각도 내림차순)

### [High] mangle 충돌 시 응답 tool_call이 엉뚱한 툴로 복원되어 실행됨 (openai/anthropic/gemini 공통)
- 위치: src/provider/openai.rs:299-307,334-354 / src/provider/anthropic.rs:446-457 / src/provider/gemini.rs:355-361
- 분류: correctness
- 시나리오: MCP 툴 `server.tool`(`server__tool`로 mangle)과 리터럴 툴 `server__tool`이 동시에 정의됨 → `mangle_name`은 mangled==name이면 맵에 넣지 않으므로(304) 맵에는 `server__tool→server.tool`만 존재 → 모델이 리터럴 `server__tool`을 호출하면 복원 단계에서 맵 hit으로 `server.tool`로 이름이 교체됨 → `execute_tool`이 모델이 고른 인자로 엉뚱한 툴을 실행하고, auto_approve/session_approved 판정도 잘못된 이름으로 내림.
- 근거:
```rust
// openai.rs:299-307
fn mangle_name(v: &mut Value, map: &mut HashMap<String, String>) {
    let Some(name) = v.as_str() else { return; };
    let mangled = mangle(name);
    if mangled != name {
        map.insert(mangled.clone(), name.to_string());
        *v = Value::String(mangled);
    }
}
// openai.rs:347-350 — 복원 시 맵 hit이면 무조건 교체
if let Some(n) = call.get_mut("function").and_then(|f| f.get_mut("name"))
    && let Some(orig) = n.as_str().and_then(|s| map.get(s)).cloned()
{ *n = Value::String(orig); }
```
- 수정 제안: 맵 구축 시 충돌을 검출해 충돌하는 모든 이름에 `__<sha256[:8]>` 접미 경로를 적용하거나 빌드 시 에러. anthropic/gemini의 동일 클래스(replacen 폴백 포함)를 한 번에 수정.
- 검증: 검증 에이전트가 runtime.rs execute_tool(686-688)까지 전 경로 재추적해 확정, 오케스트레이터 독회로 재확인.

### [High] 채널 백프레셔 시 Update 청크 try_send 드롭으로 응답 텍스트가 영구 유실됨
- 위치: src/channel.rs:147-159 (라우터), 370-382 (버퍼링), 363-368 (flush)
- 분류: correctness | leak(데이터)
- 시나리오: 긴 응답 스트리밍 중 채널 전송이 느려지면(Slack 1msg/s, Discord 429) per-chat 64슬롯 채널 포화 → 라우터가 Update를 `try_send`로 무음 드롭 → 드롭된 청크는 `run_turn`의 `buf`에 영원히 없고, PromptDone 결과에는 전체 텍스트가 없어(363-368은 `[stop]` 마커만 전송) 답변 중간이 조용히 잘림.
- 근거:
```rust
// channel.rs:156-159
match &ev {
    ClientEvent::Update(_) => { let _ = tx.try_send(ev); }   // 포화 시 무음 드롭
    _ => { let _ = tx.send(ev).await; }
}
// channel.rs:376-378 — 도착한 청크만 버퍼링
if let Some(t) = u["content"]["text"].as_str() { buf.push_str(t); ... }
```
- 수정 제안: per-chat 채널 unbounded 전환 + 중간 Update 병합(같은 텍스트 스트림), 또는 드롭 발생 시 유실 표시 후 최종 전체 텍스트 재전송.
- 검증: 검증 에이전트가 "전체 텍스트 재전송 경로 부재"를 client.rs 이벤트 계약까지 재추적해 확정, 오케스트레이터 독회로 재확인.

### [Medium] is_context_overflow 오탐 → 요청하지 않은 모델/프로바이더로 무단 promotion
- 위치: src/api.rs:551-565 (판정), 511 (호출), 692-695,722-724 (translate 경로)
- 분류: correctness
- 시나리오: 중간 프록시가 413/4xx에 "request too large", "too many tokens" 등 제네릭 문구를 반환 → 소문자 substring 매칭 오판 → `context_promotion_target`으로 원 요청 무단 재전송 → 미요청 모델 과금 + 실제 원인(바디 크기) 은폐.
- 근거:
```rust
fn is_context_overflow(text: &str) -> bool {
    let t = text.to_lowercase();
    ["context_length_exceeded", "context length", ..., "request too large"]
        .iter().any(|s| t.contains(s))
}
```
- 수정 제안: 오탐 강한 시그니처는 에러 `code` 필드/프로바이더별 셰이프로 한정.
- 검증: 검증 에이전트가 passthrough/translate 양 경로에서 오판→재전송 추적, 확정(Medium — target 미설정 시 미발동).

### [Medium] 에러 버퍼 >1MB·타임아웃·collect 실패가 빈 본문으로 삼켜짐
- 위치: src/api.rs:501-509
- 분류: error-handling
- 시나리오: upstream이 1MB 초과 컨텍스트 오버플로 에러를 반환하거나 30s 타임아웃/스트림 에러 발생 → `_ => error_response(status, "")` 분기가 promotion 재시도 없이 빈 본문 반환 → 디버깅 정보 완전 소실, 가능한 promotion도 미실행.
- 근거:
```rust
let buf = match tokio::time::timeout(Duration::from_secs(30),
    crate::provider::collect_stream(up.stream)).await {
    Ok(Ok(b)) if b.len() <= (1 << 20) => b,
    _ => return error_response(status, ""),   // 에러 원인 전부 소실
};
```
- 수정 제안: 캡 초과분은 잘라 판정에 사용하고 promotion 진행, 실패 시 잘라낸 텍스트 반환.
- 검증: 검증 에이전트가 timeout/collect 에러까지 같은 분기로 삼켜짐을 추가 확인, 확정.

### [Medium] live_prompts Mutex를 store await 구간 동안 보유 — 전 커넥션 cancel/prompt 지연
- 위치: src/rpc.rs:563-596 (session/prompt), 456-487 (session/delete)
- 분류: concurrency
- 시나리오: session/prompt가 락 보유 중 `store.session_exists().await`, session/delete 태스크도 락 보유 중 `delete_session().await` → SQLite 락/대형 delete로 수 초 걸리면 모든 커넥션의 cancel/disconnect/prompt가 같은 락에서 대기하는 데몬 전역 컨보이.
- 근거:
```rust
let mut map = state.live_prompts.lock().await;
match state.store.session_exists(&session_id).await { ... }   // 락 보유 중 await
```
- 수정 제안: store 호출을 락 밖으로 빼고 재획득 후 contains_key 재확인.
- 검증: 검증 에이전트 확정(Medium — delete 폴 루프는 반복마다 락 해제하므로 store 호출 구간 한정).

### [Medium] `stalled` 플래그가 연결 수명 전체 sticky — 한 번 타임아웃하면 알림 영구 무음 드롭
- 위치: src/rpc.rs:162-177 (notify), 127-128, 268 (선언/초기화)
- 분류: error-handling
- 시나리오: 느린 클라이언트가 64슬롯 송신 채널을 채우고 notify 전송이 SEND_TIMEOUT=10s 초과 1회 → `stalled=true` 이후 리셋 경로 없음(전체 파일에서 store(true)뿐) → 클라이언트가 따라잡은 뒤에도 스트림 델타/알림이 전부 조용히 드롭, 응답이 영구 절단. 회복은 재접속뿐.
- 근거:
```rust
Err(_) => { self.stalled.store(true, Ordering::Relaxed);
            warn!("dropping notification to stalled client"); }
// store(false) 없음 — request 성공 전송도 클리어하지 않음
```
- 수정 제안: 성공 전송 시 false로 리셋하거나 플래그를 활성 턴 스코프로 한정.
- 검증: 검증 에이전트가 전체 파일 재탐색으로 리셋 부재 재확인, 확정.

### [Medium] 비ASCII 토큰 추산이 실제의 약 2.25배 과대 — CJK 세션 컴팩션 과발동으로 컨텍스트 조기 유실
- 위치: src/runtime.rs:472-481 (text_tokens), 539-545 (판정)
- 분류: correctness | 경계(유니코드)
- 시나리오: 비ASCII 바이트 가중 3/4 → 3바이트 CJK 1자 = 2.25 token 가산(실제 약 1 token/char) → 컴팩션 판정이 이 추산에 전적으로 의존(window*85%)하므로 CJK 세션은 창의 ~38% 사용 시점에 발동 → 오래된 절반이 요약으로 대체되어 원문 복원 불가, decisions/경로 조기 소실. ASCII 세션 대비 CJK만 체계적 열화.
- 근거:
```rust
fn text_tokens(s: &str) -> u64 {
    s.bytes().map(|b| if b.is_ascii() { 1 } else { 3 }).sum::<u64>() / 4
}
```
- 수정 제안: CJK 바이트 가중을 실제 근사로 낮추거나 provider usage 이벤트로 보정.
- 검증: 반증 시도(방어 경로·보정 분기 탐색) 실패로 확정.

### [Medium] fts5_query sanitize가 단항 NOT을 통과시켜 검색 500 / `X AND NOT Y`의 NOT 무음 삭제
- 위치: src/store.rs:325-365
- 분류: correctness | 계약
- 시나리오: (A) `NOT urgent` → `NOT "urgent"` 그대로 MATCH — FTS5는 이항 NOT만 지원 → syntax error → search() Err. (B) `rust AND NOT gc` → pending_op 존재로 NOT 무음 삭제 → `rust AND "gc"` — 배제 조건이 사라져 반대 결과 오염. 함수 doc의 "result always parses" 계약 위반.
- 근거:
```rust
Tok::Not => {
    if pending_op.is_none() {
        pending_not = true; // unary NOT before the next operand
    }
}
```
- 수정 제안: NOT도 직전 토큰이 term일 때만 이항으로 방출, 그 외 폐기.
- 검증: 두 시나리오 모두 코드 재추적으로 재구성, 확정.

### [Medium] drain_pending이 pending 뮤텍스 보유 중 이벤트 채널 send를 await — prompt() 영구 블록 가능
- 위치: src/client.rs:584-604 (drain_pending), 417-446 (prompt)
- 분류: concurrency
- 시나리오: 이벤트 소비자가 멈춰 64캡 채널 포화 상태에서 링크 종료 → Reconnect.run이 drain_pending 안에서 `event_tx.send().await`로 정지하며 뮤텍스 보유 → prompt()가 같은 락을 타임아웃 없이 획득 시도 → 영구 블록.
- 근거:
```rust
let mut map = pending.lock().await;
for (_, p) in map.drain() { ...
    Pending::ToEvents { session_id } => { let _ = event_tx.send(...).await; } }
```
- 수정 제안: drain을 Vec으로 모으고 락 해제 후 send, 또는 try_send 기반 전환.
- 검증: 링크 종료→포화→블록 전 경로 코드 추적, 확정.

### [Medium] ticketed_url이 타임아웃 없는 reqwest::Client::new() 사용 — connect() 무한 대기
- 위치: src/client.rs:352-373
- 분류: error-handling
- 시나리오: 토큰 사용 시 connect가 POST /v1/ws_ticket을 보내는데 TCP는 받지만 응답 없는 데몬/프록시면 reqwest 기본(타임아웃 없음)으로 send().await 무한 대기 — relay 경로는 15s 바운드가 있으나 이 경로는 무바운드.
- 근거:
```rust
let resp = reqwest::Client::new().post(format!(url + "/v1/ws_ticket"))
    .bearer_auth(token).send().await.context("ws_ticket request failed")?;
```
- 수정 제선: 클라이언트에 connect/total timeout 설정 또는 tokio timeout 랩.
- 검증: 확정.

### [Medium] MCP 툴 캐시 TOCTOU — reload 제거된 서버의 툴이 영구 부활
- 위치: src/mcp.rs:169-189 (ensure_connected) vs 74-81 (reload 제거)
- 분류: concurrency
- 시나리오: ensure_connected의 `Arc::ptr_eq` 재확인(read 락)과 tools retain+insert(write 락)가 서로 다른 락 구간 → 사이에 reload가 slot 제거+tools purge를 수행하면 이후 insert가 제거된 서버의 툴을 영구 부활 → has_tool은 광고하고 call은 매번 "MCP server X not running".
- 근거: ptr_eq 재확인(171-176)과 tools.retain+insert(183-189)가 별개 inner 락 구간.
- 수정 제안: conn 설치와 tools insert를 재확인과 같은 단일 write 임계구역으로.
- 검증: 레이스 창 실존 재추적, 확정(엣지 조건부 — reload와 dial 경합).

### [Medium] MCP transport 에러 자동 재시도가 부수효과 툴을 이중 실행할 수 있음
- 위치: src/mcp.rs:322-343
- 분류: correctness
- 시나리오: TransportClosed/UnexpectedResponse는 요청이 서버에서 처리된 이후에도 발생 가능(자식 사망/응답 유실) → 파일 쓰기·메시지 전송 같은 툴이 첫 시도 실행 후 재시도로 재실행. UnexpectedResponse는 응답을 받았다는 뜻이라 재시도 분류 자체가 오류.
- 근거:
```rust
matches!(e, ServiceError::TransportSend(_)
    | ServiceError::TransportClosed | ServiceError::UnexpectedResponse)
// 322-323 주석: 서버가 처리한 호출의 재시도는 이중 실행을 안다고 인정
```
- 수정 제안: 자동 재시도 제거 후 호출자에 반환, 최소 UnexpectedResponse는 재시도 집합에서 제외.
- 검증: 확정.

### [Medium] Telegram offset이 유지처리 이전에 확정 — 크래시 시 배치 잔여 메시지 영구 유실
- 위치: src/telegram.rs:148-166
- 분류: correctness | leak(데이터)
- 시나리오: 3개 메시지 배치 도착 → offset이 처리 여부와 무관하게 id+1로 서버 확정(서버측 영속) → 1건 처리 후 프로세스 크래시 → in-memory pending 소실 → 재시작 시 getUpdates가 확정된 offset부터 재개 → 2-3번째 메시지는 영원히 재전달 안 됨.
- 근거:
```rust
if let Some(id) = u["update_id"].as_i64() {
    self.offset.fetch_max(id + 1, Ordering::Relaxed); // 서버 확정
}
self.pending.lock().await.extend(it); // 메모리에만
```
- 수정 제안: 브리지 소비 완료 후에만 offset 확정 또는 pending 영속화.
- 검증: 확정(Medium — 재배포 시 현실적 경로).

### [Medium] Discord op7/op9 재접속이 고정 5s 슬립 — identify 쿼터 소진으로 gateway ban 위험
- 위치: src/discord.rs:329-334 (294-295도 동일)
- 분류: error-handling
- 시나리오: op 9(invalid session) 지속 시 → 연결 drop → 고정 5s 대기 → 재identify. 백오프/지터/resume 없이 ~17,000 identify/일 → 계정 레벨 identify ban(최대 24h). 주석이 이 위험을 인지하면서 5s로는 부족.
- 근거:
```rust
if v["op"] == 7 || v["op"] == 9 {
    *guard = None; drop(guard);
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    continue;
}
```
- 수정 제안: op 9에 지수 백오프 + 세션 resume 재사용.
- 검증: 백오프 부재는 코드로 확정, 쿼터 수치는 docs 기반.

### [Medium] chat_sessions와 데몬 세션이 삭제 경로 없이 무한 증가
- 위치: src/channel.rs:281-291 (유일한 remove는 358의 stale 재시도뿐)
- 분류: leak
- 시나리오: 장기 운영에서 채팅 참여 수만큼 세션 생성 누적, 각 세션의 히스토리는 daemon Store에 계속 적립 → 메모리/디스크 무한 증가.
- 근거: session_for가 insert 후 제거 경로 없음.
- 수정 제안: idle 타임아웃 후 session/delete + 캐시 제거 또는 LRU 캡.
- 검증: 확정.

### [Medium] 브리지 permission 타임아웃 300s 하드코딩 — runtime 설정과 드리프트
- 위치: src/channel.rs:410-413 vs runtime.rs:28 + config permission_timeout_secs
- 분류: contract
- 시나리오: (a) 설정 60s → 데몬이 60s에 deny 후 턴 종료, 브리지는 300s까지 대기 → 사용자가 90s에 allow 입력 시 이미 해소된 요청에 응답(무시)되며 잘못된 피드백. (b) 설정 600s → 브리지가 300s에 자동 deny해 데몬의 긴 창을 조기 종료. rpc 측은 설정값(+30s)을 존중하는데 브리지만 하드코딩.
- 근거:
```rust
// Bound the wait by the daemon's default permission timeout ...
let timeout = std::time::Duration::from_secs(300);
```
- 수정 제안: 단일 상수/설정 공유.
- 검증: 양방향 시나리오 코드 재구성, 확정.

### [Medium] 릴레이 서버 — 데몬 교체 창에서 클라이언트가 죽은 터널 tx에 고착
- 위치: src/bin/damon-relay.rs:352-355,383-399,413-415
- 분류: concurrency
- 시나리오: 클라이언트 세션이 시작 시 daemon_tx를 한 번 clone → 직후 데몬 재접속하면 클라이언트 프레임은 죽은 tx로 `let _ = send` 무음 소실, attach/disconnect 통지도 구세대 tx로 감 → 좀비 클라이언트가 clients 슬롯과 전역 캡 1024를 점유, 신규 데몬은 이 클라이언트를 영원히 못 봄.
- 근거: recv_task가 clone된 tx를 계속 사용(383-399), 전송 실패 시 세션 정리 없음.
- 수정 제안: 전송 실패(Closed) 시 세션 종료 또는 소유자를 터널 세대 식별자로 기록해 구세대 정리.
- 검증: 라인 재대조로 확정.

### [Medium] 세션 DB·데이터 디렉터리가 기본 퍼미션으로 생성 — 대화 전사가 타 사용자 판독 가능
- 위치: src/store.rs:26-31 (생성), src/bin/damond.rs:138-141 (경로)
- 분류: security
- 시나리오: create_dir_all(umask 022 → 0755) + SQLite 기본 0644 → 멀티유저 호스트에서 다른 로컬 사용자가 전체 세션 대화·툴 입출력을 읽음. config.rs:333-341은 같은 위협 모델에서 설정 파일 0600을 경고하는데 데이터 측은 무처리(자기 불일치).
- 근거:
```rust
if let Some(parent) = path.parent() {
    std::fs::create_dir_all(parent)...;
}
let conn = Connection::open(path)
```
- 수정 제안: store open 시 0700/0600 설정, 기존 파일이 느슨하면 경고.
- 검증: 검증 에이전트가 Low→Medium 상향 판정, 확정.

### [Medium] glob_match의 `?`가 바이트 단위 매칭 — 비ASCII 모델명 라우팅 미탐
- 위치: src/config.rs:448-486
- 분류: correctness | 경계(유니코드)
- 시나리오: doc은 "`?` one char"이나 구현은 바이트 비교 → `ollama-?` vs `ollama-é`(2바이트)에서 `?`가 1바이트만 소모 → false → 해당 패턴 라우팅이 default 프로바이더로 폴백. 문서와 다른 동작.
- 근거:
```rust
if pi < p.len() && (p[pi] == b'?' || p[pi] == s[si]) {
    pi += 1; si += 1;   // 1바이트만 소모
}
```
- 수정 제안: char_indices 기반 순회 또는 문서 정정.
- 검증: 확정.

### [Medium] npm client redial의 WebSocket open 대기에 타임아웃 부재 — 초기 connect와 비대칭
- 위치: npm/client.mjs:137-152
- 분류: error-handling
- 시나리오: 초기 connect()는 10초 게이트가 있으나 #redial의 open await는 없음 → 방화벽 SYN drop 블랙홀 호스트에서 수 분 정체, 데몬이 살아나도 #connected 미해소로 모든 호출이 10초 타임아웃.
- 근거: connect()는 10s 바운드(67-73), redial open 대기(140-142)는 무바운드.
- 수정 제안: redial open에 동일 10초 바운드 추가.
- 검증: 확정.

### [Medium] phase4 통합 테스트의 유일한 툴 결과 persist 검증이 tautology
- 위치: tests/phase4.rs:239-240
- 분류: test(거짓 통과)
- 시나리오: `assert!(tool_msg["content"]...contains("content"))` — 런타임 persist 형식상 content 키는 어떤 경로에서도 직렬화에 필수라 단언이 논리적으로 항상 참 → args echo 유실·에러 봉투 오염을 전부 가림. 'Tool result persisted' 검증력 사실상 0.
- 근거:
```rust
let tool_msg = msgs.iter().find(|m| m["role"] == "tool").unwrap();
assert!(tool_msg["content"].as_str().unwrap().contains("content"));
```
- 수정 제안: 목업 echo(`{}`)가 결과 텍스트로 전파됐는지 단언으로 교체.
- 검증: runtime persist 형식 대조로 항상-참 확인, 확정.

### [Medium] anthropic: OpenAI string `stop`을 stop_sequences에 그대로 복사 — 매 요청 400
- 위치: src/provider/anthropic.rs:241-247
- 분류: contract
- 시나리오: OpenAI 와이어는 `"stop": "END"`(문자열) 허용 → 그대로 stop_sequences에 복사 → Anthropic은 배열 요구 → 매 요청 400. 배열 클라이언트는 무영향.
- 근거:
```rust
if let Some(stop) = body.get("stop_sequences").or_else(|| body.get("stop"))
    .filter(|v| !v.is_null()) {
    out["stop_sequences"] = stop.clone();
}
```
- 수정 제안: 문자열이면 [stop] 랩, empty/null 드롭.
- 검증: 확정.

### [Medium] anthropic: mid-stream overloaded_error가 generic error로 매핑 — RateLimited 백오프 경로 상실
- 위치: src/provider/anthropic.rs:652-673
- 분류: error-handling
- 시나리오: Anthropic은 스트림 시작 후 과부하를 SSE error 이벤트로 전달 → 이 분기는 error.type을 무시하고 plain anyhow 반환 → 모든 RateLimited 소비자(runtime.rs:171, api.rs:681/712)는 downcast 기반이라 백오프/429 미적용 → 긴 턴이 과부하에서 가시 실패.
- 근거:
```rust
Some("error") => {
    done = true;
    let msg = v["error"]["message"].as_str().unwrap_or("upstream stream error");
    return Some((Err(anyhow::anyhow!("anthropic stream error: {msg}")), ...));
}
```
- 수정 제안: type이 overloaded/rate_limit면 Err(RateLimited) 승격(부분 출력 정책 확인 후).
- 검증: downcast 기반 소비자 전수 확인으로 방어 부재 확정.

### [Medium] Retry-After 음수값이 Duration::from_secs_f64에서 panic — 턴이 패닉으로 사망
- 위치: src/provider/mod.rs:38-46 (호출처 gemini.rs:302,336 / anthropic.rs:358,394 / responses.rs:237,255)
- 분류: error-handling | 경계
- 시나리오: 호환 업스트림/프록시가 429/503에 `Retry-After: -1` 반환 → 파싱 성공, .min(60.0)은 하한만 클램프 → from_secs_f64 panic → 요청 태스크가 백오프 대신 패닉으로 언와인드.
- 근거:
```rust
.and_then(|v| v.parse::<f64>().ok())
.unwrap_or(2.0);
std::time::Duration::from_secs_f64(secs.min(60.0))
```
- 수정 제안: `if !(secs.is_finite() && secs > 0.0) { secs = 2.0 }` 가드.
- 검증: High→Medium 하향(호위적 업스트림 필요) + NaN 절반 반증(f64::min은 NaN 아닌 피연산자 반환), 음수 panic은 확정.

### [Medium] gemini 안전차단이 침묵 성공(비스트리밍)/generic 에러(스트리밍)로 은폐
- 위치: src/provider/gemini.rs:394-397 (gemini_to_openai), 520-534 (EOF 경로)
- 분류: correctness
- 시나리오: 프롬프트가 안전필터에 차단 → 200 + promptFeedback.blockReason만 존재(candidates 없음) → 비스트리밍은 finish_reason "stop" + 빈 content(모델 침묵과 구분 불가), 스트리밍은 "stream ended without finishReason"으로 blockReason 마스킹. promptFeedback은 파일 전체에서 미독.
- 근거:
```rust
let finish_reason = match v["candidates"][0]["finishReason"].as_str() {
    Some("STOP") if !tool_calls.is_empty() => "tool_calls",
    Some("MAX_TOKENS") => "length",
    _ => "stop",
};
```
- 수정 제안: promptFeedback.blockReason을 양 경로에서 읽어 반영.
- 검증: 확정.

### [Medium] collect_stream이 바이트 캡 없이 전체 스트림 버퍼링 — 메모리 고갈 여지
- 위치: src/provider/mod.rs:217-226
- 분류: leak(리소스)
- 시나리오: passthrough는 30s 타임아웃만 존재(api.rs:502-505) — 빠른 upstream이 30s 내 수백 MB 투입 가능. openai.rs:216/273/290의 에러바디 읽기는 타임아웃 없이 120s read-idle만 — 드립피드 upstream이 Vec를 무한 성장.
- 근거:
```rust
let mut buf = Vec::new();
while let Some(chunk) = s.next().await { buf.extend_from_slice(&chunk?); }
```
- 수정 제안: 시간 바운드와 무관한 최대 바이트 가드 추가.
- 검증: 호출처별 바운드 재확인, 확정.

### [Medium] inband render_tools가 배열형 system content를 통째로 파괴
- 위치: src/provider/inband.rs:38-44
- 분류: correctness
- 시나리오: system 메시지가 OpenAI 멀티파트 배열([{type:text,...}])로 들어옴 → as_str() None → prev="" → sys.content가 프롬프트 단독 문자열로 대체 → 원본 시스템 프롬프트 완전 소실.
- 근거:
```rust
let prev = sys["content"].as_str().unwrap_or("").to_string();
sys["content"] = Value::String(format!("{prev}\n\n{prompt}"));
```
- 수정 제안: 배열 content면 text part에 추가, 문자열일 때만 결합.
- 검증: 확정.

### [Medium] compat 시스템 메시지 병합이 구조화 content를 평문으로 평탄화
- 위치: src/provider/compat.rs:108-117
- 분류: correctness
- 시나리오: supports_multiple_system_messages=false 프로바이더에 system 2개(하나라도 배열 content) → content_text로 텍스트만 추출 후 prev.content를 평문 대체 → 비텍스트 part 무음 소실.
- 근거:
```rust
let prev_text = content_text(&prev["content"]);
let cur_text = content_text(&m["content"]);
prev["content"] = Value::String(format!("{prev_text}\n{cur_text}"));
```
- 수정 제안: 둘 다 문자열일 때만 병합, 배열이면 text part 배열로 병합.
- 검증: 확정.

### [Medium] responses 번역에서 멀티모달(이미지) content 무음 손실
- 위치: src/provider/responses.rs:82-91
- 분류: correctness
- 시나리오: vision 요청(content 배열에 image_url part)이 responses 경로 유입 → content_text만 남김 → 에러 없이 이미지 제거 전송 → 모델이 이미지 없이 답변(환각 유도).
- 근거:
```rust
Some("user") => input.push(json!({type:"message", role:"user",
    content:[{type:"input_text", text: super::content_text(&m["content"])}]}))
```
- 수정 제안: input_image 등 part 번역 또는 손실 시 명시적 오류.
- 검증: 확정.

### [Medium] inband 스트리밍 후처리가 Text 청크들을 index 0으로 재배치 — 이벤트 순서 역전
- 위치: src/provider/inband.rs:98-112
- 분류: correctness
- 시나리오: 전체 텍스트를 모은 뒤 `out.insert(0, Text(clean))` → Thinking 이벤트가 본문 뒤로 밀림 → 클라이언트 관찰 순서 역전.
- 근거:
```rust
StreamEvent::Text(t) => text.push_str(&t), other => out.push(other)
...
if !clean.is_empty() { out.insert(0, StreamEvent::Text(clean)); }
```
- 수정 제안: 청크별 즉시 extract 또는 원래 Text 위치 유지 삽입.
- 검증: 확정.

### [Medium] openai: tool_calls delta에 `index` 부재 시 전부 slot 0으로 뭉개짐
- 위치: src/provider/openai.rs:585-592 (+ llm.rs ToolCallAccumulator::push)
- 분류: correctness
- 시나리오: llama.cpp 서버 등 일부 게이트웨이는 index를 생략하고 id로 구분 → 모든 delta가 index 0 → 병렬 호출의 인자 프래그먼트가 한 슬롯에 push_str 연결되어 arguments 오염.
- 근거:
```rust
let index = call["index"].as_u64().unwrap_or(0) as usize;
```
- 수정 제안: index 부재 시 call id 키잉.
- 검증: Accumulator 계약 대조, 확정(Medium — 단일 호출은 무영향).
### [Low] upstream 에러 문자열(URL 포함)이 클라이언트 응답에 그대로 반환
- 위치: src/api.rs:477-482, 543-547, 699-703, 729-733, SSE Err 분기(~662-668)
- 분류: security(정보 노출)
- 시나리오: reqwest Display는 전체 upstream URL 포함("error sending request for url (https://internal-gateway.corp/...)") → 502 본문으로 그대로 반환 → 인증된(또는 loopback 무토큰) 클라이언트가 내부 upstream 토폴로지 탐지 가능.
- 근거:
```rust
Err(e) => openai_error(StatusCode::BAD_GATEWAY,
    &format!("upstream error: {e}"), "server_error"),
```
- 수정 제안: 고정 메시지 반환, 상세는 warn! 로그로.
- 검증: 병합(ApiFlow#4≡ApiSecurity#1) 후 확정 — 전 경로 인증 뒤라 Low.

### [Low] constant_time_eq 길이 불일치 조기 반환 — 자체 doc이 금지하는 길이 타이밍 오라클
- 위치: src/config.rs:310-314 (호출처 api.rs:866-871, rpc.rs:41-45)
- 분류: security(타이밍)
- 시나리오: 길이 불일치 시 XOR 루프 미실행 → 응답 시간 분포로 토큰 길이 스캔 이론 가능. 실원격 관측은 극히 어려움.
- 근거:
```rust
if a.len() != b.len() { return false; }
let mut diff = 0u8;
```
- 수정 제안: 최대 길이 순회 + 길이차를 diff에 반영.
- 검증: 병합(ApiSecurity#2≡ConfigAudit#2) 후 확정(Low).

### [Low] forward 재직렬화 시 u64/i64 초과 정수 정밀도 손실
- 위치: src/api.rs:427-441
- 분류: correctness | 경계
- 시나리오: thinking/model 치환 경로에서 전체 Value 재직렬화 → 2^63 초과 정수만 f64 반올림(serde_json 기본). 스노우플레이크 ID(<2^63)는 무영향 — 극단 엣지.
- 근거:
```rust
let mut v: serde_json::Value = serde_json::from_slice(&body)...;
if !upstream_model.is_empty() { v["model"] = json!(upstream_model); }
bytes::Bytes::from(v.to_string())
```
- 수정 제안: 바이트 레벨 치환 또는 RawValue 원문 보존.
- 검증: 검증 에이전트가 "2^63 초과→f64" 주장의 과잉 부분 반증(serde_json은 u64::MAX까지 정확), 잔여 엣지만 Low 확정.

### [Low] 취소된 툴 호출이 클라이언트에 status:failed로 통보 — 영속 행은 cancelled인데 표시 불일치
- 위치: src/runtime.rs:377-387 (content 분기는 cancelled 판별), 414-427 (status는 is_ok()만)
- 분류: contract
- 시나리오: 턴 취소 → 진행 중 툴이 Err(Cancelled) → 영속 행은 "cancelled"로 정확하나 tool_call_update.status는 failed → ACP 클라이언트가 사용자 취소를 실패로 렌더링.
- 근거:
```rust
"status": if result.is_ok() { "completed" } else { "failed" },
```
- 수정 제안: content와 동일한 cancelled 판별식 재사용.
- 검증: 확정.

### [Low] cancel_mid_tool_loop_repairs_orphan_calls 테스트가 이름이 주장하는 Cancelled 경로를 실행하지 않음
- 위치: tests/runtime.rs:212-295 (대조 src/runtime.rs:337-350, 684-686)
- 분류: test(거짓 통과 — 허위 커버 표시)
- 시나리오: 취소 유발 notify가 execute_tool 시작 전 in_progress이고, 툴 a/b는 미등록이라 취소 체크 전에 unknown-tool bail → `is::<Cancelled>()` 항상 거짓, cancelled 마킹/repair 분기 미실행. 런타임 동작 자체는 올바르나 테스트 이름·코멘트가 약속하는 검증이 무검증.
- 근거:
```rust
if !state.mcp.has_tool(&call.name) { anyhow::bail!("unknown tool {}", call.name); }
// 취소 체크(711,733)에 도달 전 반환
```
- 수정 제안: 등록된 느린 툴로 Cancelled 분기 도달 테스트로 교체.
- 검증: Medium→Low 하향(결함은 커버리지 허위 표시에 한정), 확정.

### [Low] 로컬 WS 경로에 relay 경로의 max_message_size 캡 부재
- 위치: src/client.rs:84-91, 469-493 vs src/relay.rs:543-546
- 분류: 경계
- 시나리오: 토큰 없는 ws:// 경로에서 프록시가 64MiB Text 프레임 전달 시 tungstenite 기본(무캡) 버퍼링. relay 경로는 4MiB 캡으로 동일 위협 커버 — 커버리지 불일치.
- 근거: connect_async에 WebSocketConfig 미지정.
- 수정 제안: 동일 4MiB 캡 적용.
- 검증: 확정.

### [Low] 데몬 셧다운 시 MCP 자식 프로세스 고아 가능 — Drop에 전적으로 의존
- 위치: src/mcp.rs:79,194,293,337 — cancel()/close() 호출 부재
- 분류: leak
- 시나리오: rmcp RunningService Drop은 async close를 DropGuard로 위임(Graceful shutdown 보장 약함) — 데몬 종료 훅에서 close 대기가 없어 자식이 고아로 남을 수 있음.
- 근거: rmcp-3.3.0 service.rs Drop 구현 대조 확인.
- 수정 제안: 셧다운 훅에서 각 슬롯 close_with_timeout() await.
- 검증: 확정.

### [Low] 동시 ensure_connected 호출자가 중복 자식 스폰
- 위치: src/mcp.rs:129-150
- 분류: concurrency
- 시나리오: 자식 사망 후 N 세션이 동시에 conn None + 백오프 없음을 관찰 → 각각 connect_one 스폰 → N-1개 폐기 kill. npx 같은 무거운 커맨드 N회 중복 실행.
- 근거: caller가 락 하에서 확인 후 락 밖에서 dial, conn 설치는 나중(192-195).
- 수정 제안: 슬롯별 in-flight dial 가드.
- 검증: 확정.

### [Low] 릴레이 서버 — 중복 client_id가 활성 세션을 abort 없이 덮어씀
- 위치: src/relay.rs:415-499
- 분류: concurrency
- 시나리오: 호환/적대 relay가 같은 id로 connect 통지 재전송 → insert가 이전 엔트리 덮어씀(abort 없음) → 구 태스크 종료 시 done 통지가 신규 활성 세션 엔트리를 제거 → disconnect/backpressure 정리에서 안 보이고 MAX_SESSIONS 초과 가능.
- 근거: insert 시 기존 AbortHandle 미처리.
- 수정 제안: insert 시 기존 핸들 abort 또는 중복 id 거부.
- 검증: 확정.

### [Low] Slack seen_envelopes가 4096 캡에서 전체 clear — 늦은 재시도 재처리
- 위치: src/slack.rs:270-277
- 분류: correctness
- 시나리오: 바쁜 채널에서 4096개 신규 처리 후 Slack이 이전 envelope 재시도 → clear로 지워져 중복 프롬프트 실행/중복 permission 프롬프트.
- 근거:
```rust
if seen.len() >= 4096 { seen.clear(); }
if !seen.insert(eid.to_string()) { continue; }
```
- 수정 제안: 전체 clear 대신 최고(oldest) 항목만 축출하는 경계 유지.
- 검증: 확정.

### [Low] Bridge run 루프가 recv()==Ok(None)을 백오프 없이 재호출 — 서드파티 어댑터에서 버스핑 스핀
- 위치: src/channel.rs:104-109
- 분류: error-handling
- 시나리오: 어댑터가 종료 전환 중 즉시 Ok(None) 반환하면 sleep 없이 무한 재호출. Err 경로에는 5s 슬립 존재. 3개 스톡 어댑터는 롱폴/WS 수신으로 블록해 실발성 낮음.
- 근거:
```rust
Ok(None) => {}                                   // 백오프 없음
Err(e) => { warn!(...); sleep(5s).await; }
```
- 수정 제안: Ok(None)에도 백오프 적용.
- 검증: Medium→Low 하향(스톡 어댑터에서 실재하지 않음), 확정.

### [Low] session_for가 chat_sessions 뮤텍스를 new_session RPC await 동안 보유
- 위치: src/channel.rs:283-288
- 분류: concurrency
- 시나리오: 채팅 A 첫 메시지의 new_session 왕복(수 초) 동안 락 보유 → 타 채팅 첫 턴 직렬화. 데드락 없음, 1회성 이벤트.
- 근거: 락 보유 중 `client.new_session().await`.
- 수정 제안: 락 밖 new_session 후 insert 시 재확인.
- 검증: Medium→Low 하향, 확정.

### [Low] print_definition의 launchd plist에 리터럴 `~` 로그 경로 — launchd가 확장하지 않음
- 위치: src/service.rs:139-143 (install() 94-97은 정상)
- 분류: correctness
- 시나리오: print 출력을 plist로 저장해 load하면 StandardOutPath가 리터럴 `~/` 취급 → 로그 쓰기 실패·stdout 유실.
- 근거:
```rust
let log = "~/Library/Logs/damond.log";
return launchd_plist(exe, config, log);
```
- 수정 제안: BaseDirs 절대 경로 사용.
- 검증: 확정.

### [Low] systemd_escape 미처리 `%` / schtasks /TR 따옴표 이스케이프 미처리
- 위치: src/service.rs:42-44, 67-70
- 분류: correctness | 경계
- 시나리오: 설치 경로의 `%` → systemd 지정자(%h 등)로 해석되어 경로 변조, 기동 실패. 공백 포함 경로 + cmd.exe는 백슬래시 이스케이프 미처리로 /TR 조기 종료 가능.
- 근거:
```rust
fn systemd_escape(s: &str) -> String { s.replace('\\', "\\\\").replace('"', "\\\"") }
```
- 수정 제안: `%` → `%%`, cmd 규칙에 맞는 /TR 조립.
- 검증: 확정.

### [Low] 릴레이 서버 — 캡 초과 시 에러 프레임 없이 소켓 무응답 종료
- 위치: src/bin/damon-relay.rs:128-136, 335-343
- 분류: error-handling
- 시나리오: name_taken/unauthorized와 달리 원인 구분 불가 → 클라이언트/데몬이 네트워크 단절로 오판, 캡 상황 재시도 폭주(self-amplifying).
- 근거: on_upgrade 내 슬롯 release 후 즉시 return.
- 수정 제안: {"error":"over_capacity"} 프레임 또는 upgrade 전 429/503.
- 검증: 확정.

### [Low] 릴레이 서버 — clients 맵 insert 전 도착한 프레임 응답 무음 드랍
- 위치: src/bin/damon-relay.rs:384-399, 405-409, 413-415
- 분류: concurrency
- 시나리오: recv_task가 insert 전 spawn되어 두 sender 간 순서 보장 없음 → 극히 짧은 창에서 빠른 요청 응답이 get None → 데몬 펌프가 폐기 → 클라이언트 hang. '등록 전 응답 없음' 주석과 불일치.
- 근거: spawn(384)이 insert(409) 선행.
- 수정 제안: insert를 spawn 전으로 이동 또는 미지 id 응답 warn.
- 검증: 확정.

### [Low] 플랫폼 bins --token CLI 인자가 ps로 토큰 노출
- 위치: src/bin/damon-slack.rs:18-20 (discord/telegram 동일)
- 분류: security(시크릿)
- 시나리오: `#[arg(long, env = "DAMON_TOKEN")]` — env 대체재 존재하지만 플래그 사용 시 프로세스 목록에 토큰 노출.
- 근거: clap long 인자 정의.
- 수정 제안: env 전용화 또는 문서 경고.
- 검증: 확정.

### [Low] CLI가 EPIPE에서 panic — exit 101, panic 메시지 노출
- 위치: src/bin/damon.rs:112,134,159,241-243
- 분류: error-handling
- 시나리오: `damon sessions | head -1` — Rust는 Unix에서 SIGPIPE SIG_IGN → println! panic("failed printing to stdout") → exit 101.
- 근거: stdout 쓰기 후 flush/println 경로.
- 수정 제안: EPIPE 시 조용히 exit(0) 또는 SIGPIPE 기본 복원.
- 검증: 확정.

### [Low] RUST_LOG가 .env에서 무시됨 — dotenvy 로드가 tracing init 이후
- 위치: src/bin/damond.rs:59-62 vs src/config.rs:321-333
- 분류: config
- 시나리오: config-dir .env의 RUST_LOG=debug는 EnvFilter 구성 이후 적재 → 무시됨. 같은 .env의 env: 시크릿은 나중 resolve라 동작 — 일관성 없는 관측 동작.
- 근거: tracing init이 Config::load 선행.
- 수정 제안: dotenvy를 tracing init 전으로 이동.
- 검증: 확정.

### [Low] ModelMeta에 deny_unknown_fields 부재 — [models] 오타 키 침묵
- 위치: src/config.rs:62-75
- 분류: config
- 시나리오: `contex_window` 오타가 에러 없이 무시 → compaction 임계가 기본값으로 폴백. 같은 파일의 다른 섹션은 즉시 에러.
- 근거: 타 구조체는 모두 deny_unknown_fields 명시, ModelMeta만 부재.
- 수정 제안: 어트리뷰트 추가.

### [Low] ensure_config가 생성 후 chmod — 0600 적용 전 창
- 위치: src/config.rs:510-520
- 분류: security | 경계
- 시나리오: fs::write(0644&~umask) 후 set_permissions(0600) — 창 실재하나 STARTER_CONFIG는 주석뿐이라 노출물 없음. set_permissions 실패 시 0644인 채 Err.
- 근거:
```rust
std::fs::write(path, STARTER_CONFIG)?;
... set_permissions(path, Permissions::from_mode(0o600))?;
```
- 수정 제안: create_new(true).mode(0o600) 원자적 생성.
- 검증: 확정(Low 유지 — 실질 피해 경로 부재).

### [Low] OAuth 토큰 엔드포인트가 비JSON 에러 반환 시 HTTP 상태 코드 소실
- 위치: src/oauth.rs:216-219, 289-292
- 분류: error-handling
- 시나리오: 400/502를 HTML/텍스트로 반환(프록시 오류 페이지) → `resp.json().await?`가 status 검사보다 선행해 디코드 에러만 남음 → invalid_grant(재로그인) vs 502(재시도) 구분 불가.
- 근거:
```rust
let status = resp.status();
let v: serde_json::Value = resp.json().await?;
if !status.is_success() { bail!("token exchange failed ({status}): {v}"); }
```
- 수정 제안: 파싱 실패 시에도 bail!에 상태 코드 포함(exchange/refresh 양쪽).
- 검증: 확정.

### [Low] install.js 체크섬이 동일 오리진(GitHub) — 서명 부재 TOFU
- 위치: npm/install.js:24-25, 84-96
- 분류: security(공급망)
- 시나리오: GitHub 계정/릴리스 침해 시 tarball+.sha256 동시 위조로 검증 통과 → 악성 바이너리 0755 설치. 전송 오류 감지만 제공.
- 근거: curl로 같은 release에서 tar.gz와 .sha256 fetch 후 대조.
- 수정 제안: 별도 오리진/서명 검증 추가.
- 검증: 확정.

### [Low] install.js 다운로드에 타임아웃 부재 — postinstall 무기한 정체
- 위치: npm/install.js:52-68
- 분류: error-handling
- 시나리오: curl에 --max-time/--connect-timeout 없음, Node fetch에 AbortSignal 없음 → 응답 정지 네트워크에서 설치 정체.
- 근거: curl -fsSL 플래그 목록.
- 수정 제안: 타임아웃 플래그/AbortSignal 추가.
- 검증: 확정.

### [Low] install.js 실패 경로에서 부분 tarball 잔존
- 위치: npm/install.js:52-54, 87-97
- 분류: error-handling
- 시나리오: 전송 중단/체크섬 페치 실패 시 tarball 미삭제(불일치 경로 97행만 삭제) → bin/에 부분 파일 잔존, 진단 혼란.
- 근거: 실패 분기에 unlink 부재.
- 수정 제안: 실패 시 정리 추가.
- 검증: 확정(curl 경로 한정).

### [Low] client.mjs ticketedUrl이 쿼리스트링 포함 URL에서 오염된 티켓 URL 생성
- 위치: npm/client.mjs:338-347
- 분류: 경계
- 시나리오: `ws://host/ws?x=1` 입력 → `/\/ws$/` 미매치 → `http://host/ws?x=1/v1/ws_ticket` 호출 → 404 예외. connect 시점 명시 예외라 침묵 오동작 아님.
- 근거: 정규식 end-anchor.
- 수정 제안: 쿼리 분리 후 path 매칭.
- 검증: 확정.

### [Low] install.js Windows 런처가 '#!/bin/sh' 스크립트 — sh 보장 없음
- 위치: npm/install.js:104-112 (+ package.json bin)
- 분류: config
- 시나리오: 의존성 0 패키지에서 확장자 없는 sh 런처 생성 — cmd-shim이 인터프리터를 못 찾으면 bin 실행 실패 가능. darwin 환경으로 Windows 실기 검증 불가.
- 근거: 런처 내용이 POSIX shebang.
- 수정 제안: Windows용 .cmd 런처 병행 생성.
- 검증: 정적 추적 확정(실기 검증 불가 명시).

### [Low] bench.rs 준비 대기가 deadline 검사 후 블로킹 read — hang 시 assert 도달 불가
- 위치: examples/bench.rs:137-146
- 분류: test
- 시나리오: damond가 stdout 없이 정체하면 첫 `lines().next()`가 영구 블로킹, deadline 검사에 도달 못 함 → CI 타임아웃 의존.
- 근거: deadline 검사가 read 직전에만 수행.
- 수정 제안: 읽기를 별도 태스크로 밀고 timeout 수신.
- 검증: 확정.

### [Low] bench.rs idle_rss가 조회 실패를 0으로 침묵 출력 — 무효 베이스라인
- 위치: examples/bench.rs:153-163
- 분류: test
- 시나리오: pid 조회 실패(권한/종료됨) → unwrap_or(0) → "idle RSS: 0.0 MB" 정상 종료 → 회귀 비교 시 유/무의 개선 도출.
- 근거:
```rust
sys.process(...).map(|p| p.memory()).unwrap_or(0)
```
- 수정 제안: 조회 실패 시 panic 또는 rss>0 단언.
- 검증: 확정.

### [Low] tests/mcp_server.py가 ping에 -32601 응답 — MCP 스펙의 빈 result 의무 위반
- 위치: tests/mcp_server.py:44-51
- 분류: test(목업-계약 불일치)
- 시나리오: initialize/tools/* 외 전부 method-not-found — ping 포함. 현재 테스트 스위트가 ping을 호출하지 않아 미발동이나, ping을 쓰는 rmcp 클라이언트 경로 테스트 시 오동작 목업.
- 근거: elif 체인에 ping 분기 부재.
- 수정 제안: ping → `{}` result 응답 추가.
- 검증: 확정.

### [Low] Anthropic 어댑터의 Usage{input:0}가 SSE 마지막 usage 청크 prompt_tokens=0으로 노출
- 위치: src/provider/anthropic.rs:638-644 (근원), src/api.rs:633-640 (노출점)
- 분류: contract
- 시나리오: message_start(input=N) → message_delta(input:0, output 증분)를 api.rs가 각각 usage SSE 청크로 전달 → 클라이언트가 받는 마지막 청크의 prompt_tokens가 0. 메트릭은 누적 소비로 정확.
- 근거:
```rust
Usage { input: 0, output: out.saturating_sub(start_output) }
```
- 수정 제안: message_delta에 현재 input 반영 또는 최종 usage만 전달.
- 검증: 스트림 순서 재추적으로 재현, 확정.

### [Low] openai SSE 멀티라인 data에 2개 JSON 값이면 전체 스트림 실패
- 위치: src/provider/openai.rs:366-370, 462-465, 528-531
- 분류: 경계
- 시나리오: SSE는 `data:` 여러 줄을 LF join 허용 — 하나의 JSON 값이면 `\n`은 공백이라 파싱 성공, 2개 JSON 값 이벤트(비전형적 upstream)만 "invalid SSE JSON"으로 스트림 실패.
- 근거:
```rust
let data = data_lines.join("\n");   // → serde_json::from_str
```
- 수정 제안: 파싱 실패 데이터 스킵 또는 분리 파싱.
- 검증: Medium→Low 하향(join 결과가 유효 JSON이면 파싱됨이 확인됨), 확정.

### [Low] openai SSE 파서의 라인당 buf.drain이 O(n²) memmove
- 위치: src/provider/openai.rs:424-445
- 분류: performance
- 시나리오: 각 라인마다 offset 0부터 재스캔 + 전체 잔여 버퍼 memmove. 실제 토큰 스트림은 poll당 작은 버퍼라 거의 선형 — 대형 청크/소형 라인 병리 입력에서만 열화.
- 근거:
```rust
let pos = buf.iter().position(|&b| b == b'\n')...;
buf.drain(..=pos);
```
- 수정 제안: 스캔 오프셋 유지 또는 memchr 단일 패스.
- 검증: Medium→Low 하향(현실적 입력 기준), 확정.

### [Low] openai strict-retry 에러 경로가 content_type을 위조하고 헤더를 소실
- 위치: src/provider/openai.rs:167-176
- 분류: correctness
- 시나리오: 본문에 "strict" 미포함 400 → 응답 재조립 시 content_type을 무조건 application/json으로, 실제 헤더 소실 → content-type 분기 소비자가 text/plain 에러를 오분류.
- 근거:
```rust
content_type: "application/json".into(),
stream: Box::pin(futures::stream::once(async move { Ok(Bytes::from(err)) })),
```
- 수정 제안: resp.text() 전에 headers에서 content-type 인출.
- 검증: 확정.

### [Low] CI 워크플로에 최상위 permissions floor 부재
- 위치: .github/workflows/ci.yml:1-13
- 분류: security(CI)
- 시나리오: test/audit 잡(pull_request 트리거)이 토큰 권한 선언 없음 — repo 기본이 read/write면 제3자 action 침해 시 쓰기 권한 탈취 가능. pull_request_target 미사용이라 현재 구성에서는 강화 항목.
- 근거: release 잡만 `permissions: contents: write` 선언.
- 수정 제안: 워크플로 레벨 `permissions: contents: read`.
- 검증: 확정.

### [Low] CI actions가 mutable 태그 핀 — 공급망 강화 갭
- 위치: .github/workflows/ci.yml:17,21,25,74,95,115,131
- 분류: security(공급망)
- 시나리오: checkout@v4, rust-cache@v2, audit-check@v2, rust-toolchain@stable, gh-release@v2, setup-node@v4 — 태그 force-move 시 NPM_TOKEN/CARGO_REGISTRY_TOKEN 보유 잡에서 공격자 코드 실행.
- 근거: 전 action이 태그 참조.
- 수정 제안: commit SHA 핀.
- 검증: 확정.

### [Low] rust-toolchain `channel = "stable"` — 컴파일러 플로팅
- 위치: rust-toolchain.toml:2
- 분류: config
- 시나리오: 새 stable에서 rustc 동작/lint 변화 시 코드 무변경으로 CI(clippy -D warnings)·릴리스 빌드 변동. edition 2024 최소(1.85) 미강제.
- 근거: channel="stable".
- 수정 제안: 버전 핀 또는 rust-version = 1.85 추가.
- 검증: 확정.

### [Low] set-version.sh가 Formula sha256 갱신을 reminder 에코로만 처리
- 위치: scripts/set-version.sh:44-50
- 분류: config(릴리스)
- 시나리오: 0.3.0 태그 후 사람이 3개 digest를 손으로 못 고치면 모든 `brew install`이 sha256 mismatch로 실패(소리 나는 실패).
- 근거: `echo "reminder: update Formula sha256"`.
- 수정 제안: 릴리스 CI가 .sha256 아티팩트로 digest 커밋.
- 검증: 확정.

---

### 2. 커버리지 표 (모듈 → 조사 에이전트 → 검증 에이전트 → 상태)

| 범위 | 조사 | 검증 | 상태 |
|---|---|---|---|
| src/api.rs (893) | ApiSecurity + ApiFlow | VerifyApi | 완료 |
| src/rpc.rs (715) | RpcSocket | VerifyRpc | 완료 |
| src/runtime.rs (775) | RuntimeCore | VerifyRuntime | 완료 |
| src/store.rs (654) | StoreAudit | VerifyRuntime | 완료 |
| src/relay.rs (659) | RelayCrypto | VerifyClientMcp | 완료 |
| src/client.rs (596) + examples/client.rs | ClientLib | VerifyClientMcp | 완료 |
| src/channel.rs (461) + src/service.rs (154) | ChannelCore | VerifyChannels | 완료 |
| src/slack.rs + src/telegram.rs + src/discord.rs | Platforms | VerifyPlatformsDeps | 완료 |
| src/mcp.rs (395) | McpAudit | VerifyClientMcp | 완료 |
| src/oauth.rs (315) | OauthAudit | VerifyConfigBins | 완료 |
| src/config.rs (593) + config.example.toml | ConfigAudit | VerifyConfigBins | 완료 |
| src/llm.rs (207) | LlmTypes-2 | VerifyLlm | 완료 |
| src/provider/anthropic.rs (756) | ProvAnthropic | VerifyProviders | 완료 |
| src/provider/openai.rs (620) | ProvOpenai-2 | VerifyOpenai | 완료 |
| src/provider/responses.rs + compat.rs + inband.rs | ProvResponses | VerifyProviders | 완료 |
| src/provider/gemini.rs + mod.rs + discovery.rs | ProvMisc | VerifyProviders | 완료 |
| src/bin/damond.rs + src/bin/damon.rs | BinDaemon | VerifyConfigBins | 완료 |
| src/bin/damon-relay.rs + platform bins 3종 | BinRelaySrv(-2) | VerifyRelaySrv | 완료 |
| npm/client.mjs + install.js + package.json | NpmAudit | VerifyNpmTests | 완료 |
| tests/providers.rs 1-750 | TestsProvidersA | — (결함 0건, 검증 대상 없음) | 완료 |
| tests/providers.rs 751-1493 | TestsProvidersB | VerifyNpmTests | 완료 |
| tests/runtime.rs (1167) | TestsRuntime | VerifyRuntime | 완료 |
| tests/{rpc,api,relay,client,oauth}.rs | TestsRpcApi | — (결함 0건) | 완료 |
| tests/{channels,store,service,boot,phase4}.rs + mcp_server.py + examples/bench.rs | TestsCore | VerifyNpmTests | 완료 |
| Cargo.toml, rust-toolchain.toml, ci.yml, set-version.sh, Formula/damon.rb | DepsConfig | VerifyPlatformsDeps | 완료 |

High 스팟체크: 2건 전부 오케스트레이터가 해당 라인을 직접 독회해 재확인(openai.rs:299-354, channel.rs:144-165,363-403). Critical 0건.

### 3. 검증 후 결함 없음 목록 (차기 감사 중복 배제 재료)

- **store.rs**: 전 질의 `?N` 파라미터 바인딩(문자열 조립은 cutoff/JSON 배열뿐, 모두 바인딩) — SQL 인젝션 없음. 마이그레이션 컬럼 재확인 멱등. append/delete/cleanup 트랜잭션 원자성. messages() 단일 tx 일관 읽기. 빈 쿼리/없는 세션 계약. 세션 ID 파일명 미사용. 락 across await 없음. WAL+busy_timeout.
- **api.rs 인증/티켓/레이트리밋**: ws_ticket 32B OS RNG + 원자적 원샷 소비 + 만료 재확인. auth_token_cache fail-closed(Err 캐시, 빈 토큰 거부, reload 전 재확인). require_token 비루프백 fail-closed, loopback 무토큰 시 loopback Origin만 허용. Bearer strip + 상수시간 비교. rate_limit는 ConnectInfo 부재 시 no-op(스푸핑 헤더 불신), refill 수학 정확. /metrics 게이트. openai_error 자체는 고정 메시지+kind. forward는 클라이언트 Authorization을 업스트림으로 릴레이하지 않음(프로바이더 키 서버측 주입). promotion 재귀 depth 캡(순환 target 안전), passthrough 무재귀.
- **rpc.rs 인증/정리**: 티켓 원자적 제거, fail-closed. is_localhost_origin — IPv6 bracket/후행 점/대소문자/포트/userinfo/hex·8진 IP/zone-id/Origin:null 전부 fail-closed. PROMPT_SLOTS 세마포어 panic 포함 반환. PendingGuard Drop 정리. disconnect가 자기 conn_id 프롬프트만 취소.
- **runtime.rs**: 모델 해석 구간 락 홀딩 없음. chat_stream TTFB 타임아웃+1회 재시도+RateLimited 백오프+cancel select 완비. 스트림 루프 stall 타이머 이벤트마다 리셋. 취소/스트림 에러 시 부분 텍스트 영속화+미완 툴콜 드롭(orphan 방지). tool row 영속화+persist 실패 시 cancelled repair 행(쌍 불변식 유지). truncate_tool_output char-boundary 안전. 컴팩션 경계 while 루프+summary 선행 주입. 컴팩션 실패/타임아웃 시 set_compaction 미기록(영구 유실 방지). perm_lock 획득 중 cancel select+session_approved 락 하 재확인. request_permission 에러/거부 모두 deny, always-allow 세션 스코프 격리. 시크릿 로그 유출 없음.
- **relay.rs E2E**: client-proves-first(무인가 (pubkey,proof) 수확 차단). proof sha256(token||mine||theirs) 이중 pubkey 바인딩. 방향 분리 키 d2c/c2d(reflection 차단). was_contributory 양측. seq==expected strict decrypt fail-closed(재생/재정렬 거부). OS RNG. run_tunnel 5s/60s 백오프, 시크릿 per-attempt 재해석, 해석 실패 시 소리 나는 거부. ws:// 경고 정확([::1] 포함). auth 프레임 URL 쿼리 미사용. 4MiB 프레임 캡 양측. HANDSHAKE_TIMEOUT 양측. urlencoding RFC3986 unreserved only, UTF-8 안전. 시크릿 미로그. u64 seq 랩은 2^64 프레임 필요 — 이론적으로만 존재.
- **client.rs**: E2E 핸드셰이크 daemon측과 완전 대칭(proof 순서/상수시간 비교/seq 0 시작/방향 매핑). 전송 실패 시 pending 제거+3회 한 재전송, 서버 에러 미재시도. Reconnect supervisor의 writer 스왑→Connected 순서, 백오프 리셋. ws_transport 프레임 처리(Ping/Pong skip, Close/Err 종료). wait_connected deadline 정확. 토큰은 Authorization 헤더만. malformed JSON/비u64 id 무시. tungstenite가 프레임 경계+UTF-8 검증(청크 경로 없음).
- **mcp.rs**: command/args/env 직접 exec(셸 해석 없음). stdio 데드락 없음(stderr inherit). 30s init 타임아웃 양 경로. 300s tool 타임아웃, conn 락 미보유. backoff 시프트 오버플로 불가. 자식 사망 → TransportClosed 재접속 경로. non-object args 거부. 설정 변경 시 세션 승인 무효화.
- **channel.rs**: permission reply 단일 락 check+consume(그룹챗 승격 방지). active_turns 이중 프롬프트 방지. TurnGuard RAII(패닉 시 해제). stale session 1회 재시도+retried 플래그. end_turn의 세션/permission 정리. plist XML 이스케이프.
- **slack.rs/telegram.rs/discord.rs**: telegram 토큰 URL 새니타이즈 전 에러 경로. slack/discord 토큰 bearer/WS payload만. 429/Retry-After 준수(3 sender 일관: f64 파싱/30s 캡/1.0 기본/단일 재시도). 전 경로 타임아웃 존재(WS liveness 포함). floor_char_boundary UTF-8 안전 분할. 빈 텍스트/bot·self author 거부. discord <@!id> 변형, hello 검증, heartbeat clamp+Drop abort. slack 전 envelope ack.
- **oauth.rs**: state/PKCE 불일치 bail. static Mutex 단일 비행 리프레시+락 하 만료 재확인+rotation 유지. Debug redact. keyring NoEntry/기타 에러 분기. 만료 판정 Unix초+60s skew, 실패는 보수적. 테스트 훅 debug-only. 토큰 파일 0600. 콜백 서버 없음(바인딩 노출 경로 자체 없음).
- **config.rs**: SecretRef parse 형식 검증/빈 값 거부, env 누락·keychain 에러 분리 전파. resolve_command 타임아웃 kill+wait, drain 스레드로 파이프 블로킹 회피, 비정상 종료/빈 출력 에러. load/reload 실패 시 이전 설정 유지. glob_match 알고리즘 자체(연속 *, 백트래킹, 빈 패턴 경계) 정확 — `?` 바이트 결함은 별도 보고. validate 화이트리스트+oauth 센티널 예외. watch 100ms 디바운스, 로드 성공 시에만 스왑.
- **llm.rs**: parse_partial_json 정확(escape/쉼표 추적/char-boundary/종료 보장). finish의 id+name 조건 필터가 전 프로바이더 생산과 일치. StopReason 매핑 6곳 일관(rpc 커버 포함). StreamEvent는 serde 미파생(직렬화 비대칭 경로 없음). gemini usageMetadata 단발, anthropic usage 증분은 메트릭 정확.
- **provider/anthropic.rs**: SSE 버퍼링/UTF-8 경계 안전. EOF 무-message_stop은 에러(가짜 완결 방지). usage 이중 계산 방지(message_start 기준+saturating_sub). dense tool index. thinking signature/redacted verbatim 재생. 요청 folding(user/tool 연속 병합, tool_result 선행). thinking budget 시 max_tokens 승격, 툴 히스토리 무 thinking 시 드롭. cache_control 위치. 401 강제 리프레시 1회 재시도. 429/529→RateLimited. stop_reason 매핑.
- **provider/openai.rs**: raw bytes 버퍼+`\n` 디코드로 UTF-8 청크 경계 안전. [DONE]/finish_reason 없는 EOF는 Err. usage 동승 수집. base URL 슬래시 정규화(이중/누락 없음). 셰이핑은 POST /chat/completions 정확 매치. 멀티 툴콜 청크 큐 순서 보존. tool_choice/재생 메시지 tool_calls 이름도 mangle.
- **provider/responses.rs**: SSE framing(CRLF/UTF-8/Done/pending-Usage-then-Done). response.failed/mid-stream error 실에러화, incomplete→Length. sparse→dense remap 일관. Accumulator 통합(id+name 조건, 256 OOM 가드). translate_request 병합 순서. 429 양 경로 Retry-After.
- **provider/compat.rs**: apply 플래그 순서, requires_tool_result_name id→name 맵+developer 역할, mistral_id 요청 내 페어링.
- **provider/inband.rs**: extract_tool_calls block_len 산술, malformed/빈 이름 블록 텍스트 보존, 호출 없을 때 no-op.
- **provider/gemini.rs**: 요청 번역(role folding, tool-name mangle, systemInstruction) 결함 미발견. usageMetadata 최종 단발.
- **provider/discovery.rs**: 결함 미발견.
- **provider/mod.rs**: http_client connect 15s+read-idle 120s 하드 바운드(Never Client::new). retry_after 상한 60s, NaN 시 60.0(f64::min 성질 — 패닉 없음). content_text 배열/문자열 처리. build_providers 부분 실패 수집(부트 유지).
- **damond.rs**: 비루프백 auth_token 게이트가 Store/MCP 스폰 선행(부작용 없는 실패). Store-open/bind 실패는 서빙 전이라 부분 기동 없음. SIGTERM/SIGINT graceful+live_prompts cancel+2s persist 대기. TLS/plain 대칭. watch capacity-1 try_send coalescing은 설계.
- **damon-relay.rs**: 등록 시크릿 constant_time_eq. 무시크릿 시 빈 auth만 허용(프루빙 차단). 이름 재사용: is_closed 정리+name_taken 명시 거부+same_channel 검사. 캡 증분+upgrade 후 재검사(pre-upgrade race 보완). valid_name 로그 위조 차단. release 맵 무한 성장 없음. Ping/Pong 유지. select! 정리.
- **npm/**: execSync 상수+인용(셸 주입 없음). fail-closed 순서(0바이트 가드→체크섬→추출→6바이너리 확인→chmod). client.mjs 시크릿 처리(토큰 URL 노출 없음, 로그 유출 없음). #pending 정리 가드, 소켓 식별 가드로 이중 redial 방지. JS 측 E2E 코드 부재 확인 — client.mjs는 로컬 /ws 직결이라 Rust relay E2E와의 불일치 결함은 성립 불가(repo map의 "JS 클라이언트+E2E"는 stale 기술).
- **tests/**: tests/providers.rs 1-750 — 16개 테스트 전부 실값 단언(mock이 실제 HTTP/직렬화 경로 통과, 음성 케이스 포함). tests/{rpc,api,relay,client,oauth}.rs — no-token/wrong-token 401, 티켓 원샷 2회차 401, E2E 리플레이/재정렬/오류 토큰 거부 전부 정확한 값 검증. tests/{channels,store,service,boot}.rs — 실제 서버/바이너리/DB 구동 실검증. oauth 테스트 ENV_LOCK 직렬화. ENV mutation 경합 없음.
- **빌드/CI/설정**: config.example.toml이 deny_unknown_fields Config 스키마와 필드별 일치(복사해도 부팅 실패 없음). ci.yml pull_request만 사용(pull_request_target 없음), 시크릿은 tag-gated 잡만, rust-cache 잡별 키 분리. set-version.sh semver 사전 검증(특수문자 주입 없음). Formula URL/빈 수가 CI 아티팩트·package.json과 일치. Cargo.toml feature 전원 실재, default=[] 불활성.

### 4. 미조사 영역과 이유

- docs/, README*, CONTRIBUTING*, CHANGELOG.md, LICENSE-*: 문서 — 코드 결함 관점 부적합으로 제외.
- Cargo.lock, target/, node_modules, 벤더 코드: EXCLUDES. (rmcp 3.3.0 소스는 mcp 결함 입증 근거로만 부분 인용.)
- npm Windows cmd-shim 실동작, Slack idle-ping 정확 주기: 감사 환경(darwin)에서 플랫폼 실기 검증 불가 — 해당 후보는 정적 추적으로 판정 후 [INFERENCE] 명시.
- 1차 배치 중 ProvOpenai/LlmTypes/BinRelaySrv 3건은 API rate limit(429)으로 실패 → 동일 지시로 재배치(ProvOpenai-2/LlmTypes-2/BinRelaySrv-2)하여 범위 손실 없이 완수.

### 5. 통계

| 구분 | 수 |
|---|---|
| 승인 발견 — Critical | 0 |
| 승인 발견 — High | 2 |
| 승인 발견 — Medium | 29 |
| 승인 발견 — Low | 38 |
| **승인 발견 합계** | **69** |
| 검증 폐기 | 7 (rate_limit eviction 성능 주장, 무효 JSON 위장, resolve_command 비UTF-8, relay 세션 슬롯 leak, llm index>=256 드롭, tests 갭 봉인 2건) |
| 병합 | 4그룹 (upstream 에러 노출 2→1, rate eviction 2→1, constant_time_eq 2→1, mangle 충돌 3→1) |
| 기존 중복 폐기 | 0 (첫 감사 — 기존 findings 부재) |
| 후보 총계 | 81건 → 병합 후 76 unique → 승인 69 |

검증 단계 정정 사항(스카웃 주장 중 반증된 것): "005"는 u64 파싱 성공(선행 0 허용), Retry-After NaN은 f64::min 성질로 패닉 없음, serde_json은 u64::MAX까지 정확 파싱 — 각 findings의 검증 란에 반영.

MODEL_RULE: 서브에이전트 전원(조사 26 + 검증 12)이 스폰 기본값으로 세션 모델(zai/glm-5.3-flash)을 상속. 모델 교체/대체 없음. 읽기 전용 준수 — 감사 대상 파일의 생성/수정/삭제 없음(스크래치·본 보고서만 생성).

---

## 2026-09-19 — Remediation (재검증 → 수정 → 검증)

수정 전 모든 발견을 파일 소유권 기준 15개 에이전트가 코드 재독회로 3차 재검증했고, 승인분은 최소 수정 + 회귀 테스트로 처리했다. 이후 오케스트레이터 통합 패치(openai Retry-After 스레딩, MCP shutdown 호출부 연결)와 전체 검증을 수행했다.

### 결과 요약

| 처분 | 수 | 비고 |
|---|---|---|
| FIXED (수정 + 검증) | 67 | 하단 근거 참조 |
| NO-FIX (재검증으로 불필요 확정) | 1 | api.rs u64 정밀도 — 원본 바디 패스스루 스킵이 이미 구현 존재, 잔여 엣지(u64::MAX/i64::MIN 초과)만 코드 주석으로 문서화 |
| DEFERRED (외부 인프라 전제) | 1 | npm install.js TOFU 체크섬 — 서명 인프라(SHA256SUMS+minisign 등) 필요, docs/release.md 기존 항목 |

### 검증 근거

- `cargo test` 전체 스위트: **exit 0** (lib 단위 36건 — 기존 4건에서 +32 신규, api 10, boot 1, channels 13, client 3, oauth 4, phase4 4, providers 34, relay 5, rpc 6, runtime 13, service 10, store 6)
- `cargo clippy --lib --bins --tests --examples -- -D warnings`: 클린 (CI 게이트 동일)
- 행위 스모크(에이전트 실시): 릴레이 데몬 kill → 클라이언트 소켓 3초 내 EOF(좀비 소멸) / `damon health | 닫힌 파이프` → exit 141 무패닉 / config-dir .env RUST_LOG=debug → 부트 디버그 로그 출력 / bench 실실행(idle RSS 22.2MB, 타임아웃 panic 경로 실증) / npm 블랙홀 소켓 redial 10s 타임아웃 후 백오프 재시도 / install.js 부분 tarball 정리·.cmd 생성
- 구(舊) 동작을 핀하던 tests/providers.rs 3건은 새 계약에 맞게 갱신(툴 선언 추가 2건, RateLimited downcast 1건) → providers 34/34

### 주요 수정 (High 2건 포함)

1. **[High] mangle 충돌**: 3개 프로바이더 모두 충돌 감지 → `__<sha256[:8]>` 변형명 우회(단일 헬퍼 `mangled_for`/요청 전 스캔), 알려지지 않은 이름의 추측 치환 제거. 충돌 단위테스트 3건 신설.
2. **[High] 채널 Update 드롭**: per-chat 채널 unbounded 전환 — 느린 플랫폼 전송이 더 이상 응답 텍스트를 유실시키지 않고 라우터 전체의 헤드오브라인 블로킹도 제거.
3. 그 외 Medium 29건·Low 36건: audit-findings 각 항목의 "수정 제안"에 따라 수정(상세는 CHANGELOG 0.2.0 "Fixed"의 full-audit remediation 항목).

### 커밋 정책

사용자 요청에 따라 작업 트리에만 반영(39파일, +2897/−488, 사용자 기존 미커밋 변경 보존). 커밋하지 않음.

---

## 2026-09-19 — Full Audit (Re-run, New Findings)

- 대상: 리미디에이션 이후 작업 트리 전체(40파일, +2956/−488) + 전 모듈 재섬. 기존 69건(file+증상 기준) 중복 배제 전제.
- 방식: 조사 16 에이전트(모듈×관점, 최대 체인지 집중) → 적대적 검증 5 에이전트(보고자와 다른 인스턴스) → 오케스트레이터 스팟체크(Critical/High 전수 독회) + 미배정 후보 1건 직접 검증.
- 원시 근거: `audit-scratch-2026-09-19/rerun-*.md`(조사), `verify-*.md`(검증 판정 전문).
- 기존 69건 중 재제기: 중복/잔존으로 폐기(Platforms3 7, BinsOauthCfg 8, RelaySrv 2, NpmCiDeps 2, SecDiff 1 — 조사 단계 다이제스트 대조 폐기).

---

### 1. 결함 목록 (심각도 내림차순)

### [High] CJK 토큰 과소추산(0.75/자)이 85% 컴팩션 마진을 무력화 — 한국어 세션 400 웨지
- 위치: src/runtime.rs:488-492, 551
- 분류: correctness
- 시나리오: text_tokens 재작성이 3바이트 CJK 1자를 0.75 token으로 산출(주석 스스로 실제 ~1.0 인지) → 한국어 위주 세션 est=0.75×실제 → maybe_compact의 `est < window*85/100` 조기 반환 하에 발동점이 실제 1.133×창 → 창 초과 시 upstream 400(내부 경로에 overflow promotion 없음, 재시도 1회 후 턴 실패) → 실패 턴마다 user 행 적립, est가 0.85W 도달(200k 창 기준 실제 +2.6만 token)까지 연속 400 — 수백 턴 웨지, 수동 컴팩션 명령 부재. ASCII/저밀도 토크나이저 세션 무영향. 구 결함(2.25배 과대→조기 컴팩션) 수정의 정반대 방향 신규 증상.
- 근거:
```rust
// runtime.rs:488-492
/// (a 3-byte CJK char ≈ 0.75 token, close to the real ~1 token/char).
fn text_tokens(s: &str) -> u64 { (s.len() / 4) as u64 }
// runtime.rs:551
if est < window * 85 / 100 { return; }
```
- 수정 제안: 비ASCII 바이트 가중으로 3바이트 1자≈1.0 token 근사(예: `((s.len() + non_ascii/3)/4) as u64`).
- 검증: 검증 에이전트가 run_prompt 실패 경로·user 행 적립·수단 부재까지 추적 확정, 오케스트레이터 독회 재확인.

### [High] responses 번역의 문자열 `reasoning` 파스스루가 `_thinking` 병합에서 패닉 — 요청 접속 절단
- 위치: src/provider/responses.rs:201-213 (트리거 api.rs:440-441, config.rs:453-456)
- 분류: correctness(크래시)
- 시나리오: `{"model":"m:low","reasoning":"high"}` POST /v1 — api.rs가 `:low` 접미를 _thinking 주입(공식 컨벤션), 패치 신규 문자열 파스스루(205)가 out["reasoning"]을 JSON 문자열로 → 212행 `out["reasoning"]["effort"]` IndexMut 패닉(serde_json, cargo 실증 재현) → panic=abort·CatchPanic 부재로 핸들러 태스크 언와인드, 응답 없이 접속 절단(데몬 생존). 패치 전 object-only 필터가 문자열 드롭 → auto-vivify로 안전.
- 근거:
```rust
Some(v) if v.is_string() => out[key] = v.clone(),        // 205
if let Some(level) = body["_thinking"].as_str() {
    out["reasoning"]["effort"] = json!(level);            // 212 — 문자열 인덱싱 패닉
}
```
- 수정 제안: effort 병합 전 take() 후 비객체면 {} 치환.
- 검증: 검증 에이전트가 isolated cargo run으로 패닉 실증 + /v1→translate_forward 도달 경로 추적 확정, 오케스트레이터 독회 재확인.

### [High] 클라이언트 4MiB 수신 캡이 데몬 무상한 응답 프레임과 충돌 — 링크 사망 루프, 세션 영구 조회 불가
- 위치: src/client.rs:441-448, 104-131 (대조 rpc.rs:419-441, store.rs:403-432)
- 분류: correctness | contract
- 시나리오: 패치가 클라이언트 수신 4MiB 캡(데몬 인바운드와 동일 예산 의도) — 그러나 데몬 아웃바운드 무상한: session/messages 무페이지가 전체 행을 단일 JSON 프레임으로(쿼리 LIMIT 없음, 컴팩션 cutoff 이후 무상한). ≥4MiB 이력(데몬이 스스로 허용하는 ~3.9MiB 프롬프트+응답, 또는 장기 세션 누적)을 Rust 클라이언트(공개 API) 무페이지 request로 읽으면 Capacity(MessageTooLong) → 펌프 break → 링크 사망 + 동일 연결 진행 호출 전부 동반 실패 → 재전송 3회 동일 사망 후 "connection closed" 오진단. 해당 세션 영구 조회 불가. npm 클라이언트는 수신 캡 없음(무영향).
- 근거:
```rust
// client.rs:445-447
.max_message_size(Some(4 << 20)).max_frame_size(Some(4 << 20))
// store.rs:423 — LIMIT 없는 전체 행
"SELECT data FROM messages WHERE session_id = ?1 AND id > ?2 ORDER BY id"
```
- 수정 제안: 서버 측 응답 프레임 상한(페이지화/절단) 또는 클라이언트 max_message_size 상향.
- 검증: 검증 에이전트가 캡→펌프→drain_pending→재전송 전 경로 재추적(컴팩션 반증 실패), 오케스트레이터가 캡·무페이지 핸들러·쿼리 독회 재확인.

### [Medium] compat가 배열 content system 메시지를 단일-system 엔드포인트에 병합하지 않음 — 400 회귀
- 위치: src/provider/compat.rs:110-118
- 분류: contract
- 시나리오: supports_multiple_system_messages=false(다중 system 거부 전용 플래그) 프로바이더에 연속 system 중 하나라도 배열 content(일반적 SDK 직렬화) → 신규 is_string 가드가 병합 스킵 → system 2개 발송 → 400. 패치 전 평탄화 병합은 전텍스트 배열 한정 성공. 동반 테스트는 배열 보존만 단언.
- 근거:
```rust
mergeable && prev["content"].is_string() && m["content"].is_string()
```
- 수정 제안: 배열이면 text part 추가 병합(inband 패턴) 또는 전텍스트 배열 한정 평탄화.
- 검증: 검증 에이전트 확정(플래그 문서·사전 동작 대조).

### [Medium] mcp shutdown()이 dial 중인 conn 락을 무타임아웃 대기 — exit ~65s/슬롯 스톨 → SIGKILL 시 고아
- 위치: src/mcp.rs:148-156 (대조 171-185, 404-431)
- 분류: concurrency
- 시나리오: ensure_connected가 connect_one 전체(initialize 30s+tools/list 30s) conn 락 보유 — 종료 시 중단 안 되는 config watcher reload가 dial 중이면 SIGTERM 창에 shutdown()의 `slot.conn.lock().await.take()`가 슬롯당 ~60s(+close 5s) 블록, for 직렬 누적 → 2슬롯이면 systemd 90s 초과 → SIGKILL → Drop 예약 킬 미실행 → 고아 stdio 자식(훅이 막으려던 바로 그 결과). 주석의 5s exit bound와 불일치.
- 근거: shutdown()은 close에만 5s 상한, 락 획득은 무상한(원문 agent://McpAudit2·verify-Async.md).
- 수정 제안: 락 획득에도 SHUTDOWN_TIMEOUT 바운드, 만료 시 슬롯 스킵.
- 검증: 검증 에이전트가 watcher 미-abort·프롬프트 dial 제외(cancel 시 future drop)·직렬 누산 추적 확정.

### [Medium] 릴레이 세션 정리가 포화 터널 채널 드레인에 무기한 종속 — 캡 슬롯 고정 → relay 전체 429/503
- 위치: src/bin/damon-relay.rs:453-457, 483-486, 505-507
- 분류: concurrency
- 시나리오: 데몬 생존 상태로 소켓 read만 정지 → 터널 send_task TCP 백프레셔 정체, 데몬 채널(256) 포화 → 클라이언트 세션의 펌프 전송/attach 통지/disconnect 통지 3곳 모두 무타임아웃 send().await → 정리가 드레인 종속 → clients 엔트리+per-IP/전역 슬롯 점유 지속 → 전역 1024 소진 시 신규 connect 429/register 503(데몬 재개·사망까지). 부차: 포화 채널 256×4MiB≈1GiB 큐잉. 동일 파일의 try_send laggard-drop 패턴과 모순.
- 근거: 3곳의 무바운드 `.send(...).await`(원문 verify-Async.md·agent://RelaySrv).
- 수정 제안: disconnect/attach 통지 try_send 또는 짧은 timeout, 펌프 send 바운드.
- 검증: 검증 에이전트 확정(Medium — 데몬 사망 시 즉시 해소). 453-457·505-507은 HEAD 미변경이나 1차 미기록 → 신규 보고.

### [Low] rpc respond()가 채널 폐쇄 SendError를 성공 취급해 stalled 해제
- 위치: src/rpc.rs:246-250
- 분류: error-handling
- 시나리오: stalled=true 후 소켓 종료 → respond()의 tx.send() 즉시 Err(SendError) → `timeout(...).is_err()`는 Elapsed만 → 신규 `else { unstall() }` 실행 → 죽은 연결에 "client resumed reading" 거짓 로그 + "성공 송신 시에만 해제" 문서 불변식 위반. 관측 기능 영향 없음(폐쇄 채널 notify는 stalled 조기반환과 동일 무음 드롭).
- 근거:
```rust
if tokio::time::timeout(SEND_TIMEOUT, self.tx.send(..)).await.is_err() { warn!(...) }
else { self.unstall(); }   // Ok(Err(SendError))도 여기
```
- 수정 제안: Ok(Ok(()))일 때만 unstall.
- 검증: 검증 에이전트 확정(기능 영향 0 — Low).

### [Low] npm ticketedUrl 재작성이 /ws 앞 경로 프리픽스 폐기 — 프리픽스 프록시 배포에서 티켓 404 루프
- 위치: npm/client.mjs:354-361 (대조 src/client.rs:411-416)
- 분류: contract
- 시나리오: 패치가 정규식 surgery를 URL 파싱으로 교체하며 티켓 POST를 `base.origin` 기준 발송 → `ws://host/damon/ws` 프리픽스 배포에서 /damon 폐기, `origin/v1/ws_ticket` POST → 404 → "ws_ticket rejected: 404"가 redial마다 반복. 구 정규식은 /ws 접미만 제거해 프리픽스 보존했고 Rust 클라이언트도 strip_suffix("/ws")로 보존 — 비대칭이 회귀임을 확증.
- 근거:
```js
const resp = await fetchFn(`${base.origin}/v1/ws_ticket`, { ... });  // 프리픽스 소실
```
- 수정 제안: origin+(/ws 앞 prefix)+/v1/ws_ticket 조립.
- 검증: 오케스트레이터 직접 검증(구/신 diff, Rust 대응 구현, 프록시 경로 재구성; '프리픽스 미지원 설계' 반증은 구 코드·Rust 양측 보존으로 기각).

### [Low] npm 핸드셰이크 타임아웃이 CONNECTING 소켓을 닫지 않고 유기
- 위치: npm/client.mjs:147-156
- 분류: leak
- 시나리오: 신규 10s 게이트 reject 시 finally는 clearTimeout뿐, ws.close() 부재 → 진행 중 소켓이 undici·OS 타임아웃까지 핸들 점유, 느린 open 성공 시 유휴 open 소켓 영구 방치. 블랙홀 반복 시 누적(유한).
- 근거: `} finally { clearTimeout(timer); } this.#attach(ws);` — close 부재.
- 수정 제안: 타임아웃 경로에서 ws.close().
- 검증: 검증 에이전트 확정(패치 신규 경로 — connect()측 동일 패턴은 HEAD 코드, 범위 외 참고).

### [Low] npm close()가 #connected를 settle하지 않음 — 재접속 창 이후 호출 10s 대기+오진단 에러
- 위치: npm/client.mjs:329-339, 190-197, 110-117
- 분류: error-handling
- 시나리오: 링크 단절→재접속 창(#connected pending)에서 close() → 이후 호출(#call/respond/cancel/prompt)이 settle되지 않는 #connected와 10s 타이머 race → "timed out waiting for reconnect"(기대: 즉시 "connection closed"). 패치 전 동일 상황은 #ws.close() null 역참조 TypeError 즉시 크래시 — 재작성된 close 본체가 만든 새 경로.
- 근거: close() 본체에 #connected settle 부재, #ready의 #closed 선행 확인 부재.
- 수정 제안: #ready 진입 시 #closed 즉시 throw 또는 close()에서 #connected settle.
- 검증: 검증 에이전트 확정(재접속 창 한정 — Low).

### [Low] npm #ready()가 호출마다 미정리 10s 타이머 유출 — CLI 자연 종료 최대 10s 지연
- 위치: npm/client.mjs:190-197, 134
- 분류: leak
- 시나리오: Promise.race 패자 타이머를 clearTimeout/unref 없이 폐기 — 정상 호출마다 10s 생존 타이머 잔존, 미-unref 타이머가 이벤트루프를 붙잡아 마지막 호출 후 프로세스 자연 종료가 타이머 만료까지 지연. redial 백오프 슬립(134)도 close 후 최대 5s 생존.
- 근거:
```js
await Promise.race([ this.#connected,
  new Promise((_, rej) => setTimeout(() => rej(...), 10000)) ]);
```
- 수정 제안: 패자 타이머 clearTimeout 또는 unref().
- 검증: 메커니즘은 검증 에이전트도 반증 실패 — 판정 불일치(HEAD 사전 존재)만 존재 → 오케스트레이터가 탬플릿의 신규 정의(미기록)에 따라 Low 승인.

### [Low] Windows 릴리스 타르볼이 .pdb 디버그 심볼 포함 — 설치 바이트 ~35% 낭비
- 위치: .github/workflows/ci.yml:87-91
- 분류: config | performance
- 시나리오: `tar czf --exclude='*.d' damond* damon*`가 msvc 링커 산출 PDB를 수납 — 공개 v0.1.0 아티팩트 실증: PDB 6개 34.9MB(tarball의 ~35%, 압축 오버헤드 ~10MB)를 Windows npm 설치마다 다운로드·추출(install.js 멤버 필터 없음). 기능 파손 없음. 패치 이전 단계이나 1차 미기록.
- 근거: tar 글롭 + v0.1.0 아티팩트 관측(damon.pdb, damond.pdb, damon_{telegram,discord,slack,relay}.pdb).
- 수정 제안: --exclude='*.pdb' 추가.
- 검증: 검증 에이전트가 공개 아티팩트로 실증 확정.

---

### 2. 커버리지 표 (모듈 → 조사 → 검증 → 상태)

| 범위 | 조사 | 검증 | 상태 |
|---|---|---|---|
| src/provider/openai.rs (+379/−56) | ProvOpenai | — (후보 0건) | 완료 |
| src/provider/anthropic.rs + gemini.rs (+387) | ProvAnthGem | — (후보 0건) | 완료 |
| provider/{responses,mod,inband,compat,discovery}.rs + llm.rs | ProvRest | VerifyHigh | 완료 |
| src/api.rs (+54/−31) | ApiAudit | — (후보 0건) | 완료 |
| src/rpc.rs (+67/−33) | RpcAudit | VerifyAsync | 완료 |
| src/runtime.rs (+28/−22) | RuntimeAudit | VerifyHigh | 완료 |
| src/store.rs (+146/−3) | StoreAudit2 | VerifyData | 완료 |
| src/client.rs + src/relay.rs | ClientRelay | VerifyClient | 완료 |
| src/channel.rs + src/service.rs | ChanService | VerifyData | 완료 |
| src/{slack,telegram,discord}.rs (+251/−26) | Platforms3 | — (후보 0건) | 완료 |
| src/mcp.rs (+70/−31) | McpAudit2 | VerifyAsync | 완료 |
| src/bin/{damond,damon,플랫폼 3종} + config.rs + oauth.rs | BinsOauthCfg | — (후보 0건) | 완료 |
| src/bin/damon-relay.rs (+113/−36) | RelaySrv | VerifyAsync | 완료 |
| npm/* + .github/ci.yml + scripts + rust-toolchain | NpmCiDeps | VerifyClient + VerifyCi | 완료 |
| tests/ 신규·변경 전체(+424) + examples/bench.rs | TestsNew | — (후보 0건) | 완료 |
| 미커밋 diff 전체 보안 패스 | SecDiff | 오케스트레이터(회수 후보 2건 직접 검증) | 완료 |

High 3건 스팟체크: 오케스트레이터가 해당 라인 전부 직접 독회(runtime.rs:488-492·551, responses.rs:198-213·api.rs:440-441·config.rs:453-456, client.rs:441-448·rpc.rs:419-441·store.rs:403-432). Critical 0건.

### 3. 검증 후 결함 없음 목록 (차기 감사 중복 배제 재료)

- **리미디에이션 회귀 미발견**: openai mangled_for 충돌우회(originals 선스캔·재해시 루프·restore 정합, 스코프 테스트 3/3), anthropic/gemini Retry-After 스레딩 4 호출처·mid-stream RateLimited 승격(부분출력 영속 확인)·stop 랩·blockReason 단일 보고, inband 배열 보존·이벤트 순서, api overflow 시그니처 축소(실제 메시지 커버 유지)·collect_stream 64MiB 캡·에러 노출(format!({e})는 bytes_stream decode 에러 URL 미부착 — 벤더 소스 확인), rpc live_prompts 락 재구성(FK 백스톱 단일 연결+PRAGMA로 종단 검증)·float id, runtime cancelled status/content 분기 일치(소비점 전수 임의문자열 처리)·컴팩션 플래그 제거(요약 선행 주입으로 빈배열 400 불가), channel unbounded 전환(메모리 유계 논증)·TurnGuard·이중 프롬프트, store fts5 재작성(추상 트레이스상 구문오류 경로 부재)·퍼미션(WAL 사이드카 포함 실측 0600 — 하단 폐기 참조)·retention sweep, client drain 잠금/전송 분리·relay dup client_id(task::Id 가드)·E2E 불변, telegram offset 지연확정(마커 순서·재전달 상한, 테스트 3종)·discord 백오프(배가/300s 캡/READY 리셋)·slack FIFO 축출, mcp TOCTOU 단일 write 섹션(await 부재)·재시도 축소(rmcp 3.3.0 대조)·close 5s, config glob ? 스칼라 매칭(continuation 스킵·백트래킹 착지 전수 검증)·dotenvy 선행·mcp.shutdown 연결, oauth 상태코드 보존 bail·debug 훅 게이팅, bins EPIPE exit(141)·토큰 value_source 경고, relay 서버 세대 정리(same_channel 자기 세대 한정)·insert/spawn 락 커버·over_capacity 에러 프레임 fail-closed, npm redial 10s 게이트·install.js .cmd·부분 tarball 정리, CI SHA 핀 실재.
- **신규 테스트(+32) 거짓통과 없음**: 12종 개별 실행 통과 + 각 테스트가 재도입 시 실패함을 경로 추적으로 판별(cancel 0.09s 통과=5s sleep 선점, summarize 카운터 1≠2 등).
- **보안 diff 전량 클린**: 시크릿 노출·인가 fail-open·주입·암호 엔트로피·퍼미션/TOCTOU 5관점(gemini 키 헤더 이동, mcp reload 잠금 순서 ABBA 부재, oauth 훅 릴리스 컴파일 아웃 포함).
- **검증 폐기 4건**: (1) store WAL 사이드카 퍼미션 — 실제 코드 순서(퍼미션 선조임 → WAL pragma)에서 번들 SQLite(libsqlite3-sys 0.38.2 vendored)가 DB 퍼미션을 상속해 사이드카 생성, damond 순서 실측 0600 — 보고자 재현은 chmod 순서 오류; (2) channel 24h 에비션 고아/연속성 — DB 적립은 구 finding 'chat_sessions 무한 증가'와 동일 증상(중복), 연속성 리셋은 동일 finding의 승인 수정 처방 구현+기존 stale-retry UX·브리지 재시작 동작과 일관; (3) CI audit 잡 permissions — 퍼블릭 리포 contents:none 체크아웃 실동작(상류 rustsec/cargo-audit이 동일 패턴 그린, audit-check README 권장과 일치), 실패 시나리오는 비공개 전환 가정뿐; (4) npm redial ticket 페치 무타임아웃 — HEAD 사전 존재+기록된 'redial open 무타임아웃'과 같은 파일·증상 계열(중복), undici 기본 한계로 무한행 아님.

### 4. 미조사 영역과 이유

- 없음(1차와 동일 EXCLUDES: docs/, lock 파일, target/, node_modules, 벤더).
- npm Windows cmd-shim 실동작, Slack idle-ping 정확 주기: 감사 환경(darwin) 실기 불가 — 1차와 동일 사유로 정적 판정 한정.

### 5. 통계

| 구분 | 수 |
|---|---|
| 승인 발견 — Critical | 0 |
| 승인 발견 — High | 3 |
| 승인 발견 — Medium | 3 |
| 승인 발견 — Low | 6 |
| **승인 발견 합계** | **12** |
| 검증 폐기 | 4 (상세 §3) |
| 병합 | 0 |
| 기존 중복 폐기(조사 단계) | 20건 (Platforms3 7, BinsOauthCfg 8, RelaySrv 2, NpmCiDeps 2, SecDiff 1) |
| 후보 총계 | 16(조사 15 + 오케스트레이터 회수 1) → 승인 12 |

특성: 승인 12건 중 10건이 리미디에이션 패치가 유발(수정 코드 회귀), 2건(.pdb, #ready 타이머)은 사전 존재·1차 미기록. High 3건 전부 수정 코드의 반대편 과잉/비대칭(과소추산, 문자열 파스스루, 수신 캡) — "수정이 만든 새 표면" 편중 확인.

MODEL_RULE: 서브에이전트 전원(조사 16 + 검증 5)이 스폰 기본값으로 세션 모델(zai/glm-5.3-flash) 상속. 타 provider/model 대체 없음(세션 도구에 모델 선택 수단 부재 — swe-2 등 지정 요청은 하네스 차원에서 불가하여 룰의 '대체 금지' 준수로 처리). 읽기 전용 준수 — 감사 대상 파일 생성/수정/삭제 없음(스크래치·본 섹션만 추가).

---

## 2026-09-19 — Remediation (re-run, 12 defects fixed)

재감사 승인 12건을 파일 소유권 기준 8개 에이전트가 최소 수정 + 회귀테스트로 처리, 오케스트레이터가 통합 검증. 커밋하지 않음(작업 트리에만 반영 — 기존 정책 준수).

### 결과 요약

| 처분 | 수 |
|---|---|
| FIXED (수정 + 검증) | 12 (High 3 · Medium 3 · Low 6) |
| DEFERRED (외부 인프라 전제, 1차부터) | 1 (npm TOFU 체크섬) |
| NO-FIX (문서화, 1차부터) | 1 (api u64 정밀도) |

### 수정 내역 (발견 순서대로)

1. **[High] runtime CJK 과소추산** — text_tokens를 비ASCII 바이트 가중(`((len + non_ascii/3)/4)`)으로 3바이트 1자≈1.0 token. 회귀테스트 `compaction_triggers_on_cjk_history`(tests/runtime.rs)는 구 공식에서 FAILED, 신 공식 통과를 교차 검증. ASCII 산술 불변 확인.
2. **[High] responses reasoning 패닉** — effort 병합을 take()+비객체 vivify로 방어(사전-패치 의미론 복원). 회귀테스트가 수정 전 실제 패닉("cannot access key \"effort\" in JSON string") 재현 확인 후 통과.
3. **[High] 클라이언트 4MiB 캡 vs 무상한 응답** — 서버 측 상한: rpc.rs에 `MAX_RESPONSE_BYTES`(4MiB, 인바운드 max_frame_size와 동일)+`frame_capped_response` — session/messages·session/list 초과 응답을 code -32602 페이징 힌트 에러로 대체(링크 생존). 회귀테스트가 5MiB 세션에서 에러 응답+직후 정상 요구(링크 생존)를 종단간 단언. 클라이언트 캡은 유지(릴레이도 4MiB 프레임 캡이라 클라이언트 캡 상향만으론 불충분 — 판단 근거 보고에 명시).
4. **[Medium] compat 배열 system 미병합** — 문자열+문자열 빠른 경로 유지, 배열/혼합 쌍은 content_parts 정규화 후 파트 이어붙임(무손실, 단일 메시지). 결함을 고정하던 구 테스트를 회귀테스트로 교체(수정 전 FAILED 교차 검증).
5. **[Medium] mcp shutdown 락 무한 대기** — 락 획득에 SHUTDOWN_TIMEOUT 바운드, 만료 시 warn+스킵(dialer 자체 완료 경로 주석 명시). 회귀테스트가 수정 전 15s 블록 재현 → 수정 후 5.01s 완료.
6. **[Medium] relay 포화 채널에 정리 결합** — 펌프 daemon 전송 10s 바운드(DAEMON_SEND_TIMEOUT, 만료 시 세션 종료=laggard-drop 대칭), attach/disconnect 통지 try_send 전환(Full 포기, Closed 기존 해체 유지). 바이너리 내부 로직이라 테스트 자리 없음 — 기존 relay 5종 통과로 대체(계약 허용).
7. **[Low] rpc unstall** — timeout 전개를 Ok(Ok(()))/Ok(Err(_))/Err(Elapsed) 3분기로 분리해 폐쇄 채널에서 stalled를 해제하지 않게 수정. 소유자 누락으로 오케스트레이터가 직접 수정(정정 아래).

정정: 7번(rpc respond unstall, Low)은 리미디에이션 배치에서 소유자가 없어 누락됐었다 — 오케스트레이터가 통합 검증 단계에서 발견해 직접 수정 완료(rpc.rs:244-252, Ok(Ok(()))일 때만 unstall). 수정 후 전체 게이트 재실행 통과.

8. **[Low] npm ticketedUrl 프리픽스** — `${origin}${pathname.slice(0,-3)}/v1/ws_ticket`로 /ws 앞 프리픽스 보존(Rust client.rs:411-416 동일 의미론).
9. **[Low] npm 핸드셰이크 타임아웃 소켓 유기** — race finally에서 clearTimeout+CONNECTING/CLOSING 소켓 close.
10. **[Low] npm close 후 10s 행** — #ready 진입 시 #closed 즉시 throw.
11. **[Low] npm 타이머 유출** — #ready 패자 타이머 finally clearTimeout, redial 백오프 슬립 unref.
12. **[Low] Windows tarball .pdb** — `--exclude='*.pdb'` 추가(재현 스크립트로 PDB 6→0, exe 6 유지 확인).

### 통합 검증 (오케스트레이터)

- `cargo fmt` 적용(이전 리미디에이션 잔여 포맷 2파일 포함) → `cargo fmt --check` 게이트 정합.
- `cargo clippy --all-targets -- -D warnings`: 클린(CI 게이트 동일).
- `cargo test` 전체 스위트(최종 수정 후 재실행): **152 passed / 0 failed** (lib 41, api 10, boot 1, channels 13, client 3, oauth 4, phase4 4, providers 34, relay 5, rpc 7, runtime 14, service 10, store 6).
- `node --check npm/client.mjs`: 통과.
- 변경 헝크 스팟 독회: runtime.rs:492-495, responses.rs:214-221, compat.rs:112-142, rpc.rs:734-784, npm #ready/ticketedUrl, ci.yml:88 직접 확인.
- rpc.rs unstall 수정 헝크(244-252) 독회 포함.

MODEL_RULE: 수정 에이전트 전원(8) 세션 모델 상속, 대체 없음. 이번 단계는 수정 작업으로 감사 대상 파일 변경이 목적(사용자 지시) — 이전 감사의 읽기 전용 룰은 감사 단계에만 적용.
