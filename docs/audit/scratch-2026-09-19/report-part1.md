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
