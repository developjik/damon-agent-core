# rpc.rs 재감사 (RpcAudit, 2026-09-19) — 리미디에이션 회귀 중심

대상: src/rpc.rs 749줄 전수 (+67/−33 diff 전체 대조). 읽기 전용.
결론: **신규 결함 1건(Low)**. 나머지 수정 경로(stalled 리셋, live_prompts 락 스코프, FK 백스톱, float-id 파싱)은 정합성 검증 완료.

## 결함 후보

### [Low] respond()가 닫힌 채널 전송 실패를 "성공"으로 간주해 stalled 플래그를 해제
- 위치: src/rpc.rs:239-249
- 분류: correctness
- 시나리오: 클라이언트 정지(notify 전송 타임아웃 → stalled=true) 후 소켓이 끊어짐 → rx_out 펌프 종료로 수신자 drop → 실행 중인 프롬프트 태스크의 마지막 `client.respond(id, ...)`에서 `tx.send()`가 즉시 `Err(SendError)` 반환 → `tokio::time::timeout` 래퍼는 `Ok(Err(..))`를 반환(Elapsed가 아니므로 `.is_err()`==false) → 신규 `else { self.unstall(); }` 분기 실행 → 연결이 죽었는데도 "client resumed reading; notifications re-enabled" 로그가 "ws client disconnected" 이후에 기록되고, "cleared by the next **successful** send"라는 필드 자체의 문서화된 불변식이 깨짐.
- 근거:
  ```rust
  if tokio::time::timeout(SEND_TIMEOUT, self.tx.send(msg.to_string()))
      .await
      .is_err()
  {
      warn!("dropping response to stalled client");
  } else {
      self.unstall();
  }
  ```
  `timeout`의 반환형은 `Result<Result<(), SendError>, Elapsed>` — `.is_err()`는 Elapsed(정지)만 잡고, 채널 폐쇄(`Ok(Err)`)는 else로 빠진다.
- 수정 제안: `match`로 전개해 `Ok(Ok(()))`일 때만 unstall하고 `Ok(Err(_))`(채널 폐쇄)은 별도 처리(무시 또는 debug 로그)한다.
- 자체반증: 기능적 파급을 찾았다 — unstall 후 notify는 폐쇄 채널에서 `Ok(_) => {}`로 조용히 드롭되어 stalled 유지 시의 early-return과 관측 동작이 동일, request도 `Ok(Err)`에서 bail 그대로. 즉 기능 회귀 없음, 영향은 오해 유도 로그 + 플래그 불변식 위반에 한정 → Low 강등 유지.

## 검증 완료(결함 없음)

1. **stalled 리셋 경로**: unstall은 request()/respond() 성공 송신 시만. request 송신 성공은 버퍼(64)에 여유 있다는 뜻이고, 그 여유는 펌프가 실제로 drain 했을 때만 생김 → 회복 근거 타당. 플래핑 클라이언트는 재정지 시 10s 1회 재지불(자기제한 휴리스틱). notify 성공 시는 unstall 안 하는 것은 문서화된 설계(다음 request/respond가 회복 트리거). `Ordering::Relaxed`는 힌트 전용 플래그에 무결서 문제 없음(swap으로 로그 1회 보장).
2. **live_prompts 락 스코프 변경(TOCTOU)**: 패치가 exists-check를 락 밖으로 옮기며 생긴 잔여 창(삭제 커밋 후 프롬프트 등록)은 주석대로 FK 백스톱으로 봉쇄됨 — 검증: (a) Store는 단일 `tokio_rusqlite::Connection`(풀 아님), open 시 `PRAGMA foreign_keys = ON`이 해당 연결에 영구 적용, (b) `messages.session_id REFERENCES sessions(id)` 스키마 확인, (c) run_prompt 첫 연산인 user append가 `?` 전파(store.rs append는 평범한 INSERT, 오류 무시 없음), (d) delete_session은 messages_fts→messages→sessions 트랜잭션 삭제. 삭제가 user append 이후 커밋되는 서브윈도우에서도 다음 assistant append(`?`)에서 턴이 에러 종료 — 최악은 provider 1회 낭비, 무손실. delete의 busy-poll(100ms×10s)+재확인-under-lock+행삭제 전 락 해제 구조 정합. 프롬프트 insert-if-absent, conn_id 일치 시에만 제거, disconnect는 자기 소유 토큰만 cancel — 모두 유지.
3. **permission timeout 공유**: handle_socket의 req_timeout 계산(연결 시작 시 1회 스냅샷, config+30s)은 이번 diff 미포함(신규 아님). runtime 외곽 select는 live config 사용 — 방향성(하향 reload) 안전, 상향 reload 조기 bail은 선재 동작이라 패치 회귀 아님.
4. **티켓/origin 인증**: ws_handler·is_localhost_origin은 diff 미변경. 신규 float-id 파싱(`"id":5.0`)은 NaN/음수/fract/2^64 경계 가드가 정확하고 서버 발급 id는 작은 값이라 오매칭 없음.
5. **notify/request 경로**: PendingGuard 이중 remove 무해, next_id 단조 증가로 id 재사용 없음, 폐쇄 채널의 notify `Ok(_) => {}` 조용 드롭은 선재 동작.

## 커버리지
- src/rpc.rs 1-749 전수 정독(1-263, 259-523, 524-749) + 전체 diff 대조.
- 교차 검증: src/store.rs:29-153(open/pragma/스키마), 221-241(append), 542-583(session_exists/delete_session); src/runtime.rs:26-55(PERMISSION_TIMEOUT/ClientChannel::request_timeout), 57-153(run_prompt 진입·user append), 295/309/332(append 전파 지점), 700-712(permission ask); grep: foreign_keys/REFERENCES/ON DELETE 전체, request_timeout/permission_timeout_secs 전체.

## 반증 폐기 후보
- "FK 백스톱 무효(PRAGMA per-connection/풀)" → 단일 연결 + open 시 영구 적용으로 반증.
- "턴 중간 삭제가 빈 히스토리 LLM 호출/무손실 위반" → live 진입이 턴 종료까지 삭제를 봉쇄(busy-poll+거부); 서브윈도우도 다음 append `?` 전파로 에러 종료 — 반증.
- "unstall이 여전히 정지된 클라이언트 재개" → 버퍼 여유=펌프 진행의 간접 증거; 최악 10s 1회. 설계적 휴리스틱 — 반증.
- "Relaxed ordering 레이스" → 힌트 플래그, 정합성 의존 없음 — 반증.
- "req_timeout 스냅샷 vs config reload" → 이번 패치 미포함 선제 코드 — 제외.
- "float-id 파싱 오매칭" → 가드 산술로 반증.
