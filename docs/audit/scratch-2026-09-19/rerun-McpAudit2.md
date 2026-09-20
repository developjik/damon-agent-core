# mcp.rs 재감사 (McpAudit2, 2026-09-19) — 리미디에이션 패치 신규 결함만

대상: `src/mcp.rs` (434줄, +70/−31, 미커밋). 중복 배제 다이제스트 사전 로드 완료.
기존 mcp.rs 4건(툴캐시 TOCTOU / 자동재시도 이중실행 / 셧다운 고아[Low] / 동시 dial 중복 스폰[Low])과 동일 증상 재보고 없음.

## 결함 후보: 1건

### [Medium] shutdown()이 dial 중인 슬롯의 conn 락을 무한 대기 — 데몬 exit 최대 ~65s/서버 스톨
- 위치: src/mcp.rs:143-156 (shutdown 루프의 `slot.conn.lock().await.take()`), 상호작용 src/mcp.rs:171-185 (ensure_connected가 conn 가드를 dial 전체에 걸쳐 홀드)
- 분류: concurrency
- 시나리오: 설정 watcher(damond.rs:140-147, 종료 시 중단 안 됨)가 reload 중 새 서버를 추가·변경하면 ensure_connected가 `connect_one` 전체(초기화 30s + tools/list 30s 순차 타임아웃, src/mcp.rs:404-431) 동안 conn 뮤텍스를 보유한다. 이 때 SIGTERM → `state.mcp.shutdown()`(damond.rs:222)이 `slot.conn.lock().await`를 **타임아웃 없이** 대기하여 슬롯당 최대 ~60s(+close 5s) 스톨. 슬롯별 직렬 처리라 병리적 서버 2개면 systemd 기본 TimeoutStopSec 90s 초과 → SIGKILL → ChildWithCleanup::drop의 `tokio::spawn(kill)`(rmcp child_process.rs:46-56)이 런타임 해체로 실행 안 됨 → 고아 stdio 자식 — 이 훅이 막으려던 정확한 결과. 함수 자체 주석("A child that ignores shutdown must not hang daemon exit", src/mcp.rs:133)은 close에만 5s 상한을 걸고 락 획득은 무상한이다.
- 근거:
```rust
for (name, slot) in slots {
    // take() empties the slot: a call racing shutdown fails fast ...
    let Some(mut svc) = slot.conn.lock().await.take() else {   // ← 무타임아웃 대기
        continue;
    };
    if let Err(e) = svc.close_with_timeout(SHUTDOWN_TIMEOUT).await { ... }
}
// ―― 그리고 락 홀더:
let mut conn = slot.conn.lock().await;          // mcp.rs:171
...
let (svc, tools) = match connect_one(name, &cfg).await {  // mcp.rs:185 — 최대 ~60s 홀드
```
- 수정 제안: 락 획득에도 SHUTDOWN_TIMEOUT을 적용해 바이패스한다. 예: `match tokio::time::timeout(SHUTDOWN_TIMEOUT, slot.conn.lock()).await { Ok(guard) => guard.take(), Err(_) => { warn!(server=%name, "shutdown skipped: dial in flight"); continue; } }` — 건너뛴 슬롯은 기존 Drop 경로로 처리된다(현재보다 나쁠 것이 없다).
- 자체반증:
  1. "프롬프트 취소된 call이 락을 계속 잡는다" → 아님. runtime.rs:740-741의 `tokio::select!`에서 cancel이 이기면 `mcp.call` future가 drop되어 conn 가드 해제. HTTP 기반 dial은 axum graceful shutdown이 핸들러 종료를 기다린 뒤 shutdown()이 실행되므로 대부분 완료됨.
  2. "backoff가 긴 홀드를 막는다" → backoff는 시도 허용 여부만 결정, 홀드 길이와 무관. 창 경과 후 첫 시도는 full 60s 홀드 가능.
  3. "락 대기가 무한일 수 있는가" → connect_one의 두 timeout(각 30s)이 상한. 슬롯당 ~60s+5s로 유계이지만 직렬 누적 시 exit 상한 의도(5s)를 크게 벗어남.
  4. "reload가 shutdown과 동시에 도는가" → damond.rs:140 watcher 태스크는 중단·플래그 없이 프로세스 종료까지 alive. config 변경+재시작 자동화가 정확히 이 창을 만든다.

## 커버리지 (실제로 읽은 범위)
- src/mcp.rs 1-434 전문 (구조 요약 + 30-434 라인 정독)
- `git diff -- src/mcp.rs` 전체 (+70/−31) 및 `git show HEAD:src/mcp.rs` 120-230 (변경 전 대조)
- rmcp 3.3.0 소스(레지스트리): service.rs ServiceError enum(78-96), await_response/oneshot-drop→TransportClosed 매핑(549-571, 829-843), worker의 TransportSend 발생지(1408-1462), close/close_with_timeout/Drop(1099-1188), transport/child_process.rs(1-60, 168-321: ChildWithCleanup drop 시 tokio::spawn 킬)
- 소비자/호출자: src/bin/damond.rs 138-228 (watcher spawn, graceful shutdown, mcp.shutdown 호출 순서), src/runtime.rs 695-778 (execute_tool, 권한/승인, select! 취소)
- 회귀테스트 diff: tests/phase4.rs, tests/mcp_server.py, tests/runtime.rs (mcp 관련 신규: sleep 툴, BrokenPipe 처리, cancel_reaches_inflight_tool_and_persists_cancelled)
- Cargo.toml rmcp 버전 확인(3.3.0)

## 초점별 결론
1. **TOCTOU 수정**: 재확인(`Arc::ptr_eq`) 존재, still-check + tools 교체 + conn 설치가 단일 `inner.write()` 크리티컬 섹션, 내부 await 없음 확인. 락 순서(conn→inner) 역전 경로 없음(reload/shutdown 모드 inner 가드를 conn 획득 전 해제). 결함 없음.
2. **자동재시도 제거**: rmcp 소스 대조로 TransportSend=전달 실패(재시도 안전), TransportClosed/UnexpectedResponse/McpError=서버 도달 가능(재시도 금지) 분류 정확. TransportClosed 시 conn=None으로 다음 호출 재스폰 경로 유지, 호출자 에러 전파 정상. 회귀 없음.
3. **shutdown/close 훅**: 이중 close 불가(take() 단일 소유 + close_with_timeout이 handle take, 재호출 시 Ok(Closed)). close_with_timeout이 타임아웃 자체 로그(rmcp 1137-1151 확인) — 주석 정확. 유일한 결함 = 위 1건(락 획득 무상한). watcher reload가 snapshot 이후 스폰한 자식의 잔여 고아 창은 기존 "셧다운 고아" 증상과 동일→중복 배제.
4. **in-flight dial 가드/스폰 정리**: conn 락 dial 전체 홀드로 동일 슬롯 단일 dial 보장(구 중복 스폰 수정 확인). `!still` 경로 svc drop → ChildWithCleanup 비동기 킬로 정리. 슬롯 교체(제거+재추가) 시 신·구 슬롯 상호 직렬화는 안 되나 구조상 원래 그랬음(구현 결함 아님, 아래 반증 목록 4번 참조).
5. **세션 승인 무효화**: reload changed-path의 prefix 기반 승인 소거는 패치 미수정. 신규 코드 경로(shutdown/ensure_connected/call 변경)는 session_approvals 미접촉. TransportClosed 재스폰(동일 설정) 시 승인 유지는 의도된 세미너. 회귀 없음.

## 조사했으나 반증/제외한 후보
1. **watcher reload가 shutdown snapshot 이후 자식 스폰 → exit 시 고아**: 기존 "셧다운 고아(Drop 의존)[Low]"와 같은 파일+같은 증상(종료 시 고아) → 다이제스트 중복 배제. (완화 여지는 있으나 신규 증상 아님)
2. **동시 TransportClosed 핸들러가 신규 설치된 건강한 conn을 blindly None 처리**: `*slot.conn.lock().await = None` 무검사 클리어는 구 코드(재시도 경로)에도 동일 존재 → patch-introduced 아님.
3. **설정 변경이 dial 중 끼어들어 구 설정 서비스의 툴이 일시 부활**: reload changed-path이 dial 종료 후 conn=None로 정리, 비경쟁 경로도 다음 호출 전까지 구 툴 광고(기존 설계) → 신규/악화 아님.
4. **서버 제거→동일 이름 재추가 시 구 dial 실패가 신 슬롯 툴을 purge + `conn.is_some()` 단락으로 툴 미복구**: purge 블록·단락 반환 모두 patch 이전 코드 → 제외.
5. **UnexpectedResponse/McpError에서 conn 미클리어**: rmcp 의미상 서버 생존 또는 응답 수신(재시도 금지 분류와 일관) → 의도적 설계.
6. **`Err(_)` 300s 타임아웃 브랜치의 무검사 클리어 레이스**: diff 미수정(기존 코드) → 제외.
7. **ensure_connected future가 dial 중 drop되면 백오프 카운터 미증가**: 기존 구조와 동일(b.1=None 리셋 후 시도) → 제외.
8. **phase4/runtime 신규 테스트 거짓통과 검토**: echo payload 강화 assertion은 실제 왕복 검증, cancel 테스트는 실 MCP 자식 사용 — 거짓통과 없음.
