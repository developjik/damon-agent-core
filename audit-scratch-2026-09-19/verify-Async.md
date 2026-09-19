# verify-Async (Wave1 Phase2 적대적 검증, 2026-09-19) — 동시성/정리 후보 3건

기준: rerun-existing-digest.md 중복 배제 확인 완료. 읽기 전용 검증(+스크래치 기록).
git 대조: `git show HEAD:src/rpc.rs`(respond에 else 분기 없음 = 패치 신규), `git show HEAD:src/mcp.rs`(shutdown 부재 = 패치 신규), `git diff HEAD -- src/bin/damon-relay.rs`(펌프 루프 패치에서 재작성).

## 1. [RpcAudit] src/rpc.rs:239-249 — respond() 폐쇄 채널 전송실패를 성공으로 오인해 unstall
### 판정: CONFIRM — 최종 심각도 Low
- 재검증: `timeout(SEND_TIMEOUT, self.tx.send(..))` 반환형은 `Result<Result<(),SendError>,Elapsed>` — `.is_err()`는 Elapsed만 true. 수신자 drop(펌프 종료) 시 send는 즉시 `Err(SendError)` → `Ok(Err(..))` → else → `unstall()`. `git show HEAD`로 else 분기가 패치 신규임 확인(구 코드는 warn만).
- 시나리오 재구성: 성공. (1) notify() 전송 타임아웃 → stalled=true(rpc.rs:176-186). (2) 클라이언트 단절 → 아웃바운드 펌프 종료 → rx drop → tx.send 즉시 Err. (3) 디스패치 루프가 이미 읽은 요청 처리 후 respond() 호출(session/list·messages·search 등 동기 경로 rpc.rs:380-523 다수, 또는 프롬프트 완료) → else → unstall → 죽은 연결에 "client resumed reading; notifications re-enabled" 허위 info 로그 + "cleared by the next **successful** send" 문서 불변식 위반.
- 반증 시도: (a) 기능 파급 — unstall 후 notify는 폐쇄 채널에서 `Ok(_) => {}` 즉시 조용 드롭(폐쇄 채널 send는 즉시 반환, 10s 비용 없음) = stalled 유지 시 early-return과 관측 동일. request()는 `Ok(Err)`에서 bail 그대로. 행동·성능 차이 없음. (b) 단절 후 respond 도달성 — 동기 디스패치 경로 + spawn된 wind-down 태스크가 단절 이후 respond 호출. (c) 중복 — 기존 rpc.rs 증상(lock-across-await, stalled sticky)과 상이, 리셋 코드 자체의 신규 결함.
- 결론: 기능 회귀 없음, 영향 = 오해 유도 로그 + 플래그 불변식. 보고자 자체 강등(Low) 타당. 수정: `match` 전개로 `Ok(Ok(()))`만 unstall.

## 2. [McpAudit2] src/mcp.rs:143-156 — shutdown()의 무타임아웃 conn 락 대기
### 판정: CONFIRM — 최종 심각도 Medium
- 재검증: (a) shutdown 루프 `slot.conn.lock().await.take()` 무타임아웃, 슬롯 직렬 for 루프(mcp.rs:147-156). SHUTDOWN_TIMEOUT(5s)은 close_with_timeout에만 적용. (b) ensure_connected가 conn 가드를 connect_one 전체에 홀드(mcp.rs:169-230) — connect_one은 순차 30s×2(serve 초기화 30s + tools/list 30s, mcp.rs:404-431) → 최대 ~60s 홀드. (c) `git show HEAD:src/mcp.rs`에 shutdown 없음 → 패치 신규 코드의 결함. (d) watcher: damond.rs:138-147 spawn 후 abort 핸들 없음, 종료 시퀀스(graceful → live_prompts cancel → 2s sleep → mcp.shutdown(), damond.rs:222) 내내 생존 → reload가 새 서버 추가 시 ensure_connected 즉시 dial(None-arm, mcp.rs:104-127) → SIGTERM 창(최대 60s 폭)에 락 홀드. 설정편집+재시작 자동화가 정확히 이 창을 만든다.
- 시나리오 재구성: 성공. reload dial 진행 중 SIGTERM → shutdown이 락 대기(잔여 최대 ~60s) → 획득 후 close 5s → 슬롯당 ~65s, 직렬 누적 N×65s. systemd 기본 TimeoutStopSec 90s에서 병리 서버 2개 → SIGKILL → rmcp ChildWithCleanup의 비동기 킬 미실행 → stdio 고아 = 이 훅이 막으려던 정확한 결과.
- 반증 시도: (a) "프롬프트 call이 락을 오래 잡음" → runtime.rs:740-743 select!에서 cancel이 mcp.call future를 drop해 가드 해제, call() 자체도 peer clone 후 락 해제(mcp.rs:316-323) — 기각. (b) "backoff가 홀드 방지" → 허용 여부만 결정, 홀드 길이 무관 — 기각. (c) "무한 대기" → dial 타이아웃으로 유계(~60s)지만 함수 자신의 5s exit 상한 설계를 13배 위반 — 기각. (d) "reload가 shutdown과 경합 불가" → watcher 비중단 확인 — 기각. (e) HTTP graceful이 dial 완료 보장 → dial은 stdio 자식 기반, HTTP 드레인과 무관 — 기각.
- 결론: Medium 유지(종료 경로 가용성 + 감독자 하 고아 위험, reload∩SIGTERM 창 필요, 유계). 수정: 락 획득에도 SHUTDOWN_TIMEOUT 적용, 초과 시 skip(기존 Drop 경로에 양보).

## 3. [RelaySrv] src/bin/damon-relay.rs:453-457,483-486,505-507 — 터널 포화 시 세션 정리/슬롯 반환 무기한 결합
### 판정: CONFIRM — 최종 심각도 Medium
- 재검증: (a) 터널 채널 `mpsc::channel::<String>(256)`(damon-relay.rs:219), 양 라우트 max_message 4MiB → 최대 256×4MiB≈1GiB 백로그. (b) 클라이언트 펌프 `daemon_tx2.send(..).await` 무바운드(453-457, 패치에서 `let _`→`if err break`로 재작성된 루프). (c) attach(483-486)·disconnect(505-507) 통지 무바운드 — disconnect는 clients.remove **이후** 대기. (d) 슬롯(per-IP + conns_total) 반환은 on_upgrade 클로저에서 `relay_client_session(..).await` **반환 후**에만 수행(361-389) → 마지막 무바운드 send가 반환을 지연시키면 맵 엔트리와 무관하게 슬롯 고정. (e) select!(498-501)는 펌프 **종료** 대기 — recv_task가 send().await에 파킹되면 클라이언트 소켓 close를 알 수 없음(recv_task가 유일 reader인데 파킹됨, send_task는 rx.recv() 대기 중 write 실패로 단절 감지 불가) → 정리 미도달.
- 시나리오 재구성: 조건부 성공. 포화 조건: 터널 send_task의 writer.send가 TCP 백프레셔로 파킹(데몬이 읽지 않음). 데몬측 라우팅은 try_send-never-await(relay.rs:403-417, Full→세션 abort)이므로 **스케줄링되는 데몬은 라인레이트로 드레인** → 지속 포화는 데몬 이벤트루프 정체/동결(SIGSTOP·blocking-in-async) 또는 ~1GiB 백로그의 링크 정체 필요. 데몬 사망은 send Err → 즉시 해소(보고자의 High 제외 근거 타당). 유발: 플러드로 256 채널+소켓버퍼 채우고 close 반복 → 드레인 기간만큼 세션당 per-IP 1+전역 1 슬롯 핀 → 전역 1024 소진 시 /connect 429·/register 503 (다른 데몬 테넌트 포함 전체, conns_total는 register/connect 공유 155-158/345-350 확인).
- 반증 시도: (a) "백프레셔는 설계" — 활성 세션 흐름제어는 맞지만 정리(맵 제거+슬롯 반환)가 파킹된 펌프에 결합되는 것은 결함. attach-실패 경로(487-491)는 즉시 정리하며, 파일 내부 패턴(try_send laggard-drop :277-289, `let _` 무시)과 모순 — 비대칭(데몬 방향만 무바운드)이 우연이 아님을 뒷받침. (b) "데몬이 항상 드레인" — 정상 시 그러나 정체/동결 경로 존재, 드레인 지속 시간만큼 핀. (c) 중복 — 기존 bin/damon-relay.rs 3건(교체창 좀비/캡초과 무응답/insert 전 드랍)과 증상 상이. (d) 패치 도입 여부 — 펌프 루프는 패치 재작성, disconnect 통지는 선재지만 미기록 → 이번 웨이브 보고 대상(다이제스트 규칙).
- 결론: Medium(가용성; 유계 메모리, 교차 테넌트 503, 데몬 회복 시에만 해소). 수정: 통지는 try_send 또는 짧은 timeout(베스트에포트 성격), 펌프 send에 바운드/포화 시 세션 종료로 데몬→클라 direction의 laggard-drop과 대칭화.

## 요약
- 승인: 3건 전부 (1: Low CONFIRM, 2: Medium CONFIRM, 3: Medium CONFIRM)
- 폐기: 없음
