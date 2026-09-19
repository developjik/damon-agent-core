# rerun-RelaySrv — src/bin/damon-relay.rs 전수 재감사 (2026-09-19, audit-only)

범위: 509줄 전수 정독 + git diff HEAD 훙 대조. 목표는 신규 결함(리미디에이션 회귀 포함), 기존 69건/다이제스트 중복 배제.

## 결함 후보 (1건)

### [Medium] 종료된 클라이언트 세션의 정리가 터널 채널 드레인에 무기한 종속 — 캡 슬롯 점유 지연
- 위치: src/bin/damon-relay.rs:453-457 (클라이언트 펌프 daemon_tx2.send().await), :483-486 (attach 통지), :505-507 (disconnect 통지)
- 분류: concurrency (가용성/자원점유)
- 시나리오: 데몬이 프로세스 생존 상태로 read만 정지(버그/악의/이벤트루프 정체) → 터널 send_task(:245-249)가 writer.send에서 TCP 백프레셔로 정체 → 데몬 채널(mpsc 256, register_session) 포화 → 클라이언트 세션의 세 send().await가 전부 무타임아웃 블록 → 펌프 블록 시 select! 미해결로 clients 엔트리+슬롯 점유, disconnect 통지 블록 시 맵 엔트리는 이미 제거됐는데 세션 태스크가 per-IP 1+전역 1 슬롯을 계속 점유(소켓은 닫혔는데 태스크 잔존) → 다수 IP로 반복 시 전역 1024 소진, 신규 connect 429/신규 register 503(fail-closed지만 relay 전체 가용성 상실, 데몬 재개/사망까지 회복 없음). 부차: 포화 채널은 데몬당 256×4MiB≈1GiB 큐잉 가능(등록 데몬 존재 전제).
- 근거:
```rust
// :453-457  펌프 — 채널 포화 시 여기서 영구 블록 → select! 정리 미도달
if daemon_tx2
    .send(json!({"client": client_id, "data": data}).to_string())
    .await
    .is_err()
{
    break;
}
// :503-507  정리 — 엔트리 제거 후 베스트에포트 통지가 무바운드 await
state.clients.lock().await.remove(&client_id);
let _ = daemon_tx
    .send(json!({"client": client_id, "disconnect": true}).to_string())
    .await;
```
- 수정 제안: disconnect/attach 통지는 try_send(포화 시 통지 포기 — 베스트에포트 성격에 부합) 또는 짧은 timeout 바운드. 펌프의 daemon_tx2.send에도 바운드를 두거나 포화 시 세션 종료로 데몬 방향 laggard-drop(:277-289)과 대칭화.
- 자기반증: (1) "백프레셔는 설계" 반론 검토 — 파일의 캡 주석은 슬롯/메모리 바운드를 목적으로 하고 신규 attach-실패 경로(:487-491)는 즉시 정리를 수행하므로, 정리가 베스트에포트 통지 대기로 무기한 지연되는 것은 파일 내부 패턴(try_send laggard drop, let _ 무시)과 모순 → 결함 성립. (2) 데몬 프로세스 사망은 write 에러→rx 드롭→send 즉시 실패로 블록이 빠르게 해소됨을 확인 → 시나리오가 생존+정체 데몬/지속 플러드로 한정되므로 High 아님. (3) :505-507은 diff 미변경(pre-existing)이나 1차 69건·다이제스트 어디에도 미기록 → 재감사 신규 보고 대상으로 판정.

## 조사 후 반증/중복 폐기 (요약)
1. 교체 창 잔여 레이스: 클라이언트가 구세대 tx를 clone(구 daemons 엔트리 잔존 중)한 뒤 구 터널 정리의 clients 스캔 이후에 insert → 세대 정리가 못 끊는 좀비 발생 가능. 그러나 기록된 [Medium] "데몬 교체 창 클라이언트가 죽은 터널 tx에 고정"(audit-findings-2026-09-19.md:232-238)과 같은 파일+같은 증상 → 중복 배제. (수정으로 창은 수 µs로 축소, 첫 송신 실패 시 자가 소멸)
2. attach 통지 전 pump forward(데이터 프레임이 통지보다 먼저 데몬에 도착) — 기록된 [Low] "insert 전 프레임 무음 드랍"과 같은 증상류. lock-before-spawn 수정으로 응답 경로는 클라이언트락 직렬화로 안전해짐을 확인.
3. 캡 카운터: register/connect 양 경로 증감 균형(fetch_add/fetch_sub 쌍, over 경로 포함) 확인 — 일시 overshoot 창은 과승인뿐, 누수 없음 → 기각.
4. 터널 정리와 세션 정리의 이중 clients.remove — no-op, 불변식 유지.
5. over_capacity 프레임(신규): 양 경로 fail-closed(에러 프레임+close), 시크릿/내부정보 노출 없음, 슬롯 반납 후 발송 순서 문제 없음.
6. 세대 로직(same_channel, 신규): 조기 교체·구 터널 잔존 정리 경합에서 daemons 제거 가드(:314-317)와 클라이언트 스캔 필터(:302)가 정확히 자기 세대만 끊음을 경합 추적으로 검증 — 회귀 없음. 4튜플 전환의 모든 construct/deconstruct 사이트 갱신 확인.
7. 인증/시크릿/등록(diff 비변경): constant_time_eq, 무시크릿 빈 auth만 허용, is_closed 재점유, name_taken 명시 거부 재확인 — 회귀 없음, 시크릿 로그/에코 없음.
8. Ping/Pong(Ok(_) => continue 양 펌프 유지), select! 정리, insert/spawn 순서(락 홀드 중 await 없음, 펌프는 clients 락 미사용 → 데드락 없음) 재확인.

## 커버리지
- src/bin/damon-relay.rs 1-509 전수(분할 정독 2회+라인앵커 grep), git diff HEAD --unified=3(전 훙 일람), audit-scratch-2026-09-19/rerun-existing-digest.md 전문, audit-findings-2026-09-19.md relay 관련 섹션.
- 미조사/제외: Cargo.lock, target/, docs/ 등 지정 EXCLUDES. 데몬/클라이언트 측 파일(src/relay.rs, client.rs, npm/)은 타 에이전트 범위.
