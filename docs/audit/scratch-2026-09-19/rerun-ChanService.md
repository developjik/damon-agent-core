# 2차 재감사 — ChanService (src/channel.rs + src/service.rs) / 2026-09-19

대상: 리미디에이션 패치(미커밋) 중 channel.rs(+77/−25), service.rs(+44). 신규 결함만 보고.

## 결함 후보: 1건

### [Medium] 24h chat_sessions 에비스전이 데몬 세션을 고아로 남기고 대화 연속성을 무음 리셋
- 위치: src/channel.rs:307-311 (참조: src/api.rs:117-141, src/config.rs:49-50, src/channel.rs:318-328 대조)
- 분류: leak | correctness
- 시나리오: 기본 설정(`session_retention_days` Unset = "keep all", config.rs:49-50)으로 운영 중인 봇에서 채팅이 24h+ 유휴 → 다음 메시지에서 `session_for` 패스트패스 미스 → `map.retain`이 해당 엔트리 제거 → 슬로우패스가 신규 `new_session` 생성. 이때 (a) 데몬은 구 세션을 여전히 보유(기본값엔 리텐션 스윕 태스크가 api.rs:121 `if let Some(days)` 조건으로 아예 스폰되지 않음)하므로 사용자가 설정한 keep-all 정책과 무관하게 대화 히스토리/컨텍스트가 무음 상실, (b) 구 세션은 `delete_session` 없이 매핑만 버려져 영구 고아 — 주석("the daemon's own sweep collects it")과 달리 기본 설정에선 수거 주체가 존재하지 않아 스토어 행이 계속 축적. 같은 파일의 이중생성 경합 경로(318-328)는 고아 방지를 위해 `delete_session`을 호출하므로, 에비스전 경로만 누락된 것은 의도가 아닌 결함으로 보임.
- 근거:
```rust
// src/channel.rs:307-311
// Evict entries idle for over 24h so the map can't grow
// without bound. Only the map entry goes — the daemon
// session itself stays under the daemon's own retention
// sweep (policy unchanged).
map.retain(|_, e| e.last_used.elapsed() < SESSION_IDLE_EVICTION);
```
```rust
// src/api.rs:120-121 — 스윍은 Some일 때만 스폰 (기본 None = 스폰 없음)
let retention_days = state.config.read().session_retention_days;
if let Some(days) = retention_days {
```
- 수정 제안: retain으로 제거되는 엔트리의 세션에 `delete_session`을 호출(실패 시 기존 warn 패턴 재사용)하거나, `session_retention_days`가 Some인 경우에만(또는 그 값−여유) 에비스전하도록 구성값을 브리지에 전파.
- 자체반증 시도: (1) "의도된 제품 동작(24h 후 리셋) 아닌가" — 주석은 메모리 바운딩 목적으로 서술되고 안전성 근거로 부정확한 스윕 주장을 사용 중이며, keep-all 기본값·이중생성 경로와의 비대칭이 의도 아님을 시사. (2) "데몬 스윍이 수거한다" — 기본 설정(Unset)에서 스윕 태스크 미생성으로 반증. (3) "구 세션을 다시 찾을 경로가 있는가" — chat↔session 매핑은 브리지 메모리뿐(디스크 저장 없음)으로 재발견 불가, 반증 실패. (4) ">24h 짜리 진행 중 턴이 잘리는 경로" — 턴 시작 시 last_used 갱신 + 모든 요청에 타임아웃(perm 300s+30s 등)이 있어 단일 턴이 24h를 넘는 현실적 경로 없음, 해당 변형은 기각.

## 커버리지 (실제 읽은 범위)
- src/channel.rs 1-513 전체 정독 + diff 7개 헝크 전수: Demux unbounded 전환, ChatSession/SESSION_IDLE_EVICTION 신설, run() Ok(None) 5s 슬립, 라우터 단일 경로 send, session_for 재작성(락 범위/경합/eviction), run_turn unbounded_channel, PERMISSION_TIMEOUT 상수 치환.
- src/service.rs 1-194 전체 정독 + diff 4개 헝크 전수: systemd_escape % 추가, schtasks_escape 신설(2계층 파싱 추적), print_definition macOS home 해석.
- 교차 검증: src/runtime.rs:26-28,53-55,700-712(PERMISSION_TIMEOUT 정의/설정 오버라이드), src/config.rs:43-50, src/api.rs:115-148, src/rpc.rs:130-133,270-278(req_timeout는 설정+30s 존중), src/client.rs:237-318(respond/events/new_session/delete_session 시그니처), src/telegram.rs:172-218(recv None 의미론), tests/service.rs:21-108, tests/channels.rs(브리지 E2E), audit-findings-2026-09-19.md(중복 배제 원문 대조).

## 조사 후 반증/중복 폐기 후보
1. 브리지 permission 타임아웃이 `config.permission_timeout_secs`(설정 시)와 여전히 드리프트 — 1차 #220(a)(b) 시나리오와 동일 파일·동일 증상(기본값 정렬만 개선) → 중복 배제.
2. unbounded 전환의 무한 메모리 성장(느린 소비자+장기 스트림) — demux 엔트리는 턴 생명주기와 일치, 턴별 이벤트 스트림은 모델 응답으로 유한, 라우터는 잠금 분리 후 send → 유한. 기각.
3. 라우터 순서 보장 회귀 — 단일 태스크 FIFO, PromptDone은 세션 Updates 이후 도착 순서 유지. 기각.
4. Ok(None) 5s 슬립의 지연/시맨틱 회귀 — 3개 스톡 어댑터는 recv 내부 블록(텔레그램 30s 롱폴, 디스코드/슬랙 WS 루프), None은 빈 폴에 한정. 기각.
5. session_for 이중생성 경합 처리 — active_turns가 동일 채팅 직렬화로 도달 불가 + 방어 코드 자체도 정확(drop(map) 후 await). 기각.
6. schtasks_escape 2계층 인용 결함 — layer1(cmd/CommandLineToArgvW) 추적상 MS argv 규칙과 일치(인용 전 백슬래시 2배, 경로 내 `"`는 Windows 파일명에 불가능, 종단 `\` 파일 경로도 무효). tests/service.rs:77 기대치와도 일치. 기각.
7. systemd_escape 이스케이프 순서 간섭(`\`→`"`→`%`) — 단계별 조합 추적 결과 모두 원형 복원됨. 기각.
8. print_definition 경로 폴백/XML — launchd_plist 내부 xml_escape 처리, install()과 대칭. 기각.
9. 기본 설정에서 데몬 스토어 무한 축적 그 자체 — 1차 #212와 동일 증상(후보 1건의 근거로만 인용, 별도 보고 안 함).
