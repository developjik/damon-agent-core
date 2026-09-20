# 재감사 보고 — ClientRelay (src/client.rs, src/relay.rs) · 2026-09-19

대상: 리미디에이션 패치 후 src/client.rs(+45/−9), src/relay.rs(+42/−10). 신규 결함만.
중복 배제: `rerun-existing-digest.md` 대조 완료.

## 결함 후보 (1건)

### [High] 클라이언트 WS 4MiB 수신 캡이 정상 대형 응답(session/messages)을 차단 — 링크 끊김·재다이얼 루프
- 위치: src/client.rs:441-448 (`ws_connect_config`, 적용 지점 src/client.rs:106-110, 122-127) / 연쇄 경로 src/rpc.rs:425-440, src/store.rs:403-432, src/client.rs:476
- 분류: correctness
- 시나리오: 누적 메시지가 4MiB를 넘는 세션(예: 데몬 자체의 4MiB 인바운드 프레임 캡[rpc.rs:83-84]이 허용하는 ~3.9MiB 사용자 프롬프트 1건 + 어시스턴트 응답, 또는 장기 텍스트 세션 — 어시스턴트 텍스트는 무절단, tool 출력만 기본 8KiB 절단)에서 클라이언트가 `request("session/messages", {"sessionId": s})`를 limit 없이 호출 → rpc.rs:432 `store.messages()`는 LIMIT 없이 전체 행 반환(store.rs:403-432, 컴팩션 컷오프 이후 행은 바이트 상한 없음; `messages_paged`도 호출자가 큰 limit을 주면 u32::MAX까지 무제한) → `respond()`가 단일 JSON 프레임(4MiB+)으로 직렬화해 송신(rpc.rs 아웃바운드엔 크기 상한 없음, SEND_TIMEOUT만 존재) → 클라이언트의 새 캡(client.rs:446 `max_message_size(4<<20)`)에서 tungstenite `Capacity(MessageTooLong)` 읽기 에러 → ws_transport 펌프 `Err(_) => break`(client.rs:476) → 링크 사망 → drain_pending이 해당 연결의 **모든** 진행 중 요청/프롬프트를 "connection closed"로 실패 처리 → 재다이얼 후 request() 재시도가 같은 세션을 다시 요청 → 동일 초과 프레임 → 3회 시도 후 "connection closed". 그 세션 히스토리는 참조 클라이언트로 영구 조회 불가 + 시도마다 공유 연결이 끊겨 타 세션 진행 프롬프트/이벤트까지 연쇄 실패. 패치 전 기본 캡(64MiB)에서는 통과하던 프레임.
- 근거:
  ```rust
  // src/client.rs:441-448 (신규)
  fn ws_connect_config() -> ...WebSocketConfig {
      tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
          .max_message_size(Some(4 << 20))
          .max_frame_size(Some(4 << 20))
  }
  // src/rpc.rs:432,440 — 응답 무절단 (아웃바운드 바이트 상한 없음)
  } else { state.store.messages(&session_id).await };
  Ok(messages) => { client.respond(id, Ok(json!({"messages": messages}))).await; }
  // src/client.rs:474-476 — 초과 프레임 = 치명 에러 = 링크 사망
  Ok(Message::Close(_)) => break,
  Ok(_) => continue,
  Err(_) => break,
  ```
- 수정 제안: (a) 데몬 측 session/messages(및 session/list) 응답을 서버에서 페이지/절단해 단일 프레임 상한을 보장하거나, (b) 클라이언트 max_message_size를 데몬 최대 아웃바운드 프레임보다 크게(예: 8MiB, max_frame_size는 4MiB 유지) 설정. 어느 쪽이든 인바운드 4MiB 허용 ↔ 아웃바운드 4MiB 거절 비대칭을 한쪽에서 해소해야 함.
- 자체반증: (1) 컴팩션이 반환 크기를 묶는지 확인 — 컷오프 이전 행만 요약으로 치환할 뿐 이후 행은 LIMIT 없음, 컴팩션은 프로바이더 overflow 트리거 기반이라 반환 집합 바이트 상한 아님. (2) rpc.rs 리미디에이션 diff에 아웃바운드 상한 추가 여부 확인 — 없음. (3) messages_paged 우회 가능 — 호출자가 limit을 줄 때만. (4) tungstenite 동작 확인 — max_message_size 초과 시 Err이고 펌프는 break. 반증 실패, 결함 유지.

## 결함 0건 판정 후보 없음 이상 — 위 1건만 신규

## 검증 후 결함 없음 (패치 변경 5개 경로)
1. **drain_pending 잠금/전송 분리(client.rs:604-627)**: 스냅샷 후 삽입되는 신규 엔트리의 유실 경로(죽은 sink로 전송 성공→미해결)는 패치 전에도 동일 인터리빙(drain 반환 후 삽입)이 존재 — 증상 동일, 신규 아님. handle_frame과 drain은 동일 태스크에서 순차 실행 — 이중 해결·중복 PromptDone 불가. handle_frame은 맵 제거 후 전송하므로 drain 스냅샷과 배타.
2. **relay.rs 중복 client_id 교체(abort+task::Id 가드)**: 정상 종료→재접속 순서 뒤바뀐 모든 인터리빙(교체 전 done 큐잉, 교체 후 구 done 도착, disconnect→재접속→구 done) 추적 — id 가드가 모두 정확히 분기. done 채널 full 시 모니터가 대기하되 select 루프가 계속 소진하므로 교착 없음. abort 후 구 세션의 자식 태스크(pipe/decrypt/encrypt 브리지)는 채널 폐쇄로 모두 종료 확인.
3. **client_connect read_pump abort(relay.rs:570-622)**: 핸드셰이크 실패 시 reader는 abort로, writer는 raw_out_tx drop→쓰기 펌프 종료로 해제 — 양 절반 모두 해제되어 소켓 즉시 폐쇄, 릴레이 per-IP 슬롯 누수 해소 확인. 에러 매핑(Ok(Err)→원본 에러, Err→타임아웃)은 기존 `.context("E2E handshake timed out")??`와 의미 동일.
4. **ticketed_url 타임아웃(client.rs:414-428)**: reqwest 전체 15s(본문 읽기 포함)·connect 10s — 재다이얼 백오프(100ms→5s)와 상호작용 이상 없음. builder 실패는 에러 전파.
5. **E2E 암호 경로**: 교체 abort는 신규 핸드셰이크와 상태 공유 없음(키페어/seq 세션별 독립). seq strict/direction 키 변경 없음.

## 조사 후 반증/제외 후보
- `connect_async_with_config` 여전히 타임아웃 없음(WS 다이얼 구간 무한 대기 가능) — 기록된 "client.rs — ticketed_url 무타임아웃"과 같은 파일·같은 증상 계열이며 패치가 건드리지 않은 구간(사전 존재) → 제외.
- relay.rs stale disconnect 공지가 교체된 신규 세션 엔트리를 제거(태스크는 채널 폐쇄로 자연 종료) — 패치 전에도 remove는 무조건이어서 동일 결과(사전 존재, 악성 릴레이 전제) → 제외.
- 교체 abort 후 구 세션 진행 프롬프트의 `{"client": id}` 프레임이 신규 클라이언트로 크로스톡 — 패치 전 orphan 태스크도 동일 경로 출력(사전 존재) → 제외.
- tokio task::Id 재사용으로 done 가드 우회 — tokio 1.53.1, 런타임별 단조 증가 카운터, 재사용 없음 → 반증.
- drain_pending 이중 PromptDone/재전송 중복 — 상기 검증 항목 1에서 배타성 확인 → 반증.
- session/list 무페이지 응답(세션 ~2만 개 시 4MiB 초과) — 본 결함과 동일 근본 원인으로 본 건에 포함 기재.

## 커버리지
- src/client.rs 1-632 전체 정독 (diff 5개 헝크: connect/redial 설정, ticketed_url, ws_connect_config, drain_pending)
- src/relay.rs 1-677 전체 정독 (diff 5개 헝크: done 채널 타입, 세션 교체 abort, done 가드, read_pump abort)
- 경계 소비측 교차검증: src/rpc.rs:59-250(ws_handler 캡/펌프/WsClient), 379-460(session/list·messages), 463-623(search·prompt); src/store.rs:379-453(messages/messages_full), 639-690(paged/list); src/provider/mod.rs:234-235(COLLECT_STREAM_MAX 64MiB); bin/damon-relay.rs 캡 위치(154-156, 353-355); Cargo.toml(edition 2024)/Cargo.lock(tokio 1.53.1)
- 전체 diff stat(40파일) + rpc.rs diff 스캔(아웃바운드 상한 없음 확인)
