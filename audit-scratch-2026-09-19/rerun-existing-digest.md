# 기존 findings 중복 배제 다이제스트 (2026-09-19 1차 감사 + 리미디에이션)

규칙: 아래와 **같은 파일+같은 증상**은 재보고 금지(중복 폐기 통계에만 집계).
단, **수정 코드 자체의 새 결함**(예: unbounded 전환 후 새로 생긴 메모리 증가, mangled_for의 새 경계 버그)은
증상이 다르므로 **신규**로 보고한다. 전문은 `audit-findings-2026-09-19.md` (69건 + 검증 후 결함없음 목록 §3).

## 기록된 결함 (파일 — 증상 요약)
- provider/openai.rs — mangle 충돌 시 엉뚱 툴 복원[High,수정됨]; tool_calls delta index 부재→slot0; SSE 멀티라인 2-JSON 실패; SSE buf.drain O(n²); strict-retry 에러가 content_type 위조+헤더 소실
- provider/anthropic.rs — string stop→stop_sequences 400; mid-stream overloaded→generic(백오프 상실); Usage{input:0} 노출
- provider/gemini.rs — promptFeedback.blockReason 은폐(침묵 성공/EOF generic)
- provider/mod.rs — Retry-After 음수→from_secs_f64 panic; collect_stream 바이트 캡 부재
- provider/inband.rs — 배열 system content 파괴; Text 청크 index0 재배치 순서 역전
- provider/compat.rs — 구조화 content 평문 평탄화 병합
- provider/responses.rs — 멀티모달(이미지) 무음 손실
- api.rs — is_context_overflow 오탐→무단 promotion; 에러버퍼>1MB/타임아웃 빈 본문; upstream 에러문자열(URL) 노출[Low]; forward 재직렬화 u64 정밀도[Low, NO-FIX 주석 문서화됨]
- rpc.rs — live_prompts 락 across store await; stalled 플래그 sticky(리셋 부재)
- runtime.rs — 비ASCII 토큰 2.25배 과대; cancelled 툴이 status:failed 통보[Low]; tests cancel 미도달[Low]
- store.rs — fts5 단항 NOT 통과/무음 삭제; DB·데이터디렉터리 기본 퍼미션[Medium,수정됨]
- client.rs — drain_pending 락 across send; ticketed_url 무타임아웃; 로컬 WS max_message_size 캡 부재[Low]
- mcp.rs — 툴캐시 TOCTOU 부활; transport 에러 자동재시도 이중실행; 셧다운 고아(Drop 의존)[Low]; 동시 ensure_connected 중복 스폰[Low]
- telegram.rs — offset 처리 전 확정→크래시 시 유실
- discord.rs — op7/op9 고정 5s 재접속(백오프/resume 부재)
- slack.rs — seen_envelopes 4096 전체 clear
- channel.rs — Update try_send 드롭 텍스트 유실[High,수정됨]; chat_sessions 무한 증가; permission timeout 300s 하드코딩; recv Ok(None) 버스핑[Low]; session_for 락 across await[Low]
- service.rs — launchd plist 리터럴 ~; systemd_escape % / schtasks 따옴표
- bin/damon-relay.rs — 데몬 교체 창 좀비 클라이언트; 캡 초과 무응답 종료[Low]; insert 전 프레임 드랍[Low]
- relay.rs — 중복 client_id 덮어쓰기(abort 없음)[Low]
- oauth.rs — 비JSON 에러 시 상태코드 소실
- config.rs — glob_match ? 바이트 매칭; constant_time_eq 조기 반환[Low]; ModelMeta deny_unknown_fields 부재[Low]; ensure_config chmod 창[Low]
- bin/damond.rs — RUST_LOG dotenv 이후 로드[Low]
- bin/damon.rs — EPIPE panic exit 101[Low]
- bin/damon-{slack,discord,telegram}.rs — --token ps 노출[Low]
- npm/client.mjs — redial open 무타임아웃; ticketedUrl 쿼리스트링 오염[Low]
- npm/install.js — TOFU 체크섬[Low,DEFERRED]; 다운로드 무타임아웃; 부분 tarball 잔존; Windows sh 런처
- tests/phase4.rs — tool persist tautology; tests/mcp_server.py — ping -32601
- examples/bench.rs — deadline 후 블로킹 read; idle_rss 0 침묵
- .github/workflows/ci.yml — permissions floor 부재; mutable 태그 핀
- rust-toolchain.toml — channel=stable 플로팅
- scripts/set-version.sh — Formula sha256 reminder 에코만

## 검증 후 결함 없음 (요약 — 이 영역 재제기는 반증 강도 높여야)
store SQL 전 ?바인딩/트랜잭션 원자성; api 인증·티켓 원샷·rate limit 수학·promotion depth캡; rpc is_localhost_origin fail-closed·PROMPT_SLOTS·PendingGuard; runtime TTFB/백오프/stall 타이머·컴팩션 경계·perm_lock·시크릿 미로그; relay E2E proof/방향분리키/seq strict/4MiB캡; client E2E 대칭·재전송 3회·백오프; mcp 셸해석 없음·타임아웃·백오프 시프트; channel TurnGuard·이중프롬프트 방지; 3 플랫폼 429/Retry-After 준수·floor_char_boundary; oauth PKCE/static Mutex 단일비행/토큰파일 0600; config SecretRef 전파·resolve_command kill+drain·watch 디바운스; llm parse_partial_json/StopReason 6곳 일치; anthropic SSE UTF-8경계·EOF 무stop=에러·cache_control·401 재시도; openai raw bytes 디코드·[DONE] EOF·base URL 정규화; responses SSE framing·sparse remap; gemini 번역; discovery; mod http_client 바운드·NaN→60.0; damond 부트 순서·graceful; damon-relay 등록 시크릿 constant_time·name_taken; npm execSync 상수·fail-closed 순서; tests 1차 통과분 실값 단언; config.example와 스키마 일치; Cargo features 실재.

## 리미디에이션 (같은 날, 미커밋 40파일 +2956/−488)
67건 FIXED + 회귀테스트 +32, NO-FIX 1(api u64), DEFERRED 1(TOFU). cargo test/clippy 전체 통과 보고됨.
주요 수정: mangled_for 충돌우회 3 프로바이더, channel unbounded 전환, Retry-After 가드 스레딩, MCP shutdown 훅, stalled 리셋, glob char 매칭, store 0600/0700, telegram offset 지연확정, discord 백오프, relay 세대 정리 등.
