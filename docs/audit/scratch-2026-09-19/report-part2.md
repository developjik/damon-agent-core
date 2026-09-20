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
