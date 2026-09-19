
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
