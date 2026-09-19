# Coverage draft (2026-09-19) — 고정 가능 부분

## 커버리지 표 (모듈 → 담당 조사 에이전트 → 검증 에이전트 → 상태)

| 범위 | 조사 | 검증 | 상태 |
|---|---|---|---|
| src/api.rs (893) | ApiSecurity + ApiFlow | VerifyApi | 완료 |
| src/rpc.rs (715) | RpcSocket | VerifyRpc | 완료 |
| src/runtime.rs (775) | RuntimeCore | VerifyRuntime | 완료 |
| src/store.rs (654) | StoreAudit | VerifyRuntime | 완료 |
| src/relay.rs (659) | RelayCrypto | VerifyClientMcp | 대기 |
| src/client.rs (596) + examples/client.rs | ClientLib | VerifyClientMcp | 대기 |
| src/channel.rs (461) + src/service.rs (154) | ChannelCore | VerifyChannels | 완료 |
| src/slack.rs + src/telegram.rs + src/discord.rs | Platforms | VerifyPlatformsDeps | 완료 |
| src/mcp.rs (395) | McpAudit | VerifyClientMcp | 대기 |
| src/oauth.rs (315) | OauthAudit | VerifyConfigBins | 완료 |
| src/config.rs (593) + config.example.toml | ConfigAudit | VerifyConfigBins | 완료 |
| src/llm.rs (207) | LlmTypes-2 | VerifyLlm | 대기 |
| src/provider/anthropic.rs (756) | ProvAnthropic | VerifyProviders | 대기 |
| src/provider/openai.rs (620) | ProvOpenai-2 | VerifyOpenai | 대기 |
| src/provider/responses.rs + compat.rs + inband.rs | ProvResponses | VerifyProviders | 대기 |
| src/provider/gemini.rs + mod.rs + discovery.rs | ProvMisc | VerifyProviders | 대기 |
| src/bin/damond.rs + src/bin/damon.rs | BinDaemon | VerifyConfigBins | 완료 |
| src/bin/damon-relay.rs + platform bins | BinRelaySrv(-2) | VerifyRelaySrv | 완료 |
| npm/client.mjs + install.js + package.json | NpmAudit | VerifyNpmTests | 대기 |
| tests/providers.rs 1-750 | TestsProvidersA | — (결함 0건, 검증 대상 없음) | 완료 |
| tests/providers.rs 751-1493 | TestsProvidersB | VerifyNpmTests | 대기 |
| tests/runtime.rs | TestsRuntime | VerifyRuntime | 완료 |
| tests/{rpc,api,relay,client,oauth}.rs | TestsRpcApi | — (결함 0건) | 완료 |
| tests/{channels,store,service,boot,phase4}.rs + mcp_server.py + examples/bench.rs | TestsCore | VerifyNpmTests | 대기 |
| Cargo.toml, rust-toolchain.toml, ci.yml, set-version.sh, Formula | DepsConfig | VerifyPlatformsDeps | 완료 |

## 미조사 영역
- docs/, README*.md, CONTRIBUTING*.md, CHANGELOG.md, LICENSE-*: 문서 — 코드 결함 관점 부적합
- Cargo.lock: EXCLUDES
- provOpenai/LlmTypes 1차 시도는 API 429로 실패 → 동일 지시로 재배치(ProvOpenai-2, LlmTypes-2)하여 완수

## MODEL_RULE 준수
- 서브에이전트 전원 스폰 기본값(세션 모델 zai/glm-5.3-flash 상속). 모델 지정/교체 없음.
