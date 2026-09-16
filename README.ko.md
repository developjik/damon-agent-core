# Damon agent core

[English](README.md) | **한국어**

daemon의 말장난이자 실제 아키텍처. macOS + Windows + Linux에서 항상 떠 있는, 오픈소스 멀티 프로바이더 에이전트 코어.

만드는 건 데몬 하나뿐. API가 제대로면 어떤 프로그램이든 얇은 클라이언트로 붙는다 — 내 것이든 남의 것이든.

## 구조

```
[web] [CLI] [telegram] [discord] [slack] [browser ext] [mobile] [desktop app]   전부 thin client
                      |
        단일 API: ACP over WS/JSON-RPC + OpenAI 호환 HTTP
                      |
               Damon core (local daemon · Rust)
                ├─ 에이전트 런타임 (tool loop)
                ├─ provider 어댑터 (OpenAI 호환 LLM API)
                ├─ 세션 / 메모리 저장소
                └─ 툴 시스템 = MCP 클라이언트 (고래 지도, 쉼표 ECOS, TradingView 연결)
```
데스크톱 앱(Electron/Tauri)도 같은 thin client — 직접 만들지 않고, 상주 데몬에 attach하는 패턴을 문서/예제로 제공한다.

## 원칙

1. 코어가 먼저, 서피스는 나중. 서피스부터 만들면 죽는다.
2. 코어는 단일 API로만 노출한다. 그래야 "여러 군데서 가져다 쓰기"가 공짜가 된다.
3. API 키는 OS 자격증명 저장소(macOS Keychain, Windows Credential Manager, Linux Secret Service)에만 둔다. 코드와 저장소에는 절대.
4. 프로토콜은 표준을 재사용한다(OpenAI 호환 스키마, ACP, MCP). 어댑터 레이어를 직접 발명하지 않는다.
5. 코어는 macOS, Windows, Linux 모두에서 돌아야 한다. 플랫폼 종속 기능(키체인, 데몬 등록, 경로)은 어댑터 뒤에 둔다.
6. API가 곧 제품이다. 안정적이고 버전드된 계약 — 한 번 공개한 API는 깨지지 않는다.
7. 붙이기 쉬워야 한다. 설치 한 줄, 단일 바이너리/패키지 배포, 클라이언트 예제와 문서가 코어와 함께 나간다.
8. 성능은 기능이다. 상주 프로세스니까 idle 점유(RAM/CPU)가 작아야 하고, API는 프로바이더 호출 대비 오버헤드 ~0 — 스트리밍은 버퍼링 없이 패스스루. 콜드스타트 빠르고, 동시 세션에서 저하 없어야 한다.

## 결정 (Phase 0 확정)

- 코어 역할: 에이전트 런타임 — 코어가 tool loop·세션·툴을 소유. 프록시는 OpenAI 호환 엔드포인트로 제공
- 언어/런타임: Rust — 단일 바이너리, 최소 footprint, ACP 레퍼런스 생태계
- API: 자체 API(WS/JSON-RPC, 풀기능) + OpenAI 호환 HTTP 엔드포인트(드롭인)
- 프로토콜: ACP 서버(클라이언트↔코어) + MCP 클라이언트(코어↔툴)
- 로컬 인증: 127.0.0.1 바인드 기본 + 토큰 옵션(멀티유저·원격 대비)
- OS: macOS + Windows + Linux
- 라이선스: MIT + Apache-2.0 듀얼
- 설정: TOML, 플랫폼 디렉터리, 핫리로드
- 레퍼런스 클라이언트: CLI

## 미결정 사항 (Phase 진입 시 결정 — 미리 정하면 과잉설계)

- Phase 1: 데몬 등록(launchd / Windows Service / Task Scheduler), 자동시작, 단일 인스턴스·포트 정책, 로그 위치/로테이션
- Phase 2: 스토리지(SQLite vs 파일), 세션 모델(포킹/컴팩션), 메모리 정의(대화 이력 vs 장기기억/RAG), 툴 소스(MCP), 툴 실행 권한·샌드박스
- Phase 3: 클라이언트 SDK(TS/Python 직접 제작 vs OpenAPI 스펙 생성)
- Phase 4: 원격 릴레이 — E2E 암호화, 페어링, NAT traversal(Tailscale 재사용 vs 자체 릴레이)
- Phase 5: 배포 채널(npm/brew/바이너리), CI 매트릭스, 이름/레지스트리 충돌 확인, 기여 정책(DCO)

## 로드맵

- Phase 0: 스펙 한 장 + 게이트 확정 (완료)
- Phase 1: 코어 데몬 부팅(Rust), 프로바이더 어댑터 1종, 헬스체크 엔드포인트, 성능 기준선 벤치(idle footprint·스트리밍 오버헤드)
- Phase 2: 메모리 + 툴 시스템 (완료 — SQLite 세션 저장소, MCP 클라이언트, WS/JSON-RPC ACP API, tool loop)
- Phase 3: 레퍼런스 CLI 클라이언트 + 연동 가이드 (완료 — `damon` CLI, `damon::client` 라이브러리, docs/integration.md)
- Phase 4: 채널 확장 + 원격 릴레이(E2E) (완료 — `damon-telegram`/`damon-discord`/`damon-slack` 어댑터, wss/TLS, 비루프백 토큰 강제, `damon-relay` + X25519/AES-256-GCM E2E 터널)
- Phase 5: 오픈소스 릴리스 (완료 — MIT/Apache-2.0, `damond service install`, CI 매트릭스, npm `damon-agent`, crates.io `damon-core`)

## Quickstart

```sh
cargo run --bin damond            # 첫 실행 시 스타터 config 생성
damond --print-config-path        # config 위치 확인
curl localhost:9470/health
curl localhost:9470/v1/chat/completions -d '{"model":"gpt-4o","messages":[...],"stream":true}'
```

CLI: `damon health` / `damon sessions` / `damon chat` / `damon prompt "..."` — `--url`, `--token` (또는 `DAMON_TOKEN`)

채널 어댑터: `damon-telegram --bot-token …` / `damon-discord --bot-token …` / `damon-slack --app-token xapp-… --bot-token xoxb-…` — 채널별 세션 자동 매핑, 스트리밍 응답, "allow"/"deny" 답장으로 툴 권한 승인. 새 채널은 `damon_core::channel::ChannelApi` 구현 + `Bridge` 연결로 추가.

원격 접근: `tls_cert`/`tls_key` 설정 시 wss 서빙. 비루프백 바인드는 `auth_token` 필수. 자체 릴레이: `damon-relay` (공개 서버, 0.0.0.0:8080) + 데몬 `[relay]` 설정 → 아웃바운드 터널, X25519+AES-256-GCM E2E. 클라이언트: `damon --relay ws://relay:8080 --relay-name <name> --token <auth_token>`.

## 기준선 (Apple M4, release)

- idle RSS: 9.3 MB (Phase 1) → 11.7 MB (Phase 2)
- 스트리밍 오버헤드: TTFB +0.7ms, total +0.9ms (20청크 SSE, 로컬 mock 대비)
- 측정: `cargo run --release --example bench`
