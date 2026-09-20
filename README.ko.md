# Damon

[English](README.md) | **한국어**

[![ci](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml/badge.svg)](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/damon-core.svg)](https://crates.io/crates/damon-core)
[![npm](https://img.shields.io/npm/v/damon-agent.svg)](https://www.npmjs.com/package/damon-agent)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

**데몬 하나. 모든 서피스.**

Damon은 Rust로 만든 로컬 상주 에이전트 코어다. 어려운 부분 — tool loop, 세션, 메모리, 프로바이더별 quirks, 시크릿 — 을 전부 코어가 소유하고 단일 API로 노출한다. CLI, 텔레그램 봇, 데스크톱 앱, 웹 UI는 전부 같은 상주 데몬에 붙는 얇은 클라이언트가 된다.

이름은 *daemon*의 말장난이자, 말 그대로 실제 아키텍처다.

## 왜 Damon인가

대부분의 에이전트 스택은 서피스를 먼저 고르게 하고, 그 안에 툴과 메모리를 억지로 끼워 넣는다. Damon은 반대다: 코어가 제품이고, 서피스는 전부 소모품이다.

- **OpenAI 호환 엔드포인트** — 기존 OpenAI 클라이언트를 `http://127.0.0.1:9470/v1`에 그대로 향하게 하면 된다. Anthropic, Gemini, Responses API 모델도 OpenAI 스키마로 번역되므로 클라이언트는 어느 프로바이더가 응답했는지 신경 쓸 필요가 없다.
- **진짜 에이전트 런타임** — 세션, 스트리밍, tool call, 권한 프롬프트, 취소, 컨텍스트 컴팩션. `/ws`에서 ACP형 JSON-RPC over WebSocket으로 노출.
- **모든 프로바이더, 하나의 config** — OpenAI, Anthropic, Gemini, OpenRouter, Groq, DeepSeek, vLLM, Ollama(자동 탐지, 설정 불필요). 구독제도 마찬가지 — Claude Pro/Max, ChatGPT Plus/Pro, Kimi For Coding, GitHub Copilot, SuperGrok: `damond login <프로바이더>`, API 키 불필요. 그리고 omp 패리티 프리셋: `GROQ_API_KEY` 등 키 env만 있으면 해당 백엔드가 스스로 등록된다(`damond presets`로 목록 확인). 모델 glob이 요청을 라우팅하고, `model:low/medium/high` 접미사가 프로바이더별 thinking 제어로 매핑된다.
- **MCP로 툴 연결** — stdio MCP 서버를 TOML에 선언하면 `server.tool` 네임스페이스로 tool loop에 합류. 서버별 `auto_approve` 또는 대화형 권한 프롬프트.
- **채팅 채널 기본 제공** — Telegram, Discord, Slack 어댑터가 별도 바이너리로 나간다. 채널별 세션 자동 매핑, 스트리밍 응답, `allow`/`deny` 답장으로 tool 승인.
- **시크릿은 평문으로 디스크에 남지 않는다** — `env:`, `keychain:`, `!cmd` 참조만 허용, 리터럴 키는 거부. OAuth 로그인은 토큰을 OS 키체인에 저장하고 자동 갱신한다.
- **어디서든 접근** — 자체 인증서로 `wss` 서빙, 또는 공개 호스트에 `damon-relay`를 띄우면 데몬이 아웃바운드로 연결한다(인바운드 포트 불필요). 터널은 X25519 + AES-256-GCM으로 E2E 암호화 — 릴레이는 암호문만 본다.
- **상주하도록 설계** — idle RSS ~12MB, 스트리밍은 1ms 미만 오버헤드로 패스스루, 빠른 콜드스타트, 동시 세션에서도 저하 없음.

## 설치

```sh
npm install -g damon-agent        # npm (프리빌트 바이너리)
cargo install damon-core          # crates.io
brew install developjik/tap/damon # Homebrew tap
```

또는 [GitHub Releases](https://github.com/developjik/damon-agent-core/releases)에서 플랫폼별 tarball — macOS(arm64/x86_64), Linux, Windows.

OS 서비스로 등록해 항상 띄워두기:

```sh
damond service install   # launchd / systemd user / Task Scheduler
damond service print     # 설치 전 정의 미리보기
```

## Quickstart

```sh
damond                          # 첫 실행 시 스타터 config 생성
damond --print-config-path      # config 위치 확인
damond doctor                   # config·시크릿·프로바이더 연결 검증
```

프로바이더 키를 추가하고(env var, 키체인, `op read …` — 리터럴 금지):

```sh
curl localhost:9470/health
curl -N localhost:9470/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

번들 CLI로 대화:

```sh
damon chat                      # 대화형 REPL (스트리밍, tool 프롬프트)
damon chat --model claude/claude-sonnet-4-5   # 세션에 모델 고정
damon prompt "이 레포 요약해줘" --model gpt-4o:high
damon sessions                  # 세션 목록
damon resume <id>               # 세션 이어하기
damon search "error timeout"      # 전체 이력 FTS5 구문 검색
```

## 구조

```
[CLI] [Telegram] [Discord] [Slack] [web] [desktop app] [직접 만든 코드]
                              |
        단일 API: OpenAI 호환 HTTP + ACP형 WS/JSON-RPC
                              |
                    Damon core (상주 데몬 · Rust)
                     ├─ 에이전트 런타임 — tool loop, 권한, 컴팩션
                     ├─ provider 어댑터 — OpenAI / Anthropic / Gemini / Responses
                     ├─ 세션·메모리 저장소 — SQLite + FTS5
                     └─ MCP 클라이언트 — 모든 stdio MCP 서버의 툴
```

데스크톱 앱(Electron/Tauri)도 같은 thin client다: `ws://127.0.0.1:9470/ws`에 attach하거나 `damond`를 sidecar로 띄운다. 와이어 프로토콜과 Node/Python/Rust 복붙 클라이언트는 [docs/integration.md](docs/integration.md) 참조.

내장 웹 UI도 있다: `http://127.0.0.1:9470/ui` — 세션, 스트리밍,
툴 권한 프롬프트까지 설치 없이 바로 사용.

## 설정

플랫폼 config 디렉터리의 TOML 파일 하나. 변경 시 핫리로드:

```toml
[providers.default]
api      = "openai-completions"
base_url = "https://api.openai.com/v1"
api_key  = "env:OPENAI_API_KEY"     # env:VAR | keychain:svc/acct | "!op read …"
models   = ["gpt-*"]

[providers.claude]
api     = "anthropic-messages"
api_key = "oauth"                   # `damond login anthropic` → OS 키체인
models  = ["claude-*"]

[providers.chatgpt]
api     = "openai-responses"
api_key = "oauth"                   # `damond login openai` (ChatGPT Plus/Pro)
models  = ["gpt-5*", "codex-*"]     # base_url은 ChatGPT 백엔드가 기본

[mcp_servers.filesystem]
command = "npx"
args    = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
auto_approve = false                # 호출마다 클라이언트에 권한 프롬프트
```

Ollama는 설정이 아예 필요 없다 — damond가 `$OLLAMA_HOST`를 프로브하고 발견된 모델을 자동 라우팅한다. 엔드포인트 quirks(Mistral tool id, `max_completion_tokens`, 로컬 모델용 in-band tools 등)는 프로바이더별 `compat` 플래그로 처리. 전체 레퍼런스: [config.example.toml](config.example.toml).

## 채팅 채널

```sh
damon-telegram --bot-token <token>
damon-discord  --bot-token <token>
damon-slack    --app-token xapp-… --bot-token xoxb-…
```

채널의 각 채팅이 고유한 데몬 세션에 매핑되고, 응답은 스트리밍되며, tool 권한 요청은 `allow`/`deny` 답장으로 승인한다. 새 채널은 `damon_core::channel::ChannelApi`(`ready`/`recv`/`send`)를 구현해 `Bridge`에 넘기면 된다 — 세션 매핑, 이벤트 demux, 권한 흐름은 이미 구현돼 있다.

## 원격 접근

- **Tailscale**(권장): `ws://<tailscale-ip>:9470/ws`로 attach — WireGuard E2E, 데몬 설정 변경 없음.
- **직접 TLS**: `tls_cert`/`tls_key` 설정 시 `wss` 서빙. 비루프백 바인드는 `auth_token` 없이 기동을 거부한다.
- **자체 릴레이**: 공개 호스트에 `damon-relay`를 띄우고 데몬 config에 `[relay]` 추가 — 데몬이 아웃바운드로 연결하므로 인바운드 포트 불필요. 클라이언트는 `damon --relay ws://relay:8080 --relay-name <name> --token <auth_token>`으로 접속. X25519 키 교환 + `sha256(auth_token ‖ pubkey)` 증명 → AES-256-GCM; 릴레이는 평문을 볼 수 없다.

## 성능

Apple M4, release 빌드 기준선:

| 지표 | 값 |
|---|---|
| Idle RSS | 11.7 MB |
| 스트리밍 오버헤드 | TTFB +0.7ms, total +0.9ms (20청크 SSE, 로컬 mock 대비) |

재현: `cargo run --release --example bench`

## 문서

- [docs/install.ko.md](docs/install.ko.md) ([en](docs/install.md)) — 설치 경로, 서비스 등록, OAuth 로그인
- [docs/integration.ko.md](docs/integration.ko.md) ([en](docs/integration.md)) — 와이어 프로토콜, provider/compat 레퍼런스, 최소 클라이언트
- [docs/release.ko.md](docs/release.ko.md) ([en](docs/release.md)) — 릴리스 체크리스트
- [config.example.toml](config.example.toml) — 모든 옵션 주석 포함
- [examples/client.rs](examples/client.rs) — Rust 클라이언트 (`cargo run --example client`)
- `import { DamonClient } from "damon-agent"` — npm 패키지에 포함된 무의존성 Node 클라이언트

## 기여

[CONTRIBUTING.md](CONTRIBUTING.md) 참조. CI는 macOS, Windows, Linux에서 `cargo test`를 돌린다.

## 라이선스

[MIT](LICENSE-MIT) 또는 [Apache-2.0](LICENSE-APACHE) 듀얼 라이선스 — 선택 가능.
