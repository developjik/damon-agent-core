# Damon

[English](README.md) | **한국어**

[![ci](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml/badge.svg)](https://github.com/developjik/damon-agent-core/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/damon-core.svg)](https://crates.io/crates/damon-core)
[![npm](https://img.shields.io/npm/v/damon-agent.svg)](https://www.npmjs.com/package/damon-agent)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

**데몬 하나. 모든 코딩 에이전트.**

Damon은 Rust로 만든 로컬 에이전트 컨트롤 데몬이다. Claude Code, Codex CLI, Oh My Pi를 각자의 네이티브 CLI로 구동한다 — 모델·툴·인증·컨텍스트 관리는 전부 에이전트 몫이고, Damon은 세션, 권한 릴레이, 검색 가능한 히스토리, 채팅 채널, 원격 접근을 소유한다. CLI, 텔레그램 봇, 데스크톱 앱, 웹 UI는 전부 같은 상주 데몬에 붙는 얇은 클라이언트다.

이름은 *daemon*의 말장난이자, 말 그대로 실제 아키텍처다.

## 왜 Damon인가

이미 에이전트 CLI를 구독으로 쓰고 있다면 **설정이 하나도 없다**. Damon은 설치된 CLI를 감지하고, 로그인 상태와 툴을 그대로 물려받는다. 토큰 로테이션도, 프로바이더 번역도, API 키도 더 이상 Damon 몫이 아니다.

- **설정 없는 백엔드** — 카탈로그 CLI 바이너리가 PATH에 있으면 자동 등록: `claude`(stream-json), `codex`(app-server), `omp`(RPC 모드). 그 외 에이전트는 `[backends.X]` 블록을 명시하면 연결된다.
- **권한은 채널 그대로** — 에이전트의 권한 요청을 웹 UI, CLI, 텔레그램/디스코드/슬랙 채팅의 `allow`/`deny` 답장으로 릴레이한다.
- **검색 가능한 히스토리** — 모든 대화를 SQLite + FTS5에 기록. `damon search "error timeout"`으로 전체 이력 전문 검색.
- **채팅 채널 내장** — Telegram, Discord, Slack 어댑터가 별도 바이너리로 나간다. 채널별 세션 자동 매핑, 스트리밍 응답.
- **시크릿은 Damon에 없다** — 에이전트가 자기 인증을 관리한다. Damon config는 포트와 토큰 정도뿐.
- **어디서든 접근** — 자체 인증서로 `wss` 서빙, 또는 공개 호스트에 `damon-relay`를 띄우면 데몬이 아웃바운드로 연결한다(인바운드 포트 불필요). 터널은 X25519 + AES-256-GCM으로 E2E 암호화.
- **상주하도록 설계** — Rust 데몬 본체는 가볍다(에이전트 프로세스는 에이전트 몫).

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
damond doctor                   # 어떤 백엔드가 설치됐는지 확인
```

에이전트 CLI가 설치·로그인돼 있으면 그게 전부다:

```sh
curl localhost:9470/health
# {"backends":["claude","omp"],"status":"ok",...}
```

번들 웹 UI로 대화: `http://127.0.0.1:9470/ui` — 세션, 스트리밍, 권한 프롬프트까지, 설치 불필요.

## 구조

```
[CLI] [Telegram] [Discord] [Slack] [web] [desktop app] [직접 만든 코드]
                              |
        단일 API: JSON-RPC over WebSocket (`/ws`, 프로토콜 v2)
                              |
                    Damon core (상주 데몬 · Rust)
                     ├─ 백엔드 레지스트리 — 설치된 CLI 탐지·구동·재시작
                     ├─ 세션 매니저 — 생명주기, 권한 릴레이, 취소
                     ├─ 세션 저장소 — SQLite + FTS5 전문 검색
                     └─ 채널 브리지 / E2E 릴레이
                              |
            Claude Code · Codex CLI · Oh My Pi  (네이티브 CLI subprocess)
             — 모델, 툴, 구독 인증, 컨텍스트는 전부 에이전트 소유
```

와이어 프로토콜은 [docs/protocol-v2.md](docs/protocol-v2.md), Node/Python/Rust 복붙 클라이언트는 [docs/integration.ko.md](docs/integration.ko.md) 참조.

## 설정

플랫폼 config 디렉터리의 TOML 파일 하나. 변경 시 핫리로드:

```toml
# 아무 것도 없어도 동작한다. 아래는 전부 선택.

# bind = "127.0.0.1:9470"          # 기본값; 변경은 재시작 필요
# auth_token = "env:DAMON_TOKEN"   # 비루프백 바인드에 필수
# default_backend = "claude"       # session.create가 backend를 생략할 때

# 백엔드 구동 오버라이드 (예: 로컬 어댑터 빌드 사용):
# [backends.claude]
# command = "/opt/claude"
# [backends.claude.env]
# ANTHROPIC_MODEL = "claude-sonnet-4-5"

# 모든 세션에 전달할 MCP 서버 (에이전트가 직접 구동·승인):
# [mcp_servers.filesystem]
# command = "npx"
# args    = ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
```

전체 레퍼런스: [config.example.toml](config.example.toml).

## 채팅 채널

```sh
damon-telegram --bot-token <token>
damon-discord  --bot-token <token>
damon-slack    --app-token xapp-… --bot-token xoxb-…
```

채널의 각 채팅이 고유한 에이전트 세션에 매핑되고, 응답은 스트리밍되며, tool 권한 요청은 `allow`/`deny` 답장으로 승인한다. 새 채널은 `damon_core::channel::ChannelApi`(`ready`/`recv`/`send`)를 구현해 `Bridge`에 넘기면 된다 — 세션 매핑, 이벤트 demux, 권한 흐름은 이미 구현돼 있다.

## 원격 접근

- **Tailscale**(권장): `ws://<tailscale-ip>:9470/ws`로 attach — WireGuard E2E, 데몬 설정 변경 없음.
- **직접 TLS**: `tls_cert`/`tls_key` 설정 시 `wss` 서빙. 비루프백 바인드는 `auth_token` 없이 기동을 거부한다.
- **자체 릴레이**: 공개 호스트에 `damon-relay`를 띄우고 데몬 config에 `[relay]` 추가 — 데몬이 아웃바운드로 연결하므로 인바운드 포트 불필요. X25519 키 교환 + `sha256(auth_token ‖ pubkey)` 증명 → AES-256-GCM; 릴레이는 평문을 볼 수 없다.

## 문서

- [docs/protocol-v2.md](docs/protocol-v2.md) — 와이어 프로토콜 (메서드, 이벤트, 타입)
- [docs/install.ko.md](docs/install.ko.md) ([en](docs/install.md)) — 설치 경로, 서비스 등록
- [docs/integration.ko.md](docs/integration.ko.md) ([en](docs/integration.md)) — 에이전트 연결, 최소 클라이언트
- [docs/release.ko.md](docs/release.ko.md) ([en](docs/release.md)) — 릴리스 체크리스트
- [config.example.toml](config.example.toml) — 모든 옵션 주석 포함
- `import { DamonClient } from "damon-agent"` — npm 패키지에 포함된 무의존성 Node 클라이언트
- `pip install damon-agent` — asyncio Python 클라이언트 ([python/](python))

## 기여

[CONTRIBUTING.md](CONTRIBUTING.md) 참조. CI는 macOS, Windows, Linux에서 `cargo test`를 돌린다.

## 라이선스

[MIT](LICENSE-MIT) 또는 [Apache-2.0](LICENSE-APACHE) 듀얼 라이선스 — 선택 가능.
