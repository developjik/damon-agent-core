# 설치

[English](install.md) | **한국어**

## 검증된 설치 경로

| 경로 | 상태 |
| --- | --- |
| GitHub Releases tarball | CI 태그 빌드로 생성됨 (4개 타겟) |
| `npm install -g damon-agent` | CI가 태그 푸시 시 publish; postinstall이 릴리스 tarball 다운로드 |
| `cargo install damon-core` | CI가 태그 푸시 시 crates.io publish |
| `brew install developjik/tap/damon` | tap 리포지토리 필요 — 아래 참조 |


## 바이너리

GitHub Releases에서 플랫폼별 tarball:

```sh
curl -fsSL https://github.com/developjik/damon-agent-core/releases/latest/download/damon-aarch64-apple-darwin.tar.gz | tar xz
```

tarball에는 `damond`, `damon`, `damon-telegram`, `damon-discord`, `damon-slack`, `damon-relay`가 들어 있다.

## npm

```sh
npm install -g damon-agent
```

6개 바이너리가 PATH에 링크된다. 패키지는 무의존성 Node 클라이언트도
export한다 (`import { DamonClient } from "damon-agent"`) —
[integration.ko.md](integration.ko.md) 참조.

## cargo

```sh
cargo install damon-core
```

## Homebrew

tap 리포지토리(`developjik/homebrew-tap`)가 생성되면:

```sh
brew install developjik/tap/damon
```

formula는 이 리포지토리의 `Formula/damon.rb` — 릴리스마다 `sha256`을 갱신해야 함.

## 상주 서비스 등록

```sh
damond service install    # launchd (macOS) / systemd user (Linux) / Task Scheduler (Windows)
damond service print      # 설치 전 정의 확인
```

## 설정

```sh
damond --print-config-path   # config.toml 위치
```

`config.example.toml` 참조. 에이전트 CLI의 로그인은 각 CLI의 자체 인증을
그대로 사용한다(탐지된 CLI — `claude`, `codex`, `omp`, `cursor`, `amp`,
`kimi`, `qwen`, `gemini` — 가 각자 인증한다) — Damon이 에이전트 토큰을
보관하지 않는다.


## 원격 릴레이

공개 서버에 `damon-relay`를 띄우고(기본 0.0.0.0:8080), 데몬 config에:

```toml
[relay]
url  = "ws://your-relay:8080"
name = "my-daemon"
```

데몬이 아웃바운드로 연결 — 인바운드 포트 불필요. 클라이언트:

```sh
damon --relay ws://your-relay:8080 --relay-name my-daemon --token <auth_token> chat
```

E2E: X25519 키 교환 + `sha256(auth_token || pubkey)` 증명 → AES-256-GCM. 릴레이는 암호문만 본다.

**릴레이가 웹 UI도 서빙한다.** 아무 브라우저(셀룰러 데이터를 쓰는 폰 포함)로
`http://your-relay:8080/`를 열고 데몬 이름과 `auth_token`을 입력하면, 페이지가
같은 릴레이를 통해 종단간 암호화로 다시 접속한다 — VPN도, 포트포워딩도, 별도
호스팅도 필요 없다.
