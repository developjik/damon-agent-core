# 설치

## 검증된 설치 경로

| 경로 | 상태 |
| --- | --- |
| GitHub Releases tarball | CI 태그 빌드로 생성됨 (4개 타겟) |
| `npm install -g damon-agent` | CI가 태그 푸시 시 publish; postinstall이 릴리스 tarball 다운로드 |
| `cargo install damon-core` | CI가 태그 푸시 시 crates.io publish |
| `brew install developjik/tap/damon` | tap 리포지토리 생성 전 — 아래 참조 |


## 바이너리

GitHub Releases에서 플랫폼별 tarball:

```sh
curl -fsSL https://github.com/developjik/damon-agent-core/releases/latest/download/damon-aarch64-apple-darwin.tar.gz | tar xz
```

## npm

```sh
npm install -g damon-agent
```

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

`config.example.toml` 참조. provider API 키는 `env:` 또는 `keychain:` 참조만 허용.

## OAuth 로그인 (구독제)

```sh
damond login anthropic      # Claude Pro/Max — 브라우저 승인 후 코드 붙여넣기
damond login openai         # ChatGPT Plus/Pro — 브라우저 승인 후 콜백 URL 붙여넣기
damond login kimi-code      # Kimi For Coding — 디바이스 플로우, 승인될 때까지 폴링
damond login github-copilot # GitHub Copilot — 디바이스 플로우
damond login xai-oauth      # SuperGrok / X Premium+ — 디바이스 플로우
damond logout anthropic     # 또는: damond logout <프로바이더>
```

config에서 `api_key = "oauth"`(또는 `"oauth:<flavor>"`)로 설정하면 데몬이
키체인에서 토큰을 읽고 자동 갱신한다. 짝: `anthropic` ↔
`anthropic-messages`, `openai` / `xai-oauth` ↔ `openai-responses`,
`kimi-code` / `github-copilot` ↔ `openai-completions`. 로그인하면 해당
프리셋 프로바이더가 부팅 때 자동 등록된다(`damond presets` 참조).

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
