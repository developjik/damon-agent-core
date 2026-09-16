# 배포 전 수동 설정 체크리스트

코드·CI는 준비 완료(`cargo package` 검증 통과, `npm pack` 검증 통과, 테스트 51개 통과).
아래는 **사람이 직접 해야 하는 것**만 정리. 순서대로 진행하면 됨.

## 현재 상태 (2026-09-16 확인)

| 항목 | 상태 |
|---|---|
| npm `damon-agent` | 미발행 — 이름 사용 가능 |
| crates.io `damon-core` | 미발행 — 이름 사용 가능 |
| GitHub Releases / git tag | 없음 |
| `developjik/homebrew-tap` | 저장소 없음 (404) |

---

## 1. GitHub Secrets 등록

`developjik/damon-agent-core` → **Settings → Secrets and variables → Actions → New repository secret**

| Secret | 발급처 | 비고 |
|---|---|---|
| `CARGO_REGISTRY_TOKEN` | crates.io → Account Settings → API Tokens → New Token | publish 권한 |
| `NPM_TOKEN` | npmjs.com → Access Tokens → Granular Access Token | publish 권한. **OIDC 방식(§4) 선택 시 불필요** |

`GITHUB_TOKEN`은 자동 제공 — 설정 불필요.

## 2. 첫 릴리스 — 태그 푸시

버전은 이미 `0.1.0`으로 3곳(Cargo.toml / npm/package.json / Formula) 동기화되어 있음. 바로 태그:

```sh
git tag v0.1.0 && git push origin v0.1.0
```

CI 순서: `test`(macOS/Windows/Linux) → `release`(4 타겟 tarball → GitHub Releases) → `publish-npm` → `publish-crates`.

- npm/crates 발행이 실패해도 tarball 릴리스는 유지됨 — Actions 탭에서 해당 job 로그 확인 후 재실행하면 됨
- `install.js`가 `v{package.json version}` 태그의 tarball을 받으므로 **태그와 버전이 반드시 일치해야 함**

## 3. Homebrew tap

1. GitHub에 `developjik/homebrew-tap` **public** 저장소 생성
2. 이 저장소의 `Formula/damon.rb`를 tap 저장소의 `Formula/damon.rb`로 복사
3. 릴리스 tarball이 생긴 **후** sha256을 계산해 플레이스홀더(`0000…`) 교체:

```sh
curl -fsSL https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-aarch64-apple-darwin.tar.gz | shasum -a 256
curl -fsSL https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-x86_64-apple-darwin.tar.gz | shasum -a 256
```

4. 검증: `brew install developjik/tap/damon && damond --version`

주의: formula는 `on_macos`만 정의됨 — Linux brew 사용자는 tarball/`cargo install` 경로로 안내.

## 4. (선택) npm Trusted Publishing — 토큰 없이 발행

토큰 방식 그대로 쓸 거면 이 섹션 스킵.

1. 첫 버전은 수동 발행(패키지가 npm에 존재해야 trusted publisher 설정 가능):
   ```sh
   cd npm && npm login && npm publish --access public
   ```
2. npmjs.com → `damon-agent` → Settings → Trusted Publisher:
   repo `developjik/damon-agent-core`, workflow `ci.yml`
3. `ci.yml`의 `publish-npm` job 수정:
   - `permissions: id-token: write` 추가
   - `node-version: "24"` (npm ≥ 11.5.1 필요 — node 20은 npm 10)
   - `registry-url`, `NODE_AUTH_TOKEN` 제거
   - `npm publish --access public --provenance`

## 5. 배포 후 검증

```sh
npm i -g damon-agent && damond --version          # npm 경로
cargo install damon-core                         # crates.io 경로
curl -fsSL https://github.com/developjik/damon-agent-core/releases/latest/download/damon-aarch64-apple-darwin.tar.gz | tar xz
brew install developjik/tap/damon                # §3 완료 후
```

## 6. 다음 릴리스부터 반복 절차

```sh
scripts/set-version.sh 0.2.0     # Cargo.toml + package.json + Formula 동기화
git commit -sm "release v0.2.0"  # DCO: -s 필수 (CONTRIBUTING.md)
git tag v0.2.0 && git push origin v0.2.0
# 릴리스 후 Formula sha256 갱신 (매번 필요 — CI 자동화 검토)
```

---

## 부록: 코드 측 개선 후보 (설정 아님 — 나중에 작업 가능)

- `SHA256SUMS`를 릴리스에 포함 + `install.js`에서 검증 (현재 무결성 검증 없음)
- CI 타겟 추가: `aarch64-unknown-linux-gnu`(`ubuntu-24.04-arm` 러너), musl/Docker 이미지(`damon-relay` VPS 배포용)
- 원격 MCP 서버: rmcp `transport-streamable-http-client-reqwest` 피처로 지원 가능(현재 stdio만)
- WS 프로토콜 JSON Schema, `Provider` trait 개방(현재 closed enum)
- `Cargo.toml`: `homepage`/`documentation` 필드, CHANGELOG

완료됨: WS/HTTP Origin 검사(무토큰 시 루프백만), `session/new`·`session/prompt`
`model` 파라미터 + `damon --model`, `mcpServers` 비어있지 않으면 `-32602` 거부,
미존재 세션 prompt 거부, npm `repository`/`homepage`/`bugs` + npm README.
