# 릴리스 체크리스트

`v*` 태그 푸시 → CI가 4개 타겟 빌드 → GitHub Releases tarball → npm + crates.io publish.
아래는 사람이 직접 해야 하는 것들.

## 1. 시크릿 등록 (repo Settings → Secrets and variables → Actions)

| 시크릿 | 발급처 | 비고 |
| --- | --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io → Account Settings → API Tokens | `cargo publish`용 |
| `NPM_TOKEN` | npmjs.com → Access Tokens → Granular Access Token | publish 권한. **2번(OIDC)을 택하면 불필요** |

`GITHUB_TOKEN`은 npmjs.com 인증 불가 (GitHub Packages 전용).

## 2. npm publish 방식 선택

### A. 토큰 방식 (지금 워크플로우 그대로)
`NPM_TOKEN` 등록만 하면 끝.

### B. Trusted Publishing (OIDC, 토큰 없음) — 권장
1. **첫 버전은 수동 발행** (패키지가 npm에 존재해야 trusted publisher 설정 가능):
   ```sh
   cd npm && npm login && npm publish --access public
   ```
2. npmjs.com → `damon-agent` 패키지 → Settings → Trusted Publisher:
   - repo: `developjik/damon-agent-core`, workflow: `ci.yml`
3. `ci.yml`의 `publish-npm` 잡 수정:
   - `permissions: id-token: write` 추가
   - `node-version: "24"` (npm ≥ 11.5.1 필요 — node 20은 npm 10)
   - `registry-url`, `NODE_AUTH_TOKEN` 제거
   - `npm publish --access public --provenance`

## 3. 버전 동기화 (태그 전 필수)

3곳이 태그와 일치해야 함:

- `Cargo.toml` → `version`
- `npm/package.json` → `version` (install.js가 이 값으로 tarball URL 생성)
- `Formula/damon.rb` → `version` + URL

## 4. 태그 푸시

```sh
git tag v0.1.0 && git push origin v0.1.0
```

## 5. Homebrew tap (별도 작업)

1. `developjik/homebrew-tap` 리포 생성
2. `Formula/damon.rb`를 tap 리포 `Formula/`로 복사
3. 릴리스 후 각 tarball의 실제 sha256으로 교체:
   ```sh
   curl -fsSL https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-aarch64-apple-darwin.tar.gz | shasum -a 256
   ```
4. 설치: `brew install developjik/tap/damon`

## 알려진 불일치 (배포 전 결정 필요)

- `damon-relay` 바이너리: tarball엔 포함되지만 `Formula/damon.rb`의 `bin.install`과 `npm/install.js`의 `expected` 목록에 없음 → brew/npm 사용자는 relay 못 씀
- `npm/package.json`의 `bin`은 `damond`, `damon`만 PATH 노출 → telegram/discord/slack 봇은 설치되지만 명령어로 안 잡힘
