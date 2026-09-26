# 릴리스 체크리스트

**한국어** | [English](release.md)

`v*` 태그 푸시 → CI가 5개 타겟 빌드 → GitHub Releases tarball → npm + crates.io publish.
아래는 사람이 직접 해야 하는 것들.

## 1. 시크릿 등록 (repo Settings → Secrets and variables → Actions)

| 시크릿 | 발급처 | 비고 |
| --- | --- | --- |
| `CARGO_REGISTRY_TOKEN` | crates.io → Account Settings → API Tokens | `cargo publish`용 |
| `NPM_TOKEN` | npmjs.com → Access Tokens → Granular Access Token | publish 권한. **2번(OIDC)을 택하면 불필요** |
| `MINISIGN_SECRET_KEY` | 선택 | 비밀번호 없는 minisign/rsign 시크릿 키 — 릴리스 `SHA256SUMS` 서명 (§6) |

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

3곳이 태그와 일치해야 한다. 스크립트로 한 번에:

```sh
scripts/set-version.sh 0.1.0
```

갱신 대상:

- `Cargo.toml` → `version`
- `npm/package.json` → `version` (install.js가 이 값으로 tarball URL 생성)
- `Formula/damon.rb` → `version` + 다운로드 URL

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

## 6. 릴리스 서명 (SHA256SUMS + minisign)

모든 태그는 5개 tarball 전체를 담은 `SHA256SUMS` 매니페스트를 발행한다.
릴리스 잡이 각 tarball을 빌드 시 계산된 `.sha256` 사이드카와 대조한
뒤 조립한다. `MINISIGN_SECRET_KEY`가 등록되어 있으면 매니페스트에
서명(`SHA256SUMS.minisig`, minisign 형식 Ed25519)까지 붙는다. 시크릿이
없으면 매니페스트만 발행되고 워크플로우가 경고한다.

키 일회성 셋업(cargo가 있는 아무 머신):

```sh
cargo install rsign2
rsign generate -s minisign.key -p damon-minisign.pub -W   # -W: 비밀번호 없음
```

1. `damon-minisign.pub`를 리포 루트에 커밋 (검증자에게 필요 —
   `scripts/verify-release.sh`가 이 위치를 찾는다).
2. `minisign.key`의 내용을 리포 시크릿 `MINISIGN_SECRET_KEY`로 등록.
   이후 개인 키 파일은 디스크에서 지울 것 — 릴리스 파이프라인은
   시크릿만 있으면 된다.
3. `npm install`은 `SHA256SUMS`로 tarball을 검증한다(구 릴리스는
   타겟별 사이드카로 폴백). minisign 서명 자체는 사람/자동화가 검증:

```sh
scripts/verify-release.sh v0.4.0                 # 체크섬 + 서명(있으면)
scripts/verify-release.sh v0.4.0 --require-signature    # CI 모드: 미서명 = 실패
(에셋 디렉터리를 인자로 주면 기존 다운로드를 재검증)

## 알려진 갭 (배포 전 결정 필요)

- `damon-relay`는 tarball, `bin.install`, npm `bin`, `install.js`의
  `expected` 목록 모두에 포함됨 — 바이너리를 추가/이름 변경할 때 네
  곳을 함께 유지할 것.
- `MINISIGN_SECRET_KEY` 등록 전까지 릴리스는 서명 없는 SHA256SUMS로
  발행된다. 워크플로우는 경고하지만 실패하지는 않는다.

## 향후 개선 후보 (초기 배포 리뷰에서)

- `damon-relay` VPS 배포용 musl/Docker 이미지
  (`aarch64-unknown-linux-gnu`는 `ubuntu-24.04-arm` 러너로 빌드됨).
