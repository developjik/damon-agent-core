#!/usr/bin/env bash
# Verify a Damon release before installing it by hand:
#
#   scripts/verify-release.sh v0.4.0 [assets-dir]
#
# Downloads the release's SHA256SUMS manifest (and signature) plus every
# tarball it names into assets-dir (default: a temp dir), checks each
# sha256, and — when a signature is published — verifies the minisign
# signature against the repo's committed public key (damon-minisign.pub
# at the repo root; falls back to a copy next to this script).
#
# Signature policy: a published signature is verified when a
# minisign-compatible tool (minisign, rsign, signify) is installed; a
# missing tool or unsigned release is a LOUD WARNING unless
# --require-signature is passed, which exits non-zero instead.
#
# Checksum mismatches always fail.
set -euo pipefail

version=""
require_sig=0
args=()
for a in "$@"; do
  case "$a" in
    --require-signature) require_sig=1 ;;
    -h|--help)
      sed -n '2,16p' "$0"; exit 0 ;;
    *) args+=("$a") ;;
  esac
done
set -- "${args[@]}"
version="$1"
dir="${2:-$(mktemp -d)}"
case "$version" in
  v*) ;; # already tag-shaped
  *) version="v$version" ;;
esac

repo="developjik/damon-agent-core"
base="https://github.com/$repo/releases/download/$version"
base="${DAMON_RELEASE_BASE:-$base}"
script_dir="$(cd "$(dirname "$0")" && pwd)"

fetch() { # url out
  # https-only, except an explicit loopback base (local testing).
  if [[ "$1" == http://127.0.0.1* ]]; then
    curl --retry 5 --retry-all-errors -fsSL "$1" -o "$2"
  else
    curl --proto '=https' --tlsv1.2 --retry 5 --retry-all-errors -fsSL "$1" -o "$2"
  fi
}

sum_tool() { # file -> "hash"
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1
  else echo "sha256 tool not found (need sha256sum or shasum)" >&2; exit 1; fi
}

mkdir -p "$dir"
echo "==> fetching $base/SHA256SUMS"
fetch "$base/SHA256SUMS" "$dir/SHA256SUMS" || { echo "release $version has no SHA256SUMS asset" >&2; exit 1; }
if fetch "$base/SHA256SUMS.minisig" "$dir/SHA256SUMS.minisig" 2>/dev/null; then
  echo "==> fetched SHA256SUMS.minisig"
else
  rm -f "$dir/SHA256SUMS.minisig"
fi

cd "$dir"
fail=0
while read -r hash asset; do
  [ -n "$asset" ] || continue
  echo "==> $asset"
  if [ ! -f "$asset" ]; then fetch "$base/$asset" "$asset"; fi
  computed="$(sum_tool "$asset")"
  if [ "$computed" != "$hash" ]; then
    echo "    MISMATCH: manifest $hash, computed $computed" >&2
    fail=1
    continue
  fi
  echo "    ok ($hash)"
done < <(sed 's/\*/ /' SHA256SUMS)
[ "$fail" -eq 0 ] || { echo "checksum verification FAILED" >&2; exit 1; }

if [ -f SHA256SUMS.minisig ]; then
  pub=""
  for cand in "$script_dir/../damon-minisign.pub" "$script_dir/damon-minisign.pub"; do
    [ -f "$cand" ] && pub="$cand" && break
  done
  if [ -z "$pub" ]; then
    echo "signed manifest, but damon-minisign.pub is not present (clone the repo or copy the key next to scripts/)" >&2
    [ "$require_sig" -eq 1 ] && exit 1
    echo "!! WARNING: signature NOT verified" >&2
  elif command -v minisign >/dev/null 2>&1; then
    echo "==> verifying minisign signature (minisign)"
    minisign -Vm SHA256SUMS -p "$pub" -x SHA256SUMS.minisig
    echo "signature verified"
  elif command -v rsign >/dev/null 2>&1; then
    echo "==> verifying minisign signature (rsign)"
    rsign verify -p "$pub" -x SHA256SUMS.minisig -q SHA256SUMS
    echo "signature verified"
  elif [ "$require_sig" -eq 1 ]; then
    echo "signature published but no minisign-compatible tool found (install minisign or rsign)" >&2
    exit 1
  else
    echo "!! WARNING: signed manifest, but no minisign-compatible tool is installed — signature NOT verified" >&2
  fi
elif [ "$require_sig" -eq 1 ]; then
  echo "release $version publishes no SHA256SUMS.minisig signature" >&2
  exit 1
else
  echo "!! WARNING: release $version is unsigned (no SHA256SUMS.minisig asset)" >&2
fi

echo "all assets verified in $dir"
