#!/usr/bin/env bash
# Sync the release version across Cargo.toml, npm/package.json, and
# Formula/damon.rb. Usage: scripts/set-version.sh 0.2.0
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "usage: $0 <version>   (e.g. $0 0.2.0)" >&2
  exit 1
fi
VERSION="$1"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid semver: $VERSION" >&2
  exit 1
fi

# Portable in-place sed: GNU sed takes -i alone, BSD/macOS needs -i ''.
sed_i() {
  if sed --version >/dev/null 2>&1; then
    sed -i "$@"
  else
    sed -i '' "$@"
  fi
}

# Cargo.toml — the `version = "…"` inside [package] only. A future
# [workspace] or other table with a version key must not be rewritten.
awk -v v="$VERSION" '
  /^\[package\]/ { inpkg=1 }
  /^\[/ && !/^\[package\]/ { inpkg=0 }
  inpkg && !done && /^version = "/ { sub(/"[^"]*"/, "\"" v "\""); done=1 }
  { print }
' "$ROOT/Cargo.toml" > "$ROOT/Cargo.toml.tmp" && mv "$ROOT/Cargo.toml.tmp" "$ROOT/Cargo.toml"

# npm/package.json — top-level "version" field.
awk -v v="$VERSION" '!done && /"version":/ { sub(/"version": "[^"]*"/, "\"version\": \"" v "\""); done=1 } { print }' \
  "$ROOT/npm/package.json" > "$ROOT/npm/package.json.tmp" && mv "$ROOT/npm/package.json.tmp" "$ROOT/npm/package.json"

# Formula/damon.rb — version line + the v<ver> in both download URLs.
sed_i \
  -e "s/^  version \".*\"/  version \"$VERSION\"/" \
  -e "s|/releases/download/v[0-9][0-9A-Za-z.-]*/|/releases/download/v$VERSION/|g" \
  "$ROOT/Formula/damon.rb"

echo "version set to $VERSION in:"
echo "  Cargo.toml, npm/package.json, Formula/damon.rb"
echo "reminder: update Formula sha256 after the release tarballs exist"
