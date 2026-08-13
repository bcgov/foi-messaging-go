#!/usr/bin/env bash
# Tests for changelog-section.sh. Run via `make test-scripts`.
set -uo pipefail

script="$(dirname "$0")/changelog-section.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0

check() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "ok   - $name"
  else
    echo "FAIL - $name"
    echo "  expected: $(printf '%q' "$expected")"
    echo "  actual:   $(printf '%q' "$actual")"
    failures=$((failures + 1))
  fi
}

cat > "$tmp/CHANGELOG.md" <<'EOF'
# Changelog

Preamble that must never appear in release notes.

## [Unreleased]

## [0.2.0] - 2026-09-01

### Added
- Second release line.

## [0.1.0-rc.1] - 2026-08-12

### Added
- Release candidate line.

## [0.1.0] - 2026-08-13

### Added
- First release line.

[Unreleased]: https://example.invalid/compare/v0.2.0...HEAD
[0.2.0]: https://example.invalid/releases/tag/v0.2.0
EOF

# 1. A section in the middle of the file stops at the next heading.
check "middle section" \
  "### Added
- Second release line." \
  "$("$script" 0.2.0 "$tmp/CHANGELOG.md" | sed '/^$/d')"

# 2. A leading v is stripped, so the tag name can be passed straight through.
check "leading v stripped" \
  "### Added
- Second release line." \
  "$("$script" v0.2.0 "$tmp/CHANGELOG.md" | sed '/^$/d')"

# 3. The LAST section stops before the link-reference block. At release time
#    the version being released is always last, so this is the common case,
#    and a naive read-to-next-heading pastes the link block into the notes.
check "last section excludes link definitions" \
  "### Added
- First release line." \
  "$("$script" 0.1.0 "$tmp/CHANGELOG.md" | sed '/^$/d')"

# 4. A version must not match a heading it is merely a prefix of.
#    0.1.0 must not pick up 0.1.0-rc.1's body.
check "prerelease is not matched by its base version" \
  "### Added
- Release candidate line." \
  "$("$script" 0.1.0-rc.1 "$tmp/CHANGELOG.md" | sed '/^$/d')"

# 5. A missing version fails loudly rather than producing empty notes.
"$script" 9.9.9 "$tmp/CHANGELOG.md" >/dev/null 2>&1
check "missing version exits nonzero" "1" "$?"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed" >&2
  exit 1
fi
echo "all changelog-section tests passed"
