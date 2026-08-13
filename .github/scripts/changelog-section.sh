#!/usr/bin/env bash
# Print one version's section body from CHANGELOG.md.
#
# Used by .github/workflows/release.yml to build the GitHub Release notes.
# It exits nonzero when the version has no section, which is what stops a
# tag shipping a Release with empty notes — a mistake that cannot be
# corrected afterwards, because a published module version is immutable.
set -euo pipefail

version="${1:?usage: changelog-section.sh <version> [changelog-path]}"
changelog="${2:-CHANGELOG.md}"

# Accept either a tag name (v0.1.0) or a bare version (0.1.0), so callers can
# pass $GITHUB_REF_NAME straight through.
version="${version#v}"

# Compare headings by literal prefix rather than by regex: a version string is
# full of dots, and as a regex "0.1.0" would also match "0X1Y0". The trailing
# "]" in the prefix is what keeps 0.1.0 from matching 0.1.0-rc.1.
section="$(
  awk -v prefix="## [$version]" '
    /^## \[/ {
      if (found) exit                                   # next version: stop
      if (substr($0, 1, length(prefix)) == prefix) { found = 1; next }
    }
    # Keep a Changelog puts link-reference definitions at the bottom of the
    # file. The newest version has no following heading to stop at, so without
    # this the link block would land in the release notes.
    /^\[[^]]+\]: / { if (found) exit }
    found { print }
  ' "$changelog"
)"

if [ -z "$(printf '%s' "$section" | tr -d '[:space:]')" ]; then
  echo "error: no CHANGELOG.md section found for version $version" >&2
  echo "hint: add a '## [$version] - YYYY-MM-DD' section before tagging" >&2
  exit 1
fi

printf '%s\n' "$section"
