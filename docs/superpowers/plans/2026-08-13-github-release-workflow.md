# GitHub CI and Release Workflow — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the repository its first CI, its first release automation, and its first tag — so consuming FOI services can pin `github.com/bcgov/foi-messaging-go@v0.1.0` instead of a pseudo-version off `main`.

**Architecture:** One reusable workflow (`verify.yml`, `on: workflow_call`) is the single definition of "green"; both `ci.yml` (PRs, pushes to `main`) and `release.yml` (`v*` tags) call it, so the gates cannot drift apart. The release job runs only after `verify` passes, extracts its notes from `CHANGELOG.md` via a shell script that is itself tested, and creates the GitHub Release with `gh`. A new `make verify` target runs locally exactly what CI runs.

**Tech Stack:** GitHub Actions, GNU Make, bash + awk, `gh` (preinstalled on GitHub runners), golangci-lint v2, Go 1.25.

**Spec:** [`docs/superpowers/specs/2026-08-13-github-release-workflow-design.md`](../specs/2026-08-13-github-release-workflow-design.md)

## Global Constraints

- **A published Go module version is immutable.** Once `proxy.golang.org` has served `v0.1.0`, that content is cached permanently. It cannot be re-tagged or withdrawn — only superseded by `v0.1.1`. Every ordering decision below follows from this.
- **The git tag is the artifact.** A Go library has no build output; `proxy.golang.org` builds the module zip itself. The GitHub Release is documentation for humans and plays no part in `go get`. Do not add SBOM, provenance, signing, or registry publication.
- Actions are pinned by **version tag** (e.g. `@v5`), not by full commit SHA.
- `.golangci.yml` declares `version: "2"`. golangci-lint v1 cannot parse it and fails before running any linter.
- `setup-go` must use `go-version-file: go.mod`. Never hardcode a Go version.
- `permissions: contents: read` everywhere except the single release job, which needs `contents: write`.
- Commit style: `feat:` / `fix:` / `test:` / `docs:` / `chore:`. **No `Co-Authored-By` trailer.**
- Work happens on the existing `release-workflow` branch, which already carries the design spec.
- The first tag is **`v0.1.0`**, not `v1.0.0`. `v1.x` would freeze the API — every later break would need a `/v2` module path.

## Three deliberate refinements beyond the spec

Flagged so a reviewer sees them as decisions rather than drift.

1. **The changelog extraction lives in `.github/scripts/changelog-section.sh`, not inline in the YAML.** The spec said "extract with `awk`". Inline YAML cannot be run or tested locally; a script can, and this script is the gate that stops a Release shipping with empty notes. It gets its own test (Task 3).
2. **The extraction must also stop at link-reference definitions.** Keep a Changelog puts `[0.1.0]: https://…` lines at the bottom of the file. For the *newest* version — always the case at release time — there is no following `## [` heading to stop at, so a naive "read to next heading" would paste the link block into the Release notes. Task 3 tests this explicitly.
3. **`make verify` also runs the script test**, via a `test-scripts` target. The spec's Makefile sketch had four prerequisites; this makes five. A gate that guards releases should not itself be untested.

## File Structure

| File | Responsibility |
| --- | --- |
| `Makefile` | **Modify.** `GOTESTFLAGS` variable, `test-scripts` and `verify` targets. |
| `CHANGELOG.md` | **Create.** Keep a Changelog format, seeded with `[0.1.0]`. The source of Release notes. |
| `.github/scripts/changelog-section.sh` | **Create.** Print one version's section body; exit 1 if absent or empty. |
| `.github/scripts/changelog-section_test.sh` | **Create.** Five cases against fixture changelogs. |
| `.github/workflows/verify.yml` | **Create.** Reusable. Three parallel jobs: `lint`, `test`, `integration`. |
| `.github/workflows/ci.yml` | **Create.** PRs + pushes to `main` → calls verify. Owns the cancelling concurrency group. |
| `.github/workflows/release.yml` | **Create.** `v*` tags → verify, then create the Release. **No** concurrency group. |
| `CLAUDE.md:40` | **Modify.** "There is no CI yet" becomes false. |
| `README.md` | **Modify.** CI badge, status paragraph, Releasing section. |

---

### Task 1: `make verify`

Nothing else can be validated locally until one command runs every gate. This task comes first so every later task can be checked with it.

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Produces: `make verify` (runs lint + all three test tiers + the script test, with `-race -count=1`); `GOTESTFLAGS` overridable per-invocation on all three test targets. Tasks 4 and 5 invoke these targets from CI.

- [ ] **Step 1: Add `GOTESTFLAGS` and thread it through the three test targets**

Replace the `.PHONY` line and the three test targets in `Makefile`. Keep every existing comment — they explain non-obvious things about the tiers.

```make
.PHONY: build test test-examples test-all test-integration test-scripts verify lint tidy up down

# Extra flags for the test tiers. CI and `verify` set -race -count=1; plain
# `make test` stays fast for local iteration.
GOTESTFLAGS ?=

build:
	go build ./...

test:
	go test $(GOTESTFLAGS) ./...

# examples/telemetry is its own Go module, so ./... from the root does not
# descend into it. It needs its own invocation, and it is easy to forget:
# it holds the test pinning the exported Prometheus metric names.
test-examples:
	cd examples/telemetry && go test $(GOTESTFLAGS) ./...

# Every non-Docker test tier. Use this rather than `test` alone.
test-all: test test-examples

test-integration:
	go test -tags=integration $(GOTESTFLAGS) ./...
```

- [ ] **Step 2: Add `test-scripts` and `verify`**

Add after `test-integration`, before `lint`:

```make
test-scripts:
	.github/scripts/changelog-section_test.sh

# Exactly what CI runs, in one command. Run this before pushing a release
# tag: a published module version cannot be corrected, only superseded.
#
# The target-specific variable propagates to every prerequisite, so the
# tiers need no duplicate -race targets.
verify: GOTESTFLAGS = -race -count=1
verify: lint test test-examples test-integration test-scripts
```

- [ ] **Step 3: Verify `GOTESTFLAGS` reaches the test targets**

Run: `make test GOTESTFLAGS='-race -count=1' --dry-run`
Expected: prints `go test -race -count=1 ./...`

Run: `make test --dry-run`
Expected: prints `go test  ./...` — unchanged behaviour when the variable is unset.

- [ ] **Step 4: Verify the variable propagates from `verify` to its prerequisites**

Run: `make verify --dry-run`
Expected: the `go test` lines all carry `-race -count=1`. The `test-scripts` line will appear but the script does not exist yet — that is expected and Task 3 creates it.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "chore: add GOTESTFLAGS and a verify target running every gate"
```

---

### Task 2: `CHANGELOG.md`

**Files:**
- Create: `CHANGELOG.md`

**Interfaces:**
- Produces: a `## [0.1.0] - 2026-08-13` heading whose exact shape Task 3's script parses, and link-reference definitions at the bottom that the script must *not* include in extracted notes.

- [ ] **Step 1: Write the changelog**

The `[0.1.0]` entry summarizes phases 0–4 from the commit history, since there are no pull requests to generate notes from. Heading format is load-bearing: `## [VERSION] - DATE`.

```markdown
# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Note that this file versions **the library**. An event's `schema_version` is a
separate, per-contract version owned by the contracts repository, and handler
dispatch matches on its major component only (PRD §11).

## [Unreleased]

## [0.1.0] - 2026-08-13

First tagged release. The library is feature-complete against
[PRD v1.1](docs/foi-messaging-go-prd-v1.1.md), but the API is not yet frozen:
under `v0.x` the Go toolchain will not auto-upgrade across minor versions, and
breaking changes cost only a minor bump. Promotion to `v1.0.0` follows the
first FOI service integration.

### Added

- Strongly-typed event envelope with generic payloads, `event_id` as UUIDv7,
  and publish-time timestamping.
- `Publisher` and `Publish`, keyed on a typed `EventDef` (topic + event type +
  schema version), writing to `{StreamPrefix}:{Topic}` Redis streams. Errors
  are returned synchronously; there is no buffering or publish retry.
- `Consumer`, `RegisterHandler[T]`, and `RegisterRawHandler`, with two-level
  routing: topic to Redis stream, then `event_type` + **major** schema version
  to handler. A `1.0.0` handler receives `1.4.2` events.
- Correlation-ID propagation across service chains, resolved from an explicit
  option, then the context, then a fresh UUIDv7.
- At-least-once delivery: in-process retry with full-jitter backoff, a
  delivery-attempt cap enforced before decoding, reclaim of pending entries
  over `ClaimMinIdle`, and per-topic bounded concurrency.
- Handler error classification (`AsPermanent`, `AsDiscard`) and a Dead Letter
  Queue with a defined wrapper contract. Undecodable, invalid, and unversioned
  envelopes are dead-lettered rather than retried — all three are permanent by
  definition.
- OpenTelemetry spans across the publish and consume paths, with consumer
  spans as children of the producer span, plus delivery-outcome metrics
  exportable to Prometheus.
- Structured `slog` logging with `trace_id`/`span_id` correlation.
- `testing/` (`messagingtest`): `Publisher`, `Deliver`, `Dispatch`,
  `NewEvent`, and `Config`, for unit-testing handlers and publish paths
  without Redis or Docker.

### Notes

- Requires Go 1.25+ and Redis 7.0+.
- Watermill, go-redis, and watermill-redisstream do not cross the library
  boundary; applications import only the root `messaging` package.

[Unreleased]: https://github.com/bcgov/foi-messaging-go/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bcgov/foi-messaging-go/releases/tag/v0.1.0
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: add CHANGELOG.md seeded with the 0.1.0 entry"
```

---

### Task 3: The changelog extraction script and its tests

This is the gate that stops a Release shipping with empty or wrong notes, so it is written test-first.

**Files:**
- Create: `.github/scripts/changelog-section.sh`
- Test: `.github/scripts/changelog-section_test.sh`

**Interfaces:**
- Produces: `.github/scripts/changelog-section.sh <version> [changelog-path]` — prints the section body to stdout; strips a leading `v` from `<version>`; exits 1 with a message on stderr when the version has no section or the section is blank. `release.yml` (Task 5) calls it as `changelog-section.sh "$GITHUB_REF_NAME" > notes.md`.

- [ ] **Step 1: Write the failing test**

Create `.github/scripts/changelog-section_test.sh`. Five cases; the third and fourth are the ones that matter most — a Release with the link block pasted into it, or with notes silently taken from the wrong version, both ship uncorrectably.

```bash
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
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
chmod +x .github/scripts/changelog-section_test.sh
.github/scripts/changelog-section_test.sh
```

Expected: FAIL — every case errors because `changelog-section.sh` does not exist yet.

- [ ] **Step 3: Write the script**

Create `.github/scripts/changelog-section.sh`:

```bash
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
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
chmod +x .github/scripts/changelog-section.sh
make test-scripts
```

Expected: five `ok   - …` lines, then `all changelog-section tests passed`.

- [ ] **Step 5: Verify against the real changelog**

```bash
.github/scripts/changelog-section.sh v0.1.0
```

Expected: the `[0.1.0]` body from Task 2, starting at "First tagged release." and ending at the "boundary; applications import only the root `messaging` package." line — with **no** `[Unreleased]:` or `[0.1.0]:` link lines.

- [ ] **Step 6: Commit**

```bash
git add .github/scripts/changelog-section.sh .github/scripts/changelog-section_test.sh
git commit -m "feat: add the changelog section extractor that gates release notes"
```

---

### Task 4: `verify.yml` and `ci.yml`

**Files:**
- Create: `.github/workflows/verify.yml`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: a reusable workflow at `./.github/workflows/verify.yml` callable via `uses:` with no inputs and no secrets, exposing three jobs — `lint`, `test`, `integration` — whose names become the required status check names in the branch protection rule (Task 7).

- [ ] **Step 1: Confirm the current action majors**

The majors below were correct when this plan was written; confirm before committing, and substitute whatever these report.

```bash
for repo in actions/checkout actions/setup-go golangci/golangci-lint-action; do
  printf '%s: ' "$repo"
  curl -sf "https://api.github.com/repos/$repo/releases/latest" | grep '"tag_name"' | head -1
done
```

For `golangci-lint-action`, the requirement is stricter than "latest": it must be a major that installs **golangci-lint v2.x**, because `.golangci.yml` is `version: "2"`. If the latest major's README documents v1 only, step back to the newest major that supports v2.

- [ ] **Step 2: Write `verify.yml`**

```yaml
# The single definition of "green" for this repository.
#
# Both ci.yml and release.yml call this workflow, so the gates that run on a
# pull request and the gates that run on a release tag cannot drift apart.
# That matters more than it looks: the copy that rots is always the one that
# runs rarely, which would be the release copy, whose failure is the one that
# cannot be undone.
name: verify

on:
  workflow_call:

permissions:
  contents: read

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      - uses: golangci/golangci-lint-action@v8
        with:
          # Pinned rather than floating on latest: a new linter release
          # turning a check on by default must not break a release build
          # that touched no code.
          version: v2.12.2
          # Keep identical to the Makefile's lint target.
          args: run ./...

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      - run: go build ./...
      - run: make test GOTESTFLAGS='-race -count=1'
      # examples/telemetry is a separate Go module; ./... never reaches it.
      - run: make test-examples GOTESTFLAGS='-race -count=1'
      - run: make test-scripts

  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      # ubuntu-latest ships a working Docker daemon, so the Testcontainers
      # Redis in internal/testsupport.StartRedis needs no service container
      # and no setup step.
      - run: make test-integration GOTESTFLAGS='-race -count=1'
```

- [ ] **Step 3: Write `ci.yml`**

```yaml
name: ci

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

# Only ci.yml cancels superseded runs. release.yml deliberately has no
# concurrency group: a cancelled release run would leave a tag with no
# GitHub Release.
concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  verify:
    uses: ./.github/workflows/verify.yml
```

- [ ] **Step 4: Lint the workflow files**

```bash
go install github.com/rhysd/actionlint/cmd/actionlint@latest
"$(go env GOPATH)/bin/actionlint" .github/workflows/*.yml
```

Expected: no output. If `go install` cannot reach the network, skip it — Step 6 gets the same signal from GitHub itself, just more slowly.

`actionlint` is a standalone tool install; it must not appear in `go.mod`. Confirm with `git diff --exit-code go.mod go.sum` (expected: no output).

- [ ] **Step 5: Confirm the gates pass locally before pushing**

```bash
make verify
```

Expected: lint clean, all three test tiers pass, five script tests pass. The integration tier needs Docker running and takes the longest by a wide margin.

- [ ] **Step 6: Commit and push, then confirm CI runs**

```bash
git add .github/workflows/verify.yml .github/workflows/ci.yml
git commit -m "ci: add the reusable verify workflow and CI on PRs and main"
git push -u origin release-workflow
```

Then open a pull request from `release-workflow` to `main` and confirm all three jobs (`lint`, `test`, `integration`) appear and pass. **Do not merge yet** — Task 5 lands on the same branch.

This is also where any BC Gov organization policy blocking Actions surfaces. Better here than at tag time.

---

### Task 5: `release.yml`

**Files:**
- Create: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: `./.github/workflows/verify.yml` (Task 4), `.github/scripts/changelog-section.sh` (Task 3).
- Produces: a GitHub Release per `v*` tag, titled with the tag, bodied from the matching `CHANGELOG.md` section, marked prerelease when the tag contains a `-`.

- [ ] **Step 1: Write `release.yml`**

```yaml
# Cuts a GitHub Release when a v* tag is pushed.
#
# The tag is the artifact: proxy.golang.org fetches the repository and builds
# the module zip itself, so GitHub is not in the `go get` path at all. This
# Release exists to give humans a changelog at a stable URL.
#
# The tag is pushed by hand and therefore already exists when these gates run.
# The workflow can withhold the Release; it cannot withhold the tag. See the
# spec's §6 for the recovery procedure when a tag turns out to be bad.
name: release

on:
  push:
    tags: ['v*']

permissions:
  contents: read

jobs:
  verify:
    uses: ./.github/workflows/verify.yml

  release:
    needs: verify
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v5

      # Fails the job when the tag has no matching CHANGELOG.md section,
      # rather than publishing a Release with an empty body.
      - name: Extract the changelog section for this tag
        run: .github/scripts/changelog-section.sh "$GITHUB_REF_NAME" > notes.md

      - name: Create the GitHub Release
        env:
          # Same-repository release: the default token is sufficient. No PAT,
          # no GitHub App, no organization secret.
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          flags=()
          # A hyphen in a semver tag means a prerelease: v0.1.0-rc.1.
          case "$GITHUB_REF_NAME" in
            *-*) flags+=(--prerelease) ;;
          esac
          gh release create "$GITHUB_REF_NAME" \
            --title "$GITHUB_REF_NAME" \
            --notes-file notes.md \
            "${flags[@]}"
```

- [ ] **Step 2: Lint it**

```bash
"$(go env GOPATH)/bin/actionlint" .github/workflows/*.yml
```

Expected: no output.

- [ ] **Step 3: Commit and push**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add the tag-triggered release workflow"
git push
```

Expected: the PR from Task 4 re-runs and stays green. `release.yml` does not run — no tag was pushed.

---

### Task 6: Documentation

**Files:**
- Modify: `CLAUDE.md:40`
- Modify: `README.md`

- [ ] **Step 1: Correct `CLAUDE.md`**

Line 40 currently reads `There is no CI yet; run lint and all three test tiers locally.` and becomes false the moment Task 4 lands. `CLAUDE.md` itself notes that past review rounds repeatedly caught the docs claiming untrue behaviour. Replace that line with:

```markdown
CI (`.github/workflows/ci.yml`) runs three jobs on every pull request and push to
`main` — `lint`, `test` (unit + `examples/telemetry` + the changelog script test),
and `integration` — all under `-race -count=1`, via the reusable
`.github/workflows/verify.yml`. `make verify` runs exactly the same set locally,
and is what you should run before pushing a release tag: `release.yml` calls the
same reusable workflow, so a tag whose gates fail produces no GitHub Release —
but the tag itself still exists, and a module version that reaches
proxy.golang.org can never be corrected, only superseded.
```

- [ ] **Step 2: Add the CI badge to `README.md`**

Insert directly under the `# foi-messaging-go` heading on line 1:

```markdown
[![ci](https://github.com/bcgov/foi-messaging-go/actions/workflows/ci.yml/badge.svg)](https://github.com/bcgov/foi-messaging-go/actions/workflows/ci.yml)
```

- [ ] **Step 3: Update the status paragraph**

The blockquote on line 5 says the API "is being implemented" — which stopped being true when Phase 4 landed — and points readers at a v1.0.0 that now has a stated precondition. Replace it with:

```markdown
> **Status: released as `v0.1.0`, pre-1.0.** The library is feature-complete against [PRD v1.1](docs/foi-messaging-go-prd-v1.1.md), but the API is not frozen: under `v0.x` breaking changes arrive as minor bumps and the Go toolchain will not auto-upgrade across them. Pin an exact version. `v1.0.0` follows the first FOI service integration.
```

- [ ] **Step 4: Add a Releasing section to `README.md`**

Add at the end of the README:

```markdown
## Releasing

Releases are cut by pushing a tag. A published Go module version is immutable —
once `proxy.golang.org` has served it, it can never be corrected, only
superseded — so the order here matters.

1. Add a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md`, and update the
   link definitions at the bottom of the file.
2. Run `make verify`. This is exactly what CI runs, including the `-race`
   integration tier, which needs Docker.
3. Confirm CI is green on `main`.
4. Tag and push:

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

`release.yml` then re-runs every gate on the tagged commit and creates the
GitHub Release from the matching changelog section. A tag with no changelog
section fails the release rather than publishing empty notes, and a tag
containing a hyphen (`v0.1.0-rc.1`) is marked as a prerelease.

If the gates fail, no Release is created — but the tag exists. Delete it
immediately (`git push --delete origin vX.Y.Z`) and re-tag; that only works
before anything has fetched the version through the module proxy. After that,
ship the fix as the next patch version.
```

- [ ] **Step 5: Commit and push**

```bash
git add CLAUDE.md README.md
git commit -m "docs: document CI, make verify, and the release procedure"
git push
```

---

### Task 7: End-to-end validation and the first tag

**This task contains irreversible steps and a human gate.** Do not run Step 6 without explicit approval.

**Files:** none — this task exercises what the previous tasks built.

- [ ] **Step 1: Merge the branch**

With the PR green, merge `release-workflow` into `main`. Confirm `ci.yml` runs on the push to `main` and passes.

- [ ] **Step 2: Prove the release workflow works, on a throwaway tag**

The first real exercise of `release.yml` must not be `v0.1.0`. Add a temporary `## [0.0.1-rc.1] - 2026-08-13` section to `CHANGELOG.md` with a single line of body, commit it, then:

```bash
git tag v0.0.1-rc.1
git push origin v0.0.1-rc.1
```

Expected: `release.yml` runs `verify` (three jobs), then `release`. A GitHub Release appears, titled `v0.0.1-rc.1`, **marked as a prerelease**, with the one-line body and no link-definition lines.

- [ ] **Step 3: Prove the changelog gate fails closed**

Delete the release and tag from Step 2:

```bash
gh release delete v0.0.1-rc.1 --yes    # or use the GitHub web UI
git push --delete origin v0.0.1-rc.1
git tag -d v0.0.1-rc.1
```

Remove the temporary changelog section and commit. Then tag a version that has no changelog section at all:

```bash
git tag v0.0.2-rc.1
git push origin v0.0.2-rc.1
```

Expected: `verify` passes, the `release` job **fails** at the extraction step with `error: no CHANGELOG.md section found for version 0.0.2-rc.1`, and **no Release is created**. This is the single most important behaviour to confirm — it is what stands between a mistake and an uncorrectable one.

Clean up:

```bash
git push --delete origin v0.0.2-rc.1
git tag -d v0.0.2-rc.1
```

- [ ] **Step 4: Apply the repository settings**

These cannot be committed and may need BC Gov organization admin:

- Branch protection on `main` requiring status checks `lint`, `test`, and `integration`.
- A tag protection rule on `v*` so only maintainers can create a release tag.
- Leave default workflow permissions read-only; `release.yml` raises them at the job level.

- [ ] **Step 5: Confirm the changelog date**

`CHANGELOG.md` carries `## [0.1.0] - 2026-08-13`. If the tag is being cut on a different day, correct the date and the heading before tagging, and commit.

- [ ] **Step 6: HUMAN GATE — tag `v0.1.0`**

**Stop. This step is irreversible.** `v0.1.0` can never be reissued with different content. Confirm with the maintainer that Steps 1–5 all passed, then:

```bash
git checkout main && git pull
make verify
git tag v0.1.0
git push origin v0.1.0
```

- [ ] **Step 7: Confirm the release and the module**

- The GitHub Release `v0.1.0` exists, is **not** marked prerelease, and its body is the `[0.1.0]` changelog section.
- The module resolves:

  ```bash
  GOPROXY=https://proxy.golang.org go list -m github.com/bcgov/foi-messaging-go@v0.1.0
  ```

  Expected: `github.com/bcgov/foi-messaging-go v0.1.0`. The first request may take a few seconds while the proxy fetches and builds the zip.
- Within a few minutes, `https://pkg.go.dev/github.com/bcgov/foi-messaging-go@v0.1.0` renders the package documentation.
