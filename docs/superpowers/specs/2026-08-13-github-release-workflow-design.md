# GitHub CI and Release Workflow — Design

Design of record for publishing `github.com/bcgov/foi-messaging-go` as a
versioned Go module. Adds the repository's first CI, its first release
automation, and its first tag.

Status: design approved 2026-08-13. Not yet implemented.

---

## 1. Purpose and governing constraint

The library is feature-complete — every phase from 0 through 4 has landed —
but it has never been released. There are no tags, so
`go get github.com/bcgov/foi-messaging-go` resolves to a pseudo-version off
whatever `main` happened to be (`v0.0.0-2026...-fab9746`). Consuming FOI
services cannot pin a version because there is no version to pin.

There is also no CI. `CLAUDE.md` states this outright: *"There is no CI yet;
run lint and all three test tiers locally."* Every claim of green to date has
rested on a human remembering to run three separate commands, one of which
needs Docker and one of which lives in a nested module that `./...` does not
reach.

**The governing constraint for everything below:**

> A published Go module version is immutable. Once `proxy.golang.org` has
> served `v0.1.0`, that content is cached permanently — it cannot be
> re-tagged, corrected, or withdrawn. The only remedy is to burn the number
> and ship `v0.1.1`.

This is not a GitHub property and no workflow can undo it. It is why the
verification design below is weighted so heavily toward *preventing* a bad
tag rather than reacting to one, and why the release job's gates are the same
gates as CI's rather than a lighter subset.

### What "artifact" means here

A Go library has no build output. There is nothing to compile, nothing to
upload, and no registry to push to. When a `v*` tag appears,
`proxy.golang.org` fetches the repository, builds the module zip *itself*,
and serves it to every consumer. GitHub is not in that path.

**The git tag is the artifact.** The GitHub Release is documentation for
humans — a changelog with a stable URL — and plays no part in `go get`. This
design therefore produces exactly two things: a tag, and a Release page
describing it. Deliberately excluded: SBOM generation, SLSA provenance,
cosign signing, and publication to any internal registry. None are required
for a public repository whose consumers reach `proxy.golang.org` normally.

---

## 2. Decisions

| Decision | Choice | Why not the alternative |
| --- | --- | --- |
| Release artifact | Tag + GitHub Release with notes | Signing/SBOM/provenance add real complexity for no consumer benefit while no BC Gov policy requires them |
| Tag creation | Manual `git push origin v0.1.0` | A bot deciding semver (release-please) needs a GitHub App or PAT in a BC Gov org, and surrenders human control of version bumps while the API is still young |
| Verification | CI on PRs *and* the same gates re-run on the tagged commit | Gating only at tag time answers "is main green?" at the moment failure is most expensive; gating only in CI ships whatever a tag happens to point at |
| First tag | `v0.1.0` | `v1.0.0` freezes the API — every later break needs a `/v2` module path and a `/v2` import suffix. No FOI service has integrated yet, so no API feedback has arrived |
| Release notes | Hand-written `CHANGELOG.md`, extracted by tag | `--generate-notes` builds from *merged pull requests*; this repo has never used one (every merge is local, e.g. `Merge phase-4-messagingtest`), so generated notes would be near-empty |

### Why `v0.1.0` and not `v1.0.0`

Feature-completeness against the PRD is not the same as API stability. Under
`v0.x` the Go toolchain treats the module as unstable and will not
auto-upgrade across minor versions, and breaking changes cost nothing but a
minor bump. Under `v1.x` every breaking change is permanent friction for
every consumer, forever.

Promote to `v1.0.0` when at least one FOI service has integrated the library
and its API has survived contact with a real caller.

---

## 3. Files

```
.github/workflows/verify.yml    # reusable (workflow_call) — the one definition of "green"
.github/workflows/ci.yml        # pull_request + push to main → calls verify
.github/workflows/release.yml   # push tags 'v*' → calls verify, then cuts the Release
CHANGELOG.md                    # Keep a Changelog format, seeded with [0.1.0]
Makefile                        # + GOTESTFLAGS, + verify target
CLAUDE.md                       # "There is no CI yet" becomes false
README.md                       # CI badge, Releasing section
```

### Why `verify.yml` is a separate reusable workflow

CI and release must run *identical* gates. If they were two copies of the
same steps they would diverge — and the copy that rots is the one that runs
rarely, which is the release copy, which is the one whose failure is
unrecoverable. `workflow_call` makes divergence impossible rather than
merely discouraged.

---

## 4. The verify workflow

Three parallel jobs, so a failure names its own tier rather than reporting
"tests failed" across three unrelated things.

| Job | Command | Notes |
| --- | --- | --- |
| `lint` | `golangci/golangci-lint-action` with `args: run ./...` | Pinned; see below |
| `test` | `make test GOTESTFLAGS='-race -count=1'` then `make test-examples GOTESTFLAGS=…` | No Docker. Fast failure signal |
| `integration` | `make test-integration GOTESTFLAGS='-race -count=1'` | Testcontainers starts its own Redis |

The test tiers go through `make` so there is one definition of each; the lint
job does not, because the action installs and invokes the tool itself and
offers no install-only mode. Its `args` must stay identical to the `lint`
target's `run ./...` — a one-line duplication, accepted because both sides are
that one line.

Shared across jobs:

- `actions/setup-go` with `go-version-file: go.mod` — the Go version tracks
  the module and cannot drift. Never hardcode `1.25`.
- `permissions: contents: read`. The release job raises this; nothing else
  does.
- A `concurrency` group keyed on the ref, cancelling superseded PR runs.
  **Release runs must not be cancellable** — see §8.

### golangci-lint version pinning

`.golangci.yml` declares `version: "2"`. golangci-lint v1 cannot parse this
schema and fails before running a single linter, so the action major must be
one that installs v2.x. Pin the tool version explicitly (`2.12.2`, matching
the version in local use) rather than floating on `latest`: a new linter
release turning a new check on by default would otherwise break a release
build that touched no code.

**Implementation task:** confirm the current `golangci/golangci-lint-action`
major that supports golangci-lint v2 before writing the file, and pin the
action to that major tag.

### Docker for the integration tier

`ubuntu-latest` runners ship a working Docker daemon, so
`internal/testsupport.StartRedis` works unmodified — no service containers, no
`docker-compose.yml` (which the tests do not use anyway). This tier is the
slowest by a wide margin and is the reason the jobs run in parallel.

### `make verify` and why the Makefile changes

CI must not contain its own copy of what each test tier means, and a
maintainer about to push an irreversible tag needs one local command that
proves the same thing CI proves.

```make
# Extra flags for the test tiers. CI and `verify` set -race -count=1;
# plain `make test` stays fast for local iteration.
GOTESTFLAGS ?=

test:
	go test $(GOTESTFLAGS) ./...

test-examples:
	cd examples/telemetry && go test $(GOTESTFLAGS) ./...

test-integration:
	go test -tags=integration $(GOTESTFLAGS) ./...

# Exactly what CI runs, in one command. Run this before pushing a release
# tag: a published module version cannot be corrected, only superseded.
verify: GOTESTFLAGS = -race -count=1
verify: lint test test-examples test-integration
```

The target-specific variable propagates to prerequisites in GNU Make, so
`verify` needs no duplicate test targets. Existing targets keep their current
behaviour when invoked directly, so no local workflow gets slower.

---

## 5. The release workflow

```yaml
on:
  push:
    tags: ['v*']
```

Two jobs: `verify` (via `workflow_call`), then `release` with
`needs: verify` and `permissions: contents: write`.

### Step 1 — version consistency gate

The tag and the changelog must agree. Tag `v0.1.0` requires a `## [0.1.0]`
section in `CHANGELOG.md`; extract that section's body with `awk` and **fail
the job if it is empty**. This is what prevents a Release shipping with no
notes, and it catches the common mistake of tagging before writing the
changelog entry.

### Step 2 — create the Release

```bash
gh release create "$GITHUB_REF_NAME" \
  --title "$GITHUB_REF_NAME" \
  --notes-file notes.md \
  $PRERELEASE_FLAG
```

`PRERELEASE_FLAG` is `--prerelease` when the tag contains a `-`, so
`v0.1.0-rc.1` is marked correctly without a second code path.

`GITHUB_TOKEN` is sufficient — this is a same-repository release. No PAT, no
GitHub App, no organization secret.

---

## 6. Failure modes and what actually protects against them

| Failure | Protection |
| --- | --- |
| Tag points at broken code | `needs: verify` — a red tag creates **no Release** |
| Tag exists anyway after a red build | Not preventable. Documented recovery below |
| Tag has no changelog entry | Version consistency gate fails the release job |
| Release ships before tests finish | `needs: verify` orders them |
| Lint drift between CI and release | Single `verify.yml` via `workflow_call` |

### The residual risk, stated plainly

Because the tag is pushed by hand, **it exists before any gate runs**. The
workflow can withhold the Release; it cannot withhold the tag. Recovery is to
delete the tag immediately (`git push --delete origin v0.1.0`), fix, and
re-tag — and that only works within the first few minutes, before
`proxy.golang.org` has been asked for the version by anything. After that the
number is permanently consumed and the fix ships as `v0.1.1`.

This is inherent to manual tagging and is accepted. The mitigations are CI
proving `main` green continuously and `make verify` before tagging, which
together mean the release gates should never be the first thing to discover a
problem.

---

## 7. Repository settings (manual, outside this change)

These cannot be committed and may need BC Gov organization admin:

- **Branch protection on `main`** requiring the `lint`, `test`, and
  `integration` checks.
- **Tag protection rule on `v*`** so only maintainers can create a release
  tag.
- Default workflow permissions left at read-only; `release.yml` raises them
  at the job level.

Actions are pinned by **version tag** (e.g. `@v5`), not by full commit SHA.

---

## 8. Testing this configuration

Workflow configuration has no unit tests, and the first real exercise of
`release.yml` must not be `v0.1.0` — a broken release workflow discovered
there costs the version number.

1. **`actionlint`** over all three files. Catches schema errors, bad
   expressions, and invalid `needs` references without pushing anything.
2. **Open a PR** with the workflows to prove `ci.yml` triggers and all three
   jobs pass on real code.
3. **Cut a throwaway `v0.0.1-rc.1` tag** to exercise `release.yml`
   end to end: verify the gates run, the changelog extraction produces the
   right body, and the Release is marked prerelease. Then delete the tag and
   the Release. Burning an rc number costs nothing.
4. **Deliberately break the changelog gate once** — tag a version with no
   matching `CHANGELOG.md` section and confirm the job fails rather than
   publishing an empty Release.

Only after step 4 passes, tag `v0.1.0`.

### Concurrency caveat

The `concurrency` group must not cancel in-progress **release** runs. A
cancelled release run leaves a tag with no Release — recoverable, but
confusing. Scope cancellation to `ci.yml`, or key the group so tag refs never
collide.

---

## 9. Documentation obligations

- **`CLAUDE.md`** — "There is no CI yet; run lint and all three test tiers
  locally" is false once this lands. Replace with a description of the three
  jobs and `make verify`. `CLAUDE.md` itself notes that past review rounds
  repeatedly caught docs claiming untrue behaviour.
- **`README.md`** — CI badge, and a short "Releasing" section: update
  `CHANGELOG.md`, `make verify`, tag, push.
- **`CHANGELOG.md`** — seeded with a `[0.1.0]` section covering phases 0–4,
  written from the existing commit history since there are no PRs to
  summarize.

---

## 10. Implementation order

1. `Makefile`: `GOTESTFLAGS` + `verify` target. Verify locally that
   `make verify` runs all four gates and that plain `make test` is unchanged.
2. `CHANGELOG.md` seeded with `[0.1.0]`.
3. `.github/workflows/verify.yml`.
4. `.github/workflows/ci.yml`; confirm green on a PR.
5. `.github/workflows/release.yml`; exercise per §8 with `v0.0.1-rc.1`.
6. Docs: `CLAUDE.md`, `README.md`.
7. Repository settings per §7.
8. Tag `v0.1.0`.

---

## 11. Risks

| Risk | Mitigation |
| --- | --- |
| Integration tier is flaky or slow on hosted runners under `-race` | Measure during step 4 of §8. If unreliable, it becomes a required check on `main` only and stays a release gate — never dropped from the release gate |
| `golangci-lint` version pin goes stale | Dependabot for `github-actions` could be added later; out of scope here |
| BC Gov org policy blocks the Actions permissions or the release token | Discovered at step 2 of §8, before any tag exists |
| `v0.1.0` proves premature and the API changes immediately | That is exactly what `v0.x` is for; cost is a minor bump |
