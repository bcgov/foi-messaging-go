# Recreating a lost consumer group — plan

Spec: [`2026-09-23-recreate-lost-consumer-group-design.md`](../specs/2026-09-23-recreate-lost-consumer-group-design.md)

Branch: `fix/recreate-lost-consumer-group`

1. **`internal/redis`: classify `NOGROUP`.** Integration tests first: destroy the
   group, then `ReadNew`, `PendingOverIdle`, `Claim` each return
   `errors.Is(err, ErrNoGroup)`. Then add `ErrNoGroup` and wrap in all three.
   Commit `feat:`.
2. **`internal/watermill`: recreate from the read loop.** Unit test with a fake
   reader whose group can be dropped; assert `EnsureGroup` is called again, the
   WARN is logged, and the next entry is delivered. Implement `recreateGroup`.
   Commit `fix:`.
3. **`internal/watermill`: recreate from the claim loop.** Same for
   `PendingOverIdle`/`Claim`. Landed in the same `fix:` commit as step 2: both
   loops share `recreateGroup`, and the tests went red together.
4. **Root integration test.** Running consumer survives `XGROUP DESTROY` and
   `DEL` of the stream. Commit `test:`.
5. **Docs.** CHANGELOG `[Unreleased]`, CLAUDE.md consume-path invariants.
   Commit `docs:`. Run `make verify`.
