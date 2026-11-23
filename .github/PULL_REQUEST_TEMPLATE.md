## Summary
<!-- What does this change and why? -->

## Changes
<!-- Bullet the concrete changes. -->

## Testing
- [ ] `ctest --test-dir build` green (unit + integration + concurrency + chaos)
- [ ] `scripts/checks/smokeTest.sh` passes
- [ ] ASan/UBSan run for memory-touching changes
- [ ] TSan run for concurrency-touching changes
- [ ] New invariants have tests (`docs/invariants.md` updated)

## Notes
<!-- Tradeoffs, follow-ups, and anything this change does not cover. -->
