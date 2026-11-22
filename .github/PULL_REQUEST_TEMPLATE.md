## Summary
<!-- What does this change and why? -->

## Changes
<!-- Bullet the concrete changes. -->

## Testing
- [ ] `ctest --test-dir build` green (unit + integration + concurrency + chaos)
- [ ] `scripts/smoke_test.sh` passes
- [ ] ASan/UBSan run for memory-touching changes
- [ ] TSan run for concurrency-touching changes
- [ ] New invariants have tests (`docs/INVARIANTS.md` updated)

## Notes
<!-- Tradeoffs, follow-ups, limitations. Be honest about what is NOT covered. -->
