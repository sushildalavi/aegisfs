# Contributing to AegisFS

Thanks for your interest. AegisFS is an original C++20 distributed file system; contributions
should keep it buildable, tested, and honest about what works.

## Development setup
```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build
ctest --test-dir build --output-on-failure
```
On macOS, install deps with Homebrew: `brew install cmake ninja openssl@3`.

## Before you open a PR
1. `ctest --test-dir build` is green (unit + integration + concurrency + chaos).
2. `scripts/smokeTest.sh` passes.
3. For concurrency-touching changes, build with `-DAEGISFS_ENABLE_TSAN=ON` and run the affected
   tests under ThreadSanitizer.
4. For memory-touching changes, build with `-DAEGISFS_ENABLE_ASAN=ON`.
5. Run `clang-format` (config in `.clang-format`).

## Commit style
Conventional commits: `feat(scope): …`, `fix(scope): …`, `test(scope): …`, `docs(scope): …`,
`perf(scope): …`, `ops(scope): …`, `chore: …`. One coherent logical change per commit. Do not add
AI/co-author trailers.

## Testing philosophy
- Every new invariant gets a test (see `docs/invariants.md`).
- No fabricated output or benchmark numbers — measure, then report, including negative results.
- Prefer real process/in-process clusters over mocks for distributed behavior.

## Where things live
- Core library: `src/`, `include/aegisfs/`.
- Tests: `tests/{unit,integration,concurrency,chaos}/`.
- Ops scripts: `scripts/`. Benchmarks: `benchmarks/`. Design docs: `docs/`.
