# Contributing

Bug reports and patches are welcome. The short version: keep it building, keep the tests green,
and say plainly what a change does and does not cover.

## Setup

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build
ctest --test-dir build --output-on-failure
```

On macOS: `brew install cmake ninja openssl@3`. On Debian or Ubuntu:
`sudo apt-get install cmake ninja-build clang libssl-dev`. `scripts/bootstrap.sh` does either.

## Before opening a pull request

1. `ctest --test-dir build` passes.
2. `scripts/checks/smokeTest.sh` passes.
3. Changes to concurrent code: build with `-DAEGISFS_ENABLE_TSAN=ON` and run the affected tests
   (`scripts/checks/sanitizerTest.sh tsan`).
4. Changes that touch memory handling: `scripts/checks/sanitizerTest.sh asan`.
5. Format with `clang-format` (the config is in `.clang-format`).

`make verify` runs the full set of checks in one go.

## Commit messages

Write a short summary line in plain English that says what changed, then a paragraph on why if the
reason is not obvious. One logical change per commit.

## Tests and benchmarks

- A new invariant gets a test, and a row in `docs/invariants.md`.
- Performance claims need numbers from `benchmarks/`, taken on the same machine and with the same
  input before and after. `benchmarks/compareBuilds.py` does that comparison for 100 MiB creates.
  Report results that did not help as well as ones that did.
- Prefer real nodes over mocks when testing distributed behaviour.

## Layout

| Path | Contents |
|---|---|
| `src/`, `include/aegisfs/` | the node, client and library code |
| `tests/` | unit, integration, concurrency and chaos tests, plus Python tests for the extensions |
| `scripts/` | cluster launcher and demo; `scripts/checks/` holds the test and verification scripts |
| `benchmarks/` | benchmark scripts and recorded results |
| `deploy/docker/` | Dockerfiles and compose file |
| `extensions/` | REST gateway and Prometheus/Grafana setup |
| `docs/` | architecture, invariants, tradeoffs, operations, benchmarks, design FAQ |
