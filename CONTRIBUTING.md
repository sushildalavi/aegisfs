# Contributing

Bug reports and patches are welcome. Keep it building, keep the tests green, and say plainly what a
change does and does not cover.

## Setup

Build as described in the [README](README.md#build). `scripts/bootstrap.sh` installs the packages
with Homebrew or `apt-get` and builds.

## Before opening a pull request

1. `ctest --test-dir build` passes.
2. `scripts/checks/smokeTest.sh` passes.
3. Changes to concurrent code: `scripts/checks/sanitizerTest.sh tsan`.
4. Changes that touch memory handling: `scripts/checks/sanitizerTest.sh asan`.
5. Format new C++ code in the style set by `.clang-format`.

`make verify` runs a build, the C++ and extension tests, the smoke test, the demo and the
reliability test.

## Commit messages

Write a short summary line in plain English that says what changed, then a paragraph on why if the
reason is not obvious. One logical change per commit.

## Tests and benchmarks

- A new invariant gets a test and a row in [docs/invariants.md](docs/invariants.md).
- Performance claims need numbers from `benchmarks/`, taken on the same machine and with the same
  input before and after. `benchmarks/compareBuilds.py` does that comparison for 100 MiB creates.
  Report results that did not help as well as ones that did.
- Prefer real nodes over mocks when testing distributed behaviour.

## Naming

C++, shell, file and directory names use camelCase, with PascalCase for C++ types. Python follows
the existing code: camelCase module names and snake_case functions. Names fixed by a tool or a
protocol keep their required form.
