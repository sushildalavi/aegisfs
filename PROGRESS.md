# AegisFS — PROGRESS

Living log of real commands, outcomes, failures, and next steps. Newest notes at the bottom
of each milestone. Commit baseline: **UNBORN** (new repository). Commits are counted from the
first project commit forward.

## Environment (verified 2026-11-22)
- Host: macOS (darwin 27.2), Apple clang 21 → C++20.
- `cmake` 4.5.1 + `ninja` 1.13.2 installed via Homebrew (user-owned `/opt/homebrew`, no sudo).
- OpenSSL@3 present (`/opt/homebrew/opt/openssl@3`) → SHA-256.
- **docker NOT installed** → multi-node cluster runs as process-local nodes on distinct ports.
  Docker Compose scenarios are marked NOT RUN. (`BLOCKER: DOCKER_UNAVAILABLE`, non-fatal.)
- GitHub CLI authenticated as **`sushildalavi`** (id 75267758) → identity verified, private repo
  `sushildalavi/aegisfs` created, remote `origin` set.
- Local git identity set: `Sushil Dalavi <75267758+sushildalavi@users.noreply.github.com>`.

## Milestone plan (status)
| M | Scope | Status |
|---|-------|--------|
| M0 | Skeleton: CMake, docs, basic exe, smoke test | in_progress |
| M1 | Node identity, framed TCP protocol, CLI, storage engine | todo |
| M2 | Membership + hash ring + successor placement | todo |
| M3 | Replicated create/get, checksums, idempotency, streaming | todo |
| M4 | Append segments, ordering, read-your-writes, merge, anti-entropy | todo |
| M5 | Failure detection, re-replication, restart reconcile, rebalance | todo |
| M6 | Concurrency, fault injection, 10-node demo, stress, sanitizers, CI | todo |
| M7 | Benchmarks, charts, docs, interview guide, resume claims | todo |

---

## M0 — Skeleton
Goal: a buildable repo with a real `aegisfs` executable, docs skeleton, unit-test harness, CI,
and a single-node smoke test. Target ~10 commits.

### Log
- Created repo meta (.gitignore, LICENSE, CLAUDE.md, build-status.json, this file).
- Next: architecture/invariants/tradeoffs docs, CMake skeleton, version header, `aegisfs` main
  with arg parsing + `help`/`version`, GoogleTest FetchContent + first unit test, CI workflow,
  smoke test script.
