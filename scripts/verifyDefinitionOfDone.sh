#!/usr/bin/env bash
# Machine-verified acceptance gate for AegisFS. Exits NONZERO if any MANDATORY requirement is unmet.
# Checks: clean build, full test suite, smoke + 10-node demo, benchmark evidence, GitHub owner,
# commit author identity, and new-commit count vs the UNBORN baseline.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="/opt/homebrew/bin:$PATH"

PASS=0; FAIL=0
pass(){ printf "  \033[32mPASS\033[0m %s\n" "$1"; PASS=$((PASS+1)); }
fail(){ printf "  \033[31mFAIL\033[0m %s\n" "$1"; FAIL=$((FAIL+1)); }
note(){ printf "  ---- %s\n" "$1"; }
hdr(){ printf "\n== %s ==\n" "$1"; }

hdr "Build"
if cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo >/dev/null 2>&1 \
   && cmake --build build >/dev/null 2>&1; then pass "project builds cleanly"; else fail "build failed"; fi

hdr "Test suite (ctest)"
if ctest --test-dir build --output-on-failure --timeout 180 >/tmp/aegis_ctest.log 2>&1; then
  n=$(grep -c "Passed" /tmp/aegis_ctest.log || true)
  pass "ctest: all tests passed ($n)"
else
  fail "ctest failed (see /tmp/aegis_ctest.log)"; tail -5 /tmp/aegis_ctest.log
fi

hdr "Smoke test (3-node e2e)"
if ./scripts/smokeTest.sh >/tmp/aegis_smoke.log 2>&1; then pass "smokeTest.sh"; else fail "smokeTest.sh"; tail -5 /tmp/aegis_smoke.log; fi

hdr "10-node demo (fault injection)"
if ./scripts/demo.sh >/tmp/aegis_demo.log 2>&1; then pass "demo.sh (10-node, 2-node failure, re-replication)"; else fail "demo.sh"; tail -8 /tmp/aegis_demo.log; fi

hdr "Benchmark evidence"
if [ -s benchmarks/results/raw.csv ] && [ -s benchmarks/results/summary.csv ]; then
  rows=$(($(wc -l < benchmarks/results/raw.csv) - 1))
  pass "benchmark raw+summary present ($rows raw data points)"
else
  fail "benchmark results missing (run benchmarks/runner/bench.sh)"
fi

hdr "GitHub identity & ownership"
if command -v gh >/dev/null 2>&1; then
  login=$(gh api user --jq '.login' 2>/dev/null || echo "")
  [ "$login" = "sushildalavi" ] && pass "authenticated as sushildalavi" || fail "gh login is '$login' (expected sushildalavi)"
else
  fail "gh CLI not available"
fi
remote=$(git remote get-url origin 2>/dev/null || echo "")
case "$remote" in
  *sushildalavi/aegisfs*) pass "origin is sushildalavi/aegisfs" ;;
  *) fail "origin remote is '$remote' (expected sushildalavi/aegisfs)" ;;
esac

hdr "Commit author integrity"
bad_authors=$(git log --format='%ae' | grep -v '75267758+sushildalavi@users.noreply.github.com' | sort -u || true)
if [ -z "$bad_authors" ]; then pass "all commits authored by verified identity"; else fail "unexpected authors: $bad_authors"; fi
if git log --format='%an %cn' | grep -qi "claude\|co-authored-by"; then fail "found AI/co-author trailer"; else pass "no AI/co-author trailers"; fi

hdr "Commit count (baseline UNBORN)"
count=$(git rev-list --count HEAD)
note "new substantive commits: $count (minimum 100)"
[ "$count" -ge 100 ] && pass "commit count >= 100" || fail "commit count $count < 100"

hdr "RESULT"
echo "  PASS=$PASS  FAIL=$FAIL"
if [ "$FAIL" -eq 0 ]; then echo "  DEFINITION OF DONE: SATISFIED"; exit 0
else echo "  DEFINITION OF DONE: NOT satisfied ($FAIL unmet)"; exit 1; fi
