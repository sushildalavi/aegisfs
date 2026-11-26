#!/usr/bin/env bash
# Runs the checks that should pass before a change is merged: a build, the full ctest suite,
# the Python tests, the smoke test, the 10-node demo, the real-process reliability run, and a check
# that the recorded benchmark data is present. Exits nonzero if any step fails.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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

hdr "Python extension tests"
if python3 tests/extensions/testExtensions.py >/tmp/aegis_pytest.log 2>&1; then
  pass "python tests passed"
else
  fail "python tests failed (see /tmp/aegis_pytest.log)"; tail -8 /tmp/aegis_pytest.log
fi

hdr "Smoke test (3-node e2e)"
if ./scripts/checks/smokeTest.sh >/tmp/aegis_smoke.log 2>&1; then pass "smokeTest.sh"; else fail "smokeTest.sh"; tail -5 /tmp/aegis_smoke.log; fi

hdr "10-node demo (fault injection)"
if ./scripts/demo.sh >/tmp/aegis_demo.log 2>&1; then pass "demo.sh (10-node, 2-node failure, re-replication)"; else fail "demo.sh"; tail -8 /tmp/aegis_demo.log; fi

hdr "Reliability test (real-process two-failure + malformed frame)"
./scripts/checks/reliabilityTest.sh >/tmp/aegis_reliability.log 2>&1
rc=$?
if [ "$rc" -eq 0 ]; then pass "reliabilityTest.sh"
elif [ "$rc" -eq 77 ]; then note "reliabilityTest.sh skipped: $(tail -1 /tmp/aegis_reliability.log)"
else fail "reliabilityTest.sh"; tail -8 /tmp/aegis_reliability.log
fi

hdr "Benchmark evidence"
if [ -s benchmarks/results/raw.csv ] && [ -s benchmarks/results/summary.csv ]; then
  rows=$(($(wc -l < benchmarks/results/raw.csv) - 1))
  pass "benchmark raw+summary present ($rows raw data points)"
else
  fail "benchmark results missing (run benchmarks/runMatrix.sh)"
fi

hdr "RESULT"
echo "  PASS=$PASS  FAIL=$FAIL"
if [ "$FAIL" -eq 0 ]; then echo "  all checks passed"; exit 0
else echo "  $FAIL check(s) failed"; exit 1; fi
