#!/usr/bin/env bash
# Build and run the sanitizer-focused C++ tests. The command fails on a real sanitizer
# finding and reports a clear skip when the compiler/runtime does not support the mode.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-asan}"
case "$MODE" in
  asan)
    BUILD="$ROOT/build-asan"
    OPTION="-DAEGISFS_ENABLE_ASAN=ON"
    FILTER='HashTest|RingTest|ProtocolTest|ManifestTest|StorageFixture|MembershipTest|CommonTest|MetricsTest|ThreadPoolTest'
    ;;
  tsan)
    BUILD="$ROOT/build-tsan"
    OPTION="-DAEGISFS_ENABLE_TSAN=ON"
    FILTER='Concurrency|Stress|Chaos|MembershipTest'
    ;;
  *)
    echo "usage: $0 {asan|tsan}" >&2
    exit 2
    ;;
esac

if ! cmake -S "$ROOT" -B "$BUILD" -G Ninja -DCMAKE_BUILD_TYPE=Debug "$OPTION"; then
  echo "SKIP: $MODE configuration is unavailable on this toolchain"
  exit 77
fi
if ! cmake --build "$BUILD" -j2; then
  echo "$MODE build failed"
  exit 1
fi
ctest --test-dir "$BUILD" --output-on-failure -R "$FILTER"
