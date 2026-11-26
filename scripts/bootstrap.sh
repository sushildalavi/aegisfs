#!/usr/bin/env bash
# Install the AegisFS toolchain and build the project. Idempotent; prefers user-level installs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== AegisFS bootstrap =="
OS="$(uname -s)"

installMac() {
  command -v brew >/dev/null || { echo "Homebrew required: https://brew.sh"; exit 1; }
  brew list cmake >/dev/null 2>&1 || brew install cmake
  brew list ninja >/dev/null 2>&1 || brew install ninja
  brew list openssl@3 >/dev/null 2>&1 || brew install openssl@3
}

installLinux() {
  if command -v apt-get >/dev/null; then
    sudo apt-get update
    sudo apt-get install -y cmake ninja-build clang libssl-dev
  else
    echo "Install cmake, ninja, clang, and libssl-dev with your package manager."; exit 1
  fi
}

case "$OS" in
  Darwin) installMac ;;
  Linux)  installLinux ;;
  *) echo "unsupported OS: $OS"; exit 1 ;;
esac

echo "== Configure + build =="
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build
echo "== Done. Try: scripts/checks/smokeTest.sh =="
