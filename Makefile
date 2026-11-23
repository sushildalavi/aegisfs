# Convenience wrapper around the CMake/Ninja build. The canonical build is CMake; this just
# shortens the common commands.
BUILD ?= build
GEN ?= Ninja
TYPE ?= RelWithDebInfo

.PHONY: all build configure test test-python smoke demo bench asan tsan reliability compose faults clean verify

all: build

configure:
	cmake -S . -B $(BUILD) -G $(GEN) -DCMAKE_BUILD_TYPE=$(TYPE)

build: configure
	cmake --build $(BUILD)

test: build test-python
	ctest --test-dir $(BUILD) --output-on-failure

test-python:
	python3 tests/python/testExtensions.py
	python3 tests/python/testOperationHistory.py

smoke: build
	scripts/checks/smokeTest.sh

demo: build
	scripts/demo.sh

bench: build
	benchmarks/runMatrix.sh

asan:
	scripts/checks/sanitizerTest.sh asan

tsan:
	scripts/checks/sanitizerTest.sh tsan

reliability: build
	scripts/checks/reliabilityTest.sh

verify: build
	scripts/checks/verifyAll.sh

clean:
	rm -rf $(BUILD) build-asan build-tsan run run-bench

compose:
	scripts/checks/composeSmokeTest.sh

faults:
	scripts/checks/networkFaultTest.sh
