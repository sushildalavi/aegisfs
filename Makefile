# Convenience wrapper around the CMake/Ninja build. The canonical build is CMake; this just
# shortens the common commands.
BUILD ?= build
GEN ?= Ninja
TYPE ?= RelWithDebInfo

.PHONY: all build configure test test-python smoke demo bench asan tsan reliability clean verify

all: build

configure:
	cmake -S . -B $(BUILD) -G $(GEN) -DCMAKE_BUILD_TYPE=$(TYPE)

build: configure
	cmake --build $(BUILD)

test: build test-python
	ctest --test-dir $(BUILD) --output-on-failure

test-python:
	python3 tests/python/extensionTest.py
	python3 tests/python/operationHistoryTest.py

smoke: build
	scripts/smokeTest.sh

demo: build
	scripts/demo.sh

bench: build
	benchmarks/runner/bench.sh

asan:
	scripts/sanitizerTest.sh asan

tsan:
	scripts/sanitizerTest.sh tsan

reliability: build
	scripts/reliabilityTest.sh

verify: build
	scripts/verifyDefinitionOfDone.sh

clean:
	rm -rf $(BUILD) build-asan build-tsan run run-bench
