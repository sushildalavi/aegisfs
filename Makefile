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
	python3 tests/python/test_extensions.py
	python3 tests/python/test_operation_history.py

smoke: build
	scripts/checks/smoke-test.sh

demo: build
	scripts/demo.sh

bench: build
	benchmarks/run-matrix.sh

asan:
	scripts/checks/sanitizer-test.sh asan

tsan:
	scripts/checks/sanitizer-test.sh tsan

reliability: build
	scripts/checks/reliability-test.sh

verify: build
	scripts/checks/verify-all.sh

clean:
	rm -rf $(BUILD) build-asan build-tsan run run-bench
