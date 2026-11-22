# Convenience wrapper around the CMake/Ninja build. The canonical build is CMake; this just
# shortens the common commands.
BUILD ?= build
GEN ?= Ninja
TYPE ?= RelWithDebInfo

.PHONY: all build configure test smoke demo bench asan tsan clean verify

all: build

configure:
	cmake -S . -B $(BUILD) -G $(GEN) -DCMAKE_BUILD_TYPE=$(TYPE)

build: configure
	cmake --build $(BUILD)

test: build
	ctest --test-dir $(BUILD) --output-on-failure

smoke: build
	scripts/smoke_test.sh

demo: build
	scripts/demo.sh

bench: build
	benchmarks/runner/bench.sh

asan:
	cmake -S . -B build-asan -G $(GEN) -DCMAKE_BUILD_TYPE=Debug -DAEGISFS_ENABLE_ASAN=ON
	cmake --build build-asan
	ctest --test-dir build-asan --output-on-failure

tsan:
	cmake -S . -B build-tsan -G $(GEN) -DCMAKE_BUILD_TYPE=Debug -DAEGISFS_ENABLE_TSAN=ON
	cmake --build build-tsan --target concurrency_test membership_test
	./build-tsan/tests/concurrency_test

verify: build
	scripts/verify_definition_of_done.sh

clean:
	rm -rf $(BUILD) build-asan build-tsan run run-bench
