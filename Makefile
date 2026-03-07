# Foxing Build Automation

.PHONY: all check build release install-deps clean test test-json test-quick test-baseline test-compare benchmark benchmark-report benchmark-baseline

all: check build

check:
	cargo check --workspace --all-targets

build:
	cargo build --workspace

release:
	cargo build --release --workspace

install-deps:
	sudo dnf install -y clang llvm libbpf-devel bpftool cargo

clean:
	cargo clean

test:
	python3 tests/harness.py --human

test-json:
	python3 tests/harness.py

test-quick:
	python3 tests/harness.py --scale small --workload small_files,large_files --human

test-baseline:
	python3 tests/harness.py --save-baseline

test-compare:
	python3 tests/harness.py --compare tests/baseline.json

benchmark:
	python3 tests/harness.py --benchmark --iterations 3 --human

benchmark-quick:
	python3 tests/harness.py --benchmark --iterations 1 --scale small --human

benchmark-report:
	python3 tests/harness.py --benchmark --iterations 3 --report tests/benchmark-report.md --human

benchmark-baseline:
	python3 tests/harness.py --benchmark --iterations 5 --save-baseline
