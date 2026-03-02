# Foxing Build Automation

.PHONY: all check build release install-deps clean test test-json test-quick test-baseline test-compare

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
