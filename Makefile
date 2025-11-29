# Foxing Build Automation

.PHONY: all check build release install-deps clean

all: check build

check:
	cargo check --workspace --all-targets

build:
	cargo build

release:
	cargo build --release

install-deps:
	sudo dnf install -y clang llvm libbpf-devel bpftool cargo

clean:
	cargo clean
