# Foxing Build Automation

VERSION ?= 0.7.0
DESTDIR ?=
PREFIX ?= /usr

.PHONY: all check build release dist dist-debug install install-deps clean test test-json test-quick test-baseline test-compare benchmark benchmark-report benchmark-baseline doc doc-open tarball man completions

all: check build

check:
	cargo check --workspace --all-targets

build:
	cargo build --workspace

release:
	cargo build --release --workspace

dist: release

dist-debug:
	cargo build --profile release-debug --workspace

man:
	cargo run -p xtask -- man

completions:
	cargo run -p xtask -- completions

install: dist man completions
	install -Dm755 target/release/fxcp $(DESTDIR)$(PREFIX)/bin/fxcp
	install -Dm755 target/release/foxingd $(DESTDIR)$(PREFIX)/bin/foxingd
	install -Dm644 config.toml.example $(DESTDIR)/etc/foxing/config.toml.example
	install -Dm644 dist/systemd/foxingd.service $(DESTDIR)/usr/lib/systemd/system/foxingd.service
	install -Dm644 dist/systemd/foxingd-sysusers.conf $(DESTDIR)/usr/lib/sysusers.d/foxingd.conf
	install -Dm644 dist/systemd/foxingd-tmpfiles.conf $(DESTDIR)/usr/lib/tmpfiles.d/foxingd.conf
	install -Dm644 dist/man/fxcp.1 $(DESTDIR)$(PREFIX)/share/man/man1/fxcp.1
	install -Dm644 dist/man/foxingd.1 $(DESTDIR)$(PREFIX)/share/man/man1/foxingd.1
	install -Dm644 dist/completions/fxcp.bash $(DESTDIR)$(PREFIX)/share/bash-completion/completions/fxcp
	install -Dm644 dist/completions/foxingd.bash $(DESTDIR)$(PREFIX)/share/bash-completion/completions/foxingd
	install -Dm644 dist/completions/fxcp.zsh $(DESTDIR)$(PREFIX)/share/zsh/site-functions/_fxcp
	install -Dm644 dist/completions/foxingd.zsh $(DESTDIR)$(PREFIX)/share/zsh/site-functions/_foxingd
	install -Dm644 dist/completions/fxcp.fish $(DESTDIR)$(PREFIX)/share/fish/vendor_completions.d/fxcp.fish
	install -Dm644 dist/completions/foxingd.fish $(DESTDIR)$(PREFIX)/share/fish/vendor_completions.d/foxingd.fish

tarball:
	git archive --format=tar.gz --prefix=foxing-$(VERSION)/ HEAD > foxing-$(VERSION).tar.gz

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

doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open
