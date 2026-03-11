%global crate foxing
%global version 0.6.1

Name:           foxing
Version:        %{version}
Release:        6%{?dist}
Summary:        eBPF-powered filesystem replication daemon and smart copy tool
License:        GPL-2.0-or-later
URL:            https://codeberg.org/aenertia/foxing
Source0:        %{url}/archive/v%{version}.tar.gz#/%{crate}-%{version}.tar.gz
Source1:        %{crate}-%{version}-vendor.tar.gz

BuildRequires:  clang
BuildRequires:  llvm
BuildRequires:  libbpf-devel
BuildRequires:  bpftool
BuildRequires:  elfutils-libelf-devel
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
BuildRequires:  systemd-rpm-macros
BuildRequires:  curl
# Rust nightly installed via rustup during %%build

ExclusiveArch:  x86_64 aarch64 ppc64le s390x
# riscv64 excluded: libbpf bpf_tracing.h lacks RISC-V register definitions for BPF_KPROBE

# Disable debuginfo/debugsource — binaries are stripped by cargo
%global debug_package %{nil}

%description
Foxing is an eBPF-powered filesystem replication system. It provides
foxingd, a BPF-based replication daemon, and fxcp, a smart filesystem
copy tool with CoW/reflink and io_uring support.

# ---- Subpackage: fxcp ----
%package -n fxcp
Summary:        Smart filesystem copy tool with CoW/reflink support
Requires:       glibc

%description -n fxcp
fxcp is a smart filesystem copy tool — an rsync replacement with
CoW/reflink, io_uring, BLAKE3 delta detection, and adaptive tuning.
Does not require root or BPF.

# ---- Subpackage: foxingd ----
%package -n foxingd
Summary:        eBPF filesystem replication daemon
Conflicts:      fxcp
Requires:       openssl-libs
Requires:       elfutils-libelf
Requires:       zlib
Requires:       libzstd
Requires:       libbpf
Requires:       bpftool
Requires(pre):  systemd
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd

%description -n foxingd
foxingd is an eBPF-powered filesystem replication daemon that watches
filesystem events via BPF kprobes and replicates changes to one or more
targets with adaptive tuning, versioning, and Prometheus metrics.
foxingd is a superset of fxcp — it provides fxcp via symlink dispatch.
Requires Linux kernel 6.12+ with BTF support.

%prep
%autosetup -n %{crate}-%{version} -p1
tar xf %{SOURCE1}
mkdir -p .cargo
cat > .cargo/config.toml << 'EOF'
[source.crates-io]
replace-with = "vendored-sources"

[source.vendored-sources]
directory = "vendor"
EOF

%build
export RUSTUP_HOME=%{_builddir}/.rustup
export CARGO_HOME=%{_builddir}/.cargo-home
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain nightly --profile minimal --no-modify-path
export PATH="$CARGO_HOME/bin:$PATH"
rustup component add rustfmt

# Override Fedora RUSTFLAGS which set -Cstrip=none and specs that
# may not be compatible with nightly Rust
FOXING_RUSTFLAGS="-Copt-level=3 -Cforce-frame-pointers=yes"
# io-uring prebuilt bindings are x86_64/aarch64 only; on s390x/ppc64le
# the struct layouts are identical (io_uring is arch-independent), so
# skip the arch check rather than using bindgen (which needs newer headers)
%ifarch s390x ppc64le
FOXING_RUSTFLAGS="$FOXING_RUSTFLAGS --cfg=io_uring_skip_arch_check"
%endif
export RUSTFLAGS="$FOXING_RUSTFLAGS"

cargo build --release --workspace --offline

# Generate man pages and shell completions
cargo run -p xtask --offline -- all

%install
install -Dpm 755 target/release/fxcp %{buildroot}%{_bindir}/fxcp
install -Dpm 755 target/release/foxingd %{buildroot}%{_bindir}/foxingd
install -Dpm 644 config.toml.example %{buildroot}%{_sysconfdir}/foxing/config.toml.example
install -Dpm 644 dist/systemd/foxingd.service %{buildroot}%{_unitdir}/foxingd.service
install -Dpm 644 dist/systemd/foxingd-sysusers.conf %{buildroot}%{_sysusersdir}/foxingd.conf
install -Dpm 644 dist/systemd/foxingd-tmpfiles.conf %{buildroot}%{_tmpfilesdir}/foxingd.conf
# Man pages
install -Dpm 644 dist/man/fxcp.1 %{buildroot}%{_mandir}/man1/fxcp.1
install -Dpm 644 dist/man/foxingd.1 %{buildroot}%{_mandir}/man1/foxingd.1
# Bash completions
install -Dpm 644 dist/completions/fxcp.bash %{buildroot}%{_datadir}/bash-completion/completions/fxcp
install -Dpm 644 dist/completions/foxingd.bash %{buildroot}%{_datadir}/bash-completion/completions/foxingd
# Zsh completions
install -Dpm 644 dist/completions/fxcp.zsh %{buildroot}%{_datadir}/zsh/site-functions/_fxcp
install -Dpm 644 dist/completions/foxingd.zsh %{buildroot}%{_datadir}/zsh/site-functions/_foxingd
# Fish completions
install -Dpm 644 dist/completions/fxcp.fish %{buildroot}%{_datadir}/fish/vendor_completions.d/fxcp.fish
install -Dpm 644 dist/completions/foxingd.fish %{buildroot}%{_datadir}/fish/vendor_completions.d/foxingd.fish

%pre -n foxingd
%sysusers_create_compat dist/systemd/foxingd-sysusers.conf

%post -n foxingd
%systemd_post foxingd.service
%tmpfiles_create foxingd.conf
# Create fxcp symlink (foxingd is a superset)
ln -sf foxingd %{_bindir}/fxcp 2>/dev/null || :

%preun -n foxingd
%systemd_preun foxingd.service
# Remove fxcp symlink if it points to foxingd
if [ "$(readlink %{_bindir}/fxcp 2>/dev/null)" = "foxingd" ]; then
    rm -f %{_bindir}/fxcp
fi

%postun -n foxingd
%systemd_postun_with_restart foxingd.service

%files -n fxcp
%license LICENSE
%doc README.md
%{_bindir}/fxcp
%{_mandir}/man1/fxcp.1*
%{_datadir}/bash-completion/completions/fxcp
%{_datadir}/zsh/site-functions/_fxcp
%{_datadir}/fish/vendor_completions.d/fxcp.fish

%files -n foxingd
%license LICENSE
%doc README.md config.toml.example
%{_bindir}/foxingd
%ghost %{_bindir}/fxcp
%dir %{_sysconfdir}/foxing
%config(noreplace) %{_sysconfdir}/foxing/config.toml.example
%{_unitdir}/foxingd.service
%{_sysusersdir}/foxingd.conf
%{_tmpfilesdir}/foxingd.conf
%{_mandir}/man1/foxingd.1*
%{_mandir}/man1/fxcp.1*
%{_datadir}/bash-completion/completions/foxingd
%{_datadir}/bash-completion/completions/fxcp
%{_datadir}/zsh/site-functions/_foxingd
%{_datadir}/zsh/site-functions/_fxcp
%{_datadir}/fish/vendor_completions.d/foxingd.fish
%{_datadir}/fish/vendor_completions.d/fxcp.fish

%changelog
* Wed Mar 11 2026 Joel Wirāmu Pauling <aenertia@aenertia.net> - 0.6.1-1
- Expand architecture support: ppc64le, s390x, riscv64
- Fix aarch64 build: portable pointer types in btrfs ioctls
- Add fxcp symlink in foxingd package
- Add man pages, shell completions (bash/zsh/fish)
- Stripped release builds with thin LTO

* Tue Mar 10 2026 Joel Wirāmu Pauling <aenertia@aenertia.net> - 0.6.0-1
- Initial package
