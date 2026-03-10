// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// foxingd/build.rs — Build script — libbpf-cargo BPF skeleton generation

//! Cargo build script that compiles mirror.bpf.c via libbpf-cargo
//! and generates Rust skeleton bindings for BPF program interaction.

use libbpf_cargo::SkeletonBuilder;
use std::{env, path::PathBuf, process::Command, fs};

fn main() {
    let bpf_src = "src/bpf/mirror.bpf.c";
    let vmlinux = "src/bpf/vmlinux.h";
    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("mirror.skel.rs");

    // Ensure vmlinux.h exists using bpftool if not present
    if !std::path::Path::new(vmlinux).exists() {
        // Attempt to dump vmlinux using bpftool
        let output = Command::new("bpftool")
            .args(&["btf", "dump", "file", "/sys/kernel/btf/vmlinux", "format", "c"])
            .output()
            .expect("Failed to execute bpftool. Is it installed and available at /sys/kernel/btf/vmlinux?");
            
        if !output.status.success() {
            panic!("bpftool failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        fs::write(vmlinux, output.stdout).expect("Failed to write vmlinux.h");
    }
    
    // Detect target architecture for BPF compilation
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let bpf_arch = match arch.as_str() {
        "x86_64" | "x86" => "__TARGET_ARCH_x86",
        "aarch64"        => "__TARGET_ARCH_arm64",
        "arm"            => "__TARGET_ARCH_arm",
        "riscv64"        => "__TARGET_ARCH_riscv",
        "powerpc64"      => "__TARGET_ARCH_powerpc",
        "s390x"          => "__TARGET_ARCH_s390",
        "loongarch64"    => "__TARGET_ARCH_loongarch",
        other => panic!("Unsupported BPF target architecture: {other}"),
    };

    // Compile the BPF code into a Rust skeleton
    SkeletonBuilder::new()
        .source(bpf_src)
        .clang_args(["-I", "src/bpf", &format!("-D{bpf_arch}")])
        .build_and_generate(&out)
        .expect("BPF compilation failed");
        
    println!("cargo:rerun-if-changed={}", bpf_src);
}
