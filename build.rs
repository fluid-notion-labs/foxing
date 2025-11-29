// File: foxing/build.rs | Index: 2 of 21 | Function: Build script for compiling eBPF C code into Rust skeletons.
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
    
    // Compile the BPF code into a Rust skeleton
    SkeletonBuilder::new()
        .source(bpf_src)
        .clang_args(["-I", "src/bpf", "-D__TARGET_ARCH_x86"])
        .build_and_generate(&out)
        .expect("BPF compilation failed");
        
    println!("cargo:rerun-if-changed={}", bpf_src);
}
