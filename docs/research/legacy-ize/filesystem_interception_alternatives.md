# Filesystem Interception Alternatives to FUSE

## Executive Summary

FUSE (Filesystem in Userspace) provides a powerful way to implement custom filesystems, but comes with significant overhead and complexity. This document explores alternative approaches for intercepting filesystem operations with minimal impact on the normal flow of operations.

The key alternatives are:

1. **eBPF/BPF** - Kernel-level tracing with minimal overhead
2. **fanotify** - Linux file access notification API
3. **inotify** - File system event monitoring (limited)
4. **audit subsystem** - Linux audit framework
5. **LD_PRELOAD** - Userspace library interposition
6. **ptrace** - Process tracing (high overhead)
7. **Kernel modules** - Direct kernel integration

---

## Comparison Matrix

| Approach | Overhead | Write Capture | Read Capture | Content Access | Requires Root | Complexity |
|----------|----------|---------------|--------------|----------------|---------------|------------|
| eBPF | Very Low | ✅ | ✅ | ⚠️ Limited | Yes | High |
| fanotify | Low | ✅ | ✅ | ✅ Yes | Yes | Medium |
| inotify | Low | ✅ Events only | ❌ | ❌ | No | Low |
| audit | Low | ✅ | ✅ | ❌ | Yes | Medium |
| LD_PRELOAD | Medium | ✅ | ✅ | ✅ Yes | No | Medium |
| ptrace | High | ✅ | ✅ | ✅ Yes | No* | High |
| Kernel module | Very Low | ✅ | ✅ | ✅ Yes | Yes | Very High |
| FUSE | High | ✅ | ✅ | ✅ Yes | No* | High |

---

## 1. eBPF (Extended Berkeley Packet Filter)

### Overview

eBPF allows running sandboxed programs in the Linux kernel without modifying kernel source code or loading kernel modules. Originally designed for packet filtering, it has evolved into a general-purpose kernel instrumentation framework.

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Application                          │
│                              │                                   │
│                         open("/file")                            │
│                              │                                   │
├──────────────────────────────┼───────────────────────────────────┤
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Kernel VFS Layer                         ││
│  │                          │                                  ││
│  │   ┌──────────────────────┼─────────────────────────┐       ││
│  │   │           eBPF Tracepoint/Kprobe               │       ││
│  │   │                      │                          │       ││
│  │   │    ┌─────────────────▼──────────────────┐      │       ││
│  │   │    │     eBPF Program (sandboxed)       │      │       ││
│  │   │    │   - Capture syscall args           │      │       ││
│  │   │    │   - Send to userspace ring buffer  │      │       ││
│  │   │    │   - Cannot modify data flow        │      │       ││
│  │   │    └─────────────────┬──────────────────┘      │       ││
│  │   │                      │                          │       ││
│  │   └──────────────────────┼─────────────────────────┘       ││
│  │                          ▼                                  ││
│  │                   Actual Filesystem                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│                         KERNEL SPACE                             │
└─────────────────────────────────────────────────────────────────┘
```

### Relevant eBPF Attachment Points

```c
// Tracepoints for file operations
tracepoint:syscalls:sys_enter_open
tracepoint:syscalls:sys_enter_openat
tracepoint:syscalls:sys_enter_read
tracepoint:syscalls:sys_enter_write
tracepoint:syscalls:sys_enter_close
tracepoint:syscalls:sys_enter_rename
tracepoint:syscalls:sys_enter_unlink
tracepoint:syscalls:sys_enter_truncate

// Kprobes for VFS layer
kprobe:vfs_read
kprobe:vfs_write
kprobe:vfs_open
kprobe:vfs_unlink
kprobe:vfs_rename

// LSM (Linux Security Module) hooks - newer kernels
lsm:file_open
lsm:file_permission
```

### Rust Implementation with `aya`

```rust
use aya::{
    programs::{TracePoint, KProbe},
    maps::perf::AsyncPerfEventArray,
    Bpf,
};
use aya_log::BpfLogger;
use tokio::signal;

// eBPF program (compiled separately)
// In: probe.bpf.c
/*
SEC("tracepoint/syscalls/sys_enter_write")
int trace_write(struct trace_event_raw_sys_enter* ctx) {
    struct write_event event = {};
    event.pid = bpf_get_current_pid_tgid() >> 32;
    event.fd = ctx->args[0];
    event.count = ctx->args[2];
    bpf_get_current_comm(&event.comm, sizeof(event.comm));
    bpf_perf_event_output(ctx, &events, BPF_F_CURRENT_CPU, &event, sizeof(event));
    return 0;
}
*/

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Load eBPF program
    let mut bpf = Bpf::load(include_bytes_aligned!("probe.bpf.o"))?;
    
    // Attach to tracepoint
    let program: &mut TracePoint = bpf.program_mut("trace_write").unwrap().try_into()?;
    program.load()?;
    program.attach("syscalls", "sys_enter_write")?;
    
    // Read events from perf buffer
    let mut perf_array = AsyncPerfEventArray::try_from(bpf.map_mut("events")?)?;
    
    for cpu_id in online_cpus()? {
        let mut buf = perf_array.open(cpu_id, None)?;
        
        tokio::spawn(async move {
            let mut buffers = (0..10)
                .map(|_| BytesMut::with_capacity(1024))
                .collect::<Vec<_>>();
            
            loop {
                let events = buf.read_events(&mut buffers).await.unwrap();
                for buf in buffers.iter().take(events.read) {
                    let event: WriteEvent = unsafe { ptr::read(buf.as_ptr() as *const _) };
                    println!("Write: pid={}, fd={}, count={}", 
                             event.pid, event.fd, event.count);
                }
            }
        });
    }
    
    signal::ctrl_c().await?;
    Ok(())
}
```

### Pros
- **Extremely low overhead** - runs in kernel, minimal context switches
- **No modification to application** - completely transparent
- **Safe** - eBPF verifier prevents crashes/hangs
- **Production ready** - used by major companies (Netflix, Facebook, Google)
- **Rich ecosystem** - bpftrace, bcc, aya (Rust)

### Cons
- **Cannot modify data** - observation only (mostly)
- **Limited content access** - can only read limited bytes from buffers
- **Requires root** - or CAP_BPF capability
- **Kernel version dependent** - features vary by kernel version
- **Complex development** - need to understand kernel internals

### Content Capture Limitations

eBPF can capture:
- File paths (from arguments)
- File descriptors
- Read/write offsets and sizes
- Process information

eBPF **cannot easily** capture:
- Full file contents (limited buffer reads)
- Data after encryption/compression

### Relevant Projects

- **[aya](https://github.com/aya-rs/aya)** - Pure Rust eBPF library
- **[bpftrace](https://github.com/iovisor/bpftrace)** - High-level tracing language
- **[bcc](https://github.com/iovisor/bcc)** - BPF Compiler Collection
- **[tracee](https://github.com/aquasecurity/tracee)** - Security event tracing

---

## 2. fanotify (File Access Notification)

### Overview

fanotify is a Linux API for file access notification. Unlike inotify, it can:
- Monitor entire mount points
- Provide file descriptors to the accessed files
- Block operations for permission decisions

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Application                          │
│                              │                                   │
│                         open("/file")                            │
│                              │                                   │
├──────────────────────────────┼───────────────────────────────────┤
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Kernel VFS Layer                         ││
│  │                          │                                  ││
│  │   ┌──────────────────────┼─────────────────────────┐       ││
│  │   │         fanotify hook                          │       ││
│  │   │                      │                          │       ││
│  │   │    ┌─────────────────▼──────────────────┐      │       ││
│  │   │    │   Event queued to fanotify fd      │      │       ││
│  │   │    │   (optionally blocks for response) │      │       ││
│  │   │    └─────────────────┬──────────────────┘      │       ││
│  │   │                      │                          │       ││
│  │   └──────────────────────┼─────────────────────────┘       ││
│  │                          ▼                                  ││
│  │                   Actual Filesystem                         ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                  │
├──────────────────────────────┼──────────────────────────────────┤
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              fanotify Listener (User Space)                 ││
│  │   - Receives file access events                             ││
│  │   - Gets fd to accessed file (can read content!)            ││
│  │   - Can allow/deny (FAN_OPEN_PERM, etc.)                    ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│                        USER SPACE                                │
└─────────────────────────────────────────────────────────────────┘
```

### Event Types

```c
// Notification events (non-blocking)
FAN_ACCESS          // File accessed (read)
FAN_MODIFY          // File modified (write)  
FAN_CLOSE_WRITE     // File closed after write
FAN_CLOSE_NOWRITE   // File closed (read-only)
FAN_OPEN            // File opened
FAN_OPEN_EXEC       // File opened for exec
FAN_ATTRIB          // Metadata changed
FAN_CREATE          // File/dir created
FAN_DELETE          // File/dir deleted
FAN_DELETE_SELF     // Watched file deleted
FAN_MOVED_FROM      // File moved from
FAN_MOVED_TO        // File moved to
FAN_RENAME          // File renamed

// Permission events (blocking - listener must respond)
FAN_OPEN_PERM       // Permission to open
FAN_OPEN_EXEC_PERM  // Permission to exec
FAN_ACCESS_PERM     // Permission to access
```

### Rust Implementation with `nix`

```rust
use nix::sys::fanotify::{
    Fanotify, FanotifyResponse, InitFlags, MarkFlags, MaskFlags,
    EventFlags, Response,
};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::fs::File;
use std::io::Read;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize fanotify
    let fan = Fanotify::init(
        InitFlags::FAN_CLASS_CONTENT | InitFlags::FAN_UNLIMITED_QUEUE,
        EventFlags::O_RDONLY | EventFlags::O_LARGEFILE,
    )?;
    
    // Mark a mount point for monitoring
    fan.mark(
        MarkFlags::FAN_MARK_ADD | MarkFlags::FAN_MARK_MOUNT,
        MaskFlags::FAN_MODIFY | MaskFlags::FAN_CLOSE_WRITE | 
        MaskFlags::FAN_OPEN | MaskFlags::FAN_ACCESS,
        None,  // No dirfd
        Some(Path::new("/home/user/watched")),
    )?;
    
    // Event loop
    let mut buffer = vec![0u8; 4096];
    loop {
        let events = fan.read_events(&mut buffer)?;
        
        for event in events {
            println!("Event: {:?}", event.mask());
            
            // Get file descriptor to the actual file!
            if let Some(fd) = event.fd() {
                // We can read the file content here
                let mut file = unsafe { File::from_raw_fd(fd.as_raw_fd()) };
                let mut content = String::new();
                
                if event.mask().contains(MaskFlags::FAN_CLOSE_WRITE) {
                    // File was modified - capture new content
                    file.read_to_string(&mut content)?;
                    println!("Modified content: {}", &content[..100.min(content.len())]);
                }
                
                // For permission events, must respond
                if event.mask().contains(MaskFlags::FAN_OPEN_PERM) {
                    fan.write_response(FanotifyResponse::new(
                        fd,
                        Response::Allow,
                    ))?;
                }
            }
        }
    }
}
```

### fanotify with FAN_REPORT_FID (Modern Approach)

Linux 5.1+ supports `FAN_REPORT_FID` which provides file identification without keeping file descriptors open:

```rust
// With FAN_REPORT_FID (Linux 5.1+)
let fan = Fanotify::init(
    InitFlags::FAN_CLASS_NOTIF | InitFlags::FAN_REPORT_FID | InitFlags::FAN_REPORT_NAME,
    EventFlags::O_RDONLY,
)?;

// Events now include:
// - File handle (fsid + handle)
// - Directory file handle
// - File name

// Can reconstruct path with name_to_handle_at() / open_by_handle_at()
```

### Pros
- **Access to file content** - receives fd to the file
- **Can capture writes** - via FAN_CLOSE_WRITE
- **Mount-point wide** - no need to watch individual files
- **Permission control** - can allow/deny operations
- **Moderate overhead** - less than FUSE

### Cons
- **Requires CAP_SYS_ADMIN** - or root
- **Linux only** - no macOS/Windows support
- **After-the-fact for writes** - sees write after it happens
- **Cannot modify content** - observation only
- **Limited metadata** - newer kernels needed for full info

### Relevant Projects

- **[fanotify-rs](https://github.com/jbaublitz/fanotify-rs)** - Rust bindings (unmaintained)
- **[nix](https://github.com/nix-rust/nix)** - Unix API bindings including fanotify
- **ClamAV** - Uses fanotify for on-access scanning

---

## 3. inotify

### Overview

inotify is the older, simpler file notification API. It monitors files and directories for events but has significant limitations compared to fanotify.

### Limitations

- **No file content access** - only event notifications
- **Must watch individual paths** - no mount-point watching
- **No permission control** - cannot block operations
- **Watch limits** - limited number of watches

### Rust Implementation

```rust
use inotify::{Inotify, WatchMask};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut inotify = Inotify::init()?;
    
    inotify.watches().add(
        "/path/to/watch",
        WatchMask::MODIFY | WatchMask::CREATE | WatchMask::DELETE,
    )?;
    
    let mut buffer = [0u8; 4096];
    loop {
        let events = inotify.read_events_blocking(&mut buffer)?;
        for event in events {
            println!("Event: {:?} on {:?}", event.mask, event.name);
        }
    }
}
```

### Use Case for Ize

inotify could be useful as a **lightweight trigger** - detect that a file changed, then use other means to capture the actual change. But it's not sufficient on its own for capturing file contents.

---

## 4. Linux Audit Subsystem

### Overview

The Linux audit subsystem provides comprehensive logging of system events, including all file access. It's designed for security auditing and compliance.

### How It Works

```bash
# Add audit rule for file access
auditctl -w /path/to/watch -p rwxa -k mykey

# View audit logs
ausearch -k mykey
```

### Programmatic Access

```rust
use std::process::Command;

// Add audit rule
Command::new("auditctl")
    .args(["-w", "/path/to/watch", "-p", "rwxa", "-k", "ize"])
    .status()?;

// Read audit events from /var/log/audit/audit.log
// Or use netlink to receive events directly
```

### Pros
- **Comprehensive** - captures all syscalls
- **Already in kernel** - no additional modules needed
- **Structured logging** - standardized format

### Cons
- **No content capture** - logs events only
- **Performance impact** - can slow down system
- **Log-based** - not real-time streaming (unless netlink)
- **Requires root** - to configure rules

---

## 5. LD_PRELOAD Library Interposition

### Overview

LD_PRELOAD allows injecting a shared library that intercepts libc function calls. This is a userspace-only solution that doesn't require kernel access.

### How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        User Application                          │
│                              │                                   │
│                         write(fd, buf, n)                        │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │              LD_PRELOAD Interceptor Library                 ││
│  │                                                              ││
│  │   ssize_t write(int fd, void *buf, size_t n) {             ││
│  │       // Log the write                                       ││
│  │       log_write(fd, buf, n);                                ││
│  │                                                              ││
│  │       // Call real write                                     ││
│  │       return real_write(fd, buf, n);                        ││
│  │   }                                                          ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                        libc.so                               ││
│  │                     (real write())                           ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│                              ▼                                   │
│                         KERNEL                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation (C, loaded via LD_PRELOAD)

```c
// intercept.c - compile with: gcc -shared -fPIC -o intercept.so intercept.c -ldl

#define _GNU_SOURCE
#include <dlfcn.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>

// Function pointer to real write
static ssize_t (*real_write)(int, const void*, size_t) = NULL;
static int (*real_open)(const char*, int, ...) = NULL;

// Initialize function pointers
__attribute__((constructor))
void init() {
    real_write = dlsym(RTLD_NEXT, "write");
    real_open = dlsym(RTLD_NEXT, "open");
}

// Intercept write
ssize_t write(int fd, const void *buf, size_t count) {
    // Log to stderr or a file
    char path[256];
    snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
    char realpath_buf[4096];
    ssize_t len = readlink(path, realpath_buf, sizeof(realpath_buf)-1);
    if (len > 0) {
        realpath_buf[len] = '\0';
        fprintf(stderr, "[INTERCEPT] write(%s, %zu bytes)\n", realpath_buf, count);
    }
    
    // Call real write
    return real_write(fd, buf, count);
}

// Intercept open
int open(const char *pathname, int flags, ...) {
    fprintf(stderr, "[INTERCEPT] open(%s, %d)\n", pathname, flags);
    
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    
    return real_open(pathname, flags, mode);
}
```

### Usage

```bash
LD_PRELOAD=/path/to/intercept.so ./myprogram
```

### Rust Implementation with `redhook`

```rust
// Using the redhook crate
use redhook::hook;
use libc::{c_int, c_void, size_t, ssize_t};

hook! {
    unsafe fn write(fd: c_int, buf: *const c_void, count: size_t) -> ssize_t => my_write {
        // Log the write
        eprintln!("[INTERCEPT] write(fd={}, count={})", fd, count);
        
        // Call original
        real!(write)(fd, buf, count)
    }
}
```

### Pros
- **Full content access** - sees all data
- **Can modify data** - can change what's written
- **No root required** - just LD_PRELOAD
- **Works per-process** - targeted interception

### Cons
- **Per-process only** - must launch with LD_PRELOAD
- **Bypassable** - static linking, direct syscalls bypass it
- **Not transparent** - modifies application environment
- **Complex for all ops** - many functions to intercept

---

## 6. ptrace

### Overview

ptrace allows a process to observe and control another process's execution, including syscalls. It's how debuggers (gdb) and strace work.

### Limitations for Our Use Case

- **Extremely high overhead** - stops process on every syscall
- **One tracer per process** - conflicts with debuggers
- **Complex** - must handle all syscalls
- **Not suitable for production** - too slow

### When It's Useful

- Debugging
- Development testing
- One-off analysis

---

## 7. Kernel Modules

### Overview

A custom kernel module can intercept filesystem operations at the VFS layer, providing the ultimate control and lowest overhead.

### Approaches

1. **LSM (Linux Security Module)** - Hook security checks
2. **VFS layer hooks** - Replace filesystem operations
3. **Stackable filesystem** - Layer on top of existing FS

### Example: LSM Hook

```c
// Simplified LSM hook for file_open
static int my_file_open(struct file *file) {
    // Log the open
    pr_info("File opened: %s\n", file->f_path.dentry->d_name.name);
    
    // Return 0 to allow, negative to deny
    return 0;
}

static struct security_hook_list my_hooks[] = {
    LSM_HOOK_INIT(file_open, my_file_open),
};
```

### Pros
- **Lowest overhead** - runs in kernel
- **Full control** - can modify anything
- **All processes** - system-wide

### Cons
- **Dangerous** - bugs crash the system
- **Complex** - kernel development is hard
- **Maintenance burden** - must track kernel changes
- **Requires root** - to load module
- **GPL implications** - kernel modules are derivative works

---

## 8. Comparative Analysis for Ize

### Requirements Recap

Ize needs to:
1. Detect file modifications
2. Capture file content (for versioning)
3. Track metadata changes
4. Minimize performance impact
5. Work transparently

### Evaluation

| Requirement | eBPF | fanotify | inotify | FUSE |
|-------------|------|----------|---------|------|
| Detect mods | ✅ | ✅ | ✅ | ✅ |
| Capture content | ⚠️ Limited | ✅ | ❌ | ✅ |
| Track metadata | ✅ | ✅ | ⚠️ | ✅ |
| Low overhead | ✅ | ✅ | ✅ | ❌ |
| Transparent | ✅ | ✅ | ✅ | ⚠️ |

### Recommended Approach: Hybrid

**Primary: fanotify for write detection and content capture**
- Monitor `FAN_CLOSE_WRITE` events
- Read file content when modification detected
- Low overhead, transparent operation

**Secondary: eBPF for detailed tracing (optional)**
- Capture syscall-level details
- Performance monitoring
- Debugging aid

**Fallback: FUSE for special cases**
- When pre-operation interception is needed
- For specific directories requiring tight control

### Architecture Proposal

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ize Daemon                                │
│                                                                  │
│  ┌────────────────────┐    ┌────────────────────┐              │
│  │  fanotify Watcher  │    │   eBPF Tracer      │              │
│  │                    │    │   (optional)        │              │
│  │  - FAN_CLOSE_WRITE │    │   - Detailed logs   │              │
│  │  - FAN_CREATE      │    │   - Performance     │              │
│  │  - FAN_DELETE      │    │                     │              │
│  └─────────┬──────────┘    └─────────┬──────────┘              │
│            │                          │                          │
│            ▼                          ▼                          │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Event Processor                          ││
│  │                                                              ││
│  │  - Debounce rapid events                                    ││
│  │  - Read file content on write                               ││
│  │  - Calculate diffs/hashes                                   ││
│  │  - Store in database                                        ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Storage Backend                          ││
│  │                    (SQLite + Chunks)                        ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: fanotify-based Watcher

```rust
// Simplified API
pub struct IzeWatcher {
    fanotify: Fanotify,
    watched_paths: Vec<PathBuf>,
}

impl IzeWatcher {
    pub fn new() -> Result<Self>;
    pub fn watch(&mut self, path: &Path) -> Result<()>;
    pub fn unwatch(&mut self, path: &Path) -> Result<()>;
    
    // Returns stream of file events
    pub fn events(&self) -> impl Stream<Item = FileEvent>;
}

pub enum FileEvent {
    Modified { path: PathBuf, content: Vec<u8> },
    Created { path: PathBuf },
    Deleted { path: PathBuf },
    Renamed { from: PathBuf, to: PathBuf },
}
```

### Phase 2: Optional eBPF Integration

```rust
// For detailed tracing when needed
pub struct IzeTracer {
    bpf: Bpf,
}

impl IzeTracer {
    pub fn new() -> Result<Self>;
    pub fn trace_process(&mut self, pid: u32) -> Result<()>;
    pub fn events(&self) -> impl Stream<Item = SyscallEvent>;
}
```

### Phase 3: Hybrid Mode

```rust
pub struct Ize {
    watcher: IzeWatcher,     // Always running
    tracer: Option<IzeTracer>, // Optional detailed tracing
    storage: Storage,
}
```

---

## Conclusion

For Ize's use case of transparent file versioning:

1. **fanotify is the best primary approach** - provides the right balance of:
   - Content access (via file descriptors)
   - Low overhead
   - Transparent operation
   - Write detection

2. **eBPF is valuable for supplementary tracing** - useful for:
   - Debugging
   - Performance analysis
   - Detailed syscall logging

3. **FUSE should be optional** - keep for:
   - Scenarios requiring pre-operation hooks
   - Specific directory isolation
   - Compatibility with existing FUSE-based workflows

4. **inotify is insufficient** - lacks content access

5. **LD_PRELOAD is too invasive** - not transparent

The recommended path forward is to implement a fanotify-based watcher as the primary file monitoring mechanism, with optional eBPF tracing for detailed analysis. This provides the transparency and low overhead that Ize needs while still capturing all necessary file modifications.

---

## References

- [eBPF Documentation](https://ebpf.io/)
- [fanotify(7) man page](https://man7.org/linux/man-pages/man7/fanotify.7.html)
- [inotify(7) man page](https://man7.org/linux/man-pages/man7/inotify.7.html)
- [Linux Audit Documentation](https://github.com/linux-audit/audit-documentation)
- [aya - Rust