import os

def disable_device_filter():
    path = "src/bpf/mirror.bpf.c"
    if not os.path.exists(path):
        print(f"Skipping {path} (not found)")
        return

    with open(path, "r") as f:
        lines = f.readlines()

    new_lines = []
    
    # We want to comment out the device ID check:
    # if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;
    
    for line in lines:
        if "if (!bpf_map_lookup_elem(&watched_devs, &dev_id))" in line:
            # Comment it out
            new_lines.append("// " + line)
            print("  [FIX] Disabled BPF device ID filter (Promiscuous Mode)")
        else:
            new_lines.append(line)

    with open(path, "w") as f:
        f.writelines(new_lines)
    print(f"✅ Changes applied to {path}")

if __name__ == "__main__":
    print("Applying BPF Promiscuous Mode (Fixing Loopback ID Mismatch)...")
    disable_device_filter()
    print("Done.")
```

This change puts the BPF probe into "promiscuous mode". It will send *every* XFS write event on the system to the userspace daemon. The daemon's `worker.rs` logic already has a path check (`target_cfg.allow(...)`), but the primary `BTreeMap` lookup in `bpf.rs` (`if let Some(qs) = queues.get(&raw.dev)`) will still filter events based on the *userspace* view of the device ID.

**Wait!** If we disable the kernel-side filter, the events will reach `src/bpf.rs`, but `src/bpf.rs` *also* filters by device ID:

```rust
// src/bpf.rs
if let Some(qs) = queues.get(&raw.dev) { 
    for q in qs { q.push(evt.clone()); } 
}
