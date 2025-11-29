import os

def enable_bpf_logging():
    path = "src/bpf/mirror.bpf.c"
    if not os.path.exists(path):
        print(f"Skipping {path} (not found)")
        return

    with open(path, "r") as f:
        lines = f.readlines()

    new_lines = []
    
    # We want to insert a bpf_printk BEFORE the map lookup check.
    # Target line: if (!bpf_map_lookup_elem(&watched_devs, &dev_id)) return 0;
    
    # We will log: "DEV CHECK: Kernel saw dev_id X"
    
    found = False
    for line in lines:
        if "if (!bpf_map_lookup_elem(&watched_devs, &dev_id))" in line:
            # Add logging before the check
            indent = line[:line.find("if")]
            new_lines.append(f'{indent}bpf_printk("FOXING-DEBUG: Write detected on dev_id: %u\\n", dev_id);\n')
            new_lines.append(line)
            found = True
        else:
            new_lines.append(line)

    if found:
        with open(path, "w") as f:
            f.writelines(new_lines)
        print(f"✅ Enabled BPF kernel debugging in {path}")
    else:
        print(f"ℹ  Could not locate insertion point in {path}")

if __name__ == "__main__":
    enable_bpf_logging()

