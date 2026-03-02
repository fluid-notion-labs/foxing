"""Workload generators for the foxing test harness.

Each generator creates a filesystem tree under `root` and returns a stats dict:
    {"files": N, "bytes": B, "dirs": D}

All content is deterministic-random (seeded urandom) so results are reproducible.
"""

import os
import random
import stat
from pathlib import Path

# ---------------------------------------------------------------------------
# Scale presets: (multiplier applied to default counts/sizes)
# ---------------------------------------------------------------------------
SCALE = {
    "small": {"file_mult": 0.1, "size_mult": 0.1, "depth_mult": 0.5, "width_mult": 0.6},
    "default": {"file_mult": 1.0, "size_mult": 1.0, "depth_mult": 1.0, "width_mult": 1.0},
    "large": {"file_mult": 10.0, "size_mult": 5.0, "depth_mult": 1.5, "width_mult": 1.6},
}


def _write_random_file(path: Path, size: int) -> int:
    """Write `size` bytes of random data. Returns bytes written."""
    chunk = 65536
    written = 0
    with open(path, "wb") as f:
        while written < size:
            n = min(chunk, size - written)
            f.write(os.urandom(n))
            written += n
    return written


def generate_small_files(root: Path, count: int = 10000, size: int = 4096) -> dict:
    """Many small files in a flat directory."""
    root.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    for i in range(count):
        p = root / f"file_{i:06d}.dat"
        total_bytes += _write_random_file(p, size)
    return {"files": count, "bytes": total_bytes, "dirs": 1}


def generate_large_files(root: Path, count: int = 10, size_mb: int = 100) -> dict:
    """Few large files."""
    root.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    size = size_mb * 1024 * 1024
    for i in range(count):
        p = root / f"bigfile_{i:04d}.bin"
        total_bytes += _write_random_file(p, size)
    return {"files": count, "bytes": total_bytes, "dirs": 1}


def generate_mixed(root: Path, file_count: int = 5000) -> dict:
    """Realistic mixed source tree: code-like dirs with varied file sizes."""
    root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)
    dirs_created = set()
    total_bytes = 0
    total_files = 0

    # Create some directory structure
    subdirs = ["src", "src/core", "src/utils", "lib", "docs", "assets",
               "tests", "tests/unit", "tests/integration", "config", "data"]
    for d in subdirs:
        (root / d).mkdir(parents=True, exist_ok=True)
        dirs_created.add(d)

    for i in range(file_count):
        # Pick a random subdir
        parent = root / rng.choice(subdirs)
        # Varied file sizes: mostly small, some medium, few large
        r = rng.random()
        if r < 0.7:
            size = rng.randint(100, 8192)       # small (code-like)
            ext = rng.choice([".rs", ".py", ".txt", ".json", ".toml"])
        elif r < 0.95:
            size = rng.randint(8192, 1024 * 1024)  # medium
            ext = rng.choice([".dat", ".log", ".csv"])
        else:
            size = rng.randint(1024 * 1024, 10 * 1024 * 1024)  # large
            ext = ".bin"

        p = parent / f"item_{i:06d}{ext}"
        total_bytes += _write_random_file(p, size)
        total_files += 1

    return {"files": total_files, "bytes": total_bytes, "dirs": len(dirs_created) + 1}


def generate_deep_tree(root: Path, depth: int = 10, width: int = 5) -> dict:
    """Deeply nested directory tree with files at each level."""
    root.mkdir(parents=True, exist_ok=True)
    total_files = 0
    total_bytes = 0
    total_dirs = 0

    def _recurse(path: Path, level: int):
        nonlocal total_files, total_bytes, total_dirs
        if level >= depth:
            return
        for w in range(width):
            d = path / f"d{level}_{w}"
            d.mkdir(exist_ok=True)
            total_dirs += 1
            # Put a file in each dir
            fp = d / f"leaf_{level}_{w}.dat"
            total_bytes += _write_random_file(fp, 4096)
            total_files += 1
            _recurse(d, level + 1)

    _recurse(root, 0)
    return {"files": total_files, "bytes": total_bytes, "dirs": total_dirs + 1}


def generate_sparse(root: Path, count: int = 10, size_mb: int = 50) -> dict:
    """Sparse files with holes (using truncate). Only works on filesystems
    that support sparse files (ext4, xfs, btrfs — not tmpfs for hole reporting,
    but file still works)."""
    root.mkdir(parents=True, exist_ok=True)
    total_bytes = 0
    logical_size = size_mb * 1024 * 1024
    for i in range(count):
        p = root / f"sparse_{i:04d}.bin"
        with open(p, "wb") as f:
            # Write a small header
            header = os.urandom(4096)
            f.write(header)
            # Seek to create a hole
            f.seek(logical_size // 2)
            # Write a middle chunk
            middle = os.urandom(4096)
            f.write(middle)
            # Seek to near the end
            f.seek(logical_size - 4096)
            # Write a tail
            tail = os.urandom(4096)
            f.write(tail)
        total_bytes += logical_size  # logical size
    return {"files": count, "bytes": total_bytes, "dirs": 1}


def mutate_workload(root: Path, pct: int = 10) -> dict:
    """Modify `pct`% of files in `root` for delta/incremental testing.
    Returns stats about what was modified."""
    rng = random.Random(99)
    all_files = [p for p in root.rglob("*") if p.is_file()]
    if not all_files:
        return {"modified": 0, "added": 0, "deleted": 0}

    n_modify = max(1, len(all_files) * pct // 100)
    targets = rng.sample(all_files, min(n_modify, len(all_files)))

    modified = 0
    added = 0
    deleted = 0

    for f in targets:
        action = rng.choice(["modify", "modify", "add", "delete"])
        if action == "modify":
            # Append random data
            with open(f, "ab") as fh:
                fh.write(os.urandom(rng.randint(64, 4096)))
            modified += 1
        elif action == "add":
            # Create a new file next to it
            new_path = f.parent / f"new_{f.stem}_{rng.randint(0,9999)}.dat"
            _write_random_file(new_path, rng.randint(100, 8192))
            added += 1
        elif action == "delete":
            try:
                f.unlink()
                deleted += 1
            except OSError:
                pass

    return {"modified": modified, "added": added, "deleted": deleted}


def get_workload_stats(root: Path) -> dict:
    """Scan a directory tree and return stats."""
    total_files = 0
    total_bytes = 0
    total_dirs = 0
    for entry in root.rglob("*"):
        if entry.is_file():
            total_files += 1
            try:
                total_bytes += entry.stat().st_size
            except OSError:
                pass
        elif entry.is_dir():
            total_dirs += 1
    return {"files": total_files, "bytes": total_bytes, "dirs": total_dirs + 1}


# ---------------------------------------------------------------------------
# Registry: name -> (generator_fn, default_kwargs)
# ---------------------------------------------------------------------------
WORKLOADS = {
    "small_files": (generate_small_files, {"count": 10000, "size": 4096}),
    "large_files": (generate_large_files, {"count": 10, "size_mb": 100}),
    "mixed":       (generate_mixed,       {"file_count": 5000}),
    "deep_tree":   (generate_deep_tree,   {"depth": 10, "width": 5}),
    "sparse":      (generate_sparse,      {"count": 10, "size_mb": 50}),
}


def apply_scale(workload_name: str, scale_name: str) -> dict:
    """Return scaled kwargs for a workload."""
    _, defaults = WORKLOADS[workload_name]
    s = SCALE[scale_name]
    scaled = {}
    for k, v in defaults.items():
        if "count" in k or k == "file_count" or k == "width":
            scaled[k] = max(1, int(v * s.get("file_mult", 1.0)))
        elif "size" in k:
            scaled[k] = max(1, int(v * s.get("size_mult", 1.0)))
        elif k == "depth":
            scaled[k] = max(1, int(v * s.get("depth_mult", 1.0)))
        else:
            scaled[k] = v
    return scaled
