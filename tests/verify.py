"""Correctness verification for the foxing test harness.

Compares two directory trees (source vs target) and reports mismatches.
Uses a fast stat-first approach: only hashes files when sizes match.
"""

import hashlib
import os
from pathlib import Path


def _hash_file(path: Path) -> str:
    """SHA-256 hash of a file's contents."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            sha.update(chunk)
    return sha.hexdigest()


def verify_tree(source: Path, target: Path, check_mtime: bool = False) -> dict:
    """Compare two directory trees for correctness.

    Args:
        source: The reference (source) directory.
        target: The copy (target) directory to verify.
        check_mtime: If True, flag mtime differences (warning-level, not failure).

    Returns:
        {
            "status": "PASS" | "FAIL",
            "files_checked": int,
            "mismatches": int,
            "missing": [str, ...],       # In source but not target
            "extra": [str, ...],          # In target but not source
            "size_mismatches": [str, ...],
            "content_mismatches": [str, ...],
            "mtime_warnings": [str, ...],
            "errors": [str, ...],         # Files that couldn't be read
        }
    """
    missing = []
    extra = []
    size_mismatches = []
    content_mismatches = []
    mtime_warnings = []
    errors = []
    files_checked = 0

    # Build set of relative paths from source
    src_files = {}
    for p in source.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(source))
            try:
                src_files[rel] = p.stat()
            except OSError as e:
                errors.append(f"stat source {rel}: {e}")

    # Build set of relative paths from target
    tgt_files = {}
    for p in target.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(target))
            try:
                tgt_files[rel] = p.stat()
            except OSError as e:
                errors.append(f"stat target {rel}: {e}")

    # Check for missing files (in source but not target)
    for rel in src_files:
        if rel not in tgt_files:
            missing.append(rel)

    # Check for extra files (in target but not source)
    for rel in tgt_files:
        if rel not in src_files:
            extra.append(rel)

    # Compare files present in both
    common = set(src_files.keys()) & set(tgt_files.keys())
    for rel in sorted(common):
        files_checked += 1
        s_stat = src_files[rel]
        t_stat = tgt_files[rel]

        # Fast path: size mismatch means content definitely differs
        if s_stat.st_size != t_stat.st_size:
            size_mismatches.append(f"{rel}: src={s_stat.st_size} tgt={t_stat.st_size}")
            continue

        # Slow path: hash comparison
        try:
            src_hash = _hash_file(source / rel)
            tgt_hash = _hash_file(target / rel)
            if src_hash != tgt_hash:
                content_mismatches.append(rel)
        except OSError as e:
            errors.append(f"hash {rel}: {e}")
            continue

        # Optional mtime check (1s tolerance for fs granularity)
        if check_mtime and abs(s_stat.st_mtime - t_stat.st_mtime) > 1.0:
            mtime_warnings.append(
                f"{rel}: src_mtime={s_stat.st_mtime:.3f} tgt_mtime={t_stat.st_mtime:.3f}"
            )

    total_mismatches = len(missing) + len(extra) + len(size_mismatches) + len(content_mismatches)
    status = "PASS" if total_mismatches == 0 and not errors else "FAIL"

    # If the only issue is errors reading files, distinguish from content failures
    if total_mismatches == 0 and errors:
        status = "ERROR"

    return {
        "status": status,
        "files_checked": files_checked,
        "mismatches": total_mismatches,
        "missing": missing,
        "extra": extra,
        "size_mismatches": size_mismatches,
        "content_mismatches": content_mismatches,
        "mtime_warnings": mtime_warnings,
        "errors": errors,
    }
