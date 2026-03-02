#!/usr/bin/env python3
"""Foxing unprivileged CI test harness.

Compares foxing sync vs rsync vs cp across configurable workloads.
Outputs structured JSON optimized for agentic iteration.

Usage:
    python3 tests/harness.py                          # Run all, JSON to stdout
    python3 tests/harness.py --workload small_files    # Specific workload
    python3 tests/harness.py --tmpfs                   # Use /tmp instead of btrfs
    python3 tests/harness.py --save-baseline           # Save results as baseline.json
    python3 tests/harness.py --compare baseline.json   # Compare against baseline
    python3 tests/harness.py --tools rsync,cp          # Skip foxing sync
    python3 tests/harness.py --skip-verify             # Skip correctness checks
    python3 tests/harness.py --json                    # JSON only (default)
    python3 tests/harness.py --human                   # Human-readable summary
    python3 tests/harness.py --scale small             # Smaller workloads (faster)
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# Ensure tests/ is on the path so we can import siblings
TESTS_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

import workloads
import verify

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FOXING_BINARY = PROJECT_ROOT / "target" / "release" / "foxing"
DEFAULT_TEST_ROOT = PROJECT_ROOT / "test_harness"
TMPFS_PREFIX = "foxing_test_"
BUILD_TIMEOUT = 300  # 5 min cargo build timeout
DEFAULT_TOOL_TIMEOUT = 60  # per-tool timeout in seconds

TOOLS = {
    "rsync":  lambda src, dst: ["rsync", "-a", "--delete", f"{src}/", f"{dst}/"],
    "cp":     lambda src, dst: ["cp", "-a", f"{src}/.", f"{dst}/"],
    "foxing": lambda src, dst: [str(FOXING_BINARY), "sync", "-a", str(src), str(dst)],
}


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------
def detect_env() -> dict:
    """Gather environment info for the report."""
    uname = platform.uname()
    kernel = uname.release.split("-")[0]

    # Detect filesystem type on working dir
    fs_type = "unknown"
    reflink = False
    try:
        out = subprocess.check_output(
            ["stat", "-f", "-c", "%T", str(PROJECT_ROOT)],
            text=True, stderr=subprocess.DEVNULL
        ).strip()
        fs_type = out
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Test reflink support
    try:
        tmp_a = PROJECT_ROOT / ".reflink_test_a"
        tmp_b = PROJECT_ROOT / ".reflink_test_b"
        tmp_a.write_bytes(b"reflink_test")
        result = subprocess.run(
            ["cp", "--reflink=always", str(tmp_a), str(tmp_b)],
            capture_output=True
        )
        reflink = result.returncode == 0
        tmp_a.unlink(missing_ok=True)
        tmp_b.unlink(missing_ok=True)
    except Exception:
        pass

    # Rust version
    rust_ver = "unknown"
    try:
        rust_ver = subprocess.check_output(
            ["rustc", "--version"], text=True, stderr=subprocess.DEVNULL
        ).strip().split()[1]
    except Exception:
        pass

    # Foxing version
    foxing_ver = "unknown"
    if FOXING_BINARY.exists():
        try:
            foxing_ver = subprocess.check_output(
                [str(FOXING_BINARY), "--version"], text=True, stderr=subprocess.DEVNULL
            ).strip().split()[-1]
        except Exception:
            pass

    # CPU / Memory
    cpus = os.cpu_count() or 1
    mem_gb = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    mem_kb = int(line.split()[1])
                    mem_gb = round(mem_kb / 1024 / 1024, 1)
                    break
    except Exception:
        pass

    return {
        "kernel": kernel,
        "fs": fs_type,
        "reflink": reflink,
        "rust": rust_ver,
        "foxing": foxing_ver,
        "cpus": cpus,
        "mem_gb": mem_gb,
    }


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def build_foxing(force: bool = False) -> dict:
    """Build foxing --release. Returns build status dict."""
    if FOXING_BINARY.exists() and not force:
        # Check if source is newer than binary
        src_dir = PROJECT_ROOT / "src"
        binary_mtime = FOXING_BINARY.stat().st_mtime
        needs_rebuild = False
        for rs in src_dir.rglob("*.rs"):
            if rs.stat().st_mtime > binary_mtime:
                needs_rebuild = True
                break
        if not needs_rebuild:
            return {
                "status": "CACHED",
                "duration_ms": 0,
                "binary": str(FOXING_BINARY),
                "size_bytes": FOXING_BINARY.stat().st_size,
            }

    log("Building foxing (cargo build --release)...", file=sys.stderr)
    start = time.monotonic()
    try:
        result = subprocess.run(
            ["cargo", "build", "--release"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=BUILD_TIMEOUT,
        )
        elapsed = int((time.monotonic() - start) * 1000)
        if result.returncode != 0:
            return {
                "status": "FAIL",
                "duration_ms": elapsed,
                "error": result.stderr[-2000:] if result.stderr else "unknown build error",
            }
        return {
            "status": "PASS",
            "duration_ms": elapsed,
            "binary": str(FOXING_BINARY),
            "size_bytes": FOXING_BINARY.stat().st_size,
        }
    except subprocess.TimeoutExpired:
        return {"status": "FAIL", "duration_ms": BUILD_TIMEOUT * 1000, "error": "build timeout"}
    except Exception as e:
        return {"status": "FAIL", "duration_ms": 0, "error": str(e)}


# ---------------------------------------------------------------------------
# Tool runner
# ---------------------------------------------------------------------------
def run_tool(tool_name: str, src: Path, dst: Path, timeout: int = DEFAULT_TOOL_TIMEOUT) -> dict:
    """Run a copy tool and return timing + status."""
    cmd = TOOLS[tool_name](str(src), str(dst))

    # Ensure target dir exists (rsync and foxing create it, cp needs it)
    dst.mkdir(parents=True, exist_ok=True)

    start = time.monotonic()
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
    except subprocess.TimeoutExpired:
        return {"status": "ERROR", "duration_ms": timeout * 1000, "error": f"timeout ({timeout}s)"}
    except FileNotFoundError:
        return {"status": "SKIP", "duration_ms": 0, "error": f"{tool_name} not found"}

    status = "PASS"
    error = None

    if result.returncode != 0:
        stderr = result.stderr[-1000:] if result.stderr else ""
        # foxing: BPF crash is expected unprivileged — check if hydration still completed
        if tool_name == "foxing" and "bpf" in stderr.lower():
            # Check if any files were actually copied
            try:
                target_files = list(dst.rglob("*"))
                if any(f.is_file() for f in target_files):
                    status = "PASS"  # Hydration path worked despite BPF crash
                    error = "bpf_crash_expected"
                else:
                    status = "FAIL"
                    error = f"foxing produced no output; stderr: {stderr}"
            except Exception:
                status = "FAIL"
                error = f"exit {result.returncode}: {stderr}"
        else:
            status = "FAIL"
            error = f"exit {result.returncode}: {stderr}"

    result_dict = {"status": status, "duration_ms": elapsed_ms}
    if error:
        result_dict["error"] = error
    return result_dict


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_output_mode = "json"


def log(msg: str, **kwargs):
    """Print to stderr (never contaminates JSON stdout)."""
    print(msg, **kwargs)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Foxing CI test harness")
    p.add_argument("--workload", type=str, default=None,
                   help="Comma-separated workload names (default: all)")
    p.add_argument("--tmpfs", action="store_true",
                   help="Use /tmp (tmpfs) instead of btrfs working dir")
    p.add_argument("--save-baseline", action="store_true",
                   help="Save results to tests/baseline.json")
    p.add_argument("--compare", type=str, default=None, metavar="FILE",
                   help="Compare results against a baseline JSON file")
    p.add_argument("--tools", type=str, default=None,
                   help="Comma-separated tool names (default: rsync,cp,foxing)")
    p.add_argument("--skip-verify", action="store_true",
                   help="Skip correctness verification")
    p.add_argument("--json", action="store_true", default=True,
                   help="JSON output to stdout (default)")
    p.add_argument("--human", action="store_true",
                   help="Human-readable summary to stdout")
    p.add_argument("--scale", type=str, default="default",
                   choices=["small", "default", "large"],
                   help="Workload scale preset")
    p.add_argument("--rebuild", action="store_true",
                   help="Force rebuild of foxing binary")
    p.add_argument("--timeout", type=int, default=DEFAULT_TOOL_TIMEOUT,
                   help=f"Per-tool timeout in seconds (default: {DEFAULT_TOOL_TIMEOUT})")
    return p.parse_args()


def make_test_root(use_tmpfs: bool) -> Path:
    """Create and return the test root directory."""
    if use_tmpfs:
        d = Path(tempfile.mkdtemp(prefix=TMPFS_PREFIX, dir="/tmp"))
    else:
        d = DEFAULT_TEST_ROOT
        d.mkdir(parents=True, exist_ok=True)
    return d


def run_workload_suite(
    workload_name: str,
    tool_names: list[str],
    test_root: Path,
    scale: str,
    skip_verify: bool,
    tool_timeout: int = DEFAULT_TOOL_TIMEOUT,
) -> tuple[list[dict], list[dict]]:
    """Run all tools on a single workload. Returns (tests, comparisons)."""
    tests = []
    comparisons_data = {}  # phase -> {tool: ms}

    # Generate workload
    source_dir = test_root / "source"
    if source_dir.exists():
        shutil.rmtree(source_dir)

    gen_fn, _ = workloads.WORKLOADS[workload_name]
    scaled_kwargs = workloads.apply_scale(workload_name, scale)

    log(f"  Generating workload '{workload_name}' (scale={scale})...", file=sys.stderr)
    gen_stats = gen_fn(source_dir, **scaled_kwargs)
    log(f"    {gen_stats['files']} files, {gen_stats['bytes'] / 1024 / 1024:.1f} MB", file=sys.stderr)

    # --- Cold run: copy from scratch ---
    for tool in tool_names:
        target_dir = test_root / f"target_{tool}"
        if target_dir.exists():
            shutil.rmtree(target_dir)

        log(f"  Running {tool} (cold)...", file=sys.stderr)
        result = run_tool(tool, source_dir, target_dir, timeout=tool_timeout)

        # Compute metrics
        metrics = {
            "files": gen_stats["files"],
            "bytes": gen_stats["bytes"],
        }
        if result["duration_ms"] > 0:
            secs = result["duration_ms"] / 1000
            metrics["throughput_mbps"] = round(gen_stats["bytes"] / 1024 / 1024 / secs, 2)
            metrics["files_per_sec"] = round(gen_stats["files"] / secs, 1)

        # Verify correctness
        verify_result = None
        if not skip_verify and result["status"] in ("PASS",):
            verify_result = verify.verify_tree(source_dir, target_dir)
            if verify_result["status"] != "PASS":
                result["status"] = "FAIL"
                result["error"] = (result.get("error", "") +
                    f"; verify: {verify_result['mismatches']} mismatches").lstrip("; ")

        test_entry = {
            "id": f"{workload_name}.{tool}.cold",
            "workload": workload_name,
            "tool": tool,
            "phase": "cold",
            "status": result["status"],
            "duration_ms": result["duration_ms"],
            "metrics": metrics,
        }
        if verify_result:
            test_entry["verify"] = {
                "status": verify_result["status"],
                "files_checked": verify_result["files_checked"],
                "mismatches": verify_result["mismatches"],
            }
        if "error" in result:
            test_entry["error"] = result["error"]

        tests.append(test_entry)
        comparisons_data.setdefault("cold", {})[tool] = result["duration_ms"]

        log(f"    {result['status']} in {result['duration_ms']}ms", file=sys.stderr)

    # --- Delta run: modify 10% and re-sync ---
    log(f"  Mutating workload (10%)...", file=sys.stderr)
    mutation = workloads.mutate_workload(source_dir, pct=10)
    log(f"    modified={mutation['modified']} added={mutation['added']} deleted={mutation['deleted']}",
        file=sys.stderr)

    for tool in tool_names:
        target_dir = test_root / f"target_{tool}"

        log(f"  Running {tool} (delta)...", file=sys.stderr)
        result = run_tool(tool, source_dir, target_dir, timeout=tool_timeout)

        # Re-scan source for updated stats
        updated_stats = workloads.get_workload_stats(source_dir)
        metrics = {
            "files": updated_stats["files"],
            "bytes": updated_stats["bytes"],
        }
        if result["duration_ms"] > 0:
            secs = result["duration_ms"] / 1000
            metrics["throughput_mbps"] = round(updated_stats["bytes"] / 1024 / 1024 / secs, 2)
            metrics["files_per_sec"] = round(updated_stats["files"] / secs, 1)

        verify_result = None
        if not skip_verify and result["status"] in ("PASS",):
            verify_result = verify.verify_tree(source_dir, target_dir)
            if verify_result["status"] != "PASS":
                result["status"] = "FAIL"
                result["error"] = (result.get("error", "") +
                    f"; verify: {verify_result['mismatches']} mismatches").lstrip("; ")

        test_entry = {
            "id": f"{workload_name}.{tool}.delta",
            "workload": workload_name,
            "tool": tool,
            "phase": "delta",
            "status": result["status"],
            "duration_ms": result["duration_ms"],
            "metrics": metrics,
        }
        if verify_result:
            test_entry["verify"] = {
                "status": verify_result["status"],
                "files_checked": verify_result["files_checked"],
                "mismatches": verify_result["mismatches"],
            }
        if "error" in result:
            test_entry["error"] = result["error"]

        tests.append(test_entry)
        comparisons_data.setdefault("delta", {})[tool] = result["duration_ms"]

        log(f"    {result['status']} in {result['duration_ms']}ms", file=sys.stderr)

    # --- Build comparisons ---
    comparisons = []
    for phase, tool_times in comparisons_data.items():
        comp = {
            "workload": workload_name,
            "phase": phase,
        }
        for t in ("rsync", "cp", "foxing"):
            if t in tool_times:
                comp[f"{t}_ms"] = tool_times[t]

        # Compute ratios (>1.0 means foxing is faster)
        if "foxing" in tool_times and tool_times["foxing"] > 0:
            if "rsync" in tool_times and tool_times["rsync"] > 0:
                comp["foxing_vs_rsync"] = round(tool_times["rsync"] / tool_times["foxing"], 3)
            if "cp" in tool_times and tool_times["cp"] > 0:
                comp["foxing_vs_cp"] = round(tool_times["cp"] / tool_times["foxing"], 3)

        comparisons.append(comp)

    # Cleanup workload dirs
    for d in test_root.iterdir():
        if d.name.startswith("target_") or d.name == "source":
            shutil.rmtree(d, ignore_errors=True)

    return tests, comparisons


def build_summary(all_tests: list[dict], comparisons: list[dict]) -> dict:
    """Build the summary section of the report."""
    total = len(all_tests)
    passed = sum(1 for t in all_tests if t["status"] == "PASS")
    failed = sum(1 for t in all_tests if t["status"] == "FAIL")
    skipped = sum(1 for t in all_tests if t["status"] == "SKIP")
    errored = sum(1 for t in all_tests if t["status"] == "ERROR")

    # Find fastest tool per workload (cold phase)
    fastest = {}
    for comp in comparisons:
        if comp["phase"] != "cold":
            continue
        wl = comp["workload"]
        tool_times = {k.replace("_ms", ""): v for k, v in comp.items()
                      if k.endswith("_ms") and isinstance(v, (int, float))}
        if tool_times:
            fastest[wl] = min(tool_times, key=tool_times.get)

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "errored": errored,
        "regressions": [],  # Populated by compare_baseline
        "fastest_tool": fastest,
    }


def compare_baseline(report: dict, baseline_path: str) -> list[dict]:
    """Compare current results against a saved baseline. Returns regressions."""
    try:
        with open(baseline_path) as f:
            baseline = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        return [{"error": f"Could not load baseline: {e}"}]

    regressions = []
    # Index baseline tests by id
    baseline_tests = {t["id"]: t for t in baseline.get("tests", [])}

    for test in report.get("tests", []):
        tid = test["id"]
        if tid not in baseline_tests:
            continue
        bt = baseline_tests[tid]

        # Check for throughput regression (>10% drop)
        cur_tp = test.get("metrics", {}).get("throughput_mbps", 0)
        base_tp = bt.get("metrics", {}).get("throughput_mbps", 0)
        if base_tp > 0 and cur_tp > 0:
            ratio = cur_tp / base_tp
            if ratio < 0.9:
                regressions.append({
                    "test_id": tid,
                    "metric": "throughput_mbps",
                    "baseline": base_tp,
                    "current": cur_tp,
                    "ratio": round(ratio, 3),
                    "message": f"{tid}: throughput dropped {(1-ratio)*100:.1f}% "
                               f"({base_tp:.1f} -> {cur_tp:.1f} MB/s)",
                })

        # Check for status regression
        if bt.get("status") == "PASS" and test.get("status") != "PASS":
            regressions.append({
                "test_id": tid,
                "metric": "status",
                "baseline": "PASS",
                "current": test["status"],
                "message": f"{tid}: status regressed from PASS to {test['status']}",
            })

    return regressions


def print_human(report: dict):
    """Print a human-readable summary."""
    env = report["env"]
    build = report["build"]
    summary = report["summary"]

    print(f"\nFoxing Test Harness — {report['timestamp']}")
    print(f"  Kernel: {env['kernel']}  FS: {env['fs']}  Reflink: {env['reflink']}")
    print(f"  CPUs: {env['cpus']}  RAM: {env['mem_gb']}GB  Rust: {env['rust']}")
    print(f"  Build: {build['status']} ({build.get('duration_ms', 0)}ms)")
    print()

    # Results table
    print(f"{'Test ID':<35} {'Status':<7} {'Time':>8} {'MB/s':>8} {'Files/s':>10}")
    print("-" * 72)
    for t in report["tests"]:
        tp = t.get("metrics", {}).get("throughput_mbps", "")
        fps = t.get("metrics", {}).get("files_per_sec", "")
        tp_str = f"{tp:.1f}" if isinstance(tp, (int, float)) else ""
        fps_str = f"{fps:.0f}" if isinstance(fps, (int, float)) else ""
        print(f"{t['id']:<35} {t['status']:<7} {t['duration_ms']:>7}ms {tp_str:>8} {fps_str:>10}")

    # Comparisons
    if report.get("comparisons"):
        print()
        print("Comparisons (ratio >1.0 = foxing faster):")
        for c in report["comparisons"]:
            parts = [f"  {c['workload']}.{c['phase']}:"]
            for k in ("rsync_ms", "cp_ms", "foxing_ms"):
                if k in c:
                    parts.append(f"{k.replace('_ms','')}={c[k]}ms")
            if "foxing_vs_rsync" in c:
                parts.append(f"fox/rsync={c['foxing_vs_rsync']:.2f}")
            if "foxing_vs_cp" in c:
                parts.append(f"fox/cp={c['foxing_vs_cp']:.2f}")
            print("  ".join(parts))

    # Summary
    print()
    print(f"Total: {summary['total']}  Passed: {summary['passed']}  "
          f"Failed: {summary['failed']}  Skipped: {summary['skipped']}  "
          f"Errored: {summary.get('errored', 0)}")

    if summary.get("regressions"):
        print()
        print("REGRESSIONS:")
        for r in summary["regressions"]:
            print(f"  {r.get('message', r)}")

    if summary.get("fastest_tool"):
        print()
        print("Fastest (cold):", ", ".join(f"{k}={v}" for k, v in summary["fastest_tool"].items()))

    print()


def main():
    args = parse_args()
    global _output_mode
    _output_mode = "human" if args.human else "json"

    # Determine workloads
    if args.workload:
        wl_names = [w.strip() for w in args.workload.split(",")]
        for w in wl_names:
            if w not in workloads.WORKLOADS:
                log(f"Unknown workload: {w}", file=sys.stderr)
                log(f"Available: {', '.join(workloads.WORKLOADS.keys())}", file=sys.stderr)
                sys.exit(1)
    else:
        wl_names = list(workloads.WORKLOADS.keys())

    # Determine tools
    if args.tools:
        tool_names = [t.strip() for t in args.tools.split(",")]
        for t in tool_names:
            if t not in TOOLS:
                log(f"Unknown tool: {t}", file=sys.stderr)
                log(f"Available: {', '.join(TOOLS.keys())}", file=sys.stderr)
                sys.exit(1)
    else:
        tool_names = list(TOOLS.keys())

    # Check rsync availability
    if "rsync" in tool_names:
        if not shutil.which("rsync"):
            log("rsync not found, skipping", file=sys.stderr)
            tool_names.remove("rsync")

    # Environment
    log("Detecting environment...", file=sys.stderr)
    env = detect_env()
    log(f"  {env['fs']} reflink={env['reflink']} cpus={env['cpus']} mem={env['mem_gb']}GB",
        file=sys.stderr)

    # Build foxing if needed
    build_result = {"status": "SKIP", "duration_ms": 0}
    if "foxing" in tool_names:
        build_result = build_foxing(force=args.rebuild)
        if build_result["status"] == "FAIL":
            log(f"Build failed: {build_result.get('error', 'unknown')}", file=sys.stderr)
            tool_names.remove("foxing")

    # Create test root
    test_root = make_test_root(args.tmpfs)
    log(f"Test root: {test_root}", file=sys.stderr)

    all_tests = []
    all_comparisons = []

    try:
        for wl_name in wl_names:
            log(f"\nWorkload: {wl_name}", file=sys.stderr)
            tests, comparisons = run_workload_suite(
                wl_name, tool_names, test_root, args.scale, args.skip_verify,
                tool_timeout=args.timeout,
            )
            all_tests.extend(tests)
            all_comparisons.extend(comparisons)
    finally:
        # Cleanup test root
        if args.tmpfs and test_root.exists():
            shutil.rmtree(test_root, ignore_errors=True)
        elif test_root.exists():
            shutil.rmtree(test_root, ignore_errors=True)

    # Build report
    summary = build_summary(all_tests, all_comparisons)

    report = {
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "env": env,
        "build": build_result,
        "tests": all_tests,
        "comparisons": all_comparisons,
        "summary": summary,
    }

    # Baseline comparison
    if args.compare:
        regressions = compare_baseline(report, args.compare)
        report["summary"]["regressions"] = regressions

    # Save baseline
    if args.save_baseline:
        baseline_path = TESTS_DIR / "baseline.json"
        with open(baseline_path, "w") as f:
            json.dump(report, f, indent=2)
        log(f"Baseline saved to {baseline_path}", file=sys.stderr)

    # Output
    if args.human:
        print_human(report)
    else:
        json.dump(report, sys.stdout, indent=2)
        sys.stdout.write("\n")

    # Exit code: non-zero if any failures or regressions
    if summary["failed"] > 0 or summary.get("regressions"):
        sys.exit(1)


if __name__ == "__main__":
    main()
