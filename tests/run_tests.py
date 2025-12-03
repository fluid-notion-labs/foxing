#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
import time
import signal
import json
import logging
import random
import string
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import concurrent.futures
import urllib.request
import urllib.error
import socket

# --- Configuration ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
BINARY_PATH = PROJECT_ROOT / "target" / "release" / "foxing"
TEST_ROOT = PROJECT_ROOT / "test_env"
REPORT_ROOT = PROJECT_ROOT / "tests" / "reports"

# Loopback Settings
IMG_DIR = TEST_ROOT / "images"
MNT_SOURCE = TEST_ROOT / "source"
MNT_TARGET = TEST_ROOT / "target"
MNT_BENCH_CP = TEST_ROOT / "bench_cp"     # For cp baseline
MNT_BENCH_RSYNC = TEST_ROOT / "bench_rsync" # For rsync baseline
IMG_SIZE = "2G"

# Test Data Settings
LARGE_FILE_MB = 50
SPARSE_FILE_GB = 2
BENCHMARK_SIZE_MB = 512  # Size for performance comparison

# Daemon Settings
METRICS_PORT = 9100

# --- Logging Setup ---
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DIR = REPORT_ROOT / TIMESTAMP
RUN_DIR.mkdir(parents=True, exist_ok=True)

log_file = RUN_DIR / "full_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("FoxingTest")

# --- Helper Classes ---

class TestEnvironment:
    def __init__(self):
        self.daemon_process: Optional[subprocess.Popen] = None
        self.setup_successful = False
        self.bench_results = {}

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()

    def run_cmd(self, cmd: List[str], check=True, shell=False):
        logger.debug(f"CMD: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
        try:
            result = subprocess.run(
                cmd, 
                check=check, 
                shell=shell, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True
            )
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.cmd}")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
            raise

    def check_root(self):
        if os.geteuid() != 0:
            logger.error("This script must be run as root to manage loopback devices.")
            sys.exit(1)

    def setup(self):
        logger.info(">>> STEP 1: Environment Setup")
        self.check_root()
        
        # 1. Clean previous runs
        if TEST_ROOT.exists():
            logger.info("Cleaning previous test environment...")
            self.teardown(silent=True) # Ensure clean slate
            shutil.rmtree(TEST_ROOT, ignore_errors=True)

        # 2. Create directories
        IMG_DIR.mkdir(parents=True, exist_ok=True)
        MNT_SOURCE.mkdir(parents=True, exist_ok=True)
        MNT_TARGET.mkdir(parents=True, exist_ok=True)
        MNT_BENCH_CP.mkdir(parents=True, exist_ok=True)
        MNT_BENCH_RSYNC.mkdir(parents=True, exist_ok=True)

        # 3. Create Images
        logger.info(f"Creating {IMG_SIZE} loopback images...")
        self.run_cmd(["truncate", "-s", IMG_SIZE, str(IMG_DIR / "source.img")])
        self.run_cmd(["truncate", "-s", IMG_SIZE, str(IMG_DIR / "target.img")])

        # 4. Format XFS (Reflink enabled)
        logger.info("Formatting XFS (reflink=1)...")
        self.run_cmd(["mkfs.xfs", "-f", "-m", "reflink=1", str(IMG_DIR / "source.img")])
        self.run_cmd(["mkfs.xfs", "-f", "-m", "reflink=1", str(IMG_DIR / "target.img")])

        # 5. Mount
        logger.info("Mounting filesystems...")
        self.run_cmd(["mount", "-o", "loop,noatime", str(IMG_DIR / "source.img"), str(MNT_SOURCE)])
        self.run_cmd(["mount", "-o", "loop,noatime", str(IMG_DIR / "target.img"), str(MNT_TARGET)])

        # 6. Permissions
        self.run_cmd(["chmod", "777", str(MNT_SOURCE)])
        self.run_cmd(["chmod", "777", str(MNT_TARGET)])
        self.run_cmd(["chmod", "777", str(MNT_BENCH_CP)])
        self.run_cmd(["chmod", "777", str(MNT_BENCH_RSYNC)])

        self.setup_successful = True
        logger.info("Environment ready.")

    def generate_data(self):
        logger.info(">>> STEP 2: Data Generation")
        
        # Small files
        small_dir = MNT_SOURCE / "small_files"
        small_dir.mkdir(exist_ok=True)
        for i in range(100):
            (small_dir / f"file_{i}.txt").write_text(f"Content {i}\n" * 50)
        
        # Large Binary
        logger.info(f"Generating {LARGE_FILE_MB}MB binary...")
        with open(MNT_SOURCE / "large.bin", "wb") as f:
            f.write(os.urandom(LARGE_FILE_MB * 1024 * 1024))

        # Sparse File
        logger.info(f"Generating {SPARSE_FILE_GB}GB sparse file...")
        with open(MNT_SOURCE / "vm_disk.img", "wb") as f:
            f.seek(SPARSE_FILE_GB * 1024 * 1024 * 1024 - 1)
            f.write(b'\0')

        # Deep Nesting
        logger.info("Generating deep nesting...")
        current = MNT_SOURCE / "nest"
        for i in range(5):
            current = current / f"level_{i}"
            current.mkdir(parents=True, exist_ok=True)
            (current / "data.txt").write_text(f"Deep data {i}")
            
        # Explicit Sync to flush metadata for daemon
        subprocess.run(["sync"], check=True)

    def create_config(self):
        config_path = RUN_DIR / "foxing.toml"
        logger.info(f"Generating config at {config_path}")
        
        # NOTE: Reduced queue_max to 50k to prevent allocation timeouts
        config_content = f"""
worker_count = 4
queue_max = 50000 
global_buffer_limit = 1024
shutdown_timeout_secs = 2
metrics_port = {METRICS_PORT}
max_system_load_avg = 100.0 

[[sources]]
path = "{MNT_SOURCE.absolute()}"

  [[sources.targets]]
  path = "{MNT_TARGET.absolute()}"
  profile = "SSD"
  autotune_target_latency_ms = 50
  batch_size = 64
  enable_versioning = true
  max_versions = 5
  max_versions_size_mb = 500
  initial_sync = true
  vdo_optimization = true
"""
        config_path.write_text(config_content)
        return config_path

    def wait_for_port(self, port, timeout=15):
        """Waits for the daemon to start listening on the metrics port."""
        start = time.time()
        while time.time() - start < timeout:
            if self.daemon_process and self.daemon_process.poll() is not None:
                return False # Daemon died
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return True
            except (ConnectionRefusedError, socket.timeout):
                time.sleep(0.5)
        return False

    def start_daemon(self):
        logger.info(">>> STEP 3: Starting Daemon")
        if not BINARY_PATH.exists():
            logger.error(f"Binary not found at {BINARY_PATH}. Did you run 'cargo build --release'?")
            raise FileNotFoundError(BINARY_PATH)

        config_path = self.create_config()
        
        # Redirect outputs to files
        self.stdout_file = open(RUN_DIR / "daemon_stdout.log", "w")
        self.stderr_file = open(RUN_DIR / "daemon_stderr.log", "w")

        env = os.environ.copy()
        # Enable debug logging for deeper insights
        env["RUST_LOG"] = "info,foxing=debug" 

        self.daemon_process = subprocess.Popen(
            [str(BINARY_PATH), "daemon", "--config", str(config_path)],
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            env=env
        )
        logger.info(f"Daemon PID: {self.daemon_process.pid}")
        
        # Wait for Metrics Port (Indicates full startup)
        logger.info(f"Waiting for metrics port {METRICS_PORT}...")
        if not self.wait_for_port(METRICS_PORT, timeout=15):
            logger.error("FATAL: Daemon failed to bind metrics port within 15s.")
            # Check if it crashed
            if self.daemon_process.poll() is not None:
                logger.error(f"Daemon process exited with code {self.daemon_process.returncode}")
            else:
                logger.error("Daemon is hung or initializing too slowly.")
            raise RuntimeError("Daemon startup failure")
        else:
            logger.info("Daemon is listening.")

        # Polling for Hydration
        logger.info("Waiting for initial hydration (up to 60s)...")
        start_wait = time.time()
        hydration_complete = False
        
        check_file = MNT_TARGET / "nest" / "level_4" / "data.txt"
        
        while time.time() - start_wait < 60:
            if self.daemon_process.poll() is not None:
                logger.error("Daemon died during hydration!")
                raise RuntimeError("Daemon died during hydration")
            
            if check_file.exists():
                hydration_complete = True
                break
            time.sleep(0.5)
            
        if not hydration_complete:
            logger.warning("Hydration timed out or incomplete. Proceeding to verification to gather details.")
        else:
            logger.info(f"Hydration detected in {time.time() - start_wait:.2f}s")

    def run_basic_verification(self):
        logger.info(">>> STEP 4: Basic Verification")
        failures = []

        # Safe Checks using try/except blocks
        try:
            if not (MNT_TARGET / "large.bin").exists():
                failures.append("large.bin missing on target")
        except Exception as e:
            failures.append(f"Check 1 Error: {e}")

        # Check 2: Sparse Efficiency (VDO Check)
        src_path = MNT_SOURCE / "vm_disk.img"
        tgt_path = MNT_TARGET / "vm_disk.img"
        
        try:
            # Explicit wait for sparse file (it might be last)
            for _ in range(10):
                if tgt_path.exists(): break
                time.sleep(1)

            if tgt_path.exists():
                src_stat = src_path.stat()
                tgt_stat = tgt_path.stat()
                # Physical blocks allocated (st_blocks is 512-byte blocks)
                if tgt_stat.st_blocks > (src_stat.st_size / 512) * 0.1:
                    logger.warning(f"VDO Inefficiency: Sparse file consumed {tgt_stat.st_blocks * 512} bytes on disk.")
                else:
                    logger.info("VDO Check: Sparse file preserved correctly.")
            else:
                failures.append("vm_disk.img missing on target")
        except Exception as e:
            failures.append(f"VDO Check Error: {e}")
        
        # Check 3: Live Update
        try:
            logger.info("Test: Live Append")
            (MNT_SOURCE / "live_test.txt").write_text("Hello World")
            time.sleep(2) # Give it a moment to replicate
            if not (MNT_TARGET / "live_test.txt").exists():
                failures.append("Live file creation failed")
        except Exception as e:
            failures.append(f"Live Update Error: {e}")
        
        # Check 4: Cross-Directory Rename
        try:
            logger.info("Test: Cross-Directory Rename")
            # Wait a bit to ensure hydration map is fully populated for the nest
            time.sleep(2)
            
            (MNT_SOURCE / "move_me.txt").write_text("Moving")
            time.sleep(2) # Ensure creation propagates first
            
            nest_dir = MNT_SOURCE / "nest" / "level_0"
            os.rename(MNT_SOURCE / "move_me.txt", nest_dir / "moved_me.txt")
            time.sleep(2)
            
            if not (MNT_TARGET / "nest" / "level_0" / "moved_me.txt").exists():
                failures.append("Cross-directory rename failed")
            
            if (MNT_TARGET / "move_me.txt").exists():
                failures.append("Old file still exists after rename")
        except Exception as e:
            failures.append(f"Rename Check Error: {e}")

        if failures:
            logger.error(f"FAILURES DETECTED: {failures}")
            # Forensics: List what DOES exist
            logger.info("--- Target Directory State ---")
            subprocess.run(["ls", "-R", str(MNT_TARGET)], check=False)
            return False
        
        logger.info("Basic Verification PASSED")
        return True

    def run_torture_tests(self):
        logger.info(">>> STEP 5a: TORTURE TEST - Write/Metadata Storm")
        
        torture_dir = MNT_SOURCE / "torture_chamber"
        torture_dir.mkdir(exist_ok=True)
        
        file_count = 5000
        threads = 10
        files_per_thread = file_count // threads
        
        logger.info(f"Unleashing {threads} concurrent threads creating/renaming {file_count} files...")
        
        start_time = time.perf_counter()

        def worker_func(tid):
            for i in range(files_per_thread):
                fname = f"t{tid}_f{i}"
                fpath = torture_dir / fname
                fpath.write_text(f"Torture Data {tid}-{i}")
                new_fpath = torture_dir / f"{fname}_moved"
                os.rename(fpath, new_fpath)
                with open(new_fpath, "a") as f:
                    f.write("APPEND")
                if i % 2 == 0:
                    os.unlink(new_fpath)

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(worker_func, i) for i in range(threads)]
            concurrent.futures.wait(futures)
            
        elapsed = time.perf_counter() - start_time
        logger.info(f"Torture Storm finished in {elapsed:.2f}s. Waiting for sync...")
        
        wait_time = 15
        time.sleep(wait_time)
        
        logger.info("Verifying consistency...")
        src_files = set(os.listdir(torture_dir))
        tgt_torture_dir = MNT_TARGET / "torture_chamber"
        
        if not tgt_torture_dir.exists():
            logger.error("FAIL: Torture directory missing on target!")
            return False
            
        tgt_files = set(os.listdir(tgt_torture_dir))
        missing = src_files - tgt_files
        extras = tgt_files - src_files
        
        if missing or extras:
            logger.error(f"FAIL: Consistency check failed. Missing: {len(missing)}, Extra: {len(extras)}")
            return False
            
        logger.info(f"Torture Test PASSED. {len(src_files)} files perfectly synced.")
        return True

    def run_adversarial_tests(self):
        logger.info(">>> STEP 5b: Adversarial - Daemon Crash Recovery")
        bg_file = MNT_SOURCE / "crash_test.bin"
        
        def background_writer():
            with open(bg_file, "wb") as f:
                for _ in range(50):
                    f.write(os.urandom(1024 * 1024))
                    f.flush()
                    os.fsync(f.fileno())
                    time.sleep(0.05)
        
        t = threading.Thread(target=background_writer)
        t.start()
        time.sleep(1)
        
        logger.warning(f"KILLING DAEMON PID {self.daemon_process.pid}")
        os.kill(self.daemon_process.pid, signal.SIGKILL)
        self.daemon_process.wait()
        
        t.join()
        logger.info("Restarting Daemon...")
        self.start_daemon()
        time.sleep(5)
        
        if not (MNT_TARGET / "crash_test.bin").exists():
            logger.error("FAIL: Crash recovery failed.")
            return False
            
        src_hash = subprocess.check_output(["md5sum", str(bg_file)]).split()[0]
        tgt_hash = subprocess.check_output(["md5sum", str(MNT_TARGET / "crash_test.bin")]).split()[0]
        
        if src_hash != tgt_hash:
            logger.error(f"FAIL: MD5 Mismatch. Src: {src_hash} Tgt: {tgt_hash}")
            return False
            
        logger.info("Adversarial: Crash Recovery PASSED")
        return True

    def run_benchmarks(self):
        logger.info(">>> STEP 6: Comparative Benchmarks")
        bench_file = MNT_SOURCE / "bench.dat"
        logger.info(f"Generating {BENCHMARK_SIZE_MB}MB random payload...")
        subprocess.run(["dd", "if=/dev/urandom", f"of={bench_file}", "bs=1M", f"count={BENCHMARK_SIZE_MB}", "status=none"], check=True)
        subprocess.run(["sync"], check=True)
        
        logger.info("Benchmarking 'cp -r'...")
        start = time.perf_counter()
        subprocess.run(["cp", str(bench_file), str(MNT_BENCH_CP / "bench.dat")], check=True)
        subprocess.run(["sync"], check=True)
        duration_cp = time.perf_counter() - start
        self.bench_results['cp'] = duration_cp

        logger.info("Benchmarking 'rsync'...")
        start = time.perf_counter()
        subprocess.run(["rsync", "-a", str(bench_file), str(MNT_BENCH_RSYNC / "bench.dat")], check=True)
        subprocess.run(["sync"], check=True)
        duration_rsync = time.perf_counter() - start
        self.bench_results['rsync'] = duration_rsync

        logger.info("Benchmarking 'foxing'...")
        foxing_file = MNT_SOURCE / "bench_foxing.dat"
        foxing_target = MNT_TARGET / "bench_foxing.dat"
        shutil.copyfile(bench_file, foxing_file) 
        
        start = time.perf_counter()
        timeout = 60
        while time.perf_counter() - start < timeout:
            if foxing_target.exists():
                if foxing_target.stat().st_size == (BENCHMARK_SIZE_MB * 1024 * 1024):
                    break
            time.sleep(0.1)
        duration_foxing = time.perf_counter() - start
        
        if not foxing_target.exists() or foxing_target.stat().st_size != (BENCHMARK_SIZE_MB * 1024 * 1024):
            logger.error("Foxing benchmark timed out.")
            duration_foxing = 999.99
        
        logger.info(f"foxing: {duration_foxing:.4f}s")
        self.bench_results['foxing'] = duration_foxing
        return True

    def safe_unmount(self, path):
        if path.is_mount():
            logger.debug(f"Unmounting {path}...")
            try:
                subprocess.run(["umount", str(path)], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                pass

    def teardown(self, silent=False):
        if not silent: logger.info(">>> Teardown & Cleanup")
        if self.daemon_process:
            if self.daemon_process.poll() is None:
                if not silent: logger.info("Stopping daemon...")
                self.daemon_process.terminate()
                try:
                    self.daemon_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if not silent: logger.warning("Daemon hung, killing...")
                    self.daemon_process.kill()
            
            if hasattr(self, 'stdout_file'): self.stdout_file.close()
            if hasattr(self, 'stderr_file'): self.stderr_file.close()

        self.safe_unmount(MNT_SOURCE)
        self.safe_unmount(MNT_TARGET)
        self.safe_unmount(MNT_BENCH_CP)
        self.safe_unmount(MNT_BENCH_RSYNC)
        if not silent: logger.info("Cleanup complete.")

    def generate_ai_report(self, success):
        logger.info("Generating AI Context Report...")
        report_path = RUN_DIR / "ai_context_summary.md"
        
        daemon_stdout_log = ""
        if (RUN_DIR / "daemon_stdout.log").exists():
            with open(RUN_DIR / "daemon_stdout.log") as f:
                logs = f.readlines()
                daemon_stdout_log = "".join(logs[-30:]) 

        daemon_err = ""
        if (RUN_DIR / "daemon_stderr.log").exists():
            with open(RUN_DIR / "daemon_stderr.log") as f:
                logs = f.readlines()
                filtered = [l for l in logs if "ERROR" in l or "panic" in l.lower() or "WARN" in l]
                daemon_err = "".join(filtered[-30:]) if filtered else "No critical errors found."

        metrics_snap = "N/A"
        try:
            # Try to grab metrics even if we failed, daemon might still be up if not killed yet
            response = urllib.request.urlopen(f"http://127.0.0.1:{METRICS_PORT}/metrics", timeout=2)
            metrics_snap = response.read().decode('utf-8')
        except Exception as e:
            metrics_snap = f"Failed to fetch metrics: {e}"

        bench_table = "| Tool | Time (s) | vs Foxing |\n|---|---|---|\n"
        if self.bench_results:
            fox_time = self.bench_results.get('foxing', 1.0)
            for tool, dur in self.bench_results.items():
                ratio = f"{dur / fox_time:.2f}x" if fox_time > 0 else "N/A"
                if tool == 'foxing': ratio = "1.00x"
                bench_table += f"| {tool} | {dur:.4f} | {ratio} |\n"

        content = f"""# Foxing Test Run: {TIMESTAMP}

## Result
**Status**: {"PASS" if success else "FAIL"}

## Benchmarks ({BENCHMARK_SIZE_MB}MB Random I/O)
{bench_table}

## Environment
- **Kernel**: {os.uname().release}
- **Source**: XFS (Reflink) Loopback
- **Target**: XFS (Reflink) Loopback

## Daemon Health
### Critical Errors/Warnings (Last 30 lines Stderr)
```text
{daemon_err}
```

### Startup Log (Last 30 lines Stdout)
```text
{daemon_stdout_log}
```

### Metrics Snapshot (Partial)
```text
{metrics_snap[:2000]}...
```
"""
        report_path.write_text(content)
        logger.info(f"AI Report written to {report_path}")

def main():
    env = TestEnvironment()
    success = False
    try:
        with env:
            env.generate_data()
            env.start_daemon()
            
            # Generate Report immediately after run attempts to capture metrics before teardown kills daemon
            try:
                if env.run_basic_verification():
                    if env.run_torture_tests():
                        if env.run_adversarial_tests():
                            if env.run_benchmarks():
                                success = True
            finally:
                env.generate_ai_report(success)
                        
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.exception("Test Suite Crashed")
    finally:
        if not success:
            sys.exit(1)

if __name__ == "__main__":
    main()
