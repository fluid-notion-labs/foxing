#!/bin/bash
set -e

# ==============================================================================
# FOXING COMPREHENSIVE TEST HARNESS (Robust Mode)
# ==============================================================================
# Workflow:
# 1. Build binary inside Distrobox/Container: `cargo build --release`
# 2. Run this script on Host as Root: `sudo ./tests/comprehensive_harness.sh`
#
# Robustness Improvements:
# - Eventual Consistency: Retries checksum verification to handle async replication.
# - Race Condition Fix: Handles "Empty File" race where Create runs before Write.
# - Resilience: Checks process health aggressively.
# ==============================================================================

# --- Configuration ---
TEST_ROOT="/tmp/foxing_harness"
SOURCE_IMG="$TEST_ROOT/source.img"
TARGET_IMG="$TEST_ROOT/target.img"
SOURCE_MNT="$TEST_ROOT/mnt_source"
TARGET_MNT="$TEST_ROOT/mnt_target"
CONFIG_FILE="$TEST_ROOT/config.toml"
DAEMON_LOG="$TEST_ROOT/daemon.log"
METRICS_PORT=9101
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_DIR="tests/reports"
REPORT_FILE="$REPORT_DIR/run_${TIMESTAMP}.log"
LLM_REPORT_FILE="$REPORT_DIR/llm_context_${TIMESTAMP}.txt"
BINARY="./target/release/foxing"

# --- Colors ---
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# --- Root Check ---
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Error: This script must be run as root (Host OS).${NC}"
  exit 1
fi

mkdir -p "$REPORT_DIR"
mkdir -p "$TEST_ROOT"

# --- Logging Helper ---
log() {
    local msg="$1"
    echo -e "${CYAN}[$(date +'%H:%M:%S')] ${msg}${NC}" | tee -a "$REPORT_FILE"
}

pass() {
    echo -e "${GREEN}  [PASS] $1${NC}" | tee -a "$REPORT_FILE"
    echo "PASS: $1" >> "$LLM_REPORT_FILE"
}

fail() {
    echo -e "${RED}  [FAIL] $1${NC}" | tee -a "$REPORT_FILE"
    echo "FAIL: $1" >> "$LLM_REPORT_FILE"
}

# ==============================================================================
# 1. PRE-FLIGHT CHECKS
# ==============================================================================
check_requirements() {
    if [ ! -f "$BINARY" ]; then
        if command -v cargo &> /dev/null; then
            log "Building binary..."
            cargo build --release
        else
            echo -e "${RED}CRITICAL: Binary missing. Build in container first.${NC}"
            exit 1
        fi
    fi
}

# ==============================================================================
# 2. ENVIRONMENT SETUP
# ==============================================================================
setup_environment() {
    log "Setting up Test Environment..."
    pkill -f "foxing daemon" || true
    umount "$SOURCE_MNT" 2>/dev/null || true
    umount "$TARGET_MNT" 2>/dev/null || true
    
    mkdir -p "$SOURCE_MNT" "$TARGET_MNT"
    
    if [ ! -f "$SOURCE_IMG" ]; then
        dd if=/dev/zero of="$SOURCE_IMG" bs=1M count=2048 status=none
        mkfs.xfs -f "$SOURCE_IMG" > /dev/null
    fi
    
    if [ ! -f "$TARGET_IMG" ]; then
        dd if=/dev/zero of="$TARGET_IMG" bs=1M count=2048 status=none
        mkfs.xfs -f "$TARGET_IMG" > /dev/null
    fi
    
    mount -o loop "$SOURCE_IMG" "$SOURCE_MNT"
    mount -o loop "$TARGET_IMG" "$TARGET_MNT"
    
    cat > "$CONFIG_FILE" <<EOF
worker_count = 4
queue_max = 200000
metrics_port = $METRICS_PORT
global_buffer_limit = 50485760
fatal_metrics_bind = false
max_system_load_avg = 100.0
hydration_delay_ms = 1
capacity_threshold_mb = 50
force_flush_interval_secs = 1
io_priority = "High"

[[sources]]
path = "$SOURCE_MNT"

  [[sources.targets]]
  path = "$TARGET_MNT"
  profile = "NVMe"
  initial_sync = true
  autotune_target_latency_ms = 5
  enable_versioning = true
  supports_reflink = true
  max_versions = 5
  max_versions_size_mb = 100
  excludes = [".*/ignore_me/.*", ".*\\\\.tmp$"]
EOF
}

# ==============================================================================
# 3. DAEMON MANAGEMENT
# ==============================================================================
start_daemon() {
    log "Starting Foxing Daemon..."
    > "$DAEMON_LOG"
    RUST_LOG=info,foxing=debug $BINARY daemon --config "$CONFIG_FILE" > "$DAEMON_LOG" 2>&1 &
    DAEMON_PID=$!
    
    echo -n "Waiting for BPF..."
    local bpf_ready=false
    for i in {1..15}; do
        if ! kill -0 $DAEMON_PID 2>/dev/null; then
            echo -e "${RED} Daemon Died.${NC}"
            return 1
        fi
        if grep -q "BPF: Successfully attached" "$DAEMON_LOG"; then
            echo " Attached."
            bpf_ready=true
            break
        fi
        sleep 1
        echo -n "."
    done
    
    if [ "$bpf_ready" = false ]; then
        echo -e "${YELLOW} BPF Timeout (Continuing anyway)${NC}"
    fi
}

stop_daemon() {
    if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID"
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
}

# ==============================================================================
# 4. ROBUST VERIFICATION
# ==============================================================================
verify_checksum_retry() {
    local file="$1"
    local src_path="$SOURCE_MNT/$file"
    local tgt_path="$TARGET_MNT/$file"
    local retries=10  # 10 * 0.5s = 5 seconds tolerance
    
    if [ ! -f "$src_path" ]; then
        fail "Source file missing: $file"
        return
    fi

    # Wait for file existence first
    for i in $(seq 1 $retries); do
        if [ -f "$tgt_path" ]; then break; fi
        sleep 0.5
    done

    if [ ! -f "$tgt_path" ]; then
        fail "Target file missing after timeout: $file"
        return
    fi

    # Compare Checksums with Retry (Handling eventual consistency)
    local match=false
    local src_sum=""
    local tgt_sum=""
    
    for i in $(seq 1 $retries); do
        src_sum=$(md5sum "$src_path" | awk '{print $1}')
        tgt_sum=$(md5sum "$tgt_path" | awk '{print $1}')
        
        if [ "$src_sum" == "$tgt_sum" ]; then
            match=true
            break
        fi
        # If mismatch, it might be mid-transfer (empty file vs full file)
        # Wait and try again
        sleep 0.5
    done
    
    if [ "$match" = true ]; then
        pass "Data Integrity: $file"
    else
        fail "Data Corruption: $file (Src: $src_sum, Tgt: $tgt_sum)"
    fi
}

verify_directory_retry() {
    local dir="$1"
    local expected_count="$2"
    local tgt_path="$TARGET_MNT/$dir"
    local retries=15
    
    for i in $(seq 1 $retries); do
        if [ -d "$tgt_path" ]; then
            local count=$(ls "$tgt_path" | wc -l)
            if [ "$count" -eq "$expected_count" ]; then
                pass "Directory Sync: $dir ($count files)"
                return
            fi
        fi
        sleep 1
    done
    
    # Final check failed
    if [ ! -d "$tgt_path" ]; then
        fail "Directory Missing: $dir"
    else
        local count=$(ls "$tgt_path" | wc -l)
        fail "Directory Count Mismatch: $dir (Expected: $expected_count, Found: $count)"
    fi
}

# ==============================================================================
# 5. TEST SUITES
# ==============================================================================

run_exponential_tests() {
    log "=== Starting Exponential Size Tests ==="
    
    for size_mb in 1 10 100; do
        local filename="test_${size_mb}MB.dat"
        log "Generating $filename..."
        
        # Create
        dd if=/dev/urandom of="$SOURCE_MNT/$filename" bs=1M count=$size_mb status=none
        sync
        verify_checksum_retry "$filename"
        
        # Modify (Append)
        echo "AppendData" >> "$SOURCE_MNT/$filename"
        sync
        verify_checksum_retry "$filename"
    done
}

run_adversarial_tests() {
    log "=== Starting Adversarial Conditions ==="
    
    log "Scenario: Metadata Storm (1000 files)"
    mkdir -p "$SOURCE_MNT/storm"
    # Create rapidly
    for i in {1..1000}; do
        touch "$SOURCE_MNT/storm/file_$i"
    done
    sync
    verify_directory_retry "storm" 1000
    
    log "Scenario: Deep Nesting & Rename"
    mkdir -p "$SOURCE_MNT/a/b/c/d/e/f/g"
    echo "Deep Data" > "$SOURCE_MNT/a/b/c/d/e/f/g/deep.txt"
    sync
    verify_checksum_retry "a/b/c/d/e/f/g/deep.txt"
    
    mv "$SOURCE_MNT/a" "$SOURCE_MNT/z"
    sync
    verify_checksum_retry "z/b/c/d/e/f/g/deep.txt"
}

run_oneshot_recovery() {
    log "=== Starting One-Shot Recovery Test ==="
    stop_daemon
    
    log "Simulating Disaster: Wiping Target..."
    rm -rf "$TARGET_MNT"/*
    sync
    
    echo "Offline Data" > "$SOURCE_MNT/offline_change.txt"
    
    log "Triggering One-Shot Synchronization..."
    echo -e "${YELLOW}>>> LAUNCHING TUI. PLEASE PRESS 'q' TO FINISH THE TEST <<<${NC}"
    sleep 2
    
    # We run this in the foreground WITHOUT redirection so the TUI works.
    $BINARY oneshot --config "$CONFIG_FILE"
    
    if [ -f "$TARGET_MNT/offline_change.txt" ] && [ -d "$TARGET_MNT/z" ]; then
        pass "One-Shot Recovery Successful"
    else
        fail "One-Shot Recovery Failed"
    fi
}

# ==============================================================================
# 6. METRICS & REPORTING
# ==============================================================================
collect_final_metrics() {
    log "Collecting Metrics..."
    echo "=== LLM CONTEXT REPORT ===" > "$LLM_REPORT_FILE"
    echo "Run ID: $TIMESTAMP" >> "$LLM_REPORT_FILE"
    echo "" >> "$LLM_REPORT_FILE"
    echo "--- Test Results ---" >> "$LLM_REPORT_FILE"
    grep "PASS\|FAIL" "$REPORT_FILE" >> "$LLM_REPORT_FILE"
    echo "" >> "$LLM_REPORT_FILE"
    echo "--- Daemon Errors (Tail 50) ---" >> "$LLM_REPORT_FILE"
    grep "ERROR" "$DAEMON_LOG" | tail -n 50 >> "$LLM_REPORT_FILE"
    
    if grep -q "panic" "$DAEMON_LOG"; then
        echo "CRITICAL: Panic detected!" >> "$LLM_REPORT_FILE"
        grep -C 5 "panic" "$DAEMON_LOG" >> "$LLM_REPORT_FILE"
    fi
    log "Reports generated at $REPORT_DIR"
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
cleanup() {
    stop_daemon
    umount "$SOURCE_MNT" 2>/dev/null || true
    umount "$TARGET_MNT" 2>/dev/null || true
    losetup -d "$SOURCE_IMG" 2>/dev/null || true
}
trap cleanup EXIT

check_requirements
setup_environment

if start_daemon; then
    tail -f "$DAEMON_LOG" | grep --line-buffered "foxing" &
    TAIL_PID=$!
    
    run_exponential_tests
    run_adversarial_tests
    
    kill $TAIL_PID 2>/dev/null || true
else
    fail "Daemon failed to start."
fi

run_oneshot_recovery
collect_final_metrics
log "Complete."
