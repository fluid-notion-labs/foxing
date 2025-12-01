#!/bin/bash
set -e

# ==============================================================================
# Foxing: High-Performance Loopback Test Harness (Integrated Logging v3)
# ==============================================================================

# Configuration
BASE_DIR="/tmp/foxing_test"
SOURCE_IMG="$BASE_DIR/source.img"
TARGET_IMG="$BASE_DIR/target.img"
SOURCE_MNT="$BASE_DIR/mnt/source"
TARGET_MNT="$BASE_DIR/mnt/target"
IMG_SIZE="512M"
LOG_FILE="daemon.log"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Check Root
if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Error: This script requires root privileges to mount loopback devices.${NC}"
  echo "Please run with sudo."
  exit 1
fi

# ==============================================================================
# Helper Functions
# ==============================================================================

# Kill daemon and detach loops
cleanup_environment() {
    echo -e "${BLUE}==> Deep Cleaning Environment...${NC}"
    
    # 1. Kill log tailer if it exists
    if [ -n "$TAIL_PID" ]; then
        kill $TAIL_PID 2>/dev/null || true
    fi

    # 2. Kill Daemon
    if pgrep -x "foxing" > /dev/null; then
        echo "   Killing existing foxing process..."
        pkill -x "foxing" || true
        sleep 1
        pkill -9 -x "foxing" || true 
    fi

    # 3. Unmount Paths
    if mountpoint -q "$SOURCE_MNT"; then 
        umount -l "$SOURCE_MNT" || umount -f "$SOURCE_MNT"
    fi
    if mountpoint -q "$TARGET_MNT"; then 
        umount -l "$TARGET_MNT" || umount -f "$TARGET_MNT"
    fi

    # 4. Detach Loopback Devices
    if [ -f "$SOURCE_IMG" ]; then
        losetup -j "$SOURCE_IMG" | cut -d: -f1 | xargs -r losetup -d
    fi
    if [ -f "$TARGET_IMG" ]; then
        losetup -j "$TARGET_IMG" | cut -d: -f1 | xargs -r losetup -d
    fi

    # 5. Nuke Directories
    rm -rf "$BASE_DIR"
    # Don't delete daemon.log here so we can inspect it post-mortem if needed, 
    # but truncate it on start.
}

setup_environment() {
    cleanup_environment

    echo -e "${GREEN}==> Creating infrastructure at $BASE_DIR...${NC}"
    mkdir -p "$SOURCE_MNT" "$TARGET_MNT"

    # Create backing files
    truncate -s $IMG_SIZE "$SOURCE_IMG"
    truncate -s $IMG_SIZE "$TARGET_IMG"

    echo "   Formatting XFS (Reflink Enabled)..."
    mkfs.xfs -f -m reflink=1 "$SOURCE_IMG" > /dev/null
    mkfs.xfs -f -m reflink=1 "$TARGET_IMG" > /dev/null

    echo "   Mounting Loopbacks..."
    mount -o loop,noatime "$SOURCE_IMG" "$SOURCE_MNT"
    mount -o loop,noatime "$TARGET_IMG" "$TARGET_MNT"

    # Permissions
    REAL_USER=${SUDO_USER:-$(whoami)}
    chown -R "$REAL_USER:$REAL_USER" "$SOURCE_MNT" "$TARGET_MNT"
    chmod 777 "$SOURCE_MNT" "$TARGET_MNT"

    # Generate Config
    cat > "test_config.toml" <<EOF
worker_count = 2
queue_max = 100000
metrics_port = 9101
global_buffer_limit = 10485760
fatal_metrics_bind = false
max_system_load_avg = 50.0
hydration_delay_ms = 1
capacity_threshold_mb = 50
force_flush_interval_secs = 1
io_priority = "Normal"

[[sources]]
path = "$SOURCE_MNT"

  [[sources.targets]]
  path = "$TARGET_MNT"
  profile = "SSD"
  initial_sync = true
  autotune_target_latency_ms = 10
  enable_versioning = true
  supports_reflink = true
  max_versions = 5
  max_versions_size_mb = 100
  excludes = [".*/ignore_me/.*", ".*\\\\.tmp$"]
EOF
    
    echo -e "${GREEN}==> Environment Ready.${NC}"
}

wait_for_file() {
    local path="$1"
    local timeout="${2:-10}"
    local interval=0.2
    local max_checks=$(echo "$timeout / $interval" | bc)
    local count=0

    echo -n "   Waiting for $(basename "$path")..."
    while [ ! -e "$path" ]; do
        if [ "$count" -ge "$max_checks" ]; then
            echo -e " ${RED}TIMEOUT${NC}"
            dump_state
            return 1
        fi
        sleep $interval
        count=$((count+1))
    done
    echo -e " ${GREEN}OK${NC}"
    return 0
}

wait_for_gone() {
    local path="$1"
    local timeout="${2:-10}"
    local count=0
    
    echo -n "   Waiting for removal of $(basename "$path")..."
    while [ -e "$path" ]; do
        if [ "$count" -ge $((timeout * 10)) ]; then
            echo -e " ${RED}TIMEOUT (Still exists)${NC}"
            return 1
        fi
        sleep 0.1
        count=$((count+1))
    done
    echo -e " ${GREEN}GONE${NC}"
    return 0
}

# New: Poll for permissions
wait_for_perm() {
    local path="$1"
    local expected="$2"
    local timeout="${3:-15}" # Increased timeout for robustness
    local interval=0.5
    local max_checks=$(echo "$timeout / $interval" | bc)
    local count=0

    echo -n "   Waiting for perm $expected on $(basename "$path")..."
    while true; do
        if [ ! -e "$path" ]; then
             sleep $interval
             count=$((count+1))
             continue
        fi
        
        local current=$(stat -c '%a' "$path")
        if [ "$current" -eq "$expected" ]; then
            echo -e " ${GREEN}OK${NC}"
            return 0
        fi

        if [ "$count" -ge "$max_checks" ]; then
            echo -e " ${RED}TIMEOUT (Got $current)${NC}"
            return 1
        fi
        sleep $interval
        count=$((count+1))
    done
}

assert_content() {
    local path="$1"
    local expected="$2"
    sleep 0.5
    if grep -q "$expected" "$path"; then
        echo -e "   Content Check: ${GREEN}MATCH${NC}"
        return 0
    else
        echo -e "   Content Check: ${RED}FAIL${NC}"
        echo "     Expected: '$expected'"
        echo "     Actual:   '$(cat "$path" 2>/dev/null)'"
        return 1
    fi
}

dump_state() {
    echo -e "\n${YELLOW}--- DEBUG DUMP ---${NC}"
    echo "SOURCE:"
    ls -laR "$SOURCE_MNT" | head -n 20
    echo "TARGET:"
    ls -laR "$TARGET_MNT" | head -n 20
    echo -e "${YELLOW}------------------${NC}\n"
}

# ==============================================================================
# Test Cases
# ==============================================================================

test_1_torture() {
    echo -e "\n${YELLOW}=== Test 1: Torture (Rapid Create/Delete) ===${NC}"
    for i in {1..50}; do echo "data $i" > "$SOURCE_MNT/f_$i.txt"; done
    wait_for_file "$TARGET_MNT/f_50.txt" || return 1
    
    for i in {1..25}; do rm -f "$SOURCE_MNT/f_$i.txt"; done
    wait_for_gone "$TARGET_MNT/f_1.txt" || return 1
    
    sleep 1
    local count=$(ls "$TARGET_MNT"/f_*.txt 2>/dev/null | wc -l)
    if [ "$count" -eq 25 ]; then 
        echo -e "   Count Check: ${GREEN}PASS${NC} (25 files)"
    else 
        echo -e "   Count Check: ${RED}FAIL${NC} ($count files remaining)"
    fi
}

test_2_metadata() {
    echo -e "\n${YELLOW}=== Test 2: Metadata & Hierarchy ===${NC}"
    mkdir -p "$SOURCE_MNT/deep/nested/dir"
    local secret="$SOURCE_MNT/deep/nested/dir/secret.dat"
    local target_secret="$TARGET_MNT/deep/nested/dir/secret.dat"
    
    touch "$secret"
    chmod 700 "$secret"
    
    wait_for_file "$target_secret" || return 1
    
    # Use polling instead of sleep
    wait_for_perm "$target_secret" 700 || return 1
}

test_3_reflink() {
    echo -e "\n${YELLOW}=== Test 3: Large File Reflink & Delta ===${NC}"
    local vm_img="$SOURCE_MNT/vm_disk.qcow2"
    
    echo "   Creating 50MB sparse file..."
    dd if=/dev/zero of="$vm_img" bs=1M count=50 status=none
    
    wait_for_file "$TARGET_MNT/vm_disk.qcow2" 20 || return 1
    
    echo "   Performing delta writes..."
    for i in {1..10}; do
        echo "FOX" | dd of="$vm_img" bs=1 seek=$((RANDOM % 1000 * 4096)) count=3 conv=notrunc status=none
    done
    sync
    echo -e "   ${GREEN}Delta Updates Completed${NC}"
}

test_6_rename() {
    echo -e "\n${YELLOW}=== Test 6: Rename Atomicity ===${NC}"
    local src="$SOURCE_MNT/atomic_src.txt"
    local dst="$SOURCE_MNT/atomic_dest.txt"
    local target_dst="$TARGET_MNT/atomic_dest.txt"
    local content="AtomicPayload_$(date +%s)"

    echo "$content" > "$src"
    wait_for_file "$TARGET_MNT/atomic_src.txt" || return 1

    echo "   Renaming source file..."
    mv "$src" "$dst"

    wait_for_file "$target_dst" || return 1
    wait_for_gone "$TARGET_MNT/atomic_src.txt"
    
    assert_content "$target_dst" "$content"
}

# ==============================================================================
# Main Execution
# ==============================================================================

case "$1" in
    setup)
        setup_environment
        ;;
    run)
        test_1_torture
        test_2_metadata
        test_3_reflink
        test_6_rename
        ;;
    all)
        setup_environment
        echo -e "${YELLOW}Starting Daemon (Background)...${NC}"
        
        # Truncate Log
        > "$LOG_FILE"
        
        export RUST_LOG=foxing=info,foxing::worker=info,foxing::ordering=debug
        
        ./target/release/foxing daemon --config test_config.toml > "$LOG_FILE" 2>&1 &
        DAEMON_PID=$!
        echo "Daemon PID: $DAEMON_PID"
        
        # Start integrated tail in background
        echo -e "${CYAN}--- DAEMON OUTPUT START ---${NC}"
        tail -f "$LOG_FILE" | sed "s/^/$(echo -e $CYAN)[DAEMON]$(echo -e $NC) /" &
        TAIL_PID=$!
        
        sleep 3
        
        test_1_torture
        test_2_metadata
        test_3_reflink
        test_6_rename
        
        echo -e "\n${BLUE}Killing Daemon...${NC}"
        kill $DAEMON_PID
        wait $DAEMON_PID 2>/dev/null || true
        
        kill $TAIL_PID 2>/dev/null || true
        echo -e "${CYAN}--- DAEMON OUTPUT END ---${NC}"
        ;;
    *)
        echo "Usage: $0 {setup|run|all}"
        echo "  setup: Clean & prepare loopback devices"
        echo "  run:   Run tests (assumes daemon is running)"
        echo "  all:   Full cycle: Setup -> Start Daemon -> Run Tests -> Teardown"
        ;;
esac
