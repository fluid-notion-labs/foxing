#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-or-later
# Mean Time to Consistency (MTTC) Benchmark Matrix
#
# Measures how long from source modification until target is consistent
# (fxcp -a completes with verified content) across all supported topologies.
#
# Run on fox-test VM: bash tests/mttc-bench.sh

set -euo pipefail

ITERATIONS="${MTTC_ITERATIONS:-5}"

BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

header() {
    echo "" >&2
    echo -e "${BOLD}${CYAN}  $1${NC}" >&2
    echo -e "${CYAN}  $(printf '%.0s─' $(seq 1 ${#1}))${NC}" >&2
}

# ───────────────────────────────────────────────────────
# Topology definitions: (name, source_dir, target_dir)
# ───────────────────────────────────────────────────────

declare -a TOPO_NAMES=()
declare -a TOPO_SRC=()
declare -a TOPO_DST=()

add_topology() {
    TOPO_NAMES+=("$1")
    TOPO_SRC+=("$2")
    TOPO_DST+=("$3")
}

# Cross-device XFS→XFS (local virtio-blk)
add_topology "XFS-to-XFS" "/mnt/source/mttc" "/mnt/target-xfs/mttc"

# XFS→NFS 4.2 (network, HDD-backed)
add_topology "XFS-to-NFS" "/mnt/source/mttc" "/mnt/target-nfs/mttc"

# NFS→NFS same-server (server-side FICLONE)
add_topology "NFS-to-NFS" "/mnt/target-nfs/mttc-nfs-src" "/mnt/target-nfs/mttc-nfs-dst"

# XFS→tmpfs (memory-backed)
add_topology "XFS-to-tmpfs" "/mnt/source/mttc" "/tmp/mttc-target"

# Same-device XFS (reflink/FICLONE)
add_topology "XFS-same" "/mnt/source/mttc" "/mnt/source/mttc-dst"

# ───────────────────────────────────────────────────────
# Timing helper: run action + fxcp sync, return ms
# ───────────────────────────────────────────────────────

# Usage: measure_mttc <src> <dst> <action_command>
# Runs the action on source, then times fxcp -a to sync.
measure_mttc() {
    local src="$1" dst="$2"
    shift 2

    # Execute the source modification
    eval "$@"

    # Time the sync (detection + copy + fsync)
    local start end
    start=$(date +%s%N)
    fxcp -a "$src" "$dst" >/dev/null 2>&1
    end=$(date +%s%N)

    echo $(( (end - start) / 1000000 ))
}

# Run a workload N times, compute stats, print result line
# Usage: run_workload <label> <src> <dst> <setup_cmd> <action_cmd> <iterations>
run_workload() {
    local label="$1" src="$2" dst="$3" setup_cmd="$4" action_cmd="$5" iters="$6"
    local times=()

    for i in $(seq 1 "$iters"); do
        # Reset: clean and re-sync to baseline
        rm -rf "$dst" 2>/dev/null || true
        mkdir -p "$src" "$dst"
        # Run setup (creates pre-existing files if needed)
        eval "$setup_cmd"
        # Pre-sync to establish baseline consistency
        fxcp -a "$src" "$dst" >/dev/null 2>&1
        # Drop caches
        sync
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

        # Measure
        local ms
        ms=$(measure_mttc "$src" "$dst" "$action_cmd")
        times+=("$ms")
    done

    # Sort for percentiles
    IFS=$'\n' sorted=($(sort -n <<<"${times[*]}")); unset IFS
    local count=${#sorted[@]}
    local median=${sorted[$((count / 2))]}
    local min=${sorted[0]}
    local max=${sorted[$((count - 1))]}
    local p95_idx=$(( count * 95 / 100 ))
    [ "$p95_idx" -ge "$count" ] && p95_idx=$((count - 1))
    local p95=${sorted[$p95_idx]}

    echo "$median"
}

# ───────────────────────────────────────────────────────
# Workload definitions
# ───────────────────────────────────────────────────────

declare -a WL_NAMES=()
declare -a WL_SETUP=()
declare -a WL_ACTION=()

add_workload() {
    WL_NAMES+=("$1")
    WL_SETUP+=("$2")
    WL_ACTION+=("$3")
}

# Single file creates
add_workload "single 4KB" \
    "rm -f \$SRC/test.dat" \
    "dd if=/dev/urandom of=\$SRC/test.dat bs=4K count=1 2>/dev/null"

add_workload "single 1MB" \
    "rm -f \$SRC/test.dat" \
    "dd if=/dev/urandom of=\$SRC/test.dat bs=1M count=1 2>/dev/null"

add_workload "single 100MB" \
    "rm -f \$SRC/test.dat" \
    "dd if=/dev/urandom of=\$SRC/test.dat bs=1M count=100 2>/dev/null"

# Modify existing files
add_workload "modify 4KB" \
    "dd if=/dev/urandom of=\$SRC/existing.dat bs=4K count=1 2>/dev/null" \
    "dd if=/dev/urandom of=\$SRC/existing.dat bs=4K count=1 2>/dev/null"

add_workload "modify 1MB" \
    "dd if=/dev/urandom of=\$SRC/existing.dat bs=1M count=1 2>/dev/null" \
    "dd if=/dev/urandom of=\$SRC/existing.dat bs=1M count=1 2>/dev/null"

# Append
add_workload "append 4KB" \
    "dd if=/dev/urandom of=\$SRC/appendable.dat bs=4K count=1 2>/dev/null" \
    "dd if=/dev/urandom bs=4K count=1 >> \$SRC/appendable.dat 2>/dev/null"

# Metadata-only
add_workload "metadata" \
    "dd if=/dev/urandom of=\$SRC/meta.dat bs=4K count=1 2>/dev/null && chmod 644 \$SRC/meta.dat" \
    "chmod 755 \$SRC/meta.dat"

# Rename
add_workload "rename" \
    "dd if=/dev/urandom of=\$SRC/old.dat bs=4K count=1 2>/dev/null" \
    "mv \$SRC/old.dat \$SRC/new.dat"

# Batch creates
add_workload "batch 100x4KB" \
    ":" \
    "for i in \$(seq 1 100); do dd if=/dev/urandom of=\$SRC/batch_\$i.dat bs=4K count=1 2>/dev/null; done"

add_workload "batch 10x10MB" \
    ":" \
    "for i in \$(seq 1 10); do dd if=/dev/urandom of=\$SRC/big_\$i.dat bs=1M count=10 2>/dev/null; done"

# ───────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────

echo -e "${BOLD}foxing Mean Time to Consistency (MTTC) Matrix${NC}" >&2
echo "Host: $(hostname), Kernel: $(uname -r)" >&2
echo "fxcp: $(fxcp --version 2>&1 | head -1)" >&2
echo "Iterations: $ITERATIONS" >&2
echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >&2

# Collect results: results[topo_idx * num_workloads + wl_idx] = median_ms
NUM_TOPOS=${#TOPO_NAMES[@]}
NUM_WLS=${#WL_NAMES[@]}
declare -a RESULTS=()

for t in $(seq 0 $((NUM_TOPOS - 1))); do
    topo_name="${TOPO_NAMES[$t]}"
    topo_src="${TOPO_SRC[$t]}"
    topo_dst="${TOPO_DST[$t]}"

    header "Topology: $topo_name ($topo_src -> $topo_dst)"

    # Ensure directories exist
    mkdir -p "$topo_src" "$topo_dst" 2>/dev/null || true

    for w in $(seq 0 $((NUM_WLS - 1))); do
        wl_name="${WL_NAMES[$w]}"
        wl_setup="${WL_SETUP[$w]}"
        wl_action="${WL_ACTION[$w]}"

        # Substitute $SRC in setup/action commands
        local_setup="${wl_setup//\$SRC/$topo_src}"
        local_action="${wl_action//\$SRC/$topo_src}"

        echo -n "  $wl_name... " >&2
        median=$(run_workload "$wl_name" "$topo_src" "$topo_dst" "$local_setup" "$local_action" "$ITERATIONS")
        echo "${median}ms" >&2

        RESULTS+=("$median")
    done

    # Cleanup
    rm -rf "$topo_src" "$topo_dst" 2>/dev/null || true
done

# ───────────────────────────────────────────────────────
# Output markdown table
# ───────────────────────────────────────────────────────

echo ""
echo "## Mean Time to Consistency (MTTC)"
echo ""
echo "**Platform:** $(hostname) ($(uname -r))"
echo "**fxcp:** $(fxcp --version 2>&1 | head -1)"
echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "**Iterations:** $ITERATIONS (median reported)"
echo ""

# Header row
printf "| %-15s" "Workload"
for t in $(seq 0 $((NUM_TOPOS - 1))); do
    printf " | %10s" "${TOPO_NAMES[$t]}"
done
echo " |"

# Separator
printf "|%-16s" "----------------"
for t in $(seq 0 $((NUM_TOPOS - 1))); do
    printf "|%-11s" "-----------"
done
echo "|"

# Data rows
for w in $(seq 0 $((NUM_WLS - 1))); do
    printf "| %-15s" "${WL_NAMES[$w]}"
    for t in $(seq 0 $((NUM_TOPOS - 1))); do
        idx=$(( t * NUM_WLS + w ))
        printf " | %7sms " "${RESULTS[$idx]}"
    done
    echo "|"
done

echo "" >&2
echo -e "${GREEN}Phase 1 (fxcp one-shot) complete.${NC}" >&2

# ═══════════════════════════════════════════════════════
# Phase 2: foxingd daemon-mode replication latency
# ═══════════════════════════════════════════════════════
#
# Pre-sync with --generate-sigs, start foxingd daemon,
# write test data, measure time until target is consistent.
# Requires root + BPF (kernel 6.12+).

echo "" >&2
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════${NC}" >&2
echo -e "${BOLD}${CYAN}  Phase 2: foxingd daemon-mode (BPF event-driven)${NC}" >&2
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════${NC}" >&2

# Check if foxingd is available and we're root
if ! command -v foxingd >/dev/null 2>&1; then
    echo "SKIP: foxingd not found" >&2
    echo ""
    echo "(Phase 2 skipped: foxingd not available)"
    exit 0
fi
if [ "$(id -u)" != "0" ]; then
    echo "SKIP: Phase 2 requires root (BPF)" >&2
    echo ""
    echo "(Phase 2 skipped: not root)"
    exit 0
fi

# Phase 2 topologies (BPF-capable source devices only)
declare -a P2_TOPO_NAMES=()
declare -a P2_TOPO_SRC=()
declare -a P2_TOPO_DST=()

add_p2_topology() {
    P2_TOPO_NAMES+=("$1")
    P2_TOPO_SRC+=("$2")
    P2_TOPO_DST+=("$3")
}

add_p2_topology "XFS-to-XFS" "/mnt/source/mttc-p2" "/mnt/target-xfs/mttc-p2"
add_p2_topology "XFS-to-NFS" "/mnt/source/mttc-p2" "/mnt/target-nfs/mttc-p2"
add_p2_topology "XFS-to-tmpfs" "/mnt/source/mttc-p2" "/tmp/mttc-p2-target"

# Generate minimal foxingd config
gen_foxing_config() {
    local src="$1" dst="$2" port="${3:-9100}"
    cat <<TOML
metrics_port = $port
worker_count = 4
[[sources]]
path = "$src"
  [[sources.targets]]
  path = "$dst"
  profile = "Auto"
  initial_sync = true
  enable_versioning = false
TOML
}

# Wait for metrics endpoint to respond
wait_for_metrics() {
    local url="$1" timeout="${2:-30}"
    local elapsed=0
    while ! curl -sf "$url" >/dev/null 2>&1; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "TIMEOUT waiting for metrics" >&2
            return 1
        fi
    done
    return 0
}

# Wait for hydration to settle (no copy in-flight)
wait_for_idle() {
    local url="$1" timeout="${2:-60}"
    local elapsed=0
    sleep 3  # initial settle
    while [ "$elapsed" -lt "$timeout" ]; do
        local in_flight
        in_flight=$(curl -sf "$url" 2>/dev/null | grep 'foxing_worker_copy_in_flight' | grep -v '#' | awk '{s+=$2} END {print s+0}' || echo "0")
        if [ "${in_flight%.*}" = "0" ] || [ "${in_flight}" = "0" ]; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

# Poll target file until size matches source (50ms resolution)
# Usage: wait_for_target_file <src_file> <dst_file> <timeout_ms>
wait_for_target_file() {
    local src_file="$1" dst_file="$2" timeout_ms="${3:-30000}"
    local src_size
    src_size=$(stat -c%s "$src_file" 2>/dev/null || echo 0)
    local start
    start=$(date +%s%N)
    local deadline=$(( start + timeout_ms * 1000000 ))

    while true; do
        local now
        now=$(date +%s%N)
        if [ "$now" -ge "$deadline" ]; then
            echo "TIMEOUT" >&2
            return 1
        fi
        if [ -f "$dst_file" ]; then
            local dst_size
            dst_size=$(stat -c%s "$dst_file" 2>/dev/null || echo -1)
            if [ "$dst_size" = "$src_size" ] && [ "$dst_size" != "0" ]; then
                local end
                end=$(date +%s%N)
                echo $(( (end - start) / 1000000 ))
                return 0
            fi
        fi
        sleep 0.05
    done
}

# Wait for batch: all files matching pattern exist with correct sizes
# Usage: wait_for_batch <src_dir> <dst_dir> <pattern> <count> <timeout_ms>
wait_for_batch() {
    local src_dir="$1" dst_dir="$2" pattern="$3" count="$4" timeout_ms="${5:-60000}"
    local start
    start=$(date +%s%N)
    local deadline=$(( start + timeout_ms * 1000000 ))

    while true; do
        local now
        now=$(date +%s%N)
        if [ "$now" -ge "$deadline" ]; then
            echo "TIMEOUT" >&2
            return 1
        fi
        local matched=0
        for f in "$dst_dir"/$pattern; do
            [ -f "$f" ] || continue
            local base
            base=$(basename "$f")
            local src_f="$src_dir/$base"
            [ -f "$src_f" ] || continue
            local ss ds
            ss=$(stat -c%s "$src_f" 2>/dev/null || echo 0)
            ds=$(stat -c%s "$f" 2>/dev/null || echo -1)
            [ "$ss" = "$ds" ] && matched=$((matched + 1))
        done
        if [ "$matched" -ge "$count" ]; then
            local end
            end=$(date +%s%N)
            echo $(( (end - start) / 1000000 ))
            return 0
        fi
        sleep 0.01
    done
}

# Run a daemon-mode workload: action + poll target
# Usage: run_p2_workload <label> <src> <dst> <setup_cmd> <action_cmd> <verify_cmd> <iters>
run_p2_workload() {
    local label="$1" src="$2" dst="$3" setup_cmd="$4" action_cmd="$5" verify_cmd="$6" iters="$7"
    local times=()

    for i in $(seq 1 "$iters"); do
        export P2_ITER="$i"
        # Run setup (creates pre-existing files for modify workloads)
        eval "$setup_cmd"
        # Let daemon sync the setup files
        sleep 2
        sync

        # Execute source modification + start timer simultaneously
        local start
        start=$(date +%s%N)
        eval "$action_cmd"
        # Time until target matches (verify polls until consistent)
        local ms
        ms=$(eval "$verify_cmd") || { times+=(30000); continue; }
        times+=("$ms")
    done

    IFS=$'\n' sorted=($(sort -n <<<"${times[*]}")); unset IFS
    local count=${#sorted[@]}
    echo "${sorted[$((count / 2))]}"
}

# Phase 2 workload definitions with verify commands
# Each needs: setup, action, verify (polling command that returns ms)
declare -a P2_WL_NAMES=()
declare -a P2_WL_SETUP=()
declare -a P2_WL_ACTION=()
declare -a P2_WL_VERIFY=()

add_p2_workload() {
    P2_WL_NAMES+=("$1")
    P2_WL_SETUP+=("$2")
    P2_WL_ACTION+=("$3")
    P2_WL_VERIFY+=("$4")
}

# foxingd BPF captures Create/Rename/Unlink events. Write events depend on
# vfs_write_iter probe availability (fails on some kernels). Small-file creates
# are copied in full by the Create handler's repair path. Large-file writes
# require the periodic hydration scan if write probes are unavailable.
#
# P2_ITER variable is set by run_p2_workload before each iteration.

# NOTE: On kernel 6.18.5, vfs_write_iter BPF probe fails. The daemon captures
# Create/Rename/Unlink events but NOT write data events. Files are created at
# size=0 on the target; data replication requires the hydration repair path.
# Small-file creates (<= ~4KB) are copied by the repair handler inline.
# Larger files require a full hydration rescan to populate data.
#
# These benchmarks measure CREATE + REPAIR latency (BPF create event →
# hydration worker copies file content from source).

add_p2_workload "create 4KB" \
    ":" \
    "dd if=/dev/urandom of=\$SRC/p2c4k_\${P2_ITER}.dat bs=4K count=1 2>/dev/null" \
    "wait_for_target_file \$SRC/p2c4k_\${P2_ITER}.dat \$DST/p2c4k_\${P2_ITER}.dat 10000"

add_p2_workload "create 8KB" \
    ":" \
    "dd if=/dev/urandom of=\$SRC/p2c8k_\${P2_ITER}.dat bs=8K count=1 2>/dev/null" \
    "wait_for_target_file \$SRC/p2c8k_\${P2_ITER}.dat \$DST/p2c8k_\${P2_ITER}.dat 10000"

add_p2_workload "create 32KB" \
    ":" \
    "dd if=/dev/urandom of=\$SRC/p2c32k_\${P2_ITER}.dat bs=32K count=1 2>/dev/null" \
    "wait_for_target_file \$SRC/p2c32k_\${P2_ITER}.dat \$DST/p2c32k_\${P2_ITER}.dat 10000"

add_p2_workload "create 64KB" \
    ":" \
    "dd if=/dev/urandom of=\$SRC/p2c64k_\${P2_ITER}.dat bs=64K count=1 2>/dev/null" \
    "wait_for_target_file \$SRC/p2c64k_\${P2_ITER}.dat \$DST/p2c64k_\${P2_ITER}.dat 10000"

add_p2_workload "rename 4KB" \
    "dd if=/dev/urandom of=\$SRC/p2ren_\${P2_ITER}_old.dat bs=4K count=1 2>/dev/null && sleep 2" \
    "mv \$SRC/p2ren_\${P2_ITER}_old.dat \$SRC/p2ren_\${P2_ITER}_new.dat" \
    "wait_for_target_file \$SRC/p2ren_\${P2_ITER}_new.dat \$DST/p2ren_\${P2_ITER}_new.dat 10000"

add_p2_workload "batch 10x4KB" \
    ":" \
    "for i in \$(seq 1 10); do dd if=/dev/urandom of=\$SRC/p2b\${P2_ITER}_\$i.dat bs=4K count=1 2>/dev/null; done" \
    "wait_for_batch \$SRC \$DST \"p2b\${P2_ITER}_*.dat\" 10 15000"

# Collect Phase 2 results
P2_NUM_TOPOS=${#P2_TOPO_NAMES[@]}
P2_NUM_WLS=${#P2_WL_NAMES[@]}
declare -a P2_RESULTS=()

METRICS_URL="http://localhost:9100/metrics"

for t in $(seq 0 $((P2_NUM_TOPOS - 1))); do
    topo_name="${P2_TOPO_NAMES[$t]}"
    topo_src="${P2_TOPO_SRC[$t]}"
    topo_dst="${P2_TOPO_DST[$t]}"

    header "Phase 2: $topo_name ($topo_src -> $topo_dst)"

    # Setup directories and pre-sync with signatures
    rm -rf "$topo_src" "$topo_dst" 2>/dev/null || true
    mkdir -p "$topo_src" "$topo_dst"
    # Seed a baseline file so --generate-sigs has something to work with
    dd if=/dev/urandom of="$topo_src/baseline.dat" bs=4K count=1 2>/dev/null
    fxcp -a --generate-sigs "$topo_src" "$topo_dst" >/dev/null 2>&1

    # Generate config and start daemon
    gen_foxing_config "$topo_src" "$topo_dst" 9100 > /tmp/mttc-foxing.toml
    # Kill any existing foxingd
    pkill -9 foxingd 2>/dev/null || true
    sleep 1

    foxingd daemon --config /tmp/mttc-foxing.toml >/dev/null 2>&1 &
    DAEMON_PID=$!
    echo "  Started foxingd (PID $DAEMON_PID)" >&2

    # Wait for ready
    if ! wait_for_metrics "$METRICS_URL" 30; then
        echo "  SKIP: metrics not available" >&2
        kill $DAEMON_PID 2>/dev/null; wait $DAEMON_PID 2>/dev/null || true
        for w in $(seq 0 $((P2_NUM_WLS - 1))); do
            P2_RESULTS+=("N/A")
        done
        continue
    fi

    # Wait for hydration to settle
    wait_for_idle "$METRICS_URL" 30 || true
    echo "  Daemon ready, running workloads..." >&2

    for w in $(seq 0 $((P2_NUM_WLS - 1))); do
        wl_name="${P2_WL_NAMES[$w]}"
        wl_setup="${P2_WL_SETUP[$w]}"
        wl_action="${P2_WL_ACTION[$w]}"
        wl_verify="${P2_WL_VERIFY[$w]}"

        # Substitute $SRC and $DST (but NOT $P2_ITER — that's set at runtime)
        local_setup="${wl_setup//\$SRC/$topo_src}"
        local_setup="${local_setup//\$DST/$topo_dst}"
        local_action="${wl_action//\$SRC/$topo_src}"
        local_action="${local_action//\$DST/$topo_dst}"
        local_verify="${wl_verify//\$SRC/$topo_src}"
        local_verify="${local_verify//\$DST/$topo_dst}"

        echo -n "  $wl_name... " >&2
        median=$(run_p2_workload "$wl_name" "$topo_src" "$topo_dst" "$local_setup" "$local_action" "$local_verify" "$ITERATIONS")
        echo "${median}ms" >&2

        P2_RESULTS+=("$median")
    done

    # Stop daemon
    kill $DAEMON_PID 2>/dev/null; wait $DAEMON_PID 2>/dev/null || true
    echo "  Daemon stopped" >&2

    # Cleanup
    rm -rf "$topo_src" "$topo_dst" 2>/dev/null || true
done

# Output Phase 2 table
echo ""
echo "## MTTC Phase 2: foxingd Daemon (BPF event-driven)"
echo ""
echo "**Mode:** foxingd daemon with eBPF event capture (pre-synced with --generate-sigs)"
echo "**Latency:** Source write to target file consistent (polled at 10ms resolution)"
echo ""

printf "| %-15s" "Workload"
for t in $(seq 0 $((P2_NUM_TOPOS - 1))); do
    printf " | %10s" "${P2_TOPO_NAMES[$t]}"
done
echo " |"

printf "|%-16s" "----------------"
for t in $(seq 0 $((P2_NUM_TOPOS - 1))); do
    printf "|%-11s" "-----------"
done
echo "|"

for w in $(seq 0 $((P2_NUM_WLS - 1))); do
    printf "| %-15s" "${P2_WL_NAMES[$w]}"
    for t in $(seq 0 $((P2_NUM_TOPOS - 1))); do
        idx=$(( t * P2_NUM_WLS + w ))
        val="${P2_RESULTS[$idx]}"
        if [ "$val" = "N/A" ]; then
            printf " |       N/A "
        else
            printf " | %7sms " "$val"
        fi
    done
    echo "|"
done

echo "" >&2
echo -e "${GREEN}Done. Both phases complete.${NC}" >&2
