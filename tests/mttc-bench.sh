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
echo -e "${GREEN}Done. Paste the table above into BENCHMARKS.md.${NC}" >&2
