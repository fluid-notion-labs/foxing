#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-or-later
# NFS Performance Test Suite for foxing v0.5.1
#
# Tests fxcp/foxingd vs cp/rsync on NFS 4.2 targets.
# Run on fox-test VM: bash tests/nfs-perf.sh
#
# Workloads:
#   1. Small files (1000 x 4KB)   — per-file RPC overhead
#   2. Large file  (1 x 100MB)    — bulk throughput
#   3. Mixed       (500 files, ~128MB) — realistic
#   4. Many tiny   (5000 x 1KB)   — metadata storm
#   5. NFS→NFS same-server        — server-side copy
#   6. Resync (delta, no changes)  — fast resume
#   7. Verify mode overhead        — --verify cost

set -euo pipefail

SRC="/mnt/source/nfs-perf"
DST_NFS="/mnt/target-nfs/nfs-perf"
DST_XFS="/mnt/target-xfs/nfs-perf"
ITERATIONS=3

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

header() {
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════${NC}"
}

cleanup() {
    rm -rf "$SRC" "$DST_NFS" "$DST_XFS" 2>/dev/null || true
}

# Generate test data
gen_small_files() {
    local dir="$1" count="${2:-1000}" size="${3:-4096}"
    mkdir -p "$dir"
    for i in $(seq 1 "$count"); do
        dd if=/dev/urandom of="$dir/file_$i.dat" bs="$size" count=1 2>/dev/null
    done
}

gen_large_file() {
    local dir="$1" size_mb="${2:-100}"
    mkdir -p "$dir"
    dd if=/dev/urandom of="$dir/large.dat" bs=1M count="$size_mb" 2>/dev/null
}

gen_mixed() {
    local dir="$1"
    mkdir -p "$dir/small" "$dir/medium" "$dir/large"
    # 400 x 4KB small files
    for i in $(seq 1 400); do
        dd if=/dev/urandom of="$dir/small/s_$i.dat" bs=4K count=1 2>/dev/null
    done
    # 90 x 128KB medium files
    for i in $(seq 1 90); do
        dd if=/dev/urandom of="$dir/medium/m_$i.dat" bs=128K count=1 2>/dev/null
    done
    # 10 x 10MB large files
    for i in $(seq 1 10); do
        dd if=/dev/urandom of="$dir/large/l_$i.dat" bs=1M count=10 2>/dev/null
    done
}

gen_tiny_files() {
    local dir="$1" count="${2:-5000}"
    mkdir -p "$dir"
    for i in $(seq 1 "$count"); do
        echo "tiny-file-content-$i-$(date +%s%N)" > "$dir/tiny_$i.txt"
    done
}

# Run a timed command, return median of N iterations
# Usage: bench "label" <iterations> <command...>
bench() {
    local label="$1"
    shift
    local iters="$1"
    shift
    local times=()

    for i in $(seq 1 "$iters"); do
        # Clean target before each run
        rm -rf "$DST_NFS" "$DST_XFS" 2>/dev/null || true
        sync
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

        local start end elapsed
        start=$(date +%s%N)
        eval "$@" >/dev/null 2>&1
        end=$(date +%s%N)
        elapsed=$(( (end - start) / 1000000 ))
        times+=("$elapsed")
    done

    # Sort and pick median
    IFS=$'\n' sorted=($(sort -n <<<"${times[*]}")); unset IFS
    local mid=$(( ${#sorted[@]} / 2 ))
    local median="${sorted[$mid]}"
    local min="${sorted[0]}"
    local max="${sorted[${#sorted[@]}-1]}"

    printf "  %-28s %7s ms  (min=%s max=%s)\n" "$label" "$median" "$min" "$max" >&2
    echo "$median"
}

# Run a timed command (single iteration, with GNU time for RSS)
bench_once() {
    local label="$1"
    shift
    rm -rf "$DST_NFS" "$DST_XFS" 2>/dev/null || true
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

    local tmpfile
    tmpfile=$(mktemp)
    local start end elapsed
    start=$(date +%s%N)
    /usr/bin/time -v "$@" >"$tmpfile" 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))

    local rss
    rss=$(grep "Maximum resident" "$tmpfile" | awk '{print $NF}')
    rm -f "$tmpfile"

    printf "  %-28s %7s ms  RSS=%s KB\n" "$label" "$elapsed" "$rss"
}

echo -e "${BOLD}foxing NFS Performance Test Suite v0.5.1${NC}"
echo "Host: $(hostname), Kernel: $(uname -r)"
echo "NFS: $(mount | grep target-nfs | grep -oP 'vers=\S+')"
echo "Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

trap cleanup EXIT

# ─────────────────────────────────────────────────────────
header "Test 1: Small Files (1000 x 4KB) → NFS"
# ─────────────────────────────────────────────────────────
echo "Generating 1000 x 4KB files..."
cleanup
gen_small_files "$SRC" 1000 4096
echo ""

T1_CP=$(bench "cp -a" "$ITERATIONS" "cp -a $SRC $DST_NFS")
T1_RSYNC=$(bench "rsync -a" "$ITERATIONS" "rsync -a $SRC/ $DST_NFS/")
T1_FXCP=$(bench "fxcp -a" "$ITERATIONS" "fxcp -a $SRC $DST_NFS")

echo ""
echo -e "  ${YELLOW}rsync/fxcp ratio: $(echo "scale=2; $T1_RSYNC / $T1_FXCP" | bc)x${NC}"
echo -e "  ${YELLOW}cp/fxcp ratio:    $(echo "scale=2; $T1_CP / $T1_FXCP" | bc)x${NC}"

# ─────────────────────────────────────────────────────────
header "Test 2: Large File (1 x 100MB) → NFS"
# ─────────────────────────────────────────────────────────
echo "Generating 100MB file..."
cleanup
gen_large_file "$SRC" 100
echo ""

T2_CP=$(bench "cp -a" "$ITERATIONS" "mkdir -p $DST_NFS && cp -a $SRC/large.dat $DST_NFS/")
T2_RSYNC=$(bench "rsync -a" "$ITERATIONS" "rsync -a $SRC/ $DST_NFS/")
T2_FXCP=$(bench "fxcp -a" "$ITERATIONS" "fxcp -a $SRC $DST_NFS")

echo ""
echo -e "  ${YELLOW}rsync/fxcp ratio: $(echo "scale=2; $T2_RSYNC / $T2_FXCP" | bc)x${NC}"
THROUGHPUT=$(echo "scale=1; 100 * 1000 / $T2_FXCP" | bc)
echo -e "  ${YELLOW}fxcp throughput: ~${THROUGHPUT} MB/s${NC}"

# ─────────────────────────────────────────────────────────
header "Test 3: Mixed Workload (500 files, ~128MB) → NFS"
# ─────────────────────────────────────────────────────────
echo "Generating mixed workload..."
cleanup
gen_mixed "$SRC"
echo ""

T3_CP=$(bench "cp -a" "$ITERATIONS" "cp -a $SRC $DST_NFS")
T3_RSYNC=$(bench "rsync -a" "$ITERATIONS" "rsync -a $SRC/ $DST_NFS/")
T3_FXCP=$(bench "fxcp -a" "$ITERATIONS" "fxcp -a $SRC $DST_NFS")

echo ""
echo -e "  ${YELLOW}rsync/fxcp ratio: $(echo "scale=2; $T3_RSYNC / $T3_FXCP" | bc)x${NC}"

# ─────────────────────────────────────────────────────────
header "Test 4: Many Tiny Files (5000 x ~30B) → NFS"
# ─────────────────────────────────────────────────────────
echo "Generating 5000 tiny files..."
cleanup
gen_tiny_files "$SRC" 5000
echo ""

T4_CP=$(bench "cp -a" "$ITERATIONS" "cp -a $SRC $DST_NFS")
T4_RSYNC=$(bench "rsync -a" "$ITERATIONS" "rsync -a $SRC/ $DST_NFS/")
T4_FXCP=$(bench "fxcp -a" "$ITERATIONS" "fxcp -a $SRC $DST_NFS")

echo ""
echo -e "  ${YELLOW}rsync/fxcp ratio: $(echo "scale=2; $T4_RSYNC / $T4_FXCP" | bc)x${NC}"

# ─────────────────────────────────────────────────────────
header "Test 5: XFS→NFS vs XFS→XFS (100MB, cross-device)"
# ─────────────────────────────────────────────────────────
echo "Generating 100MB file..."
cleanup
gen_large_file "$SRC" 100
echo ""

echo "XFS to NFS:" >&2
T5_NFS=$(bench "fxcp -a to NFS" "$ITERATIONS" "fxcp -a $SRC $DST_NFS")
echo "XFS to XFS:" >&2
T5_XFS=$(bench "fxcp -a to XFS" "$ITERATIONS" "fxcp -a $SRC $DST_XFS")
echo ""
NFS_OVERHEAD=$(echo "scale=1; $T5_NFS * 100 / $T5_XFS - 100" | bc)
echo -e "  ${YELLOW}NFS overhead vs XFS: ${NFS_OVERHEAD}%${NC}"

# ─────────────────────────────────────────────────────────
header "Test 6: Resync — No Changes (fast resume)"
# ─────────────────────────────────────────────────────────
echo "Initial copy..."
cleanup
gen_mixed "$SRC"
fxcp -a "$SRC" "$DST_NFS" >/dev/null 2>&1
echo "Resync (all files up-to-date):"
echo ""

# Don't clean target — we want to measure skip speed
T6_RSYNC_VALS=()
T6_FXCP_VALS=()
for i in $(seq 1 "$ITERATIONS"); do
    sync
    start=$(date +%s%N)
    rsync -a "$SRC/" "$DST_NFS/" >/dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    T6_RSYNC_VALS+=("$elapsed")

    start=$(date +%s%N)
    fxcp -a "$SRC" "$DST_NFS" >/dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    T6_FXCP_VALS+=("$elapsed")
done

IFS=$'\n' sorted=($(sort -n <<<"${T6_RSYNC_VALS[*]}")); unset IFS
T6_RSYNC="${sorted[$(( ${#sorted[@]} / 2 ))]}"
IFS=$'\n' sorted=($(sort -n <<<"${T6_FXCP_VALS[*]}")); unset IFS
T6_FXCP="${sorted[$(( ${#sorted[@]} / 2 ))]}"

printf "  %-28s %7s ms\n" "rsync -a (no changes)" "$T6_RSYNC"
printf "  %-28s %7s ms\n" "fxcp -a (no changes)" "$T6_FXCP"
echo ""
echo -e "  ${YELLOW}rsync/fxcp skip ratio: $(echo "scale=2; $T6_RSYNC / $T6_FXCP" | bc)x${NC}"

# ─────────────────────────────────────────────────────────
header "Test 7: --verify Overhead"
# ─────────────────────────────────────────────────────────
echo "Measuring verify overhead on 500-file mixed workload → NFS..."
cleanup
gen_mixed "$SRC"
# Pre-copy
fxcp -a "$SRC" "$DST_NFS" >/dev/null 2>&1
echo ""

# Resync without verify
start=$(date +%s%N)
fxcp -a "$SRC" "$DST_NFS" >/dev/null 2>&1
end=$(date +%s%N)
T7_NO_VERIFY=$(( (end - start) / 1000000 ))

# Resync with verify
start=$(date +%s%N)
fxcp -a --verify "$SRC" "$DST_NFS" >/dev/null 2>&1
end=$(date +%s%N)
T7_VERIFY=$(( (end - start) / 1000000 ))

printf "  %-28s %7s ms\n" "fxcp -a (no verify)" "$T7_NO_VERIFY"
printf "  %-28s %7s ms\n" "fxcp -a --verify" "$T7_VERIFY"
OVERHEAD=$(( T7_VERIFY - T7_NO_VERIFY ))
echo ""
echo -e "  ${YELLOW}--verify overhead: +${OVERHEAD}ms${NC}"

# ─────────────────────────────────────────────────────────
header "Test 8: NFS→NFS Same-Server Copy (server-side)"
# ─────────────────────────────────────────────────────────
echo "Generating 100MB on NFS source..."
rm -rf "$DST_NFS/ssc-src" "$DST_NFS/ssc-dst" 2>/dev/null
mkdir -p "$DST_NFS/ssc-src"
dd if=/dev/urandom of="$DST_NFS/ssc-src/large.dat" bs=1M count=100 2>/dev/null
echo ""

T8_RSYNC_VALS=()
T8_FXCP_VALS=()
for i in $(seq 1 "$ITERATIONS"); do
    rm -rf "$DST_NFS/ssc-dst" 2>/dev/null

    start=$(date +%s%N)
    rsync -a "$DST_NFS/ssc-src/" "$DST_NFS/ssc-dst/" >/dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    T8_RSYNC_VALS+=("$elapsed")

    rm -rf "$DST_NFS/ssc-dst" 2>/dev/null

    start=$(date +%s%N)
    fxcp -a "$DST_NFS/ssc-src" "$DST_NFS/ssc-dst" >/dev/null 2>&1
    end=$(date +%s%N)
    elapsed=$(( (end - start) / 1000000 ))
    T8_FXCP_VALS+=("$elapsed")
done

IFS=$'\n' sorted=($(sort -n <<<"${T8_RSYNC_VALS[*]}")); unset IFS
T8_RSYNC="${sorted[$(( ${#sorted[@]} / 2 ))]}"
IFS=$'\n' sorted=($(sort -n <<<"${T8_FXCP_VALS[*]}")); unset IFS
T8_FXCP="${sorted[$(( ${#sorted[@]} / 2 ))]}"

printf "  %-28s %7s ms\n" "rsync -a NFS→NFS" "$T8_RSYNC"
printf "  %-28s %7s ms\n" "fxcp -a NFS→NFS" "$T8_FXCP"
echo ""
echo -e "  ${YELLOW}rsync/fxcp server-side: $(echo "scale=2; $T8_RSYNC / $T8_FXCP" | bc)x${NC}"

rm -rf "$DST_NFS/ssc-src" "$DST_NFS/ssc-dst" 2>/dev/null

# ─────────────────────────────────────────────────────────
header "Summary"
# ─────────────────────────────────────────────────────────
echo ""
R1=$(echo "scale=2; $T1_RSYNC / $T1_FXCP" | bc)
R2=$(echo "scale=2; $T2_RSYNC / $T2_FXCP" | bc)
R3=$(echo "scale=2; $T3_RSYNC / $T3_FXCP" | bc)
R4=$(echo "scale=2; $T4_RSYNC / $T4_FXCP" | bc)
R6=$(echo "scale=2; $T6_RSYNC / $T6_FXCP" | bc)
R8=$(echo "scale=2; $T8_RSYNC / $T8_FXCP" | bc)

printf "  %-30s %8s %8s %8s %10s\n" "Workload" "cp" "rsync" "fxcp" "rsync/fxcp"
printf "  %-30s %8s %8s %8s %10s\n" "------------------------------" "--------" "--------" "--------" "----------"
printf "  %-30s %7sms %7sms %7sms %9sx\n" "1000x4KB to NFS" "$T1_CP" "$T1_RSYNC" "$T1_FXCP" "$R1"
printf "  %-30s %7sms %7sms %7sms %9sx\n" "1x100MB to NFS" "$T2_CP" "$T2_RSYNC" "$T2_FXCP" "$R2"
printf "  %-30s %7sms %7sms %7sms %9sx\n" "500 mixed to NFS" "$T3_CP" "$T3_RSYNC" "$T3_FXCP" "$R3"
printf "  %-30s %7sms %7sms %7sms %9sx\n" "5000 tiny to NFS" "$T4_CP" "$T4_RSYNC" "$T4_FXCP" "$R4"
printf "  %-30s %8s %7sms %7sms %9sx\n" "resync (no changes)" "-" "$T6_RSYNC" "$T6_FXCP" "$R6"
printf "  %-30s %8s %7sms %7sms %9sx\n" "NFS-to-NFS server-side 100MB" "-" "$T8_RSYNC" "$T8_FXCP" "$R8"
echo ""
echo -e "  ${GREEN}--verify overhead: +${OVERHEAD}ms on 500-file mixed workload${NC}"
echo -e "  ${GREEN}XFS-to-NFS overhead vs XFS-to-XFS: ${NFS_OVERHEAD}%${NC}"
echo ""
echo "Done."
