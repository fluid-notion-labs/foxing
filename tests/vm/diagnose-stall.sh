#!/usr/bin/env bash
# diagnose-stall.sh — Capture system-level diagnostics when a foxingd worker stall is detected.
#
# Usage: diagnose-stall.sh <foxingd_pid> <output_dir> [duration_secs]
#
# Runs perf, BPF tracing, /proc introspection, and metrics diffing to produce
# a diagnosis-summary.txt with a STALL_CONFIRMED / MAKING_PROGRESS verdict.

set -uo pipefail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ts() { date '+%Y-%m-%dT%H:%M:%S'; }
log() { echo "[$(ts)] $*"; }
warn() { echo "[$(ts)] WARNING: $*" >&2; }

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <foxingd_pid> <output_dir> [duration_secs]" >&2
    exit 1
fi

PID="$1"
OUT_DIR="$2"
DURATION="${3:-5}"

if ! [[ -d "/proc/$PID" ]]; then
    echo "ERROR: PID $PID does not exist" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
log "Diagnosing foxingd PID=$PID  output=$OUT_DIR  window=${DURATION}s"

METRICS_URL="http://localhost:9100/metrics"

# ===================================================================
# 1. Thread state dump
# ===================================================================
log "Capturing thread state dump ..."
THREAD_FILE="$OUT_DIR/thread-states.txt"
{
    echo "=== Thread State Dump (PID $PID) ==="
    echo ""

    # --- wchan histogram ---
    echo "--- wchan histogram ---"
    wchan_counts=""
    for wchan_path in /proc/"$PID"/task/*/wchan; do
        if [[ -r "$wchan_path" ]]; then
            fn=$(cat "$wchan_path" 2>/dev/null) || fn="<unreadable>"
            [[ -z "$fn" ]] && fn="<running>"
            wchan_counts="${wchan_counts}${fn}"$'\n'
        fi
    done
    if [[ -n "$wchan_counts" ]]; then
        echo "$wchan_counts" | sort | uniq -c | sort -rn
    else
        echo "(no threads found)"
    fi

    echo ""
    echo "--- per-thread status ---"
    for status_path in /proc/"$PID"/task/*/status; do
        if [[ -r "$status_path" ]]; then
            tid=$(echo "$status_path" | grep -oP 'task/\K[0-9]+')
            state=$(grep '^State:' "$status_path" 2>/dev/null | awk '{print $2, $3}') || state="?"
            wchan_path="/proc/$PID/task/$tid/wchan"
            wchan=$(cat "$wchan_path" 2>/dev/null) || wchan="?"
            [[ -z "$wchan" ]] && wchan="<running>"
            printf "  tid=%-8s state=%-20s wchan=%s\n" "$tid" "$state" "$wchan"
        fi
    done
} > "$THREAD_FILE" 2>&1
log "Thread state dump saved to $THREAD_FILE"

# ===================================================================
# 2. perf stat snapshot
# ===================================================================
PERF_FILE="$OUT_DIR/perf-stat.txt"
if command -v perf &>/dev/null; then
    log "Running perf stat for ${DURATION}s ..."
    # Use software/tracepoint events only — PMC unreliable in KVM
    perf stat -p "$PID" \
        -e context-switches,page-faults,task-clock \
        sleep "$DURATION" \
        > "$PERF_FILE" 2>&1 || warn "perf stat returned non-zero"
    log "perf stat saved to $PERF_FILE"
else
    warn "perf not installed — skipping perf stat"
    echo "(perf not available)" > "$PERF_FILE"
fi

# ===================================================================
# 3. offcputime
# ===================================================================
OFFCPU_FILE="$OUT_DIR/offcputime.txt"
if command -v offcputime-bpfcc &>/dev/null; then
    log "Running offcputime-bpfcc for ${DURATION}s ..."
    offcputime-bpfcc -p "$PID" "$DURATION" > "$OFFCPU_FILE" 2>&1 || warn "offcputime returned non-zero"
    log "offcputime saved to $OFFCPU_FILE"
else
    warn "offcputime-bpfcc not installed — skipping"
    echo "(offcputime-bpfcc not available)" > "$OFFCPU_FILE"
fi

# ===================================================================
# 4. nfsslower
# ===================================================================
NFS_FILE="$OUT_DIR/nfsslower.txt"
if command -v nfsslower-bpfcc &>/dev/null; then
    log "Running nfsslower-bpfcc (>10ms) for ${DURATION}s ..."
    timeout "$((DURATION + 2))" nfsslower-bpfcc 10 -d "$DURATION" > "$NFS_FILE" 2>&1 || warn "nfsslower returned non-zero"
    log "nfsslower saved to $NFS_FILE"
else
    warn "nfsslower-bpfcc not installed — skipping"
    echo "(nfsslower-bpfcc not available)" > "$NFS_FILE"
fi

# ===================================================================
# 5. foxingd metrics delta
# ===================================================================
METRICS_BEFORE="$OUT_DIR/metrics-before.txt"
METRICS_AFTER="$OUT_DIR/metrics-after.txt"
METRICS_DELTA_FILE="$OUT_DIR/metrics-delta.txt"

# Metrics we care about (grep patterns)
METRIC_KEYS=(
    "foxing_copy_method_standard_total"
    "foxing_worker_retry_queue_size"
    "foxing_events_dropped"
    "foxing_tuner_state"
)

extract_metric() {
    # $1 = metrics file, $2 = metric name
    # Sum all matching lines (handles per-worker labels)
    grep "^${2}" "$1" 2>/dev/null \
        | grep -v '^#' \
        | awk '{s += $NF} END {printf "%.0f\n", s}'
}

if command -v curl &>/dev/null; then
    log "Fetching metrics (before) ..."
    curl -sf "$METRICS_URL" > "$METRICS_BEFORE" 2>/dev/null || warn "Could not fetch metrics (before)"

    # Wait for the observation window (perf/offcputime run in parallel above only
    # if backgrounded; here we just sleep the remainder if those already consumed it).
    # Since perf stat already sleeps $DURATION, the window has largely elapsed by now.
    # Grab the "after" snapshot.
    log "Fetching metrics (after) ..."
    curl -sf "$METRICS_URL" > "$METRICS_AFTER" 2>/dev/null || warn "Could not fetch metrics (after)"

    {
        echo "=== Metrics Delta (${DURATION}s window) ==="
        printf "%-50s %12s %12s %12s\n" "METRIC" "BEFORE" "AFTER" "DELTA"
        printf "%-50s %12s %12s %12s\n" "------" "------" "-----" "-----"
        for key in "${METRIC_KEYS[@]}"; do
            before=$(extract_metric "$METRICS_BEFORE" "$key")
            after=$(extract_metric "$METRICS_AFTER" "$key")
            before=${before:-0}
            after=${after:-0}
            delta=$((after - before))
            printf "%-50s %12s %12s %12s\n" "$key" "$before" "$after" "$delta"
        done
    } > "$METRICS_DELTA_FILE" 2>&1
    log "Metrics delta saved to $METRICS_DELTA_FILE"
else
    warn "curl not installed — skipping metrics"
    echo "(curl not available)" > "$METRICS_DELTA_FILE"
fi

# ===================================================================
# 6. io_uring fd info
# ===================================================================
IOURING_FILE="$OUT_DIR/iouring-fdinfo.txt"
log "Scanning io_uring file descriptors ..."
{
    echo "=== io_uring fdinfo for PID $PID ==="
    found=0
    for fd_path in /proc/"$PID"/fdinfo/*; do
        if [[ -r "$fd_path" ]]; then
            if grep -q 'IoUringFd' "$fd_path" 2>/dev/null || grep -q 'io_uring' "$fd_path" 2>/dev/null; then
                fd_num=$(basename "$fd_path")
                echo "--- fd $fd_num ---"
                cat "$fd_path" 2>/dev/null
                echo ""
                found=$((found + 1))
            fi
        fi
    done
    if [[ $found -eq 0 ]]; then
        echo "(no io_uring descriptors found)"
    else
        echo "Total io_uring fds: $found"
    fi
} > "$IOURING_FILE" 2>&1
log "io_uring fdinfo saved to $IOURING_FILE"

# ===================================================================
# 7. Summary
# ===================================================================
SUMMARY_FILE="$OUT_DIR/diagnosis-summary.txt"
log "Generating summary ..."

{
    echo "============================================================"
    echo "  foxingd Stall Diagnosis — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  PID=$PID  window=${DURATION}s"
    echo "============================================================"
    echo ""

    # --- Thread state histogram ---
    echo ">> Thread State Histogram (top wchan functions)"
    if [[ -f "$THREAD_FILE" ]]; then
        # Extract the wchan histogram section (between "wchan histogram" and next blank line)
        sed -n '/^--- wchan histogram ---$/,/^$/p' "$THREAD_FILE" | grep -v '^---' | head -20
    else
        echo "  (thread data unavailable)"
    fi
    echo ""

    # --- offcputime top stacks ---
    echo ">> Top offcputime Stacks (first 20 lines)"
    if [[ -f "$OFFCPU_FILE" ]] && ! grep -q 'not available' "$OFFCPU_FILE" 2>/dev/null; then
        head -20 "$OFFCPU_FILE"
    else
        echo "  (offcputime data unavailable)"
    fi
    echo ""

    # --- NFS slow ops count ---
    echo ">> NFS Slow Ops (>10ms)"
    if [[ -f "$NFS_FILE" ]] && ! grep -q 'not available' "$NFS_FILE" 2>/dev/null; then
        nfs_count=$(grep -cP '^\s*\d' "$NFS_FILE" 2>/dev/null) || nfs_count=0
        echo "  Count: $nfs_count"
        if [[ $nfs_count -gt 0 ]]; then
            head -10 "$NFS_FILE"
        fi
    else
        echo "  (nfsslower data unavailable)"
    fi
    echo ""

    # --- Metrics delta table ---
    echo ">> Metrics Delta"
    if [[ -f "$METRICS_DELTA_FILE" ]] && ! grep -q 'not available' "$METRICS_DELTA_FILE" 2>/dev/null; then
        cat "$METRICS_DELTA_FILE"
    else
        echo "  (metrics data unavailable)"
    fi
    echo ""

    # --- Verdict ---
    echo ">> Verdict"
    copy_delta=0
    if [[ -f "$METRICS_AFTER" && -f "$METRICS_BEFORE" ]]; then
        before_copy=$(extract_metric "$METRICS_BEFORE" "foxing_copy_method_standard_total")
        after_copy=$(extract_metric "$METRICS_AFTER" "foxing_copy_method_standard_total")
        before_copy=${before_copy:-0}
        after_copy=${after_copy:-0}
        copy_delta=$((after_copy - before_copy))
    fi

    if [[ $copy_delta -eq 0 ]]; then
        echo "  STALL_CONFIRMED — copy_method_standard_total delta = 0 over ${DURATION}s"
    else
        echo "  MAKING_PROGRESS — copy_method_standard_total delta = $copy_delta over ${DURATION}s"
    fi
    echo ""
    echo "============================================================"

} > "$SUMMARY_FILE" 2>&1

log "Summary written to $SUMMARY_FILE"
log "All diagnostics saved to $OUT_DIR/"

# Print the summary to stdout as well
cat "$SUMMARY_FILE"
