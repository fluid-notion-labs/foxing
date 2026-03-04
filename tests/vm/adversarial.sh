#!/usr/bin/env bash
# adversarial.sh — 6-phase adversarial stress test for foxingd (XFS → NFS)
#
# Tests BBR tuner, PoisonCabinet, CircuitBreaker, elastic coalescer, and
# sidecar resync under adversarial I/O conditions replicating from fast
# NVMe-backed XFS to slow HDD-backed NFS.
#
# Usage:
#   bash adversarial.sh              # Run all phases
#   bash adversarial.sh --phase 3    # Run specific phase
#   bash adversarial.sh --from 4     # Run phases 4-6
set -uo pipefail
# Note: -e omitted intentionally — test harness tolerates failures and reports them

# ============================================================================
# Configuration
# ============================================================================
SOURCE="/mnt/source"
TARGET="/mnt/target-nfs"
CONFIG="/tmp/foxingd-adversarial.toml"
FOXINGD="/usr/local/bin/foxingd"
METRICS_URL="http://localhost:9100/metrics"
STATUS_URL="http://localhost:9100/status"
REPORT_DIR="/tmp/adversarial-results"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# NFS mount info for Phase 4 remount
NFS_SERVER="awa.3d.ae.net.nz"
NFS_EXPORT="/nfs_final/working/foxing/test-target"
NFS_OPTS="soft,timeo=50,retrans=3,rsize=1048576,wsize=1048576"

# Timing — stall detection means we break early, these are hard limits
STALL_TIMEOUT=30            # No progress for 30s = stall, abort wait
HYDRATION_TIMEOUT=120       # 2 min hard limit for hydration
CONVERGENCE_TIMEOUT=90      # 90s hard limit for convergence
RESYNC_TIMEOUT=60           # 1 min for resync after remount
RESUME_TIMEOUT=60           # 1 min for dirty-flag resume

# Baseline data (populated by phase0)
BASELINE_CP_MBPS=0
BASELINE_RSYNC_MBPS=0

# Phase control
RUN_PHASE=""
FROM_PHASE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase) RUN_PHASE="$2"; shift 2 ;;
        --from)  FROM_PHASE="$2"; shift 2 ;;
        *)       echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ============================================================================
# Helpers
# ============================================================================
REPORT_FILE="$REPORT_DIR/adversarial-report_$(date +%Y%m%d_%H%M%S).md"
PHASE_RESULTS=()
OVERALL_START=$(date +%s)
FOXINGD_PID=""

mkdir -p "$REPORT_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }
fail() { log "FAIL: $*"; }
pass() { log "PASS: $*"; }
signal() { log "SIGNAL: $*"; }

collect_metrics() {
    local label="$1"
    bash "$SCRIPT_DIR/collect-metrics.sh" "$label" "$REPORT_DIR" 2>/dev/null || true
}

wait_for_metrics() {
    local timeout="${1:-30}"
    local elapsed=0
    while ! curl -sf "$METRICS_URL" >/dev/null 2>&1; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $timeout ]]; then
            log "ERROR: foxingd metrics not available after ${timeout}s"
            return 1
        fi
    done
    return 0
}

get_metric() {
    local name="$1"
    curl -sf "$METRICS_URL" 2>/dev/null | grep "^${name}" | tail -1 | awk '{print $2}'
}

get_metric_sum() {
    local name="$1"
    curl -sf "$METRICS_URL" 2>/dev/null \
        | grep "^${name}{" \
        | awk '{s+=$2} END {print s+0}'
}

get_tuner_states() {
    curl -sf "$METRICS_URL" 2>/dev/null | grep '^foxing_tuner_state{' || true
}

get_copy_count() {
    # Total copies across all methods + repair completions — THE key progress indicator
    local std off ref repair
    std=$(get_metric_sum "foxing_copy_method_standard_total")
    off=$(get_metric_sum "foxing_copy_method_offload_total")
    ref=$(get_metric_sum "foxing_copy_method_reflink_total")
    repair=$(get_metric "foxing_events_repair_completed_total")
    echo "$std $off $ref ${repair:-0}" | awk '{print $1+$2+$3+$4}'
}

# wait_for_progress: wait for a count to reach target, with stall detection
# Usage: wait_for_progress "label" <timeout> <stall_timeout> <target> <count_cmd>
# Returns: 0=converged, 1=stalled, 2=timed_out
wait_for_progress() {
    local label="$1" timeout="$2" stall="$3" target="$4"
    shift 4
    local count_cmd="$*"

    local elapsed=0 last_count=-1 stall_elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        sleep 5
        elapsed=$((elapsed + 5))

        local count
        count=$(eval "$count_cmd" 2>/dev/null || echo "0")
        local copies
        copies=$(get_copy_count)

        log "  t+${elapsed}s: ${label}=${count}/${target} copies=${copies}"

        if [[ $count -ge $target ]]; then
            return 0  # converged
        fi

        # Stall detection
        if [[ "$count" == "$last_count" ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            if [[ $stall_elapsed -ge $stall ]]; then
                log "  STALL: no progress for ${stall}s (stuck at ${count}/${target})"
                return 1
            fi
        else
            stall_elapsed=0
        fi
        last_count="$count"
    done
    return 2  # timed out
}

start_foxingd() {
    log "Starting foxingd..."
    pkill -x foxingd 2>/dev/null || true
    sleep 1
    "$FOXINGD" daemon -c "$CONFIG" > "$REPORT_DIR/foxingd.log" 2>&1 &
    FOXINGD_PID=$!
    log "foxingd PID=$FOXINGD_PID"

    if ! wait_for_metrics 30; then
        log "ERROR: foxingd failed to start"
        tail -30 "$REPORT_DIR/foxingd.log"
        return 1
    fi
    log "foxingd metrics available"
}

stop_foxingd() {
    if [[ -n "${FOXINGD_PID:-}" ]] && kill -0 "$FOXINGD_PID" 2>/dev/null; then
        log "Stopping foxingd (PID=$FOXINGD_PID)..."
        kill "$FOXINGD_PID" 2>/dev/null || true
        # Wait up to 5s for graceful shutdown, then SIGKILL
        local w=0
        while kill -0 "$FOXINGD_PID" 2>/dev/null && [[ $w -lt 5 ]]; do
            sleep 1
            w=$((w + 1))
        done
        if kill -0 "$FOXINGD_PID" 2>/dev/null; then
            log "foxingd did not stop gracefully, sending SIGKILL"
            kill -9 "$FOXINGD_PID" 2>/dev/null || true
        fi
        wait "$FOXINGD_PID" 2>/dev/null || true
    fi
    pkill -9 -x foxingd 2>/dev/null || true
    sleep 1
    FOXINGD_PID=""
}

should_run() {
    local phase=$1
    if [[ -n "$RUN_PHASE" ]]; then
        [[ "$phase" == "$RUN_PHASE" ]]
    else
        [[ "$phase" -ge "$FROM_PHASE" ]]
    fi
}

record_result() {
    local phase="$1" name="$2" result="$3" duration="$4"
    shift 4
    local signals="$*"
    PHASE_RESULTS+=("$phase|$name|$result|${duration}s|$signals")
    log ">> Phase $phase: $result (${duration}s) $signals"
}

clean_source() {
    log "Cleaning source test data..."
    find "$SOURCE" -mindepth 1 -maxdepth 1 -name 'adversarial-*' -exec rm -rf {} + 2>/dev/null || true
}

clean_target() {
    log "Cleaning target test data..."
    find "$TARGET" -mindepth 1 -maxdepth 1 -name 'adversarial-*' -exec rm -rf {} + 2>/dev/null || true
}

ensure_foxingd() {
    if ! curl -sf "$METRICS_URL" >/dev/null 2>&1; then
        if ! start_foxingd; then
            log "ERROR: foxingd failed to start"
            return 1
        fi
        sleep 3
    fi
    return 0
}

diagnose_stall() {
    local pid="$1" label="$2"
    local diag_dir="$REPORT_DIR/diag-${label}"
    if [[ -x "$SCRIPT_DIR/diagnose-stall.sh" ]] && [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        log "Running stall diagnostics (5s capture)..."
        bash "$SCRIPT_DIR/diagnose-stall.sh" "$pid" "$diag_dir" 5 2>/dev/null || true
        if [[ -f "$diag_dir/diagnosis-summary.txt" ]]; then
            log "Diagnosis: $(tail -1 "$diag_dir/diagnosis-summary.txt")"
        fi
    fi
}

# ============================================================================
# Phase 0: Baseline rsync/cp Timing
# ============================================================================
phase0() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 0: Baseline NFS Throughput (rsync + cp)"
    log "============================================"

    local signals=""
    local result="PASS"
    local baseline_dir="$SOURCE/adversarial-baseline"
    local baseline_tgt="$TARGET/adversarial-baseline"

    clean_source
    clean_target
    mkdir -p "$baseline_dir"

    # Generate baseline dataset: 100 x 4KB + 10 x 1MB + 1 x 50MB
    log "Generating baseline dataset..."
    for i in $(seq 1 100); do
        dd if=/dev/urandom of="$baseline_dir/small_${i}.dat" bs=4096 count=1 2>/dev/null
    done
    for i in $(seq 1 10); do
        dd if=/dev/urandom of="$baseline_dir/medium_${i}.dat" bs=1M count=1 2>/dev/null
    done
    dd if=/dev/urandom of="$baseline_dir/large_1.dat" bs=1M count=50 2>/dev/null

    local total_bytes
    total_bytes=$(du -sb "$baseline_dir" | awk '{print $1}')
    local total_mb=$((total_bytes / 1048576))
    log "Baseline dataset: 111 files, ${total_mb}MB"

    # --- cp baseline ---
    log "Running cp baseline..."
    rm -rf "$baseline_tgt" 2>/dev/null || true
    local cp_start=$(date +%s%N)
    cp -r "$baseline_dir" "$baseline_tgt"
    sync
    local cp_end=$(date +%s%N)
    local cp_ms=$(( (cp_end - cp_start) / 1000000 ))
    if [[ $cp_ms -gt 0 ]]; then
        BASELINE_CP_MBPS=$(( total_mb * 1000 / cp_ms ))
    fi
    log "  cp: ${cp_ms}ms (${BASELINE_CP_MBPS} MB/s)"

    # Verify cp
    local cp_count
    cp_count=$(find "$baseline_tgt" -type f 2>/dev/null | wc -l)
    if [[ $cp_count -ne 111 ]]; then
        fail "cp baseline: only $cp_count/111 files copied"
        signals="${signals}cp_broken "
        result="FAIL"
    fi

    # --- rsync baseline ---
    if command -v rsync &>/dev/null; then
        rm -rf "$baseline_tgt" 2>/dev/null || true
        log "Running rsync baseline..."
        local rsync_start=$(date +%s%N)
        rsync -a "$baseline_dir/" "$baseline_tgt/"
        sync
        local rsync_end=$(date +%s%N)
        local rsync_ms=$(( (rsync_end - rsync_start) / 1000000 ))
        if [[ $rsync_ms -gt 0 ]]; then
            BASELINE_RSYNC_MBPS=$(( total_mb * 1000 / rsync_ms ))
        fi
        log "  rsync: ${rsync_ms}ms (${BASELINE_RSYNC_MBPS} MB/s)"

        # Verify rsync
        local rsync_count
        rsync_count=$(find "$baseline_tgt" -type f 2>/dev/null | wc -l)
        if [[ $rsync_count -ne 111 ]]; then
            fail "rsync baseline: only $rsync_count/111 files"
            signals="${signals}rsync_broken "
            result="FAIL"
        fi
    else
        log "rsync not installed — skipping rsync baseline"
        BASELINE_RSYNC_MBPS=0
    fi

    # Cleanup baseline
    rm -rf "$baseline_dir" "$baseline_tgt" 2>/dev/null || true

    log "Baseline: cp=${BASELINE_CP_MBPS}MB/s rsync=${BASELINE_RSYNC_MBPS}MB/s"

    if [[ $BASELINE_CP_MBPS -eq 0 ]] && [[ $BASELINE_RSYNC_MBPS -eq 0 ]]; then
        signal "NFS target has zero throughput — all subsequent phases will fail"
        signals="${signals}nfs_zero_throughput "
        result="FAIL"
    fi

    local phase_end=$(date +%s)
    record_result 0 "Baseline NFS Throughput" "$result" "$((phase_end - phase_start))" "cp=${BASELINE_CP_MBPS}MB/s rsync=${BASELINE_RSYNC_MBPS}MB/s $signals"
}

# ============================================================================
# Phase 1: Heavy Initial Hydration (BBR Startup→Drain)
# ============================================================================
phase1() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 1: Heavy Initial Hydration (BBR Startup→Drain)"
    log "============================================"

    local signals=""
    local result="PASS"
    local test_dir="$SOURCE/adversarial-hydration"

    stop_foxingd
    clean_source
    clean_target

    # Generate 5000 mixed files (4KB - 50MB)
    log "Generating 5000 mixed files on XFS source..."
    mkdir -p "$test_dir"
    for i in $(seq 1 4000); do
        dd if=/dev/urandom of="$test_dir/small_${i}.dat" bs=4096 count=1 2>/dev/null
    done
    for i in $(seq 1 800); do
        local size=$((RANDOM % 1024 + 64))
        dd if=/dev/urandom of="$test_dir/medium_${i}.dat" bs=1024 count="$size" 2>/dev/null
    done
    for i in $(seq 1 150); do
        local size=$((RANDOM % 10 + 1))
        dd if=/dev/urandom of="$test_dir/large_${i}.dat" bs=1M count="$size" 2>/dev/null
    done
    for i in $(seq 1 50); do
        local size=$((RANDOM % 40 + 10))
        dd if=/dev/urandom of="$test_dir/xlarge_${i}.dat" bs=1M count="$size" 2>/dev/null
    done

    local total_files
    total_files=$(find "$test_dir" -type f | wc -l)
    local total_bytes
    total_bytes=$(du -sb "$test_dir" | awk '{print $1}')
    log "Generated $total_files files ($(numfmt --to=iec "$total_bytes"))"

    # Record pre-copy baseline
    local pre_copies
    collect_metrics "phase1-pre"

    if ! start_foxingd; then
        record_result 1 "Heavy Initial Hydration" "FAIL" "$(($(date +%s) - phase_start))" "foxingd_start_failed"
        return
    fi

    local initial_copies
    initial_copies=$(get_copy_count)

    # Monitor with stall detection
    log "Monitoring hydration (stall=${STALL_TIMEOUT}s, timeout=${HYDRATION_TIMEOUT}s)..."
    local saw_startup=false saw_drain=false tuner_trace=""

    local elapsed=0 last_tgt=0 stall_elapsed=0
    while [[ $elapsed -lt $HYDRATION_TIMEOUT ]]; do
        sleep 5
        elapsed=$((elapsed + 5))

        local states
        states=$(get_tuner_states)
        echo "$states" | grep -q ' 0$\| 0\.0$' && saw_startup=true
        echo "$states" | grep -q ' 1$\| 1\.0$' && saw_drain=true

        local current_state
        current_state=$(echo "$states" | head -1 | awk '{print $2}' || echo "?")
        tuner_trace="${tuner_trace} ${elapsed}s:${current_state}"

        local copies
        copies=$(get_copy_count)
        local copy_delta=$((${copies:-0} - ${initial_copies:-0}))

        local tgt_count
        tgt_count=$(find "$TARGET/adversarial-hydration" -type f 2>/dev/null | wc -l)

        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: target=${tgt_count}/${total_files} copies=${copies} repairs=${repair:-0} tuner=${current_state}"

        if [[ $tgt_count -ge $total_files ]]; then
            log "Target converged!"
            break
        fi

        # Stall detection on both target file count AND copy operations
        if [[ $tgt_count -eq $last_tgt ]] && [[ $copy_delta -eq ${last_copy_delta:-0} ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            if [[ $stall_elapsed -ge $STALL_TIMEOUT ]]; then
                signal "STALL: no progress for ${STALL_TIMEOUT}s (target=${tgt_count}, copies=${copy_delta})"
                signals="${signals}STALLED "
                diagnose_stall "${FOXINGD_PID:-}" "phase1"
                break
            fi
        else
            stall_elapsed=0
        fi
        last_tgt=$tgt_count
        last_copy_delta=$copy_delta
    done

    collect_metrics "phase1-post"

    local tgt_final
    tgt_final=$(find "$TARGET/adversarial-hydration" -type f 2>/dev/null | wc -l)
    local final_copies
    final_copies=$(get_copy_count)
    local total_copy_delta=$((${final_copies:-0} - ${initial_copies:-0}))

    if [[ $tgt_final -lt $total_files ]]; then
        fail "Only $tgt_final/$total_files files on target (${total_copy_delta} copy ops)"
        result="FAIL"
    else
        pass "All $total_files files replicated ($total_copy_delta copy ops)"
    fi

    # Regression check vs baseline
    if [[ $BASELINE_CP_MBPS -gt 0 ]] && [[ $total_copy_delta -eq 0 ]]; then
        signal "REGRESSION: cp baseline=${BASELINE_CP_MBPS}MB/s but foxingd did 0 copies"
        signals="${signals}REGRESSION_zero_copies "
    fi

    $saw_startup || { signal "no Startup state observed"; signals="${signals}no_startup "; }
    $saw_drain || { signal "no Drain state observed"; signals="${signals}no_drain "; }

    local dropped
    dropped=$(get_metric "foxing_events_dropped")
    [[ "${dropped:-0}" != "0" ]] && [[ "${dropped:-0}" != "0.0" ]] && signals="${signals}events_dropped=$dropped "

    log "Tuner trace:$tuner_trace"

    local phase_end=$(date +%s)
    record_result 1 "Heavy Initial Hydration" "$result" "$((phase_end - phase_start))" "$signals"
}

# ============================================================================
# Phase 2: Live Write Storm (Coalescer + Back-pressure)
# ============================================================================
phase2() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 2: Live Write Storm (Coalescer + Back-pressure)"
    log "============================================"

    local signals=""
    local result="PASS"
    local test_dir="$SOURCE/adversarial-writestorm"
    local transient_dir="$SOURCE/adversarial-transient"

    ensure_foxingd || return

    collect_metrics "phase2-pre"
    local pre_copies
    pre_copies=$(get_copy_count)
    local pre_coalesced
    pre_coalesced=$(get_metric "foxing_coalesced_writes")

    # --- fio write storm ---
    log "Generating writes via fio (30s)..."
    mkdir -p "$test_dir"
    fio --name=writestorm --directory="$test_dir" --rw=randwrite --bs=4k \
        --size=4k --nrfiles=500 --numjobs=4 --runtime=30 --time_based \
        --group_reporting --minimal > "$REPORT_DIR/fio_phase2.txt" 2>&1 &
    local fio_pid=$!

    # --- Transient create→unlink ---
    log "Generating 1000 transient files..."
    mkdir -p "$transient_dir"
    for i in $(seq 1 1000); do
        dd if=/dev/urandom of="$transient_dir/transient_${i}.tmp" bs=4096 count=1 2>/dev/null
        rm -f "$transient_dir/transient_${i}.tmp"
    done &
    local trans_pid=$!

    wait "$fio_pid" 2>/dev/null || true
    wait "$trans_pid" 2>/dev/null || true
    rmdir "$transient_dir" 2>/dev/null || true

    log "Write storm complete. Monitoring convergence..."

    local src_count
    src_count=$(find "$test_dir" -type f 2>/dev/null | wc -l)

    # Wait with stall detection
    local elapsed=0 last_copies=${pre_copies} stall_elapsed=0
    while [[ $elapsed -lt $CONVERGENCE_TIMEOUT ]]; do
        sleep 5
        elapsed=$((elapsed + 5))

        local copies
        copies=$(get_copy_count)
        local retry_size
        retry_size=$(get_metric_sum "foxing_worker_retry_queue_size")
        local tgt_count
        tgt_count=$(find "$TARGET/adversarial-writestorm" -type f 2>/dev/null | wc -l)

        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: target=${tgt_count}/${src_count} copies=${copies} repairs=${repair:-0} retries=${retry_size}"

        # Converged if retry empty and copies match
        if [[ "${retry_size:-0}" == "0" ]] || [[ "${retry_size:-0}" == "0.0" ]]; then
            if [[ $tgt_count -ge $src_count ]] && [[ $src_count -gt 0 ]]; then
                log "  Converged"
                break
            fi
        fi

        # Stall detection
        if [[ "${copies}" == "${last_copies}" ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            if [[ $stall_elapsed -ge $STALL_TIMEOUT ]]; then
                signal "STALL: no copy progress for ${STALL_TIMEOUT}s"
                signals="${signals}STALLED "
                diagnose_stall "${FOXINGD_PID:-}" "phase2"
                break
            fi
        else
            stall_elapsed=0
        fi
        last_copies=$copies
    done

    collect_metrics "phase2-post"

    local post_copies
    post_copies=$(get_copy_count)
    local copy_delta=$((${post_copies:-0} - ${pre_copies:-0}))

    local post_coalesced
    post_coalesced=$(get_metric "foxing_coalesced_writes")
    local coalesce_delta
    coalesce_delta=$(echo "${post_coalesced:-0} - ${pre_coalesced:-0}" | bc 2>/dev/null || echo "0")

    log "Copy ops delta: $copy_delta  Coalesced: $coalesce_delta"

    [[ "${coalesce_delta}" == "0" ]] && { signal "No coalescing observed"; signals="${signals}no_coalescing "; }

    local tgt_count
    tgt_count=$(find "$TARGET/adversarial-writestorm" -type f 2>/dev/null | wc -l)
    if [[ $tgt_count -lt $src_count ]]; then
        fail "Write storm: $tgt_count/$src_count files on target ($copy_delta copies)"
        result="FAIL"
    else
        pass "Write storm: all $src_count files replicated"
    fi

    local ghost_transients
    ghost_transients=$(find "$TARGET/adversarial-transient" -type f 2>/dev/null | wc -l)
    if [[ $ghost_transients -gt 0 ]]; then
        signal "Transient leak: $ghost_transients ghost files"
        signals="${signals}transient_leak=$ghost_transients "
    else
        pass "Transient filter: no ghost files"
    fi

    local phase_end=$(date +%s)
    record_result 2 "Live Write Storm" "$result" "$((phase_end - phase_start))" "$signals"
}

# ============================================================================
# Phase 3: Rename Chain Storm (Ordering)
# ============================================================================
phase3() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 3: Rename Chain Storm (Ordering)"
    log "============================================"

    local signals=""
    local result="PASS"
    local test_dir="$SOURCE/adversarial-rename"

    ensure_foxingd || return

    collect_metrics "phase3-pre"
    local pre_copies
    pre_copies=$(get_copy_count)

    mkdir -p "$test_dir"

    log "Generating 100 rename chains (a→b→c→d→e)..."
    for i in $(seq 1 100); do
        local base="$test_dir/chain_${i}"
        echo "rename-chain-$i-$(date +%N)" > "${base}_a"
        mv "${base}_a" "${base}_b"
        mv "${base}_b" "${base}_c"
        mv "${base}_c" "${base}_d"
        mv "${base}_d" "${base}_e"
    done

    log "Generating 50 cross-directory renames..."
    mkdir -p "$test_dir/subdir_a" "$test_dir/subdir_b"
    for i in $(seq 1 50); do
        echo "cross-rename-$i" > "$test_dir/subdir_a/file_${i}.dat"
        mv "$test_dir/subdir_a/file_${i}.dat" "$test_dir/subdir_b/file_${i}.dat"
    done

    log "Waiting for convergence..."
    sleep 3

    local elapsed=0 last_e=0 stall_elapsed=0
    while [[ $elapsed -lt $CONVERGENCE_TIMEOUT ]]; do
        sleep 5
        elapsed=$((elapsed + 5))

        local tgt_e
        tgt_e=$(find "$TARGET/adversarial-rename" -name 'chain_*_e' -type f 2>/dev/null | wc -l)
        local tgt_cross
        tgt_cross=$(find "$TARGET/adversarial-rename/subdir_b" -type f 2>/dev/null | wc -l)
        local copies
        copies=$(get_copy_count)

        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: finals=${tgt_e}/100 cross=${tgt_cross}/50 copies=${copies} repairs=${repair:-0}"

        [[ $tgt_e -ge 100 ]] && [[ $tgt_cross -ge 50 ]] && break

        # Stall detection
        if [[ $tgt_e -eq $last_e ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            [[ $stall_elapsed -ge $STALL_TIMEOUT ]] && { signal "STALL"; signals="${signals}STALLED "; diagnose_stall "${FOXINGD_PID:-}" "phase3"; break; }
        else
            stall_elapsed=0
        fi
        last_e=$tgt_e
    done

    collect_metrics "phase3-post"

    # Check ghosts
    local ghost_a ghost_b ghost_c ghost_d
    ghost_a=$(find "$TARGET/adversarial-rename" -name 'chain_*_a' -type f 2>/dev/null | wc -l)
    ghost_b=$(find "$TARGET/adversarial-rename" -name 'chain_*_b' -type f 2>/dev/null | wc -l)
    ghost_c=$(find "$TARGET/adversarial-rename" -name 'chain_*_c' -type f 2>/dev/null | wc -l)
    ghost_d=$(find "$TARGET/adversarial-rename" -name 'chain_*_d' -type f 2>/dev/null | wc -l)
    local final_e
    final_e=$(find "$TARGET/adversarial-rename" -name 'chain_*_e' -type f 2>/dev/null | wc -l)

    local total_ghosts=$((ghost_a + ghost_b + ghost_c + ghost_d))
    [[ $total_ghosts -gt 0 ]] && { fail "Ghost intermediates: a=$ghost_a b=$ghost_b c=$ghost_c d=$ghost_d"; signals="${signals}ghosts=$total_ghosts "; result="FAIL"; }
    [[ $total_ghosts -eq 0 ]] && pass "No ghost intermediates"

    [[ $final_e -lt 100 ]] && { fail "Missing finals: $final_e/100"; signals="${signals}missing=$((100-final_e)) "; result="FAIL"; }
    [[ $final_e -ge 100 ]] && pass "All 100 final renames present"

    local cross_b
    cross_b=$(find "$TARGET/adversarial-rename/subdir_b" -type f 2>/dev/null | wc -l)
    [[ $cross_b -lt 50 ]] && { fail "Cross-rename: $cross_b/50"; result="FAIL"; }

    local phase_end=$(date +%s)
    record_result 3 "Rename Chain Storm" "$result" "$((phase_end - phase_start))" "$signals"
}

# ============================================================================
# Phase 4: NFS Target Drop + Resync (CircuitBreaker + Sidecar)
# ============================================================================
phase4() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 4: NFS Target Drop + Resync"
    log "============================================"

    local signals=""
    local result="PASS"
    local test_dir="$SOURCE/adversarial-resync"

    ensure_foxingd || return

    log "Seeding 200 files..."
    mkdir -p "$test_dir"
    for i in $(seq 1 200); do
        echo "resync-$i-$(date +%N)" > "$test_dir/file_${i}.dat"
    done

    # Short wait for initial replication
    log "Waiting for initial replication (stall=${STALL_TIMEOUT}s)..."
    local elapsed=0 last_count=0 stall_elapsed=0
    while [[ $elapsed -lt 60 ]]; do
        sleep 5
        elapsed=$((elapsed + 5))
        local tgt_count
        tgt_count=$(find "$TARGET/adversarial-resync" -type f 2>/dev/null | wc -l)
        local copies
        copies=$(get_copy_count)
        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: target=${tgt_count}/200 copies=${copies} repairs=${repair:-0}"
        [[ $tgt_count -ge 200 ]] && break
        if [[ $tgt_count -eq $last_count ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            [[ $stall_elapsed -ge $STALL_TIMEOUT ]] && { signal "STALL on initial replication"; signals="${signals}STALLED "; diagnose_stall "${FOXINGD_PID:-}" "phase4-initial"; break; }
        else
            stall_elapsed=0
        fi
        last_count=$tgt_count
    done

    collect_metrics "phase4-pre-drop"

    # Drop NFS
    log "DROPPING NFS TARGET..."
    umount -l "$TARGET" 2>/dev/null || true

    log "Writing 100 files during outage..."
    for i in $(seq 201 300); do
        echo "outage-$i" > "$test_dir/file_${i}.dat"
    done

    log "Waiting 15s for error detection..."
    for tick in $(seq 1 3); do
        sleep 5
        local retries
        retries=$(get_metric_sum "foxing_worker_retry_queue_size")
        local poison
        poison=$(get_metric "foxing_poison_cabinet_active_inodes")
        log "  t+$((tick*5))s: retries=$retries poison=$poison"
    done

    collect_metrics "phase4-during-outage"

    # Remount
    log "REMOUNTING NFS..."
    mkdir -p "$TARGET"
    mount -t nfs -o "$NFS_OPTS" "$NFS_SERVER:$NFS_EXPORT" "$TARGET"

    log "Waiting for resync..."
    elapsed=0
    last_count=0
    stall_elapsed=0
    while [[ $elapsed -lt $RESYNC_TIMEOUT ]]; do
        sleep 5
        elapsed=$((elapsed + 5))
        local tgt_count
        tgt_count=$(find "$TARGET/adversarial-resync" -type f 2>/dev/null | wc -l)
        local retries
        retries=$(get_metric_sum "foxing_worker_retry_queue_size")
        local copies
        copies=$(get_copy_count)
        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: target=${tgt_count}/300 copies=${copies} repairs=${repair:-0} retries=${retries}"
        [[ $tgt_count -ge 300 ]] && break
        if [[ $tgt_count -eq $last_count ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            [[ $stall_elapsed -ge $STALL_TIMEOUT ]] && { signal "STALL on resync"; signals="${signals}resync_STALLED "; diagnose_stall "${FOXINGD_PID:-}" "phase4-resync"; break; }
        else
            stall_elapsed=0
        fi
        last_count=$tgt_count
    done

    collect_metrics "phase4-post-resync"

    local final_count
    final_count=$(find "$TARGET/adversarial-resync" -type f 2>/dev/null | wc -l)
    if [[ $final_count -lt 300 ]]; then
        fail "Resync: $final_count/300 files"
        signals="${signals}resync_incomplete=$final_count/300 "
        result="FAIL"
    else
        pass "All 300 files after resync"
    fi

    local phase_end=$(date +%s)
    record_result 4 "NFS Target Drop + Resync" "$result" "$((phase_end - phase_start))" "$signals"
}

# ============================================================================
# Phase 5: Large File Partial Sync (Merkle + Dirty)
# ============================================================================
phase5() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 5: Large File Partial Sync (Dirty Flag)"
    log "============================================"

    local signals=""
    local result="PASS"
    local test_dir="$SOURCE/adversarial-largefile"

    stop_foxingd
    clean_source
    clean_target
    mkdir -p "$test_dir"

    # 100MB instead of 500MB — faster to generate and still tests the path
    log "Creating 100MB file..."
    dd if=/dev/urandom of="$test_dir/bigfile.dat" bs=1M count=100 2>/dev/null
    local src_hash
    src_hash=$(sha256sum "$test_dir/bigfile.dat" | awk '{print $1}')
    log "Source hash: ${src_hash:0:16}..."

    if ! start_foxingd; then
        record_result 5 "Large File Partial Sync" "FAIL" "$(($(date +%s) - phase_start))" "foxingd_start_failed"
        return
    fi

    # Wait for partial sync
    log "Waiting for partial sync..."
    local elapsed=0 partial=false
    while [[ $elapsed -lt 60 ]]; do
        sleep 3
        elapsed=$((elapsed + 3))
        local tgt_size
        tgt_size=$(stat -c '%s' "$TARGET/adversarial-largefile/bigfile.dat" 2>/dev/null || echo "0")
        local tgt_mb=$((tgt_size / 1048576))
        local copies
        copies=$(get_copy_count)
        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: ${tgt_mb}MB/100MB copies=${copies} repairs=${repair:-0}"

        if [[ $tgt_size -gt 5242880 ]] && [[ $tgt_size -lt 104857600 ]]; then
            partial=true
            log "Partial sync at ${tgt_mb}MB — killing foxingd"
            break
        fi
        # Check if fully synced already
        [[ $tgt_size -ge 104857600 ]] && { log "Fully synced before kill"; signals="${signals}full_sync_before_kill "; break; }
    done

    collect_metrics "phase5-pre-kill"

    log "SIGKILL foxingd..."
    kill -9 "$FOXINGD_PID" 2>/dev/null || true
    wait "$FOXINGD_PID" 2>/dev/null || true
    FOXINGD_PID=""
    sleep 2

    local pre_kill_size
    pre_kill_size=$(stat -c '%s' "$TARGET/adversarial-largefile/bigfile.dat" 2>/dev/null || echo "0")
    log "Target at kill: $((pre_kill_size / 1048576))MB"

    # Restart
    log "Restarting foxingd for resume..."
    if ! start_foxingd; then
        record_result 5 "Large File Partial Sync" "FAIL" "$(($(date +%s) - phase_start))" "foxingd_restart_failed"
        return
    fi

    elapsed=0
    local last_size=0 stall_elapsed=0
    while [[ $elapsed -lt $RESUME_TIMEOUT ]]; do
        sleep 5
        elapsed=$((elapsed + 5))
        local tgt_size
        tgt_size=$(stat -c '%s' "$TARGET/adversarial-largefile/bigfile.dat" 2>/dev/null || echo "0")
        local copies
        copies=$(get_copy_count)
        local repair
        repair=$(get_metric "foxing_events_repair_completed_total")
        log "  t+${elapsed}s: ${tgt_size}B / 104857600B copies=${copies} repairs=${repair:-0}"
        [[ $tgt_size -ge 104857600 ]] && break
        if [[ $tgt_size -eq $last_size ]]; then
            stall_elapsed=$((stall_elapsed + 5))
            [[ $stall_elapsed -ge $STALL_TIMEOUT ]] && { signal "STALL on resume"; signals="${signals}resume_STALLED "; diagnose_stall "${FOXINGD_PID:-}" "phase5"; break; }
        else
            stall_elapsed=0
        fi
        last_size=$tgt_size
    done

    collect_metrics "phase5-post-resume"

    local tgt_hash
    tgt_hash=$(sha256sum "$TARGET/adversarial-largefile/bigfile.dat" 2>/dev/null | awk '{print $1}')
    if [[ "$src_hash" == "$tgt_hash" ]]; then
        pass "SHA-256 match after kill/resume"
    else
        fail "Hash mismatch: src=${src_hash:0:16} tgt=${tgt_hash:0:16}"
        signals="${signals}hash_mismatch "
        result="FAIL"
    fi

    local recoveries
    recoveries=$(get_metric "foxing_journal_recoveries")
    [[ "${recoveries:-0}" != "0" ]] && [[ "${recoveries:-0}" != "0.0" ]] && pass "Journal recovery detected"
    [[ "${recoveries:-0}" == "0" ]] || [[ "${recoveries:-0}" == "0.0" ]] && { signal "No journal recovery"; signals="${signals}no_journal_recovery "; }

    local phase_end=$(date +%s)
    record_result 5 "Large File Partial Sync" "$result" "$((phase_end - phase_start))" "$signals"
}

# ============================================================================
# Phase 6: Disk Pressure (CircuitBreaker + Muted)
# ============================================================================
phase6() {
    local phase_start=$(date +%s)
    log ""
    log "============================================"
    log "PHASE 6: Disk Pressure (CircuitBreaker + Muted)"
    log "============================================"

    local signals=""
    local result="PASS"
    local test_dir="$SOURCE/adversarial-pressure"

    ensure_foxingd || return

    local tgt_avail_bytes
    tgt_avail_bytes=$(df --output=avail -B1 "$TARGET" | tail -1)
    local tgt_avail_mb=$((tgt_avail_bytes / 1048576))
    log "NFS available: ${tgt_avail_mb}MB"

    local fill_mb=$((tgt_avail_mb - 256))
    if [[ $fill_mb -le 0 ]]; then
        record_result 6 "Disk Pressure" "SKIP" "0" "target_too_full"
        return
    fi
    if [[ $fill_mb -gt 10240 ]]; then
        log "SKIP: NFS share too large (${tgt_avail_mb}MB) for safe pressure test"
        record_result 6 "Disk Pressure" "SKIP" "0" "nfs_too_large=${tgt_avail_mb}MB"
        return
    fi

    log "Filling ${fill_mb}MB..."
    dd if=/dev/zero of="$TARGET/.fill_pressure_test" bs=1M count="$fill_mb" 2>/dev/null || true
    log "Post-fill: $(($(df --output=avail -B1 "$TARGET" | tail -1) / 1048576))MB free"

    mkdir -p "$test_dir"
    for i in $(seq 1 50); do
        dd if=/dev/urandom of="$test_dir/pressure_${i}.dat" bs=4096 count=10 2>/dev/null
    done

    local saw_muted=false
    for tick in $(seq 1 12); do
        sleep 5
        local states
        states=$(get_tuner_states)
        if echo "$states" | grep -q ' 3$\| 3\.0$'; then
            saw_muted=true
            log "  t+$((tick*5))s: MUTED detected!"
            break
        fi
        local st
        st=$(echo "$states" | head -1 | awk '{print $2}')
        log "  t+$((tick*5))s: tuner=$st"
    done

    collect_metrics "phase6-during-pressure"

    $saw_muted && pass "Muted state detected" || { signal "No Muted state"; signals="${signals}no_muted "; }

    log "Releasing pressure..."
    rm -f "$TARGET/.fill_pressure_test"

    if $saw_muted; then
        local recovered=false
        for tick in $(seq 1 6); do
            sleep 5
            local states
            states=$(get_tuner_states)
            if ! echo "$states" | grep -q ' 3$\| 3\.0$'; then
                recovered=true
                log "  Recovered from Muted"
                break
            fi
        done
        $recovered && pass "Recovered from Muted" || { fail "No recovery"; signals="${signals}no_recovery "; result="FAIL"; }
    fi

    collect_metrics "phase6-post"

    local phase_end=$(date +%s)
    record_result 6 "Disk Pressure" "$result" "$((phase_end - phase_start))" "$signals"
}

# ============================================================================
# Report Generation
# ============================================================================
generate_report() {
    local overall_end=$(date +%s)
    local total_duration=$((overall_end - OVERALL_START))

    cat > "$REPORT_FILE" << REPORTEOF
# foxingd Adversarial Test Report

**Date:** $(date -Iseconds)
**Host:** $(hostname)
**Kernel:** $(uname -r)
**Duration:** ${total_duration}s
**foxingd:** $($FOXINGD --version 2>/dev/null || echo "unknown")
**Baseline:** cp=${BASELINE_CP_MBPS}MB/s rsync=${BASELINE_RSYNC_MBPS}MB/s

## Configuration

- **Source:** $SOURCE (XFS, NVMe-backed virtio-blk)
- **Target:** $TARGET (NFS → $NFS_SERVER, HDD-backed)
- **Stall Detection:** ${STALL_TIMEOUT}s no-progress threshold

## Results

| Phase | Test | Result | Duration | Signals |
|-------|------|--------|----------|---------|
REPORTEOF

    local pass_count=0 fail_count=0 skip_count=0

    for entry in "${PHASE_RESULTS[@]}"; do
        IFS='|' read -r phase name res dur sigs <<< "$entry"
        echo "| $phase | $name | $res | $dur | $sigs |" >> "$REPORT_FILE"
        case "$res" in
            PASS) pass_count=$((pass_count + 1)) ;;
            FAIL) fail_count=$((fail_count + 1)) ;;
            SKIP) skip_count=$((skip_count + 1)) ;;
        esac
    done

    cat >> "$REPORT_FILE" << REPORTEOF

## Summary

- **Passed:** $pass_count / **Failed:** $fail_count / **Skipped:** $skip_count
- **Total Duration:** ${total_duration}s

## Signals

REPORTEOF

    for entry in "${PHASE_RESULTS[@]}"; do
        IFS='|' read -r phase name res dur sigs <<< "$entry"
        [[ -n "$sigs" ]] && echo "- **P${phase} ${name}:** $sigs" >> "$REPORT_FILE"
    done

    cat >> "$REPORT_FILE" << REPORTEOF

## Diagnostic Captures

\`\`\`
$(for d in "$REPORT_DIR"/diag-*/diagnosis-summary.txt; do
    [[ -f "$d" ]] || continue
    echo "=== $(basename $(dirname "$d")) ==="
    cat "$d"
    echo
done)
\`\`\`

## foxingd Log (last 30 lines)

\`\`\`
$(tail -30 "$REPORT_DIR/foxingd.log" 2>/dev/null || echo "No log")
\`\`\`
REPORTEOF

    log ""
    log "============================================"
    log "REPORT: $REPORT_FILE"
    log "Passed=$pass_count Failed=$fail_count Skipped=$skip_count Duration=${total_duration}s"
    log "============================================"
    cat "$REPORT_FILE"
}

# ============================================================================
# Main
# ============================================================================
log "=== foxingd Adversarial Test Suite ==="
log "Stall detection: ${STALL_TIMEOUT}s"

if [[ ! -x "$FOXINGD" ]]; then
    log "ERROR: foxingd not found at $FOXINGD"
    exit 1
fi

if ! mountpoint -q "$SOURCE" 2>/dev/null; then
    log "ERROR: Source not mounted at $SOURCE"
    exit 1
fi

if ! mountpoint -q "$TARGET" 2>/dev/null; then
    log "ERROR: NFS target not mounted at $TARGET — run setup-adversarial.sh first"
    exit 1
fi

if [[ ! -f "$CONFIG" ]]; then
    log "ERROR: Config not found at $CONFIG — run setup-adversarial.sh first"
    exit 1
fi

# Run phases
should_run 0 && phase0
should_run 1 && phase1
should_run 2 && phase2
should_run 3 && phase3
should_run 4 && phase4
should_run 5 && phase5
should_run 6 && phase6

stop_foxingd
generate_report

if mountpoint -q "$TARGET" 2>/dev/null; then
    cp "$REPORT_FILE" "$TARGET/" 2>/dev/null || true
fi

log "Done."
