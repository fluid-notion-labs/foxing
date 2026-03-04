#!/usr/bin/env bash
# collect-metrics.sh — Scrape foxingd Prometheus metrics for adversarial test analysis
# Usage: bash collect-metrics.sh [label] [output_dir]
#   label:      tag for this snapshot (e.g. "phase1-pre", "phase3-post")
#   output_dir: directory for snapshots (default: /tmp/adversarial-results)
set -euo pipefail

LABEL="${1:-snapshot}"
REPORT_DIR="${2:-/tmp/adversarial-results}"
METRICS_URL="http://localhost:9100/metrics"
STATUS_URL="http://localhost:9100/status"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SNAP_FILE="$REPORT_DIR/metrics_${LABEL}_${TIMESTAMP}.txt"
STATUS_FILE="$REPORT_DIR/status_${LABEL}_${TIMESTAMP}.json"

mkdir -p "$REPORT_DIR"

# --- Scrape Prometheus metrics ---
if ! curl -sf "$METRICS_URL" > /dev/null 2>&1; then
    echo "WARN: foxingd metrics endpoint not responding at $METRICS_URL"
    echo "UNAVAILABLE at $(date -Iseconds)" > "$SNAP_FILE"
    exit 1
fi

# Full raw dump for archival
curl -sf "$METRICS_URL" > "$REPORT_DIR/raw_${LABEL}_${TIMESTAMP}.prom"

# Filtered key metrics
curl -sf "$METRICS_URL" | grep -E \
    'foxing_tuner_state|foxing_target_storage_class|foxing_target_batch_size|foxing_target_coalesce_bytes|foxing_target_flush_interval_ms|foxing_events_total|foxing_events_dropped|foxing_events_filtered|foxing_events_malformed|foxing_late_events_total|foxing_sequence_gaps_total|foxing_large_sequence_gaps|foxing_bytes_replicated|foxing_replication_latency|foxing_coalesced_writes|foxing_rename_events|foxing_governor_stress|foxing_governor_pacing|foxing_governor_throttled|foxing_governor_load|foxing_worker_retry_queue_size|foxing_worker_buffer_utilization|foxing_poison_cabinet_active|foxing_wal_coherence_failures|foxing_hydration_active|foxing_hydration_hash_skipped|foxing_hash_verifications|foxing_hash_cache_hits|foxing_copy_method_reflink|foxing_copy_method_offload|foxing_copy_method_standard|foxing_target_capacity_bytes|foxing_total_items_discovered|foxing_live_additions|foxing_sidecar_files_created|foxing_journal_recoveries|foxing_ordering_buffer_size|foxing_bpf_panic_caught|foxing_worker_copy_in_flight|foxing_worker_last_copy_epoch_ms|foxing_hydration_worker_blocked_ms|foxing_copy_timeout_total|foxing_events_repair_queued_total|foxing_events_repair_completed_total|foxing_events_repair_failed_total|foxing_events_source_gone_total' \
    | grep -v '^#' > "$SNAP_FILE"

# --- Scrape JSON status ---
curl -sf "$STATUS_URL" > "$STATUS_FILE" 2>/dev/null || echo '{"error":"unavailable"}' > "$STATUS_FILE"

# --- Extract key values for console output ---
echo "=== Metrics Snapshot: $LABEL ($TIMESTAMP) ==="

# Tuner states
echo "--- Tuner State (0=Startup,1=Drain,2=ProbeBW,3=Muted,8=Steady,99=Conservative) ---"
grep 'foxing_tuner_state{' "$SNAP_FILE" 2>/dev/null || echo "  (no tuner data)"

# Storage class
echo "--- Storage Class (0=Unknown,1=HDD,2=SATA,3=NVMe,4=Throttled) ---"
grep 'foxing_target_storage_class{' "$SNAP_FILE" 2>/dev/null || echo "  (no storage class data)"

# Event counters
echo "--- Events ---"
grep -E 'foxing_events_total\b|foxing_events_dropped\b|foxing_late_events_total\b' "$SNAP_FILE" 2>/dev/null || echo "  (no event data)"

echo "--- Event Types ---"
grep 'foxing_events_total{' "$SNAP_FILE" 2>/dev/null | sort || echo "  (no event type data)"

# Coalescer
echo "--- Coalescer ---"
grep -E 'foxing_coalesced_writes\b|foxing_target_coalesce_bytes{' "$SNAP_FILE" 2>/dev/null || echo "  (no coalesce data)"

# Reliability
echo "--- Reliability ---"
grep -E 'foxing_poison_cabinet|foxing_worker_retry_queue|foxing_wal_coherence|foxing_sidecar|foxing_journal' "$SNAP_FILE" 2>/dev/null || echo "  (no reliability data)"

# Capacity
echo "--- Capacity ---"
grep 'foxing_target_capacity_bytes' "$SNAP_FILE" 2>/dev/null || echo "  (no capacity data)"

# Governor
echo "--- Governor ---"
grep -E 'foxing_governor' "$SNAP_FILE" 2>/dev/null || echo "  (no governor data)"

# Copy methods
echo "--- Copy Methods ---"
grep 'foxing_copy_method' "$SNAP_FILE" 2>/dev/null || echo "  (no copy method data)"

# Stall Detection
echo "--- Stall Detection ---"
grep -E 'foxing_worker_copy_in_flight|foxing_worker_last_copy_epoch_ms|foxing_hydration_worker_blocked_ms|foxing_copy_timeout_total' "$SNAP_FILE" 2>/dev/null || echo "  (no stall data)"

# Repair
echo "--- Repair ---"
grep -E 'foxing_events_repair|foxing_events_source_gone' "$SNAP_FILE" 2>/dev/null || echo "  (no repair data)"

# Bytes replicated
echo "--- Data Movement ---"
grep 'foxing_bytes_replicated' "$SNAP_FILE" 2>/dev/null || echo "  (no bytes data)"

echo ""
echo "Full snapshot: $SNAP_FILE"
echo "Raw metrics:   $REPORT_DIR/raw_${LABEL}_${TIMESTAMP}.prom"
echo "JSON status:   $STATUS_FILE"
