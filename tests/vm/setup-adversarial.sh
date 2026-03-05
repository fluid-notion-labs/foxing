#!/usr/bin/env bash
# setup-adversarial.sh — Prepare koero VM for adversarial XFS→NFS stress testing
# Run on the VM: ssh root@fox-test.3d.ae.net.nz 'bash /mnt/foxing-bin/tests/vm/setup-adversarial.sh'
set -euo pipefail

NFS_SERVER="awa.3d.ae.net.nz"
NFS_EXPORT="/nfs_final/working/foxing/test-target"
NFS_MOUNT="/mnt/target-nfs"
SOURCE="/mnt/source"
CONFIG="/tmp/foxingd-adversarial.toml"
REPORT_DIR="/tmp/adversarial-results"

echo "=== foxingd Adversarial Test Setup ==="
echo "Date: $(date -Iseconds)"
echo "Kernel: $(uname -r)"
echo "Host: $(hostname)"

# --- Verify source mount ---
if ! mountpoint -q "$SOURCE" 2>/dev/null; then
    echo "ERROR: $SOURCE is not mounted. Ensure XFS source disk (vdb) is mounted."
    echo "  Try: mount /dev/vdb1 $SOURCE"
    exit 1
fi
echo "OK: Source mounted at $SOURCE ($(df -T "$SOURCE" | tail -1 | awk '{print $2}'))"

# --- Mount NFS target ---
mkdir -p "$NFS_MOUNT"
if mountpoint -q "$NFS_MOUNT" 2>/dev/null; then
    echo "OK: NFS already mounted at $NFS_MOUNT"
else
    echo "Mounting NFS: $NFS_SERVER:$NFS_EXPORT -> $NFS_MOUNT"
    # soft mount with short timeout — we intentionally stress this
    mount -t nfs -o soft,timeo=50,retrans=3,rsize=1048576,wsize=1048576,lookupcache=none,actimeo=0 \
        "$NFS_SERVER:$NFS_EXPORT" "$NFS_MOUNT"
    echo "OK: NFS mounted"
fi
echo "  NFS target: $(df -h "$NFS_MOUNT" | tail -1)"

# --- Clean previous test data ---
echo "Cleaning previous test data..."
rm -rf "$SOURCE"/adversarial-* 2>/dev/null || true
rm -rf "$NFS_MOUNT"/adversarial-* 2>/dev/null || true
rm -rf "$REPORT_DIR" 2>/dev/null || true
mkdir -p "$REPORT_DIR"

# --- Ensure foxingd binary is available ---
FOXINGD="/usr/local/bin/foxingd"
if [[ ! -x "$FOXINGD" ]]; then
    if [[ -x /mnt/foxing-bin/foxingd ]]; then
        cp /mnt/foxing-bin/foxingd "$FOXINGD"
        echo "OK: Copied foxingd from NFS to $FOXINGD"
    else
        echo "ERROR: foxingd not found. Deploy it first."
        exit 1
    fi
fi
echo "OK: foxingd at $FOXINGD ($(stat -c '%s bytes, %y' "$FOXINGD"))"

# --- Write foxingd config ---
cat > "$CONFIG" << 'EOF'
worker_count = 4
queue_max = 100000
global_buffer_limit = 4096
metrics_port = 9100
governor_psi_io_threshold = 10.0
governor_psi_cpu_threshold = 10.0
enable_content_hashing = true
hash_lite_threshold_kb = 128

[[sources]]
path = "/mnt/source"

  [[sources.targets]]
  path = "/mnt/target-nfs"
  profile = "NFS"
  initial_sync = true
  worker_count = 4
  autotune_target_latency_ms = 100
  worker_retry_initial_ms = 50
  worker_retry_max_ms = 5000
  batch_size = 32
EOF
echo "OK: Config written to $CONFIG"

# --- Kill any existing foxingd ---
if pgrep -x foxingd >/dev/null 2>&1; then
    echo "Stopping existing foxingd..."
    pkill -x foxingd || true
    sleep 2
    pkill -9 -x foxingd 2>/dev/null || true
fi

# --- Install dependencies ---
for cmd in fio sha256sum bc curl rsync; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Installing $cmd..."
        dnf install -y "$cmd" 2>/dev/null || yum install -y "$cmd" 2>/dev/null || true
    fi
done

# Install perf and bcc-tools for stall diagnostics
if ! command -v perf &>/dev/null; then
    echo "Installing perf..."
    dnf install -y perf 2>/dev/null || true
fi
if ! command -v offcputime-bpfcc &>/dev/null; then
    echo "Installing bcc-tools..."
    dnf install -y bcc-tools 2>/dev/null || true
fi

# Install kernel-devel for perf tracepoint support
if [[ ! -d "/lib/modules/$(uname -r)/build" ]]; then
    echo "Installing kernel-devel for perf tracepoints..."
    dnf install -y "kernel-devel-$(uname -r)" 2>/dev/null || true
fi

echo ""
echo "=== Setup Complete ==="
echo "  Source:  $SOURCE (XFS)"
echo "  Target:  $NFS_MOUNT (NFS → $NFS_SERVER)"
echo "  Config:  $CONFIG"
echo "  Reports: $REPORT_DIR"
echo ""
echo "Run the adversarial test:"
echo "  bash /mnt/foxing-bin/tests/vm/adversarial.sh"
