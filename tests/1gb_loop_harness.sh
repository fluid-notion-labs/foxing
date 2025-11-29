#!/bin/bash
set -e

# ==============================================================================
# Foxing Enhanced Test Environment Setup with Device Detection Validation
# ==============================================================================

BASE_DIR="/tmp/foxing_test"
SOURCE_IMG="$BASE_DIR/source.img"
TARGET_IMG="$BASE_DIR/target.img"
SOURCE_MNT="$BASE_DIR/mnt/source"
TARGET_MNT="$BASE_DIR/mnt/target"
IMG_SIZE="512M"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Please run as root${NC}"
  exit 1
fi

echo -e "${GREEN}==> Cleaning up previous runs...${NC}"
if mountpoint -q "$SOURCE_MNT"; then umount "$SOURCE_MNT"; fi
if mountpoint -q "$TARGET_MNT"; then umount "$TARGET_MNT"; fi
rm -rf "$BASE_DIR"

echo -e "${GREEN}==> Creating infrastructure at $BASE_DIR...${NC}"
mkdir -p "$SOURCE_MNT"
mkdir -p "$TARGET_MNT"

truncate -s $IMG_SIZE "$SOURCE_IMG"
truncate -s $IMG_SIZE "$TARGET_IMG"

echo -e "${GREEN}==> Formatting XFS filesystems...${NC}"
if ! command -v mkfs.xfs &> /dev/null; then
    echo -e "${RED}mkfs.xfs not found. Install xfsprogs${NC}"
    exit 1
fi

mkfs.xfs -f -m reflink=1 "$SOURCE_IMG" > /dev/null
mkfs.xfs -f -m reflink=1 "$TARGET_IMG" > /dev/null

echo -e "${GREEN}==> Mounting loopback devices...${NC}"
mount -o loop "$SOURCE_IMG" "$SOURCE_MNT"
mount -o loop "$TARGET_IMG" "$TARGET_MNT"

REAL_USER=${SUDO_USER:-$(whoami)}
chown -R "$REAL_USER:$REAL_USER" "$SOURCE_MNT"
chown -R "$REAL_USER:$REAL_USER" "$TARGET_MNT"

# ==============================================================================
# DEVICE DETECTION VALIDATION
# ==============================================================================
echo -e "${YELLOW}==> Validating device detection...${NC}"

SOURCE_DEV=$(stat -c '%d' "$SOURCE_MNT")
TARGET_DEV=$(stat -c '%d' "$TARGET_MNT")

echo -e "${GREEN}Source mount:${NC} $SOURCE_MNT"
echo -e "  Device ID (decimal): $SOURCE_DEV"
echo -e "  Device ID (hex):     $(printf '0x%08x' $SOURCE_DEV)"

echo -e "${GREEN}Target mount:${NC} $TARGET_MNT"
echo -e "  Device ID (decimal): $TARGET_DEV"
echo -e "  Device ID (hex):     $(printf '0x%08x' $TARGET_DEV)"

# Find the loop devices
SOURCE_LOOP=$(losetup -a | grep "$SOURCE_IMG" | cut -d: -f1)
TARGET_LOOP=$(losetup -a | grep "$TARGET_IMG" | cut -d: -f1)

echo -e "${GREEN}Loop devices:${NC}"
echo -e "  Source: $SOURCE_LOOP -> $SOURCE_IMG"
echo -e "  Target: $TARGET_LOOP -> $TARGET_IMG"

# Show mount table entries
echo -e "${YELLOW}==> Mount table entries:${NC}"
grep "$(basename $SOURCE_IMG)" /proc/mounts || echo "  (none)"
grep "$(basename $TARGET_IMG)" /proc/mounts || echo "  (none)"

# Show backing files
if [ -n "$SOURCE_LOOP" ]; then
    LOOP_NUM=$(echo $SOURCE_LOOP | sed 's/\/dev\/loop//')
    if [ -f "/sys/block/loop${LOOP_NUM}/loop/backing_file" ]; then
        echo -e "${GREEN}Source backing file:${NC} $(cat /sys/block/loop${LOOP_NUM}/loop/backing_file)"
    fi
fi

if [ -n "$TARGET_LOOP" ]; then
    LOOP_NUM=$(echo $TARGET_LOOP | sed 's/\/dev\/loop//')
    if [ -f "/sys/block/loop${LOOP_NUM}/loop/backing_file" ]; then
        echo -e "${GREEN}Target backing file:${NC} $(cat /sys/block/loop${LOOP_NUM}/loop/backing_file)"
    fi
fi

# ==============================================================================
# GENERATE CONFIGURATION
# ==============================================================================
CONFIG_PATH="$(pwd)/test_config.toml"
echo -e "${GREEN}==> Generating configuration: $CONFIG_PATH${NC}"

cat > "$CONFIG_PATH" <<EOF
# Generated Test Configuration for Foxing
# Device Detection Test Mode

worker_count = 2
queue_max = 50000
metrics_port = 9101
global_buffer_limit = 5242880
max_system_load_avg = 8.0
hydration_delay_ms = 5

# CRITICAL FIX: Loopback devices are small (512MB). 
# We must lower the safety threshold (default 500MB) or writes will be rejected.
capacity_threshold_mb = 50

[[sources]]
path = "$SOURCE_MNT"

  [[sources.targets]]
  path = "$TARGET_MNT"
  profile = "SSD"
  initial_sync = true
  
  autotune_target_latency_ms = 20
  
  enable_versioning = true
  max_versions = 5
  max_versions_size_mb = 100
  
  supports_reflink = true

  excludes = [
      ".*/ignore_me/.*",
      ".*\\\\.tmp$"
  ]
EOF

echo -e "${GREEN}==> Setup Complete!${NC}"
echo ""
echo -e "${YELLOW}Source Device ID:${NC} $(printf '0x%08x' $SOURCE_DEV) ($SOURCE_DEV)"
echo -e "${YELLOW}Target Device ID:${NC} $(printf '0x%08x' $TARGET_DEV) ($TARGET_DEV)"
echo ""
echo -e "${YELLOW}To run the daemon with enhanced logging:${NC}"
echo "  RUST_LOG=debug sudo ./target/release/foxing daemon --config test_config.toml"
echo ""
echo -e "${YELLOW}To generate test load:${NC}"
echo "  for i in {1..10}; do echo \"data \$i\" > $SOURCE_MNT/file_\$i.txt; done"
echo ""
echo -e "${YELLOW}To check BPF kernel logs (in another terminal):${NC}"
echo "  sudo cat /sys/kernel/debug/tracing/trace_pipe | grep FOXING"
echo ""
echo -e "${YELLOW}To verify events are being captured:${NC}"
echo "  curl http://localhost:9101/metrics | grep foxing_events_total"
