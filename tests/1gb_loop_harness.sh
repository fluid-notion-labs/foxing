#!/bin/bash
set -e

# ==============================================================================
# Foxing: High-Performance Loopback Test Harness (Optimized)
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
BLUE='\033[0;34m'
NC='\033[0m'

if [ "$EUID" -ne 0 ]; then
  echo -e "${RED}Error: This script requires root privileges to mount loopback devices.${NC}"
  echo "Please run with sudo."
  exit 1
fi

echo -e "${BLUE}==> tearing down previous environment...${NC}"
# Lazy unmount to handle busy devices
if mountpoint -q "$SOURCE_MNT"; then umount -l "$SOURCE_MNT"; fi
if mountpoint -q "$TARGET_MNT"; then umount -l "$TARGET_MNT"; fi
rm -rf "$BASE_DIR"

echo -e "${GREEN}==> Creating infrastructure at $BASE_DIR...${NC}"
mkdir -p "$SOURCE_MNT"
mkdir -p "$TARGET_MNT"

# Create sparse files
truncate -s $IMG_SIZE "$SOURCE_IMG"
truncate -s $IMG_SIZE "$TARGET_IMG"

echo -e "${GREEN}==> Formatting XFS filesystems (reflink enabled)...${NC}"
if ! command -v mkfs.xfs &> /dev/null; then
    echo -e "${RED}mkfs.xfs not found. Please install xfsprogs.${NC}"
    exit 1
fi

mkfs.xfs -f -m reflink=1 "$SOURCE_IMG" > /dev/null
mkfs.xfs -f -m reflink=1 "$TARGET_IMG" > /dev/null

echo -e "${GREEN}==> Mounting loopback devices...${NC}"
mount -o loop "$SOURCE_IMG" "$SOURCE_MNT"
mount -o loop "$TARGET_IMG" "$TARGET_MNT"

# Fix permissions so the regular user can write to source
REAL_USER=${SUDO_USER:-$(whoami)}
chown -R "$REAL_USER:$REAL_USER" "$SOURCE_MNT"
chown -R "$REAL_USER:$REAL_USER" "$TARGET_MNT"

# ==============================================================================
# DEVICE ID VALIDATION (CRITICAL FOR BPF)
# ==============================================================================
SOURCE_DEV=$(stat -c '%d' "$SOURCE_MNT")
TARGET_DEV=$(stat -c '%d' "$TARGET_MNT")

echo -e "${YELLOW}==> Topology:${NC}"
echo -e "  Source: $SOURCE_MNT (Dev ID: $SOURCE_DEV / $(printf '0x%08x' $SOURCE_DEV))"
echo -e "  Target: $TARGET_MNT (Dev ID: $TARGET_DEV / $(printf '0x%08x' $TARGET_DEV))"

if [ "$SOURCE_DEV" == "$TARGET_DEV" ]; then
    echo -e "${RED}CRITICAL ERROR: Source and Target have the same Device ID. BPF will fail.${NC}"
    exit 1
fi

# ==============================================================================
# OPTIMAL CONFIG GENERATION
# ==============================================================================
CONFIG_PATH="$(pwd)/test_config.toml"
echo -e "${GREEN}==> Generating optimized config: $CONFIG_PATH${NC}"

# Note: We set aggressive flush intervals and low latency targets 
# to make the loopback test feel "instant" for the user.
cat > "$CONFIG_PATH" <<EOF
# Generated Test Configuration for Foxing (Loopback Optimized)

# --- Global ---
worker_count = 2
queue_max = 200000
metrics_port = 9101
global_buffer_limit = 5242880 # 5MB limit for small test
fatal_metrics_bind = false

# --- Governor (Relaxed for Test) ---
# Don't throttle on dev laptop load spikes
max_system_load_avg = 50.0 
hydration_delay_ms = 1

# --- Loopback Safety ---
# Lower threshold because 512MB drives fill up fast.
capacity_threshold_mb = 50

# --- Response Tuning (The "Snappy" Settings) ---
# Commit metadata to disk every 1 second (default is 5s)
force_flush_interval_secs = 1

[[sources]]
path = "$SOURCE_MNT"

  [[sources.targets]]
  path = "$TARGET_MNT"
  profile = "SSD" 
  initial_sync = true
  
  # Aggressive: Keep batch sizes small so single file writes 
  # are processed immediately rather than waiting for coalescing.
  autotune_target_latency_ms = 10
  
  # Enable Features
  enable_versioning = true
  supports_reflink = true
  
  # Versioning Limits
  max_versions = 5
  max_versions_size_mb = 100

  excludes = [
      ".*/ignore_me/.*",
      ".*\\\\.tmp$"
  ]
EOF

echo -e "${GREEN}==> Setup Complete!${NC}"
echo ""
echo -e "${BLUE}==============================================================================${NC}"
echo -e "${BLUE}                       SUGGESTED TEST COMMANDS                                ${NC}"
echo -e "${BLUE}==============================================================================${NC}"

echo -e "\n${YELLOW}1. Run Daemon (With AI-Friendly Logging):${NC}"
echo "   This captures Debug logs but filters output to a concise file for sharing."
echo "   RUST_LOG=foxing=debug,warn ./target/release/foxing daemon --config test_config.toml 2>&1 | tee full_log.txt | grep -E --line-buffered \"ERROR|WARN|Gap|Dropped|Worker|BPF Stats\" > ai_context.log"

echo -e "\n${YELLOW}2. Generate Test Load (In another terminal):${NC}"
echo "   # Create 10 files"
echo "   for i in {1..10}; do echo \"data \$i\" > $SOURCE_MNT/file_\$i.txt; done"
echo "   "
echo "   # Modify a file (trigger versioning)"
echo "   echo \"update\" >> $SOURCE_MNT/file_1.txt"
echo "   "
echo "   # Delete a file (check propagation)"
echo "   rm $SOURCE_MNT/file_5.txt"

echo -e "\n${YELLOW}3. Verify Synchronization:${NC}"
echo "   diff -r $SOURCE_MNT $TARGET_MNT"

echo -e "\n${YELLOW}4. Inspect AI Context Log:${NC}"
echo "   cat ai_context.log"
echo ""
