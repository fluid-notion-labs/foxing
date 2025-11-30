#!/bin/bash
set -e

# ==============================================================================
# Foxing: High-Performance Loopback Test Harness (Advanced)
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

cat > "$CONFIG_PATH" <<EOF
# Generated Test Configuration for Foxing (Advanced Loopback)

# --- Global ---
worker_count = 4
queue_max = 200000
metrics_port = 9101
global_buffer_limit = 5242880 # 5MB limit for small test
fatal_metrics_bind = false

# --- Governor (Relaxed for Test) ---
max_system_load_avg = 50.0 
hydration_delay_ms = 1

# --- Loopback Safety ---
capacity_threshold_mb = 50

# --- Response Tuning ---
# 100ms Commit for near-realtime behavior in tests
force_flush_interval_secs = 1

[[sources]]
path = "$SOURCE_MNT"

  [[sources.targets]]
  path = "$TARGET_MNT"
  profile = "SSD" 
  initial_sync = true
  
  # Aggressive BBR tuning
  autotune_target_latency_ms = 50
  
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
echo -e "${BLUE}                       ADVANCED TEST COMMANDS                                 ${NC}"
echo -e "${BLUE}==============================================================================${NC}"

echo -e "\n${YELLOW}1. Run Daemon (Log Filtering):${NC}"
echo "   RUST_LOG=foxing=debug,warn ./target/release/foxing daemon --config test_config.toml 2>&1 | tee full_log.txt | grep -E --line-buffered \"ERROR|WARN|Gap|Dropped|Worker|BPF Stats\" > ai_context.log"

echo -e "\n${YELLOW}2. Torture Test (Rapid Churn):${NC}"
echo "   # Create 100 small files"
echo "   for i in {1..100}; do echo \"torture \$i\" > $SOURCE_MNT/f_\$i.txt; done"
echo "   sleep 2"
echo "   # Delete 50 of them"
echo "   for i in {1..50}; do rm $SOURCE_MNT/f_\$i.txt; done"
echo "   sync"
echo "   # Count Target (Should be 50)"
echo "   ls $TARGET_MNT/f_*.txt | wc -l"

echo -e "\n${YELLOW}3. Metadata & Hierarchy:${NC}"
echo "   mkdir -p $SOURCE_MNT/deep/nested/dir"
echo "   touch $SOURCE_MNT/deep/nested/dir/secret.dat"
echo "   chmod 700 $SOURCE_MNT/deep/nested/dir/secret.dat"
echo "   sleep 2"
echo "   # Verify permissions on target"
echo "   stat -c '%a' $TARGET_MNT/deep/nested/dir/secret.dat"

echo -e "\n${YELLOW}4. Large File Reflink Check:${NC}"
echo "   dd if=/dev/urandom of=$SOURCE_MNT/large.bin bs=1M count=50"
echo "   sync; sleep 2; ls -lh $TARGET_MNT/large.bin"
echo "   # Modify tail"
echo "   echo \"append\" >> $SOURCE_MNT/large.bin"

echo -e "\n${YELLOW}5. Rename Atomicity:${NC}"
echo "   echo \"Atomic Content\" > $SOURCE_MNT/atomic_src.txt"
echo "   mv $SOURCE_MNT/atomic_src.txt $SOURCE_MNT/atomic_dest.txt"
echo "   sleep 1"
echo "   # Target should contain atomic_dest.txt with 'Atomic Content'"
echo "   cat $TARGET_MNT/atomic_dest.txt"

echo ""
