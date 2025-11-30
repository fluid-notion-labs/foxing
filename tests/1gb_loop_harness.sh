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

cat > "$CONFIG_PATH" <<EOF
# Generated Test Configuration for Foxing (Advanced Loopback)

# --- Global ---
worker_count = 2
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

echo -e "\n${YELLOW}2. Basic Load (File Creation):${NC}"
echo "   for i in {1..5}; do echo \"data \$i\" > $SOURCE_MNT/file_\$i.txt; done"

echo -e "\n${YELLOW}3. Advanced Load (SELinux & Renames):${NC}"
echo "   # Test 1: SELinux Label Preservation"
echo "   chcon -t httpd_sys_content_t $SOURCE_MNT/file_1.txt"
echo "   # Test 2: Atomic Rename Overwrite"
echo "   mv $SOURCE_MNT/file_1.txt $SOURCE_MNT/file_2.txt"
echo "   # Test 3: Rapid Delete (The Zombie Test)"
echo "   touch $SOURCE_MNT/zombie.txt; rm $SOURCE_MNT/zombie.txt"

echo -e "\n${YELLOW}4. Versioning & Reflink Stress Test (Crucial):${NC}"
echo "   # A. Space Efficiency (Reflink Check)"
echo "   # Create a 100MB file. Target usage should jump ~100MB."
echo "   dd if=/dev/urandom of=$SOURCE_MNT/blob.bin bs=1M count=100"
echo "   sync; sleep 2; du -sh $TARGET_MNT"
echo "   # Modify 1 byte. This creates a Version Snapshot."
echo "   echo \"modification\" >> $SOURCE_MNT/blob.bin"
echo "   # If Reflinks work: Usage increases by only ~4KB (Metadata), NOT another 100MB."
echo "   sync; sleep 2; du -sh $TARGET_MNT"
echo "   "
echo "   # B. Version Rotation (Retention Check)"
echo "   # Creates 10 updates. Should only keep last 5 versions in .mirror/.versions"
echo "   for i in {1..10}; do echo \"v\$i\" >> $SOURCE_MNT/rotate.txt; sleep 1.1; done"
echo "   ls -1 $TARGET_MNT/.mirror/.versions/ | grep rotate | wc -l"

echo -e "\n${YELLOW}5. Verify:${NC}"
echo "   # Check Diff"
echo "   diff -r $SOURCE_MNT $TARGET_MNT"
echo "   # Check Metadata (requires getfattr)"
echo "   getfattr -d -m - $TARGET_MNT/file_2.txt"

echo ""
