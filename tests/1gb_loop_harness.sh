#!/bin/bash
set -e

# ==============================================================================
# Foxing Test Environment Setup
# Creates loopback XFS filesystems to simulate Source and Target drives.
# ==============================================================================

BASE_DIR="/tmp/foxing_test"
SOURCE_IMG="$BASE_DIR/source.img"
TARGET_IMG="$BASE_DIR/target.img"
SOURCE_MNT="$BASE_DIR/mnt/source"
TARGET_MNT="$BASE_DIR/mnt/target"
IMG_SIZE="512M" # Small size for quick testing

# Colors
GREEN='\033[0;32m'
NC='\033[0m' # No Color

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root (needed for mount/mkfs operations)"
  exit 1
fi

echo -e "${GREEN}==> Cleaning up previous runs...${NC}"
# Unmount if exists
if mountpoint -q "$SOURCE_MNT"; then umount "$SOURCE_MNT"; fi
if mountpoint -q "$TARGET_MNT"; then umount "$TARGET_MNT"; fi
rm -rf "$BASE_DIR"

echo -e "${GREEN}==> Creating infrastructure at $BASE_DIR...${NC}"
mkdir -p "$SOURCE_MNT"
mkdir -p "$TARGET_MNT"

# Create sparse files
truncate -s $IMG_SIZE "$SOURCE_IMG"
truncate -s $IMG_SIZE "$TARGET_IMG"

# Format as XFS (Supports reflinks for versioning)
echo -e "${GREEN}==> Formatting XFS filesystems...${NC}"
if ! command -v mkfs.xfs &> /dev/null; then
    echo "mkfs.xfs could not be found. Please install xfsprogs (dnf install xfsprogs)."
    exit 1
fi

# -f force overwrite, -m reflink=1 enables CoW versioning support
mkfs.xfs -f -m reflink=1 "$SOURCE_IMG" > /dev/null
mkfs.xfs -f -m reflink=1 "$TARGET_IMG" > /dev/null

# Mount loopback
echo -e "${GREEN}==> Mounting loopback devices...${NC}"
mount -o loop "$SOURCE_IMG" "$SOURCE_MNT"
mount -o loop "$TARGET_IMG" "$TARGET_MNT"

# Permissions: Allow current user (assuming sudo usage) to write
# We find the SUDO_USER if available, else root
REAL_USER=${SUDO_USER:-$(whoami)}
chown -R "$REAL_USER:$REAL_USER" "$SOURCE_MNT"
chown -R "$REAL_USER:$REAL_USER" "$TARGET_MNT"

# Generate Config File
CONFIG_PATH="$(pwd)/test_config.toml"
echo -e "${GREEN}==> Generating configuration: $CONFIG_PATH${NC}"

# Note: We use \\\\ to ensure the TOML file receives \\ (which TOML parses as a single regex backslash)
cat > "$CONFIG_PATH" <<EOF
# Generated Test Configuration for Foxing
worker_count = 2
queue_max = 50000
metrics_port = 9101
global_buffer_limit = 5242880 # 5MB limit for small test
max_system_load_avg = 8.0     # High threshold to prevent throttling during test
hydration_delay_ms = 5        # Fast scan

[[sources]]
path = "$SOURCE_MNT"

  [[sources.targets]]
  path = "$TARGET_MNT"
  profile = "SSD"             # Assume fast loopback
  initial_sync = true
  
  # Latency tuning
  autotune_target_latency_ms = 20
  
  # Versioning (MARS)
  enable_versioning = true
  max_versions = 5
  max_versions_size_mb = 100
  
  # Ensure we support reflinks
  supports_reflink = true

  # Ignore temp files generated during manual testing
  excludes = [
      ".*/ignore_me/.*",
      ".*\\\\.tmp$"
  ]
EOF

echo -e "${GREEN}==> Setup Complete!${NC}"
echo "Source: $SOURCE_MNT"
echo "Target: $TARGET_MNT"
echo ""
echo "To run the daemon:"
echo "  sudo ./target/release/foxing daemon --config test_config.toml"
echo ""
echo "To generate load:"
echo "  bash -c 'for i in {1..100}; do echo \"data \$i\" > $SOURCE_MNT/file_\$i.txt; done'"
