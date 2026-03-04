#!/usr/bin/env bash
# verify-sync.sh — Verify source↔target sync correctness
# Usage: bash verify-sync.sh [source_dir] [target_dir] [label]
set -euo pipefail

SOURCE="${1:-/mnt/source}"
TARGET="${2:-/mnt/target-nfs}"
LABEL="${3:-verify}"
REPORT_DIR="${4:-/tmp/adversarial-results}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
VERIFY_FILE="$REPORT_DIR/verify_${LABEL}_${TIMESTAMP}.txt"

mkdir -p "$REPORT_DIR"

echo "=== Sync Verification: $LABEL ==="
echo "Source: $SOURCE"
echo "Target: $TARGET"
echo "Time:   $(date -Iseconds)"
echo ""

PASS=true
WARNINGS=""

# --- 1. File listing comparison ---
echo "--- File Listing ---"
SRC_LIST=$(mktemp)
TGT_LIST=$(mktemp)
# Exclude .foxing sidecar directories from comparison
(cd "$SOURCE" && find . -type f ! -path './.foxing*' | sort) > "$SRC_LIST"
(cd "$TARGET" && find . -type f ! -path './.foxing*' | sort) > "$TGT_LIST"

SRC_COUNT=$(wc -l < "$SRC_LIST")
TGT_COUNT=$(wc -l < "$TGT_LIST")
echo "Source files: $SRC_COUNT"
echo "Target files: $TGT_COUNT"

MISSING_ON_TARGET=$(comm -23 "$SRC_LIST" "$TGT_LIST" | head -50)
EXTRA_ON_TARGET=$(comm -13 "$SRC_LIST" "$TGT_LIST" | head -50)

MISSING_COUNT=$(comm -23 "$SRC_LIST" "$TGT_LIST" | wc -l)
EXTRA_COUNT=$(comm -13 "$SRC_LIST" "$TGT_LIST" | wc -l)

if [[ $MISSING_COUNT -gt 0 ]]; then
    echo "FAIL: $MISSING_COUNT files missing on target"
    echo "$MISSING_ON_TARGET" | head -20 | sed 's/^/  /'
    [[ $MISSING_COUNT -gt 20 ]] && echo "  ... and $((MISSING_COUNT - 20)) more"
    PASS=false
else
    echo "OK: All source files present on target"
fi

if [[ $EXTRA_COUNT -gt 0 ]]; then
    echo "WARN: $EXTRA_COUNT extra files on target (may be stale)"
    echo "$EXTRA_ON_TARGET" | head -10 | sed 's/^/  /'
    WARNINGS="${WARNINGS}extra_files=$EXTRA_COUNT "
fi

# --- 2. Directory structure comparison ---
echo ""
echo "--- Directory Structure ---"
SRC_DIRS=$(mktemp)
TGT_DIRS=$(mktemp)
(cd "$SOURCE" && find . -type d ! -path './.foxing*' | sort) > "$SRC_DIRS"
(cd "$TARGET" && find . -type d ! -path './.foxing*' | sort) > "$TGT_DIRS"

MISSING_DIRS=$(comm -23 "$SRC_DIRS" "$TGT_DIRS" | wc -l)
if [[ $MISSING_DIRS -gt 0 ]]; then
    echo "FAIL: $MISSING_DIRS directories missing on target"
    comm -23 "$SRC_DIRS" "$TGT_DIRS" | head -10 | sed 's/^/  /'
    PASS=false
else
    echo "OK: Directory structure matches"
fi
rm -f "$SRC_DIRS" "$TGT_DIRS"

# --- 3. Content hash verification (SHA-256) ---
echo ""
echo "--- Content Hash Verification ---"
SRC_HASHES=$(mktemp)
TGT_HASHES=$(mktemp)

# Only hash files that exist on both sides
COMMON_FILES=$(comm -12 "$SRC_LIST" "$TGT_LIST")
COMMON_COUNT=$(echo "$COMMON_FILES" | grep -c '.' || true)

if [[ $COMMON_COUNT -eq 0 ]]; then
    echo "SKIP: No common files to hash"
else
    echo "Hashing $COMMON_COUNT common files..."

    # Hash in parallel batches for speed
    (cd "$SOURCE" && echo "$COMMON_FILES" | xargs -P4 -I{} sha256sum {} 2>/dev/null | sort) > "$SRC_HASHES"
    (cd "$TARGET" && echo "$COMMON_FILES" | xargs -P4 -I{} sha256sum {} 2>/dev/null | sort) > "$TGT_HASHES"

    HASH_DIFF=$(diff "$SRC_HASHES" "$TGT_HASHES" || true)
    if [[ -z "$HASH_DIFF" ]]; then
        echo "OK: All $COMMON_COUNT files match (SHA-256)"
    else
        MISMATCH_COUNT=$(diff "$SRC_HASHES" "$TGT_HASHES" | grep '^<' | wc -l)
        echo "FAIL: $MISMATCH_COUNT files have hash mismatches"
        diff "$SRC_HASHES" "$TGT_HASHES" | head -20 | sed 's/^/  /'
        PASS=false
    fi
fi

rm -f "$SRC_LIST" "$TGT_LIST" "$SRC_HASHES" "$TGT_HASHES"

# --- 4. Size comparison for common files ---
echo ""
echo "--- Size Check ---"
SIZE_MISMATCHES=0
while IFS= read -r f; do
    [[ -z "$f" ]] && continue
    SRC_SIZE=$(stat -c '%s' "$SOURCE/$f" 2>/dev/null || echo "0")
    TGT_SIZE=$(stat -c '%s' "$TARGET/$f" 2>/dev/null || echo "0")
    if [[ "$SRC_SIZE" != "$TGT_SIZE" ]]; then
        SIZE_MISMATCHES=$((SIZE_MISMATCHES + 1))
        if [[ $SIZE_MISMATCHES -le 10 ]]; then
            echo "  MISMATCH: $f (source=$SRC_SIZE, target=$TGT_SIZE)"
        fi
    fi
done <<< "$COMMON_FILES"

if [[ $SIZE_MISMATCHES -gt 0 ]]; then
    echo "FAIL: $SIZE_MISMATCHES files have size mismatches"
    PASS=false
else
    echo "OK: File sizes match"
fi

# --- Summary ---
echo ""
echo "================================="
if $PASS; then
    echo "RESULT: PASS"
    RESULT="PASS"
else
    echo "RESULT: FAIL"
    RESULT="FAIL"
fi
[[ -n "$WARNINGS" ]] && echo "WARNINGS: $WARNINGS"
echo "================================="

# Write structured result
cat > "$VERIFY_FILE" << VERIFYEOF
label=$LABEL
timestamp=$TIMESTAMP
result=$RESULT
source=$SOURCE
target=$TARGET
source_files=$SRC_COUNT
target_files=$TGT_COUNT
missing_on_target=$MISSING_COUNT
extra_on_target=$EXTRA_COUNT
missing_dirs=${MISSING_DIRS:-0}
hash_mismatches=${MISMATCH_COUNT:-0}
size_mismatches=$SIZE_MISMATCHES
common_files=$COMMON_COUNT
warnings=$WARNINGS
VERIFYEOF

echo "Verification report: $VERIFY_FILE"

# Exit with appropriate code
$PASS
