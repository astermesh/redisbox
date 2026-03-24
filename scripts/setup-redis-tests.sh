#!/usr/bin/env bash
#
# Download the Redis test suite for TCL parity testing.
#
# Usage:
#   ./scripts/setup-redis-tests.sh [redis-version]
#
# The Redis source is cloned into .redis-tests/ (git-ignored).
# Only the tests/ directory is needed; the rest is kept for
# runtest script dependencies.

set -euo pipefail

REDIS_VERSION="${1:-7.4.4}"
TARGET_DIR=".redis-tests"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_DIR"

if [ -d "$TARGET_DIR/tests/test_helper.tcl" ] 2>/dev/null || [ -f "$TARGET_DIR/tests/test_helper.tcl" ]; then
  echo "Redis test suite already exists at $TARGET_DIR/"
  echo "To re-download, remove the directory first: rm -rf $TARGET_DIR"
  exit 0
fi

echo "Downloading Redis $REDIS_VERSION test suite..."

# Clone with minimal depth
if [ -d "$TARGET_DIR" ]; then
  rm -rf "$TARGET_DIR"
fi

git clone \
  --depth 1 \
  --branch "$REDIS_VERSION" \
  --filter=blob:none \
  --sparse \
  "https://github.com/redis/redis.git" \
  "$TARGET_DIR"

cd "$TARGET_DIR"

# Enable sparse checkout for only what we need.
# Use --no-cone because `runtest` is a file, not a directory —
# cone mode (default since git 2.37) rejects non-directory patterns.
git sparse-checkout set --no-cone /tests/ /src/help.h /runtest

cd "$PROJECT_DIR"

echo ""
echo "Redis test suite ready at $TARGET_DIR/"
echo "Run tests with: npm run tcl:test"
