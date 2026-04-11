#!/usr/bin/env bash
# check_no_runtime_ddl.sh
#
# Fails the build if any Python source file outside of migrations/ issues
# bare CREATE TABLE or ALTER TABLE statements at runtime.
# Schema changes must go through Wrangler migration files under migrations/.
#
# Usage:  bash scripts/check_no_runtime_ddl.sh
# Exit 0 → clean; Exit 1 → violations found.

set -euo pipefail

SEARCH_DIRS=("src")
VIOLATION_PATTERN='(CREATE TABLE IF NOT EXISTS|ALTER TABLE)\s'
IGNORE_COMMENT_PATTERN='^\s*#'

found=0

for dir in "${SEARCH_DIRS[@]}"; do
    while IFS= read -r -d '' file; do
        while IFS= read -r line; do
            # Skip pure comment lines
            if echo "$line" | grep -qP "$IGNORE_COMMENT_PATTERN"; then
                continue
            fi
            if echo "$line" | grep -qP "$VIOLATION_PATTERN"; then
                echo "VIOLATION in $file: $line"
                found=1
            fi
        done < "$file"
    done < <(find "$dir" -name "*.py" -print0)
done

if [ "$found" -eq 1 ]; then
    echo ""
    echo "ERROR: Runtime DDL detected in source files."
    echo "Move all CREATE TABLE / ALTER TABLE statements into"
    echo "versioned SQL files under migrations/ and apply via:"
    echo "  wrangler d1 migrations apply LEADERBOARD_DB"
    exit 1
fi

echo "OK: No runtime DDL found in source files."
exit 0
