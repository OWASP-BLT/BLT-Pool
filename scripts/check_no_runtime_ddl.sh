#!/usr/bin/env bash
# check_no_runtime_ddl.sh
#
# Fails the build if any Python source file issues bare CREATE TABLE or
# ALTER TABLE statements at runtime.
#
# Usage:  bash scripts/check_no_runtime_ddl.sh
# Exit 0 → clean; Exit 1 → violations found.

set -euo pipefail

SEARCH_DIRS=("src")

found=0

for dir in "${SEARCH_DIRS[@]}"; do
    while IFS= read -r -d '' file; do
        while IFS= read -r line; do
            # Skip pure Python comment lines
            if echo "$line" | grep -qP '^\s*#'; then
                continue
            fi

            # Only flag lines where DDL appears inside a string literal
            if echo "$line" | grep -qP '(CREATE TABLE IF NOT EXISTS|ALTER TABLE)\s+\w'; then
                if echo "$line" | grep -qP "['\""]"; then
                    echo "VIOLATION in $file: $line"
                    found=1
                fi
            fi
        done < "$file"
    done < <(find "$dir" -name "*.py" -print0)
done

if [ "$found" -eq 1 ]; then
    echo ""
    echo "ERROR: Runtime DDL detected in source files."
    echo "Move all CREATE TABLE / ALTER TABLE statements into"
    echo "versioned SQL files under migrations/ and apply via:"
    echo "  bash scripts/run-migrations.sh"
    exit 1
fi

echo "OK: No runtime DDL found in source files."
exit 0
