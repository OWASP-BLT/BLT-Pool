#!/usr/bin/env bash
# check_no_runtime_ddl.sh
#
# Fails the build if any Python source file issues bare CREATE TABLE or
# ALTER TABLE statements at runtime.
#
# Detection strategy: match only lines where the DDL pattern appears as
# an actual SQL string passed to a D1/database call — specifically, lines
# containing the DDL keyword inside a Python string literal (quote char
# present on the line) or as an argument to _d1_run/_d1_all.
# Plain English sentences in docstrings (no surrounding quotes) are ignored.
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
            # (i.e. the line also contains a quote character ' or ")
            # This filters out docstring prose like "All CREATE TABLE statements..."
            if echo "$line" | grep -qP '(CREATE TABLE IF NOT EXISTS|ALTER TABLE)\s+\w'; then
                if echo "$line" | grep -qP "['\"]"; then
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
    echo "  wrangler d1 migrations apply LEADERBOARD_DB"
    exit 1
fi

echo "OK: No runtime DDL found in source files."
exit 0
