#!/bin/bash
# One-shot discovery of social-media-relevant files in the
# Impressions / Reference / Universe Estimate siblings of
# the year-level adintel directory.
#
# Run interactively on Mercury login node:
#   cd <the dir showing the 4 "* File Formats" subdirs>
#   bash probe_social_siblings.sh > probe_social_out.txt
#
# Output is plain text; review on Mercury or scp back.

set -u

YEAR_DIR="${1:-$PWD}"
echo "=============================================="
echo "Probing social-media siblings under: $YEAR_DIR"
echo "Started: $(date)"
echo "=============================================="

echo
echo "## Top-level subdirs"
ls -la "$YEAR_DIR" || true

# Enumerate the four format folders by glob.
shopt -s nullglob
for d in "$YEAR_DIR"/*"File Formats"; do
    echo
    echo "=============================================="
    echo "## $d"
    echo "=============================================="
    ls "$d"
done

echo
echo "=============================================="
echo "## Searching for social / digital / DGTLV files"
echo "=============================================="
mapfile -t HITS < <(
    find "$YEAR_DIR" \
        \( -iname '*social*' -o -iname '*digital*' \
           -o -iname '*DGTLV*' \) 2>/dev/null
)
for h in "${HITS[@]}"; do
    echo
    echo "----------------------------------------------"
    echo "HIT: $h"
    if [ -d "$h" ]; then
        echo "  (directory, listing)"
        ls -la "$h" | head -40
    else
        sz=$(stat -c '%s' "$h" 2>/dev/null \
             || stat -f '%z' "$h" 2>/dev/null)
        echo "  size: ${sz:-unknown} bytes"
        echo "  -- header (one column per line) --"
        head -1 "$h" | tr '\t' '\n' | nl
        echo "  -- first 3 data rows (truncated 400 chars) --"
        head -4 "$h" | tail -3 | cut -c1-400
        echo "  -- row count --"
        wc -l "$h"
    fi
done

echo
echo "=============================================="
echo "## Reference: anything market / DMA / geo-flavored"
echo "=============================================="
for f in $(find "$YEAR_DIR" -type f \
            \( -iname '*market*' -o -iname '*dma*' \
               -o -iname '*geo*' -o -iname '*zip*' \) \
            2>/dev/null); do
    echo
    echo "----------------------------------------------"
    echo "REF: $f"
    sz=$(stat -c '%s' "$f" 2>/dev/null \
         || stat -f '%z' "$f" 2>/dev/null)
    echo "  size: ${sz:-unknown} bytes"
    echo "  -- header --"
    head -1 "$f" | tr '\t' '\n' | nl
    echo "  -- first 5 data rows --"
    head -6 "$f" | tail -5 | cut -c1-400
    rows=$(wc -l < "$f" 2>/dev/null)
    echo "  -- row count: $rows --"
done

echo
echo "=============================================="
echo "Finished: $(date)"
echo "=============================================="
