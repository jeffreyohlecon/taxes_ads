#!/bin/bash
# For every Occurrences subdirectory under
# /kilts/adintel/2022, find the MarketCode column by
# header position, then summarize its distribution.
#
# Catches all media types in one pass: TV / radio /
# newspaper / outdoor / cinema / streaming / digital /
# magazine. Tells us which channels carry DMA-level
# geographic variation in Nielsen Ad Intel.
#
# Run on Mercury login node:
#   bash probe_geo_by_media_type.sh > probe_geo_out.txt

set -u
YEAR_DIR="${1:-/kilts/adintel/2022}"
OCC_ROOT="$YEAR_DIR/Occurrence Data File Formats"

echo "=============================================="
echo "Geo-by-media-type probe under $OCC_ROOT"
echo "Started: $(date)"
echo "=============================================="

for subdir in "$OCC_ROOT"/*/; do
    name=$(basename "$subdir")
    echo
    echo "=============================================="
    echo "## $name"
    echo "=============================================="

    # Take the first regular file in this subdir as sample.
    sample=$(find "$subdir" -maxdepth 1 -type f \
                 ! -name '.*' 2>/dev/null | sort | head -1)
    if [ -z "$sample" ]; then
        echo "  (no files)"
        continue
    fi
    sz=$(stat -c '%s' "$sample" 2>/dev/null \
         || stat -f '%z' "$sample" 2>/dev/null)
    echo "Sample file: $(basename "$sample")  (${sz} bytes)"

    # Find MarketCode column index (1-based). Try a few
    # plausible names in case schema varies (DMACode,
    # MktCode, etc.).
    header=$(head -1 "$sample" 2>/dev/null)
    if [ -z "$header" ]; then
        echo "  [empty header / unreadable]"
        continue
    fi

    col_idx=$(echo "$header" | tr '\t' '\n' | nl -ba \
        | awk -F'\t' 'BEGIN{IGNORECASE=1}
            $2 ~ /^(MarketCode|DMACode|MktCode)$/ \
            { gsub(/ /,"",$1); print $1; exit }')

    if [ -z "$col_idx" ]; then
        echo "  -- no MarketCode-like column in header --"
        echo "  header columns:"
        echo "$header" | tr '\t' '\n' | nl | head -25
        continue
    fi
    echo "MarketCode column index: $col_idx"

    # Quick row count (approx, just to know magnitude).
    nrows=$(wc -l < "$sample")
    echo "Row count: $nrows"

    # Distribution: top 10 + total unique count. Use the
    # first 1M rows for speed if file is huge.
    echo "-- top 10 MarketCode values (sampled <=1M rows) --"
    head -1000001 "$sample" | tail -n +2 \
        | cut -f"$col_idx" \
        | sort | uniq -c | sort -rn | head -10

    echo "-- unique MarketCode count (sampled <=1M rows) --"
    head -1000001 "$sample" | tail -n +2 \
        | cut -f"$col_idx" \
        | sort -u | wc -l
done

echo
echo "=============================================="
echo "Finished: $(date)"
echo "=============================================="
