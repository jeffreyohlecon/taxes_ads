#!/bin/bash
# For every Occurrences subdirectory under
# /kilts/adintel/2022, find the Spend column and sum it
# over one month. Print total spend per channel sorted
# descending — answers "which channels are big enough to
# matter for a precision boost?"
#
# Run on Mercury login node:
#   bash probe_spend_by_media_type.sh > probe_spend_out.txt

set -u
YEAR_DIR="${1:-/kilts/adintel/2022}"
MONTH="${2:-01}"
OCC_ROOT="$YEAR_DIR/Occurrence Data File Formats"

echo "=============================================="
echo "Spend-by-media-type probe under $OCC_ROOT"
echo "Sampling month: $MONTH"
echo "Started: $(date)"
echo "=============================================="

# Collect (channel, spend, rows, geo_unique) into a temp
# file for sorting at the end.
TMP=$(mktemp)
trap "rm -f $TMP" EXIT

for subdir in "$OCC_ROOT"/*/; do
    name=$(basename "$subdir")

    # Take the file matching the target month if one
    # exists, else the first file.
    sample=$(find "$subdir" -maxdepth 1 -type f \
                 ! -name '.*' -name "*_${MONTH}_*" \
                 2>/dev/null | sort | head -1)
    if [ -z "$sample" ]; then
        sample=$(find "$subdir" -maxdepth 1 -type f \
                     ! -name '.*' 2>/dev/null \
                     | sort | head -1)
    fi
    if [ -z "$sample" ]; then continue; fi

    header=$(head -1 "$sample" 2>/dev/null)
    if [ -z "$header" ]; then continue; fi

    # Spend column index (1-based).
    spend_idx=$(echo "$header" | tr '\t' '\n' | nl -ba \
        | awk -F'\t' 'BEGIN{IGNORECASE=1}
            $2 ~ /^(Spend|SpendDollars|DollarSpend|GrossSpend)$/ \
            { gsub(/ /,"",$1); print $1; exit }')

    # MarketCode column index (for geo classification).
    mkt_idx=$(echo "$header" | tr '\t' '\n' | nl -ba \
        | awk -F'\t' 'BEGIN{IGNORECASE=1}
            $2 ~ /^(MarketCode|DMACode|MktCode)$/ \
            { gsub(/ /,"",$1); print $1; exit }')

    if [ -z "$spend_idx" ]; then
        echo "## $name: no Spend column (header has: $header | head -200 chars)" >&2
        continue
    fi

    # Sum spend + count rows + count unique markets.
    # Use awk so we do it in one pass.
    awk -F'\t' -v sp="$spend_idx" -v mk="$mkt_idx" \
        -v name="$name" -v out="$TMP" '
        NR==1 { next }
        {
            s = $sp + 0
            spend += s
            rows += 1
            if (mk != "") mkts[$mk] = 1
        }
        END {
            n_mkt = 0
            for (k in mkts) n_mkt++
            printf "%-32s\t%15.0f\t%12d\t%6d\n",
                   name, spend, rows, n_mkt >> out
        }
    ' "$sample"
done

echo
echo "Channel                            Spend (USD)        Rows  #DMAs"
echo "----------------------------------------------------------------"
sort -k2 -t $'\t' -rn "$TMP"

echo
echo "=============================================="
echo "Finished: $(date)"
echo "=============================================="
