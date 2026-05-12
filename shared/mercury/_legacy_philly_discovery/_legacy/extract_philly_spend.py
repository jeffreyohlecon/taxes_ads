#!/usr/bin/env python3
"""
Extract Philadelphia DMA Spot TV ad spend for sweetened-
beverage candidate brands, 2014-2018.

Mirrors the sportsbook v2 extractor in
code/data_pipeline/ads_elasticity_aggregation/.

Filters:
  - MarketCode == 104 (PHILADELPHIA)
  - Any of PrimBrandCode/ScndBrandCode/TerBrandCode in
    candidate brand codes (from soda_brand_candidates.csv)

Years 2014-2018 use the legacy 23-column SpotTV.tsv layout
(single file at /kilts/adintel/{year}/Occurrences/SpotTV.tsv).
2018 also has the legacy file; the v2 sportsbook script
already proved this format works.

Output (in submit dir):
  philly_soda_dma_spend_{year}.csv
    Disaggregated occurrence rows. Columns include
    AdDate, BrandCode triplet, Spend, plus joined
    BrandDesc / AdvParentCode / AdvParentDesc / PCCSubCode.
"""

import gc
import os
import resource
import sys
from datetime import datetime

import pandas as pd


# --- Configuration ---
TREATED_MARKET_CODE = '104'  # PHILADELPHIA
YEARS_TO_PROCESS = ['2014', '2015', '2016', '2017', '2018']
REF_YEAR = '2017'
CANDIDATE_CSV_NAME = 'soda_brand_candidates.csv'
OUTPUT_PREFIX = 'philly_soda_dma_spend'
CHUNKSIZE = 2_000_000
MEM_LIMIT_GB = 100.0

ADINTEL_ROOT = '/kilts/adintel'

# Pre-2018 / 2018 legacy single-file format. 23 columns.
SPOT_TV_COLUMNS_LEGACY = [
    'AdDate', 'AdTime', 'MarketCode', 'MediaTypeID',
    'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
    'DistributorCode_Str', 'Units', 'TVDaypartCode',
    'Duration', 'AdCode', 'CreativeID', 'Pod',
    'PodPosition', 'PeriodYearMonth', 'DistributorID',
    'DayOfWeek', 'TimeIntervalNumber',
    'MonitorPlusProgramCode', 'Spend',
    'RegionalIndicator',
    'UC_dim_Bridge_occ_ImpSpotTV_key',
]

BRAND_COLS = [
    'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
]


def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def check_memory(label=""):
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == 'darwin':
        rss_gb = rss / (1024 ** 3)
    else:
        rss_gb = rss / (1024 ** 2)
    print(f"  [mem] {label}: {rss_gb:.2f} GB", flush=True)
    if rss_gb > MEM_LIMIT_GB:
        raise MemoryError(f"RSS {rss_gb:.1f} GB at: {label}")


def load_candidates(csv_path):
    log(f"Loading candidates {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)
    df['BrandCode'] = pd.to_numeric(
        df['BrandCode'], errors='coerce',
    ).astype('Int64')
    df = df.dropna(subset=['BrandCode']).copy()
    codes = set(df['BrandCode'])
    log(f"  {len(codes):,} unique candidate brand codes.")
    return df, codes


def load_brand_reference(year):
    path = os.path.join(
        ADINTEL_ROOT, year, 'References', 'Brand.tsv',
    )
    log(f"Loading Brand.tsv {path}")
    df = pd.read_csv(path, sep='\t', dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df['BrandCode'] = pd.to_numeric(
        df['BrandCode'], errors='coerce',
    ).astype('Int64')
    return df


def spot_tv_path(year):
    legacy = os.path.join(
        ADINTEL_ROOT, year, 'Occurrences', 'SpotTV.tsv',
    )
    if os.path.exists(legacy):
        return legacy
    raise FileNotFoundError(
        f"No legacy SpotTV.tsv for {year}; this script "
        f"only handles the 2014-2018 layout."
    )


def process_year(year, candidate_codes, brand_lookup):
    log(f"\n=== Year {year} ===")
    spath = spot_tv_path(year)
    log(f"  reading {spath}")

    reader = pd.read_csv(
        spath, sep='\t', header=None,
        names=SPOT_TV_COLUMNS_LEGACY,
        chunksize=CHUNKSIZE,
        on_bad_lines='warn',
        dtype={'MarketCode': str},
    )

    matches = []
    total_rows = 0
    for ci, chunk in enumerate(reader, 1):
        total_rows += len(chunk)
        # MarketCode filter first (cheap, ~1/210 of rows).
        chunk = chunk.loc[
            chunk['MarketCode'] == TREATED_MARKET_CODE
        ]
        if chunk.empty:
            continue
        for col in BRAND_COLS:
            chunk[col] = pd.to_numeric(
                chunk[col], errors='coerce',
            ).astype('Int64')
        mask = (
            chunk['PrimBrandCode'].isin(candidate_codes)
            | chunk['ScndBrandCode'].isin(candidate_codes)
            | chunk['TerBrandCode'].isin(candidate_codes)
        )
        if mask.any():
            matches.append(chunk.loc[mask].copy())

        if ci % 10 == 0:
            log(
                f"  chunk {ci}: {total_rows:,} rows scanned, "
                f"{sum(len(m) for m in matches):,} matched"
            )
            gc.collect()
            check_memory(f"y={year} chunk={ci}")

    if not matches:
        log(f"  No matches for {year}.")
        return

    combined = pd.concat(matches, ignore_index=True)
    del matches
    gc.collect()
    log(f"  {len(combined):,} matched rows.")

    # Convert Spend to numeric and join brand metadata on
    # PrimBrandCode (the dominant brand for the spot).
    combined['Spend'] = pd.to_numeric(
        combined['Spend'], errors='coerce',
    ).fillna(0)

    keep = [
        'BrandCode', 'BrandDesc', 'AdvParentCode',
        'PCCSubCode',
    ]
    keep = [c for c in keep if c in brand_lookup.columns]
    combined = combined.merge(
        brand_lookup[keep],
        left_on='PrimBrandCode', right_on='BrandCode',
        how='left',
    )

    out_cols = [
        'AdDate', 'AdTime', 'MarketCode',
        'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
        'BrandDesc', 'AdvParentCode', 'PCCSubCode',
        'Spend', 'Duration', 'Units',
        'TVDaypartCode', 'CreativeID', 'AdCode',
    ]
    out_cols = [c for c in out_cols if c in combined.columns]

    out_path = f"{OUTPUT_PREFIX}_{year}.csv"
    combined[out_cols].to_csv(
        out_path, index=False, float_format='%.2f',
    )
    log(f"  Wrote {out_path} ({len(combined):,} rows).")
    del combined
    gc.collect()


def main():
    log("Extraction start.")

    submit_dir = os.environ.get('SLURM_SUBMIT_DIR', os.getcwd())
    cand_path = os.path.join(submit_dir, CANDIDATE_CSV_NAME)
    if not os.path.isfile(cand_path):
        # Fallback: look in script dir.
        alt = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            CANDIDATE_CSV_NAME,
        )
        if os.path.isfile(alt):
            cand_path = alt
        else:
            raise FileNotFoundError(
                f"{CANDIDATE_CSV_NAME} not found in "
                f"{submit_dir} or script dir."
            )
    _, candidate_codes = load_candidates(cand_path)
    brand_lookup = load_brand_reference(REF_YEAR)
    check_memory("after references")

    for year in YEARS_TO_PROCESS:
        try:
            process_year(year, candidate_codes, brand_lookup)
        except Exception as exc:
            log(f"  [ERROR year {year}]: {exc}")

    log("Extraction done.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
