#!/usr/bin/env python3
"""
National Spot TV panel for one product category.

Pulls every DMA, every available year, for the brands a
category cares about (PCC subcodes from
shared/categories.py). Output is a collapsed panel:

  (MarketCode, year_month, AdvParentCode, PCCSubCode)
      -> spend_total, n_spots

One CSV per year in the submit dir:
  <category>_dma_month_panel_{year}.csv

Per project policy (taxes_ads/CLAUDE.md):
brand is the only selection filter at extraction time.
Years and DMAs are NEVER pre-filtered.

Handles two Spot TV layouts (lifted from
sb_incidence/.../find_all_dma_sportsbook_spend_v2.py):
  - 2010-2021: single SpotTV.tsv, 23 cols, no header
  - 2022+    : monthly files under
               '/kilts/adintel/{year}/Occurrence Data
                File Formats/Spot TV/', 18 cols

Usage on Mercury:
  source ~/venv/bin/activate
  # Serial across years (one job, many years):
  python3 taxes_ads/shared/mercury/extract_category_panel.py \
      --category SSB --years 2010-2023
  # Parallel via SLURM array (one task per year):
  sbatch --array=2010-2023 \
      --export=CATEGORY=SSB \
      taxes_ads/shared/mercury/run_category_panel.sh
"""

from __future__ import annotations

import argparse
import gc
import glob
import os
import resource
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from shared.categories import get as get_category  # noqa: E402


ADINTEL_ROOT = '/kilts/adintel'
REF_YEAR = '2017'
CHUNKSIZE = 2_000_000  # pandas chunksize (fallback path)
PYARROW_BLOCK_BYTES = 256 * 1024 * 1024  # 256 MB blocks
MEM_LIMIT_GB = 100.0

# Columns we actually need from each occurrence row.
# PyArrow can project to just these and skip the rest.
NEEDED_COLS = [
    'AdDate', 'MarketCode',
    'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
    'Spend', 'Units',
]

# 2010-2021: single SpotTV.tsv, 23 columns.
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
# 2022+: monthly files, 18 columns.
SPOT_TV_COLUMNS_STANDARD = [
    'AdDate', 'AdTime', 'MarketCode', 'MediaTypeID',
    'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
    'DistributorCode', 'Units', 'Spend',
    'TVDaypartCode', 'Duration', 'AdCode', 'CreativeID',
    'Pod', 'PodPosition', 'TimeIntervalNumber',
    'NielsenProgramCode',
]
BRAND_COLS = ['PrimBrandCode', 'ScndBrandCode', 'TerBrandCode']


def log(msg: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def check_memory(label: str = '') -> None:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_gb = (
        rss / (1024 ** 3) if sys.platform == 'darwin'
        else rss / (1024 ** 2)
    )
    print(f"  [mem] {label}: {rss_gb:.2f} GB", flush=True)
    if rss_gb > MEM_LIMIT_GB:
        raise MemoryError(f"RSS {rss_gb:.1f} GB at: {label}")


def load_brand_reference(year: str) -> pd.DataFrame:
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


def candidate_brand_codes(
    brand_lookup: pd.DataFrame,
    pcc_subcodes: set[str],
) -> set:
    if 'PCCSubCode' not in brand_lookup.columns:
        raise RuntimeError(
            "Brand.tsv missing PCCSubCode column; cannot "
            "derive candidates from PCC subcodes."
        )
    sub = brand_lookup[
        brand_lookup['PCCSubCode'].isin(pcc_subcodes)
    ]
    codes = set(sub['BrandCode'].dropna())
    log(
        f"  {len(codes):,} brand codes match PCC subcodes "
        f"{sorted(pcc_subcodes)}"
    )
    return codes


def spot_tv_config(year: str):
    """Return (file_list, column_names) for the year's
    Spot TV occurrences. Two layouts:
      - 2010-2021: single SpotTV.tsv (legacy 23 cols)
      - 2022+    : monthly files under
                   'Occurrence Data File Formats/Spot TV/'
                   (standard 18 cols)
    """
    yi = int(year)
    if yi <= 2021:
        path = os.path.join(
            ADINTEL_ROOT, year, 'Occurrences', 'SpotTV.tsv',
        )
        files = [path] if os.path.exists(path) else []
        return files, SPOT_TV_COLUMNS_LEGACY
    occ_dir = os.path.join(
        ADINTEL_ROOT, year,
        'Occurrence Data File Formats', 'Spot TV',
    )
    files = (
        sorted(glob.glob(os.path.join(occ_dir, '*')))
        if os.path.exists(occ_dir) else []
    )
    return files, SPOT_TV_COLUMNS_STANDARD


def process_year(
    year: str,
    candidate_codes: set,
    brand_lookup: pd.DataFrame,
    category_name: str,
) -> None:
    log(f"\n=== Year {year} ===")
    files, col_names = spot_tv_config(year)
    if not files:
        log(f"  No Spot TV files for {year}; skipping.")
        return
    log(f"  {len(files)} file(s), schema={len(col_names)} cols")

    keep_brand_cols = ['BrandCode', 'AdvParentCode', 'PCCSubCode']
    keep_brand_cols = [
        c for c in keep_brand_cols if c in brand_lookup.columns
    ]
    brand_meta = brand_lookup[keep_brand_cols].copy()

    collapsed_frames: list[pd.DataFrame] = []
    total_rows = 0
    total_matched = 0
    chunk_counter = 0

    # PyArrow CSV: multithreaded parse + column projection.
    # All target cols read as string; we coerce to numeric
    # in pandas after the filter (faster than pyarrow casts
    # on strings with mixed/empty values).
    read_opts = pacsv.ReadOptions(
        column_names=col_names,
        block_size=PYARROW_BLOCK_BYTES,
        use_threads=True,
    )
    parse_opts = pacsv.ParseOptions(
        delimiter='\t',
        invalid_row_handler=lambda _row: 'skip',
    )
    convert_opts = pacsv.ConvertOptions(
        include_columns=NEEDED_COLS,
        column_types={c: pa.string() for c in NEEDED_COLS},
        null_values=['', 'NA', 'NaN'],
        strings_can_be_null=True,
    )

    for fi, fpath in enumerate(files, 1):
        log(f"  [{fi}/{len(files)}] {os.path.basename(fpath)}")
        reader = pacsv.open_csv(
            fpath,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break
            chunk = batch.to_pandas()
            chunk_counter += 1
            total_rows += len(chunk)
            for col in BRAND_COLS:
                chunk[col] = pd.to_numeric(
                    chunk[col], errors='coerce',
                ).astype('Int64')
            mask = (
                chunk['PrimBrandCode'].isin(candidate_codes)
                | chunk['ScndBrandCode'].isin(candidate_codes)
                | chunk['TerBrandCode'].isin(candidate_codes)
            )
            if not mask.any():
                continue

            matched = chunk.loc[
                mask,
                ['AdDate', 'MarketCode',
                 'PrimBrandCode', 'Spend', 'Units'],
            ].copy()
            total_matched += len(matched)
            matched['Spend'] = pd.to_numeric(
                matched['Spend'], errors='coerce',
            ).fillna(0)
            matched['Units'] = pd.to_numeric(
                matched['Units'], errors='coerce',
            ).fillna(0)
            matched['AdDate'] = pd.to_datetime(
                matched['AdDate'], errors='coerce',
            )
            matched = matched.dropna(subset=['AdDate'])
            matched['year_month'] = (
                matched['AdDate']
                .dt.to_period('M').astype(str)
            )
            matched = matched.merge(
                brand_meta,
                left_on='PrimBrandCode',
                right_on='BrandCode',
                how='left',
            )
            agg = (
                matched
                .groupby(
                    ['MarketCode', 'year_month',
                     'AdvParentCode', 'PCCSubCode'],
                    dropna=False,
                )
                .agg(spend_total=('Spend', 'sum'),
                     units_total=('Units', 'sum'),
                     n_spots=('Spend', 'size'))
                .reset_index()
            )
            collapsed_frames.append(agg)

            if chunk_counter % 10 == 0:
                log(
                    f"    chunks={chunk_counter} "
                    f"scanned={total_rows:,} "
                    f"matched={total_matched:,} "
                    f"agg_rows_so_far="
                    f"{sum(len(f) for f in collapsed_frames):,}"
                )
                gc.collect()
                check_memory(
                    f"y={year} chunks={chunk_counter}"
                )

    if not collapsed_frames:
        log(f"  No matches for {year}.")
        return

    combined = pd.concat(collapsed_frames, ignore_index=True)
    del collapsed_frames
    gc.collect()

    final = (
        combined
        .groupby(
            ['MarketCode', 'year_month',
             'AdvParentCode', 'PCCSubCode'],
            dropna=False,
        )
        .agg(spend_total=('spend_total', 'sum'),
             units_total=('units_total', 'sum'),
             n_spots=('n_spots', 'sum'))
        .reset_index()
        .sort_values(
            ['year_month', 'MarketCode',
             'AdvParentCode', 'PCCSubCode']
        )
    )

    out_path = (
        f"{category_name.lower()}_dma_month_panel_{year}.csv"
    )
    final.to_csv(out_path, index=False, float_format='%.2f')
    log(
        f"  Wrote {out_path}: {len(final):,} rows, "
        f"total spend ${final['spend_total'].sum():,.0f}, "
        f"DMAs={final['MarketCode'].nunique()}"
    )
    del combined, final
    gc.collect()


def parse_years(spec: str) -> list[str]:
    out: list[str] = []
    for piece in spec.split(','):
        piece = piece.strip()
        if '-' in piece:
            lo, hi = piece.split('-', 1)
            out.extend(
                str(y) for y in range(int(lo), int(hi) + 1)
            )
        else:
            out.append(piece)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--category', required=True)
    ap.add_argument(
        '--years', default='2010-2018',
        help='comma list or range, e.g. 2014-2018 or '
             '2014,2015,2017',
    )
    args = ap.parse_args()

    log(f"Extraction start. category={args.category} "
        f"years={args.years}")

    cat = get_category(args.category)
    pcc = set(cat.pcc_subcodes_treated) \
        | set(cat.pcc_subcodes_placebo)
    if not pcc:
        raise RuntimeError(
            f"Category {cat.name} has no PCC subcodes "
            f"populated. Add them to shared/categories.py "
            f"before running."
        )
    log(f"  PCC subcodes: {sorted(pcc)}")

    brand_lookup = load_brand_reference(REF_YEAR)
    candidate_codes = candidate_brand_codes(brand_lookup, pcc)
    if not candidate_codes:
        raise RuntimeError(
            "No brand codes match the PCC subcodes; "
            "check Brand.tsv schema."
        )
    check_memory("after references")

    years = parse_years(args.years)
    log(f"  years to process: {years}")
    for year in years:
        try:
            process_year(
                year, candidate_codes, brand_lookup,
                cat.name,
            )
        except Exception as exc:
            log(f"  [ERROR year {year}]: {exc}")

    log("Extraction done.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
