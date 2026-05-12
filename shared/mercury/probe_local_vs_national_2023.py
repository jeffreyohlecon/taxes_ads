#!/usr/bin/env python3
"""
One-shot magnitude probe: for SSB and BEER advertisers in
2023, what fraction of TV ad spend is local Spot TV vs the
various national TV channels?

Filter: brand codes whose PCCSubCode is in
{F221, F222, F223 (SSB), F310 (beer)}.

Media types scanned (2023 standard layout):
  Spot TV
  Network TV
  Cable TV
  Syndicated TV
  Spanish Language Network TV
  Spanish Language Cable TV

Excluded by design (would double-count national buys):
  Network Clearance Spot TV
  Syndicated Clearance Spot TV

Output (single CSV, written next to submit dir):
  probe_local_vs_national_2023.csv
columns: AdvParentCode, AdvParentName, BrandCode,
         BrandDesc, PCCSubCode, SourceMediaType,
         spend_total, n_spots

Grain is one row per
(parent × brand × pcc × source media type); aggregate up
locally to parent-level if needed. DMA dimension is
dropped — this probe only needs the firm-level
local-vs-national share. Run on Mercury under SLURM (see
run_probe_local_vs_national_2023.sh).
"""

from __future__ import annotations

import gc
import glob
import os
import resource
import sys
from datetime import datetime
import pandas as pd

ADINTEL_ROOT = '/kilts/adintel'
YEAR = '2023'
REF_YEAR = '2023'
CHUNKSIZE = 2_000_000
MEM_LIMIT_GB = 50.0

PCC_TARGETS = {
    'F221', 'F222', 'F223',  # SSB (sweetened bev)
    'F310',                  # Beer
}

OCC_BASE = os.path.join(
    ADINTEL_ROOT, YEAR, 'Occurrence Data File Formats',
)
MEDIA_FOLDERS = [
    'Spot TV',
    'Network TV',
    'Cable TV',
    'Syndicated TV',
    'Spanish Language Network TV',
    'Spanish Language Cable TV',
]

NEEDED_COLS = [
    'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
    'Spend', 'Units',
]
BRAND_COLS = [
    'PrimBrandCode', 'ScndBrandCode', 'TerBrandCode',
]


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
        raise MemoryError(
            f"RSS {rss_gb:.1f} GB at: {label}"
        )


def load_brand_reference() -> pd.DataFrame:
    path = os.path.join(
        ADINTEL_ROOT, REF_YEAR, 'References', 'Brand.tsv',
    )
    log(f"Loading {path}")
    df = pd.read_csv(path, sep='\t', dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df['BrandCode'] = pd.to_numeric(
        df['BrandCode'], errors='coerce',
    ).astype('Int64')
    return df


def load_advertiser_reference() -> pd.DataFrame:
    """Parent code -> parent name. Schema:
    AdvParentCode\\tAdvParentName\\t...
    """
    path = os.path.join(
        ADINTEL_ROOT, REF_YEAR, 'References',
        'Advertiser.tsv',
    )
    log(f"Loading {path}")
    df = pd.read_csv(path, sep='\t', dtype=str)
    df.columns = [c.strip() for c in df.columns]
    if 'AdvParentCode' not in df.columns:
        log(f"  WARNING: Advertiser.tsv columns: "
            f"{list(df.columns)}")
        return pd.DataFrame(
            columns=['AdvParentCode', 'AdvParentName']
        )
    keep = ['AdvParentCode']
    name_col = next(
        (c for c in df.columns
         if c.lower().startswith('advparentname')),
        None,
    )
    if name_col:
        keep.append(name_col)
    out = df[keep].drop_duplicates('AdvParentCode')
    if name_col:
        out = out.rename(
            columns={name_col: 'AdvParentName'}
        )
    else:
        out['AdvParentName'] = ''
    return out


def candidate_brand_codes(
    brand_lookup: pd.DataFrame,
) -> set:
    sub = brand_lookup[
        brand_lookup['PCCSubCode'].isin(PCC_TARGETS)
    ]
    codes = set(sub['BrandCode'].dropna())
    log(
        f"  {len(codes):,} brand codes match "
        f"PCC subcodes {sorted(PCC_TARGETS)}"
    )
    by_pcc = (
        sub.groupby('PCCSubCode')['BrandCode']
        .nunique().sort_values(ascending=False)
    )
    for pcc, n in by_pcc.items():
        log(f"    {pcc}: {n} brands")
    return codes


def process_file(
    fpath: str,
    media_label: str,
    candidate_codes: set,
    brand_meta: pd.DataFrame,
) -> pd.DataFrame:
    """Read one occurrence file, return collapsed
    (AdvParentCode, PCCSubCode, SourceMediaType) frame.
    """
    log(f"  Reading {os.path.basename(fpath)}")
    frames: list[pd.DataFrame] = []
    rows_scanned = 0
    rows_matched = 0
    for chunk in pd.read_csv(
        fpath,
        sep='\t',
        header=0,
        usecols=lambda c: c.strip() in NEEDED_COLS,
        dtype=str,
        chunksize=CHUNKSIZE,
        low_memory=False,
        on_bad_lines='warn',
    ):
        rows_scanned += len(chunk)
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
            ['PrimBrandCode', 'Spend', 'Units'],
        ].copy()
        rows_matched += len(matched)
        matched['Spend'] = pd.to_numeric(
            matched['Spend'], errors='coerce',
        ).fillna(0.0)
        matched['Units'] = pd.to_numeric(
            matched['Units'], errors='coerce',
        ).fillna(0)
        matched = matched.merge(
            brand_meta,
            left_on='PrimBrandCode',
            right_on='BrandCode',
            how='left',
        )
        # PrimBrandCode may match a brand outside target
        # PCCs if Scnd/TerBrandCode is what triggered the
        # match; drop rows whose Prim brand isn't in our
        # target PCC set so we don't mis-attribute by PCC.
        matched = matched[
            matched['PCCSubCode'].isin(PCC_TARGETS)
        ]
        if matched.empty:
            continue
        agg = (
            matched
            .groupby(
                ['AdvParentCode', 'BrandCode',
                 'BrandDesc', 'PCCSubCode'],
                dropna=False,
            )
            .agg(
                spend_total=('Spend', 'sum'),
                n_spots=('Spend', 'size'),
            )
            .reset_index()
        )
        agg['SourceMediaType'] = media_label
        frames.append(agg)
    log(
        f"    scanned={rows_scanned:,} "
        f"matched={rows_matched:,} "
        f"frames={len(frames)}"
    )
    if not frames:
        return pd.DataFrame(
            columns=['AdvParentCode', 'PCCSubCode',
                     'SourceMediaType',
                     'spend_total', 'n_spots'],
        )
    combined = pd.concat(frames, ignore_index=True)
    return combined


def main() -> int:
    log(f"Probe start, year={YEAR}")
    brand_lookup = load_brand_reference()
    parent_lookup = load_advertiser_reference()
    candidate_codes = candidate_brand_codes(brand_lookup)
    if not candidate_codes:
        raise RuntimeError(
            "No brand codes match target PCCs."
        )

    brand_meta = brand_lookup[
        ['BrandCode', 'BrandDesc',
         'AdvParentCode', 'PCCSubCode']
    ].copy()

    all_frames: list[pd.DataFrame] = []
    for media in MEDIA_FOLDERS:
        folder = os.path.join(OCC_BASE, media)
        if not os.path.isdir(folder):
            log(f"[skip] {folder} not found")
            continue
        files = sorted(glob.glob(os.path.join(folder, '*')))
        log(f"\n=== {media}: {len(files)} file(s) ===")
        media_label = media
        for fpath in files:
            frame = process_file(
                fpath, media_label,
                candidate_codes, brand_meta,
            )
            if not frame.empty:
                all_frames.append(frame)
            gc.collect()
            check_memory(f"{media}")

    if not all_frames:
        log("No matches across any media type.")
        return 1

    combined = pd.concat(all_frames, ignore_index=True)
    final = (
        combined
        .groupby(
            ['AdvParentCode', 'BrandCode', 'BrandDesc',
             'PCCSubCode', 'SourceMediaType'],
            dropna=False,
        )
        .agg(
            spend_total=('spend_total', 'sum'),
            n_spots=('n_spots', 'sum'),
        )
        .reset_index()
    )
    final = final.merge(
        parent_lookup, on='AdvParentCode', how='left',
    )
    final = final[
        ['AdvParentCode', 'AdvParentName',
         'BrandCode', 'BrandDesc',
         'PCCSubCode', 'SourceMediaType',
         'spend_total', 'n_spots']
    ].sort_values(
        ['PCCSubCode', 'spend_total'],
        ascending=[True, False],
    )

    out_path = f'probe_local_vs_national_{YEAR}.csv'
    final.to_csv(
        out_path, index=False, float_format='%.2f',
    )
    log(
        f"Wrote {out_path}: {len(final):,} rows, "
        f"total spend "
        f"${final['spend_total'].sum():,.0f}"
    )

    log("\nTop 5 parents by total TV spend per PCC:")
    by_parent = (
        final.groupby(
            ['PCCSubCode', 'AdvParentCode',
             'AdvParentName'],
            dropna=False,
        )['spend_total'].sum().reset_index()
    )
    for pcc in sorted(by_parent['PCCSubCode'].dropna().unique()):
        sub = (
            by_parent[by_parent['PCCSubCode'] == pcc]
            .sort_values('spend_total', ascending=False)
            .head(5)
        )
        log(f"  {pcc}:")
        for _, r in sub.iterrows():
            log(
                f"    {r['AdvParentCode']:>10s} "
                f"{str(r['AdvParentName'])[:40]:<40s} "
                f"${r['spend_total']:>15,.0f}"
            )
    return 0


if __name__ == '__main__':
    sys.exit(main())
