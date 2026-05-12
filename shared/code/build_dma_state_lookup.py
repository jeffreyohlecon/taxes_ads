#!/usr/bin/env python3
"""Build DMA -> state allocation lookup.

Reads the Nielsen ZIP-to-DMA reference XLS and the
Census 2024 county population estimates, and emits

  shared/reference/dma_state_lookup.csv

with one row per (DMA, state-it-touches) plus a derived
per-DMA summary written to

  shared/reference/dma_state_summary.csv

(one row per DMA: NumStates, States, DominantState,
DominantStatePct, in_single_state, MarketCode).

Source XLS: same file generate_market_allocations.py uses
in sb_incidence. Sheet 'ZIP by DMA' has one row per
(ZIP, county, DMA, state). We collapse to (DMA x county x
state), join Census county population, then aggregate to
DMA-state pop and DMA total pop.

Run once when DMA boundaries change. Output is checked in.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
NIELSEN_XLS = (
    Path.home()
    / 'Dropbox/Gambling Papers and Data/Data Docs/'
      'Nielsen docs/Reference_Documentation/'
      'Zip to DMA Mappings 2023/Zip to DMA 2023.XLS'
)
COUNTY_POP_CSV = (
    Path.home()
    / 'Dropbox/Gambling Papers and Data/raw_data/'
      'county_populations_2024.csv'
)
DMA_LOOKUP_CSV = REPO_ROOT / 'shared' / 'reference' / 'dma_lookup.csv'
OUT_LONG = REPO_ROOT / 'shared' / 'reference' / 'dma_state_lookup.csv'
OUT_SUMMARY = (
    REPO_ROOT / 'shared' / 'reference' / 'dma_state_summary.csv'
)


def _load_zip_dma() -> pd.DataFrame:
    print(f"Loading Nielsen ZIP-by-DMA from {NIELSEN_XLS}")
    df = pd.read_excel(
        NIELSEN_XLS, sheet_name='ZIP by DMA',
        skiprows=2, header=0,
    )
    df.columns = df.iloc[0].tolist()
    df = df.iloc[1:].reset_index(drop=True)
    keep = ['DMA\nCode', 'Designated Market Area',
            'State', 'County']
    df = df[keep].copy()
    df.columns = ['DMACode', 'DMAName', 'StateAbbrev',
                  'CountyName']
    df = df.dropna()
    df['DMACode'] = pd.to_numeric(
        df['DMACode'], errors='coerce',
    ).astype('Int64')
    df = df.dropna(subset=['DMACode'])
    df['DMACode'] = df['DMACode'].astype(int)
    df['StateAbbrev'] = df['StateAbbrev'].astype(str).str.strip()
    df['CountyName'] = df['CountyName'].astype(str).str.strip()
    # Collapse zip-level to county-level: one row per
    # (DMA, county, state).
    df = df.drop_duplicates(
        subset=['DMACode', 'StateAbbrev', 'CountyName']
    ).reset_index(drop=True)
    print(f"  {len(df):,} (DMA x county x state) rows")
    return df


# Census uses full state names in STNAME; we want
# abbreviations to join to the Nielsen XLS.
_STATE_FULL_TO_ABBREV = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ',
    'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO',
    'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL',
    'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
    'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH',
    'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND',
    'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR',
    'Pennsylvania': 'PA', 'Rhode Island': 'RI',
    'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI',
    'Wyoming': 'WY', 'Puerto Rico': 'PR',
}


def _norm_county(name: str) -> str:
    n = str(name).lower().strip()
    for suffix in (
        ' county', ' parish', ' borough', ' census area',
        ' municipio', ' city and borough', ' municipality',
    ):
        if n.endswith(suffix):
            n = n[: -len(suffix)]
            break
    # Independent cities (VA, MO) end in " city" in
    # Census but the Nielsen file usually drops " city";
    # leave both forms reachable below.
    return n.replace('.', '').replace("'", '').strip()


def _load_county_pop() -> pd.DataFrame:
    print(f"Loading county population from {COUNTY_POP_CSV}")
    df = pd.read_csv(COUNTY_POP_CSV, encoding='latin-1')
    df = df[df['SUMLEV'] == 50][
        ['STNAME', 'CTYNAME', 'POPESTIMATE2023']
    ].rename(columns={'POPESTIMATE2023': 'Pop'})
    df['StateAbbrev'] = df['STNAME'].map(_STATE_FULL_TO_ABBREV)
    miss = df[df['StateAbbrev'].isna()]['STNAME'].unique()
    if len(miss):
        print(f"  WARN: state names not mapped: {list(miss)}")
    df = df.dropna(subset=['StateAbbrev'])
    df['CountyKey'] = df['CTYNAME'].apply(_norm_county)
    return df[['StateAbbrev', 'CountyKey', 'Pop']]


def _join_pop(zip_dma: pd.DataFrame,
              cpop: pd.DataFrame) -> pd.DataFrame:
    z = zip_dma.copy()
    z['CountyKey'] = z['CountyName'].apply(_norm_county)
    merged = z.merge(
        cpop, on=['StateAbbrev', 'CountyKey'], how='left',
    )
    n_miss = merged['Pop'].isna().sum()
    if n_miss:
        print(
            f"  WARN: {n_miss} (DMA x county) rows missing "
            f"Census pop after merge "
            f"(of {len(merged):,}; "
            f"={n_miss / len(merged):.1%})"
        )
        bad = merged[merged['Pop'].isna()][
            ['StateAbbrev', 'CountyName']
        ].drop_duplicates().head(15)
        print("  examples:")
        print(bad.to_string(index=False))
    merged['Pop'] = merged['Pop'].fillna(0)
    return merged


def build() -> None:
    zip_dma = _load_zip_dma()
    cpop = _load_county_pop()
    joined = _join_pop(zip_dma, cpop)

    # DMA x state population.
    dma_state = (
        joined.groupby(
            ['DMACode', 'DMAName', 'StateAbbrev'],
            as_index=False,
        )['Pop'].sum()
        .rename(columns={'Pop': 'StatePopInDMA'})
    )
    # DMA total population (denominator).
    dma_total = (
        dma_state.groupby('DMACode', as_index=False)
        ['StatePopInDMA'].sum()
        .rename(columns={'StatePopInDMA': 'DMA_Population'})
    )
    long = dma_state.merge(dma_total, on='DMACode', how='left')
    long['StatePctOfDMA'] = (
        100 * long['StatePopInDMA'] / long['DMA_Population']
    ).where(long['DMA_Population'] > 0, 0.0)

    # Add Ad Intel MarketCode (= DMACode - 400) via the
    # canonical lookup so this file plays well with the
    # extracted panels.
    dma_lk = pd.read_csv(
        DMA_LOOKUP_CSV,
        dtype={'MarketCode': str, 'DMACode': str,
               'DMAName_public': str},
    )
    dma_lk['DMACode'] = pd.to_numeric(dma_lk['DMACode'])
    long = long.merge(
        dma_lk[['MarketCode', 'DMACode']], on='DMACode',
        how='left',
    )

    # Sort DMA, then state share desc.
    long = long.sort_values(
        ['DMACode', 'StatePctOfDMA'],
        ascending=[True, False],
    )

    long_out = long[
        ['MarketCode', 'DMACode', 'DMAName', 'StateAbbrev',
         'StatePopInDMA', 'DMA_Population', 'StatePctOfDMA']
    ]
    long_out.to_csv(OUT_LONG, index=False, float_format='%.6f')
    print(f"\nWrote {OUT_LONG}: {len(long_out):,} rows")

    # Per-DMA summary.
    def _agg(g: pd.DataFrame) -> pd.Series:
        g_sorted = g.sort_values('StatePctOfDMA', ascending=False)
        return pd.Series({
            'NumStates': len(g_sorted),
            'States': ','.join(g_sorted['StateAbbrev']),
            'DominantState': g_sorted['StateAbbrev'].iloc[0],
            'DominantStatePct': g_sorted['StatePctOfDMA'].iloc[0],
            'DMA_Population': g_sorted['DMA_Population'].iloc[0],
            'MarketCode': g_sorted['MarketCode'].iloc[0],
            'DMAName': g_sorted['DMAName'].iloc[0],
        })

    summary = (
        long.groupby('DMACode', as_index=False)
        .apply(_agg, include_groups=False)
        .reset_index(drop=True)
    )
    summary['in_single_state'] = (summary['NumStates'] == 1)
    summary = summary[
        ['MarketCode', 'DMACode', 'DMAName', 'NumStates',
         'States', 'DominantState', 'DominantStatePct',
         'DMA_Population', 'in_single_state']
    ].sort_values('DMACode')
    summary.to_csv(
        OUT_SUMMARY, index=False, float_format='%.4f',
    )
    print(f"Wrote {OUT_SUMMARY}: {len(summary):,} DMAs")
    print(
        f"  in_single_state DMAs: "
        f"{int(summary['in_single_state'].sum())}"
    )


if __name__ == '__main__':
    sys.exit(build() or 0)
