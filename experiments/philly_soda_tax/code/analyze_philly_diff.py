#!/usr/bin/env python3
"""
Local analysis of Philadelphia DMA sweetened-beverage Spot TV
ad spend, before/after the Philly Beverage Tax (Jan 1, 2017).

Reads the per-year disaggregated CSVs produced by
mercury/extract_philly_spend.py. Produces:
  - Monthly aggregate (year-month × treated/control × parent)
  - Pre/post mean comparison
  - Within-firm placebo (taxed PCC vs. bottled water)
  - Plot: monthly spend with Jan 2017 cut line

Run after downloading:
  scp johl@mercury:~/philly_soda_dma_spend_*.csv \
      philly_soda_tax/extraction_outputs/

  cd philly_soda_tax/code
  python3 analyze_philly_diff.py
"""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


PROJ_ROOT = Path(__file__).resolve().parents[1]
EXTRACTION_DIR = PROJ_ROOT / 'extraction_outputs'
ANALYSIS_DIR = PROJ_ROOT / 'analysis_outputs'

EVENT_DATE = pd.Timestamp('2017-01-01')
TREATED_PCC = {'F221', 'F222', 'F223'}
PLACEBO_PCC = {'F224'}
KNOWN_BEVERAGE_PARENTS = {
    '11440':   'Coca-Cola Co',
    '14842':   'PepsiCo Inc',
    '2531252': 'JAB Holding (Dr Pepper Snapple)',
    '1568215': 'Dr Pepper Snapple Group Inc',
    '1693':    'Keurig Green Mountain',
    '20790':   'Hansen Bev (Monster)',
    '1110474': 'Red Bull GmbH',
    '551289':  'Cott Corp',
    '1809':    'Nestle SA',
}


def load_extraction() -> pd.DataFrame:
    files = sorted(
        glob.glob(str(EXTRACTION_DIR / 'philly_soda_dma_spend_*.csv'))
    )
    if not files:
        raise FileNotFoundError(
            f"No extraction CSVs in {EXTRACTION_DIR}. "
            f"Did you scp from Mercury?"
        )
    print(f"Loading {len(files)} files...")
    frames = []
    for f in files:
        df = pd.read_csv(
            f,
            dtype={
                'MarketCode': str,
                'PrimBrandCode': str,
                'ScndBrandCode': str,
                'TerBrandCode': str,
                'AdvParentCode': str,
                'PCCSubCode': str,
            },
        )
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df['AdDate'] = pd.to_datetime(df['AdDate'], errors='coerce')
    df['Spend'] = pd.to_numeric(df['Spend'], errors='coerce')
    df = df.dropna(subset=['AdDate', 'Spend'])
    df['ym'] = df['AdDate'].dt.to_period('M').dt.to_timestamp()
    df['arm'] = np.where(
        df['PCCSubCode'].isin(TREATED_PCC), 'treated',
        np.where(df['PCCSubCode'].isin(PLACEBO_PCC),
                 'placebo', 'other'),
    )
    return df


def monthly_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby(['ym', 'arm'])['Spend']
        .agg(['sum', 'count'])
        .reset_index()
        .rename(columns={'sum': 'spend_total', 'count': 'n_spots'})
    )
    return agg


def pre_post_summary(monthly: pd.DataFrame) -> pd.DataFrame:
    monthly = monthly.copy()
    monthly['period'] = np.where(
        monthly['ym'] < EVENT_DATE, 'pre', 'post',
    )
    out = (
        monthly.groupby(['arm', 'period'])
        .agg(
            monthly_spend_mean=('spend_total', 'mean'),
            monthly_spots_mean=('n_spots', 'mean'),
            n_months=('ym', 'nunique'),
        )
        .reset_index()
    )
    return out


def plot_monthly(monthly: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    for arm, sub in monthly.groupby('arm'):
        sub = sub.sort_values('ym')
        ax.plot(
            sub['ym'], sub['spend_total'] / 1e3,
            marker='o', markersize=3, linewidth=1.2,
            label=arm,
        )
    ax.axvline(
        EVENT_DATE, color='red', linestyle='--', linewidth=1,
    )
    ax.text(
        EVENT_DATE, ax.get_ylim()[1] * 0.95,
        '  Philly tax\n  Jan 1, 2017',
        color='red', va='top', ha='left', fontsize=9,
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('Spot TV spend ($ thousands)')
    ax.set_title(
        'Philadelphia DMA Spot TV spend, by PCC arm',
    )
    ax.legend(title='PCC arm')
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    print(f"Wrote {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.parse_args()

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    df = load_extraction()
    print(f"Loaded {len(df):,} occurrence rows.")
    print(df['arm'].value_counts())

    monthly = monthly_aggregate(df)
    monthly.to_csv(
        ANALYSIS_DIR / 'monthly_spend_by_arm.csv', index=False,
    )

    summary = pre_post_summary(monthly)
    summary.to_csv(
        ANALYSIS_DIR / 'pre_post_summary.csv', index=False,
    )
    print("\nPre/post summary:")
    print(summary.to_string(index=False))

    plot_monthly(monthly, ANALYSIS_DIR / 'monthly_spend.png')


if __name__ == '__main__':
    main()
