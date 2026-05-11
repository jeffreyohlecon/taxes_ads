#!/usr/bin/env python3
"""Restricted analysis: top-4 SSB parents, two cut dates."""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

PROJ_ROOT = Path(__file__).resolve().parents[1]
EXTRACTION_DIR = PROJ_ROOT / 'extraction_outputs'
ANALYSIS_DIR = PROJ_ROOT / 'analysis_outputs'

TOP = {'11440', '14842', '2531252', '1809'}
NAMES = {
    '11440': 'Coca-Cola',
    '14842': 'PepsiCo',
    '2531252': 'DrPepperSnapple',
    '1809': 'Nestle',
}
TREATED_PCC = {'F221', 'F222', 'F223'}


def main():
    files = sorted(glob.glob(
        str(EXTRACTION_DIR / 'philly_soda_dma_spend_*.csv')
    ))
    df = pd.concat([
        pd.read_csv(
            f,
            dtype={'AdvParentCode': str, 'PCCSubCode': str},
        )
        for f in files
    ], ignore_index=True)
    df['AdDate'] = pd.to_datetime(df['AdDate'])
    df['Spend'] = pd.to_numeric(df['Spend'], errors='coerce')
    df = df.dropna(subset=['AdDate', 'Spend'])
    df['ym'] = df['AdDate'].dt.to_period('M').dt.to_timestamp()

    t = df[
        df['PCCSubCode'].isin(TREATED_PCC)
        & df['AdvParentCode'].isin(TOP)
    ].copy()
    print(
        f"Top-4 treated rows: {len(t):,}, "
        f"spend ${t['Spend'].sum():,.0f}"
    )

    monthly_total = t.groupby('ym')['Spend'].sum().reset_index()
    for label, cut in [
        ('Jun 2016 (passed)', pd.Timestamp('2016-06-01')),
        ('Jan 2017 (effective)', pd.Timestamp('2017-01-01')),
    ]:
        pre = monthly_total.loc[
            monthly_total['ym'] < cut, 'Spend'
        ].mean()
        post = monthly_total.loc[
            monthly_total['ym'] >= cut, 'Spend'
        ].mean()
        n_pre = (monthly_total['ym'] < cut).sum()
        n_post = (monthly_total['ym'] >= cut).sum()
        print(
            f"  cut={label}: pre=${pre:,.0f}/mo  "
            f"post=${post:,.0f}/mo  "
            f"delta={(post-pre)/pre*100:+.1f}%  "
            f"(n_pre={n_pre}, n_post={n_post})"
        )

    monthly = t.groupby(
        ['ym', 'AdvParentCode']
    )['Spend'].sum().reset_index()
    monthly['parent'] = monthly['AdvParentCode'].map(NAMES)

    fig, ax = plt.subplots(figsize=(11, 5))
    for pc, sub in monthly.groupby('AdvParentCode'):
        sub = sub.sort_values('ym')
        ax.plot(
            sub['ym'], sub['Spend'] / 1e3,
            marker='o', markersize=3, linewidth=1.2,
            label=NAMES[pc],
        )
    ax.axvline(
        pd.Timestamp('2016-06-16'),
        color='orange', linestyle='--', linewidth=1,
        label='Tax passed (Jun 16 2016)',
    )
    ax.axvline(
        pd.Timestamp('2017-01-01'),
        color='red', linestyle='--', linewidth=1,
        label='Tax effective (Jan 1 2017)',
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('Spot TV spend ($ thousands)')
    ax.set_title(
        'Philly DMA SSB Spot TV spend by parent (top 4)'
    )
    ax.legend(loc='upper left', fontsize=8)
    fig.tight_layout()

    # Total-only plot
    fig2, ax2 = plt.subplots(figsize=(11, 5))
    mt = monthly_total.sort_values('ym')
    ax2.plot(
        mt['ym'], mt['Spend'] / 1e3,
        marker='o', markersize=3, linewidth=1.2,
        color='black',
    )
    ax2.axvline(
        pd.Timestamp('2016-06-16'),
        color='orange', linestyle='--', linewidth=1,
        label='Tax passed (Jun 16 2016)',
    )
    ax2.axvline(
        pd.Timestamp('2017-01-01'),
        color='red', linestyle='--', linewidth=1,
        label='Tax effective (Jan 1 2017)',
    )
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Spot TV spend ($ thousands)')
    ax2.set_title(
        'Philly DMA SSB Spot TV spend, top-4 parents combined'
    )
    ax2.legend(loc='upper left', fontsize=9)
    fig2.tight_layout()

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    out1 = ANALYSIS_DIR / 'monthly_spend_top4_byparent.png'
    out2 = ANALYSIS_DIR / 'monthly_spend_top4_total.png'
    fig.savefig(out1, dpi=160)
    fig2.savefig(out2, dpi=160)
    print(f"Wrote {out1}")
    print(f"Wrote {out2}")


if __name__ == '__main__':
    main()
