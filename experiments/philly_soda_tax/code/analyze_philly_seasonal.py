#!/usr/bin/env python3
"""Seasonal view: month-of-year on x-axis, one line per
year. Two panels: Coca-Cola and PepsiCo. Treated PCCs
(F221/F222/F223) only."""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

PROJ_ROOT = Path(__file__).resolve().parents[1]
EXTRACTION_DIR = PROJ_ROOT / 'extraction_outputs'
ANALYSIS_DIR = PROJ_ROOT / 'analysis_outputs'

PARENTS = [
    ('11440', 'Coca-Cola'),
    ('14842', 'PepsiCo'),
]
TREATED_PCC = {'F221', 'F222', 'F223'}
YEARS = [2014, 2015, 2016, 2017]
MONTH_LABELS = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
]
COLORS = {
    2014: '#6baed6',
    2015: '#3182bd',
    2016: '#08519c',
    2017: '#d62728',
}


def build_panel(df, parent_code):
    t = df[
        df['PCCSubCode'].isin(TREATED_PCC)
        & (df['AdvParentCode'] == parent_code)
    ].copy()
    t['year'] = t['AdDate'].dt.year
    t['month'] = t['AdDate'].dt.month
    t = t[t['year'].isin(YEARS)]
    monthly = (
        t.groupby(['year', 'month'])['Spend']
        .sum()
        .reset_index()
    )
    full = (
        pd.MultiIndex.from_product(
            [YEARS, range(1, 13)],
            names=['year', 'month'],
        )
        .to_frame(index=False)
        .merge(monthly, on=['year', 'month'], how='left')
    )
    full['Spend'] = full['Spend'].fillna(0)
    return full


def plot_panel(ax, full, title, show_legend):
    for y in YEARS:
        sub = full[full['year'] == y].sort_values('month')
        ax.plot(
            sub['month'], sub['Spend'] / 1e3,
            marker='o', markersize=4, linewidth=1.6,
            color=COLORS[y], label=str(y),
        )
    pass_val = full[
        (full['year'] == 2016) & (full['month'] == 6)
    ]['Spend'].iloc[0] / 1e3
    eff_val = full[
        (full['year'] == 2017) & (full['month'] == 1)
    ]['Spend'].iloc[0] / 1e3
    ax.scatter(
        [6], [pass_val], marker='x', s=160, linewidths=3,
        color='gold', zorder=5,
        label='Tax passed (Jun 2016)',
    )
    ax.scatter(
        [1], [eff_val], marker='x', s=160, linewidths=3,
        color='green', zorder=5,
        label='Tax effective (Jan 2017)',
    )
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(MONTH_LABELS)
    ax.set_xlabel('Month of year')
    ax.set_ylabel('Spot TV spend ($ thousands)')
    ax.set_title(title)
    if show_legend:
        ax.legend(loc='best', fontsize=8)


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

    fig, axes = plt.subplots(1, 2, figsize=(16, 5), sharey=False)
    for ax, (code, name) in zip(axes, PARENTS):
        panel = build_panel(df, code)
        print(f'{name} SSB spend in Philly DMA, by year-month:')
        print(panel.pivot(
            index='month', columns='year', values='Spend',
        ).round(0))
        plot_panel(
            ax, panel,
            f'{name} SSB Spot TV, Philly DMA (seasonal)',
            show_legend=(code == PARENTS[0][0]),
        )
    fig.tight_layout()

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    out = ANALYSIS_DIR / 'monthly_spend_coke_pepsi_seasonal.png'
    fig.savefig(out, dpi=160)
    print(f"Wrote {out}")


if __name__ == '__main__':
    main()
