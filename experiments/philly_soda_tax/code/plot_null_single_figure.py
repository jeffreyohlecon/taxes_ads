#!/usr/bin/env python3
"""
One figure to show the Philly/Pittsburgh null.

Both DMAs' monthly SSB Spot TV spend (top-4 parents,
F221/F222/F223) indexed to their own 2014-Jan ->
2016-May mean = 100. Lines should diverge post-2017
if the tax shifted advertiser behavior in Philly.

No error bars: only 1 treated and 1 comparison DMA,
so analytical clustered SEs would be meaningless.
"""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

PROJ_ROOT = Path(__file__).resolve().parents[1]
PANEL_DIR = PROJ_ROOT / 'extraction_outputs' / 'national_panel'
OUT_DIR = PROJ_ROOT / 'analysis_outputs'

TREATED_DMA = '104'
COMPARISON_DMA = '108'
EVENT_PASSED = pd.Timestamp('2016-06-01')
EVENT_EFFECTIVE = pd.Timestamp('2017-01-01')
PRE_END = pd.Timestamp('2016-06-01')  # pre = strictly before passage
PRE_START = pd.Timestamp('2014-01-01')

TOP_PARENTS = {'11440', '14842', '2531252', '1809'}
TREATED_PCC = {'F221', 'F222', 'F223'}


def load_panel() -> pd.DataFrame:
    files = sorted(glob.glob(
        str(PANEL_DIR / 'ssb_dma_month_panel_*.csv')
    ))
    df = pd.concat([
        pd.read_csv(
            f,
            dtype={
                'MarketCode': str,
                'AdvParentCode': str,
                'PCCSubCode': str,
            },
        )
        for f in files
    ], ignore_index=True)
    df['ym'] = pd.to_datetime(df['year_month'], format='%Y-%m')
    return df


def monthly_for_dma(df: pd.DataFrame, code: str) -> pd.DataFrame:
    sub = df[df['MarketCode'] == code]
    return (
        sub.groupby('ym', as_index=False)['spend_total'].sum()
    )


def index_to_pre_mean(s: pd.DataFrame) -> pd.DataFrame:
    pre_mask = (s['ym'] >= PRE_START) & (s['ym'] < PRE_END)
    base = s.loc[pre_mask, 'spend_total'].mean()
    s = s.copy()
    s['idx'] = 100.0 * s['spend_total'] / base
    return s


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_panel()
    panel = raw[
        raw['PCCSubCode'].isin(TREATED_PCC)
        & raw['AdvParentCode'].isin(TOP_PARENTS)
    ]

    philly = index_to_pre_mean(monthly_for_dma(panel, TREATED_DMA))
    pitt = index_to_pre_mean(monthly_for_dma(panel, COMPARISON_DMA))

    # Restrict to 2014-2018 to keep the window symmetric and
    # focused on the event.
    window = (
        lambda d: d[(d['ym'] >= '2014-01-01')
                    & (d['ym'] <= '2018-12-31')]
    )
    philly = window(philly)
    pitt = window(pitt)

    # 3-month rolling mean to reduce monthly noise without
    # smearing the event. Plot both raw (thin) and smoothed
    # (thick) so nothing is hidden.
    philly['idx_sm'] = philly['idx'].rolling(3, center=True).mean()
    pitt['idx_sm'] = pitt['idx'].rolling(3, center=True).mean()

    fig, ax = plt.subplots(figsize=(11, 5))

    ax.plot(philly['ym'], philly['idx'],
            color='C3', alpha=0.35, linewidth=1.0)
    ax.plot(pitt['ym'], pitt['idx'],
            color='C0', alpha=0.35, linewidth=1.0)

    ax.plot(philly['ym'], philly['idx_sm'],
            color='C3', linewidth=2.2,
            label='Philadelphia (treated)')
    ax.plot(pitt['ym'], pitt['idx_sm'],
            color='C0', linewidth=2.2,
            label='Pittsburgh (comparison)')

    ax.axhline(100, color='black', linewidth=0.6, alpha=0.5)
    ax.axvline(EVENT_PASSED, color='orange',
               linestyle='--', linewidth=1.2)
    ax.axvline(EVENT_EFFECTIVE, color='red',
               linestyle='--', linewidth=1.2)
    ax.annotate('Tax passed\nJun 2016',
                xy=(EVENT_PASSED, ax.get_ylim()[1]),
                xytext=(EVENT_PASSED + pd.Timedelta(days=10), 175),
                fontsize=9, color='darkorange')
    ax.annotate('Tax effective\nJan 2017',
                xy=(EVENT_EFFECTIVE, ax.get_ylim()[1]),
                xytext=(EVENT_EFFECTIVE + pd.Timedelta(days=10), 145),
                fontsize=9, color='darkred')

    ax.set_xlabel('Month')
    ax.set_ylabel('Monthly SSB Spot TV spend\n'
                  '(indexed: own pre-tax mean = 100)')
    ax.set_title(
        'Philly vs. Pittsburgh SSB Spot TV advertising '
        'around the Philadelphia Beverage Tax\n'
        'Top-4 SSB parents (Coke, Pepsi, DPS, Nestlé); '
        'thin = monthly, thick = 3-mo rolling mean'
    )
    ax.legend(loc='lower left', fontsize=10, frameon=False)
    fig.tight_layout()
    out = OUT_DIR / 'null_philly_vs_pitt_indexed.png'
    fig.savefig(out, dpi=180)
    print(f"Wrote {out}")
    plt.close(fig)


if __name__ == '__main__':
    main()
