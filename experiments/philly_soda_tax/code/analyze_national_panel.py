#!/usr/bin/env python3
"""
Philly Beverage Tax analysis using the national SSB panel.

Reads the collapsed national panel produced by
shared/mercury/extract_category_panel.py:
  (MarketCode, year_month, AdvParentCode, PCCSubCode)
      -> spend_total, units_total, n_spots

Builds Philadelphia (treated) vs Pittsburgh (comparison)
vs rest-of-US (donor) monthly SSB Spot TV spend series
for the top-4 SSB parents (Coca-Cola, PepsiCo, Dr Pepper
Snapple, Nestle). Produces:

  - monthly_spend_dma.png   (4 lines: treated, comparison,
                             rest-of-US, rest-of-PA-proxy)
  - event_study_philly.png  (Philly minus Pittsburgh,
                             with anticipation/post bands)
  - did_summary.csv         (raw means, deltas, 2x2 DiD)

Anticipation handled by treating Jun 2016 onwards as
"anticipation"; "post" starts Jan 2017.
"""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJ_ROOT = Path(__file__).resolve().parents[1]
PANEL_DIR = PROJ_ROOT / 'extraction_outputs' / 'national_panel'
OUT_DIR = PROJ_ROOT / 'analysis_outputs'

TREATED_DMA = '104'  # Philadelphia
COMPARISON_DMA = '108'  # Pittsburgh
EVENT_PASSED = pd.Timestamp('2016-06-01')   # tax passed
EVENT_EFFECTIVE = pd.Timestamp('2017-01-01')  # tax effective

# Top-4 SSB parents from the discovery probe.
TOP_PARENTS = {
    '11440': 'Coca-Cola',
    '14842': 'PepsiCo',
    '2531252': 'DrPepperSnapple',
    '1809': 'Nestle',
}
TREATED_PCC = {'F221', 'F222', 'F223'}


def load_panel() -> pd.DataFrame:
    files = sorted(glob.glob(
        str(PANEL_DIR / 'ssb_dma_month_panel_*.csv')
    ))
    if not files:
        raise FileNotFoundError(
            f"No panel CSVs in {PANEL_DIR}. "
            f"scp from Mercury first."
        )
    print(f"Loading {len(files)} per-year files...")
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


def filter_target(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        df['PCCSubCode'].isin(TREATED_PCC)
        & df['AdvParentCode'].isin(TOP_PARENTS)
    ].copy()


def monthly_by_dma(
    df: pd.DataFrame, dma_codes: set[str]
) -> pd.DataFrame:
    sub = df[df['MarketCode'].isin(dma_codes)]
    return (
        sub.groupby('ym')['spend_total'].sum().reset_index()
    )


def monthly_rest_of_us(
    df: pd.DataFrame, exclude: set[str]
) -> pd.DataFrame:
    sub = df[~df['MarketCode'].isin(exclude)]
    # Mean across DMAs gives a representative DMA-level
    # series; sum gives the national-minus-treated total.
    return (
        sub.groupby('ym')['spend_total']
        .agg(rou_sum='sum', rou_mean='mean',
             n_dmas='count')
        .reset_index()
    )


def plot_series(panel: pd.DataFrame) -> None:
    treated = monthly_by_dma(panel, {TREATED_DMA})
    comparison = monthly_by_dma(panel, {COMPARISON_DMA})
    rou = monthly_rest_of_us(
        panel, exclude={TREATED_DMA, COMPARISON_DMA}
    )

    fig, ax = plt.subplots(figsize=(12, 5.5))
    ax.plot(
        treated['ym'], treated['spend_total'] / 1e3,
        marker='o', markersize=3, linewidth=1.5,
        label='Philadelphia (treated)', color='C3',
    )
    ax.plot(
        comparison['ym'], comparison['spend_total'] / 1e3,
        marker='o', markersize=3, linewidth=1.5,
        label='Pittsburgh (comparison)', color='C0',
    )
    ax.plot(
        rou['ym'], rou['rou_mean'] / 1e3,
        marker='.', markersize=2, linewidth=1.0,
        label='Rest of US (DMA mean)',
        color='gray', alpha=0.7,
    )
    ax.axvline(
        EVENT_PASSED, color='orange', linestyle='--',
        linewidth=1, label='Tax passed (Jun 2016)',
    )
    ax.axvline(
        EVENT_EFFECTIVE, color='red', linestyle='--',
        linewidth=1, label='Tax effective (Jan 2017)',
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('SSB Spot TV spend ($ thousands)')
    ax.set_title(
        'Philadelphia vs Pittsburgh vs Rest of US, '
        'top-4 SSB advertisers'
    )
    ax.legend(loc='upper left', fontsize=9)
    fig.tight_layout()
    out = OUT_DIR / 'national_panel_three_series.png'
    fig.savefig(out, dpi=160)
    print(f"Wrote {out}")
    plt.close(fig)


def plot_philly_minus_pitt(panel: pd.DataFrame) -> None:
    treated = monthly_by_dma(panel, {TREATED_DMA}).rename(
        columns={'spend_total': 'philly'}
    )
    comparison = monthly_by_dma(
        panel, {COMPARISON_DMA}
    ).rename(columns={'spend_total': 'pitt'})
    diff = treated.merge(comparison, on='ym', how='outer')
    diff = diff.fillna(0).sort_values('ym')
    diff['delta'] = diff['philly'] - diff['pitt']

    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.bar(
        diff['ym'], diff['delta'] / 1e3,
        width=25, color='steelblue',
    )
    ax.axhline(0, color='black', linewidth=0.6)
    ax.axvline(
        EVENT_PASSED, color='orange', linestyle='--',
        linewidth=1, label='Tax passed',
    )
    ax.axvline(
        EVENT_EFFECTIVE, color='red', linestyle='--',
        linewidth=1, label='Tax effective',
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('Philly spend − Pittsburgh spend ($K)')
    ax.set_title(
        'Difference series: Philly minus Pittsburgh, '
        'top-4 SSB advertisers'
    )
    ax.legend(loc='upper left', fontsize=9)
    fig.tight_layout()
    out = OUT_DIR / 'philly_minus_pitt_diff.png'
    fig.savefig(out, dpi=160)
    print(f"Wrote {out}")
    plt.close(fig)
    diff.to_csv(
        OUT_DIR / 'philly_minus_pitt_diff.csv',
        index=False,
    )


def did_summary(panel: pd.DataFrame) -> pd.DataFrame:
    treated = monthly_by_dma(panel, {TREATED_DMA}).rename(
        columns={'spend_total': 'philly'}
    )
    comparison = monthly_by_dma(
        panel, {COMPARISON_DMA}
    ).rename(columns={'spend_total': 'pitt'})
    m = treated.merge(
        comparison, on='ym', how='outer'
    ).fillna(0)
    m['period'] = np.select(
        [m['ym'] < EVENT_PASSED,
         m['ym'] < EVENT_EFFECTIVE,
         m['ym'] >= EVENT_EFFECTIVE],
        ['pre', 'anticipation', 'post'],
        default='',
    )
    grp = (
        m.groupby('period')
        .agg(philly_mean=('philly', 'mean'),
             pitt_mean=('pitt', 'mean'),
             n_months=('ym', 'nunique'))
        .reindex(['pre', 'anticipation', 'post'])
        .reset_index()
    )
    pre = grp.set_index('period').loc['pre']
    post = grp.set_index('period').loc['post']
    did_passed_cut = {
        'spec': 'pre vs (anticipation+post)',
        'philly_delta':
            m.loc[m['ym'] >= EVENT_PASSED, 'philly'].mean()
            - m.loc[m['ym'] < EVENT_PASSED, 'philly'].mean(),
        'pitt_delta':
            m.loc[m['ym'] >= EVENT_PASSED, 'pitt'].mean()
            - m.loc[m['ym'] < EVENT_PASSED, 'pitt'].mean(),
    }
    did_passed_cut['DiD'] = (
        did_passed_cut['philly_delta']
        - did_passed_cut['pitt_delta']
    )
    did_eff_cut = {
        'spec': 'pre vs post (ignoring anticipation)',
        'philly_delta': post['philly_mean'] - pre['philly_mean'],
        'pitt_delta': post['pitt_mean'] - pre['pitt_mean'],
    }
    did_eff_cut['DiD'] = (
        did_eff_cut['philly_delta']
        - did_eff_cut['pitt_delta']
    )
    out_summary = pd.DataFrame([did_passed_cut, did_eff_cut])

    print("\n=== Period means ===")
    print(grp.to_string(index=False))
    print("\n=== DiD specs (Philly delta - Pittsburgh delta) ===")
    print(out_summary.to_string(index=False))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    grp.to_csv(OUT_DIR / 'period_means.csv', index=False)
    out_summary.to_csv(
        OUT_DIR / 'did_summary.csv', index=False,
    )
    return out_summary


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_panel()
    print(
        f"Loaded {len(raw):,} rows, "
        f"{raw['MarketCode'].nunique()} DMAs, "
        f"{raw['year_month'].nunique()} months."
    )
    panel = filter_target(raw)
    print(
        f"After top-4 × treated-PCC filter: "
        f"{len(panel):,} rows."
    )

    plot_series(panel)
    plot_philly_minus_pitt(panel)
    did_summary(panel)


if __name__ == '__main__':
    main()
