#!/usr/bin/env python3
"""
Symmetric 18-month pre/post DiD for the Philly Beverage Tax.

Two specs, run separately and reported side-by-side:
  A. Tax passed   (Jun 2016): pre = Dec 2014 - May 2016,
                              post = Jun 2016 - Nov 2017
  B. Tax effective (Jan 2017): pre = Jul 2015 - Dec 2016,
                               post = Jan 2017 - Jun 2018

Outcome: top-4-parent SSB Spot TV spend (Coca-Cola,
PepsiCo, Dr Pepper Snapple, Nestle), F221/F222/F223 only.

Panel construction: full DMA x year-month grid,
zero-filled for DMA-months that did not appear in the
extractor output (no SSB occurrence that month). Per
the user: missing means zero, not missing-at-random.

Comparison groups:
  - Pittsburgh (MarketCode 108): single comparison
  - Rest-of-non-Philly DMAs: mean across DMAs at each
    month

With only one treated unit, no standard errors are
reported. Point estimates only, per CLAUDE.md.
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

TREATED_DMA = '104'
COMPARISON_DMA = '108'
TREATED_PCC = {'F221', 'F222', 'F223'}
TOP_PARENTS = {'11440', '14842', '2531252', '1809'}

# 18-month symmetric windows around each event definition.
SPECS = {
    'A_passage_jun2016': {
        'event_date': pd.Timestamp('2016-06-01'),
        'pre_start':  pd.Timestamp('2014-12-01'),
        'pre_end':    pd.Timestamp('2016-05-01'),   # inclusive
        'post_start': pd.Timestamp('2016-06-01'),
        'post_end':   pd.Timestamp('2017-11-01'),   # inclusive
    },
    'B_effective_jan2017': {
        'event_date': pd.Timestamp('2017-01-01'),
        'pre_start':  pd.Timestamp('2015-07-01'),
        'pre_end':    pd.Timestamp('2016-12-01'),   # inclusive
        'post_start': pd.Timestamp('2017-01-01'),
        'post_end':   pd.Timestamp('2018-06-01'),   # inclusive
    },
}


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


def build_zero_filled_panel(raw: pd.DataFrame) -> pd.DataFrame:
    """One row per (DMA, year-month). Zero for DMA-months
    with no SSB occurrence (top-4 × treated-PCC)."""
    f = raw[
        raw['PCCSubCode'].isin(TREATED_PCC)
        & raw['AdvParentCode'].isin(TOP_PARENTS)
    ]
    agg = (
        f.groupby(['MarketCode', 'ym'])['spend_total']
        .sum().reset_index()
    )
    dmas = sorted(raw['MarketCode'].unique())
    months = pd.date_range(
        raw['ym'].min(), raw['ym'].max(), freq='MS',
    )
    grid = pd.MultiIndex.from_product(
        [dmas, months], names=['MarketCode', 'ym'],
    ).to_frame(index=False)
    panel = grid.merge(
        agg, on=['MarketCode', 'ym'], how='left',
    )
    panel['spend_total'] = panel['spend_total'].fillna(0.0)
    return panel


def compute_did(
    panel: pd.DataFrame, spec_name: str, spec: dict,
) -> dict:
    pre_mask = (
        (panel['ym'] >= spec['pre_start'])
        & (panel['ym'] <= spec['pre_end'])
    )
    post_mask = (
        (panel['ym'] >= spec['post_start'])
        & (panel['ym'] <= spec['post_end'])
    )
    pre = panel[pre_mask]
    post = panel[post_mask]

    philly_pre = pre.loc[
        pre['MarketCode'] == TREATED_DMA, 'spend_total'
    ].mean()
    philly_post = post.loc[
        post['MarketCode'] == TREATED_DMA, 'spend_total'
    ].mean()

    pitt_pre = pre.loc[
        pre['MarketCode'] == COMPARISON_DMA, 'spend_total'
    ].mean()
    pitt_post = post.loc[
        post['MarketCode'] == COMPARISON_DMA, 'spend_total'
    ].mean()

    # Rest-of-non-Philly mean: average across DMAs of each
    # month's spend, then mean across months.
    rou_pre = (
        pre.loc[pre['MarketCode'] != TREATED_DMA]
        .groupby('ym')['spend_total'].mean().mean()
    )
    rou_post = (
        post.loc[post['MarketCode'] != TREATED_DMA]
        .groupby('ym')['spend_total'].mean().mean()
    )

    return {
        'spec': spec_name,
        'event_date': spec['event_date'].date(),
        'pre_window': (
            f"{spec['pre_start'].strftime('%Y-%m')} - "
            f"{spec['pre_end'].strftime('%Y-%m')}"
        ),
        'post_window': (
            f"{spec['post_start'].strftime('%Y-%m')} - "
            f"{spec['post_end'].strftime('%Y-%m')}"
        ),
        'philly_pre':  philly_pre,
        'philly_post': philly_post,
        'philly_delta': philly_post - philly_pre,
        'pitt_pre':    pitt_pre,
        'pitt_post':   pitt_post,
        'pitt_delta':  pitt_post - pitt_pre,
        'rou_pre':     rou_pre,
        'rou_post':    rou_post,
        'rou_delta':   rou_post - rou_pre,
        'DiD_vs_pitt': (
            (philly_post - philly_pre)
            - (pitt_post - pitt_pre)
        ),
        'DiD_vs_rou': (
            (philly_post - philly_pre)
            - (rou_post - rou_pre)
        ),
    }


def plot_two_by_two(
    panel: pd.DataFrame, spec_name: str, spec: dict,
    out: Path,
) -> None:
    pre_mask = (
        (panel['ym'] >= spec['pre_start'])
        & (panel['ym'] <= spec['pre_end'])
    )
    post_mask = (
        (panel['ym'] >= spec['post_start'])
        & (panel['ym'] <= spec['post_end'])
    )

    def mean_in(p, dma=None):
        if dma is None:
            return (
                p.loc[p['MarketCode'] != TREATED_DMA]
                .groupby('ym')['spend_total'].mean().mean()
            )
        return p.loc[
            p['MarketCode'] == dma, 'spend_total'
        ].mean()

    fig, ax = plt.subplots(figsize=(8, 4.5))
    units = ['Philadelphia', 'Pittsburgh', 'Rest of US (DMA mean)']
    pre_vals = [
        mean_in(panel[pre_mask], TREATED_DMA) / 1e3,
        mean_in(panel[pre_mask], COMPARISON_DMA) / 1e3,
        mean_in(panel[pre_mask]) / 1e3,
    ]
    post_vals = [
        mean_in(panel[post_mask], TREATED_DMA) / 1e3,
        mean_in(panel[post_mask], COMPARISON_DMA) / 1e3,
        mean_in(panel[post_mask]) / 1e3,
    ]
    x = np.arange(len(units))
    w = 0.36
    ax.bar(x - w/2, pre_vals, w, label='Pre (18 mo)',
           color='lightgray', edgecolor='black')
    ax.bar(x + w/2, post_vals, w, label='Post (18 mo)',
           color='steelblue', edgecolor='black')
    ax.set_xticks(x)
    ax.set_xticklabels(units)
    ax.set_ylabel('Monthly SSB Spot TV spend ($K)')
    ax.set_title(
        f"DiD {spec_name}: pre vs post 18-mo means"
    )
    ax.legend(loc='upper right', fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"Wrote {out}")


def plot_monthly_window(
    panel: pd.DataFrame, spec_name: str, spec: dict,
    out: Path,
) -> None:
    """Monthly series for the 36-month window, three lines."""
    mask = (
        (panel['ym'] >= spec['pre_start'])
        & (panel['ym'] <= spec['post_end'])
    )
    sub = panel[mask]
    philly = (
        sub.loc[sub['MarketCode'] == TREATED_DMA]
        .sort_values('ym')
    )
    pitt = (
        sub.loc[sub['MarketCode'] == COMPARISON_DMA]
        .sort_values('ym')
    )
    rou = (
        sub.loc[sub['MarketCode'] != TREATED_DMA]
        .groupby('ym')['spend_total'].mean().reset_index()
        .sort_values('ym')
    )

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(
        philly['ym'], philly['spend_total'] / 1e3,
        marker='o', markersize=4, linewidth=1.4,
        label='Philadelphia (treated)', color='C3',
    )
    ax.plot(
        pitt['ym'], pitt['spend_total'] / 1e3,
        marker='o', markersize=4, linewidth=1.4,
        label='Pittsburgh (comparison)', color='C0',
    )
    ax.plot(
        rou['ym'], rou['spend_total'] / 1e3,
        marker='.', markersize=3, linewidth=1.0,
        label='Rest of US (DMA mean)',
        color='gray', alpha=0.8,
    )
    ax.axvline(
        spec['event_date'], color='red', linestyle='--',
        linewidth=1, label=f'Event: {spec["event_date"].date()}',
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('SSB Spot TV spend ($K)')
    ax.set_title(
        f"18-month pre/post window — {spec_name}"
    )
    ax.legend(loc='upper left', fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"Wrote {out}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_panel()
    panel = build_zero_filled_panel(raw)
    print(
        f"Zero-filled panel: {len(panel):,} obs, "
        f"{panel['MarketCode'].nunique()} DMAs, "
        f"{panel['ym'].nunique()} months, "
        f"share spend>0: "
        f"{(panel['spend_total']>0).mean()*100:.1f}%"
    )

    results = []
    for spec_name, spec in SPECS.items():
        print(f"\n=== {spec_name} ===")
        r = compute_did(panel, spec_name, spec)
        for k, v in r.items():
            if isinstance(v, float):
                print(f"  {k:14s}: {v:,.1f}")
            else:
                print(f"  {k:14s}: {v}")
        results.append(r)
        plot_two_by_two(
            panel, spec_name, spec,
            OUT_DIR / f'did_18mo_{spec_name}_means.png',
        )
        plot_monthly_window(
            panel, spec_name, spec,
            OUT_DIR / f'did_18mo_{spec_name}_monthly.png',
        )

    out_summary = pd.DataFrame(results)
    out_summary.to_csv(
        OUT_DIR / 'did_18mo_summary.csv', index=False,
    )
    print(f"\nWrote {OUT_DIR / 'did_18mo_summary.csv'}")


if __name__ == '__main__':
    main()
