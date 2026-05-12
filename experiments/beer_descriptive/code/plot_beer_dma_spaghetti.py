#!/usr/bin/env python3
"""Spaghetti plot of monthly Spot TV beer ad spend by DMA,
2014-2023.

Question this answers: are beer brands buying meaningful
amounts of *local* (Spot TV) advertising at all, or is
beer mostly national network buys?

Reads the BEER national panel (one CSV per year) extracted
by shared/mercury/extract_category_panel.py, aggregates to
(MarketCode, year_month) by summing over PCCSubCode and
AdvParentCode, and plots one line per DMA.

Outputs:
  experiments/beer_descriptive/figures/
      beer_dma_spend_spaghetti.png
      beer_dma_spend_spaghetti_log.png
      beer_dma_spend_top10_labeled.png
"""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJ_ROOT = Path(__file__).resolve().parents[3]
PANEL_DIR = (
    PROJ_ROOT / 'experiments' / 'beer_descriptive' / 'data'
)
FIG_DIR = (
    PROJ_ROOT / 'experiments' / 'beer_descriptive' / 'figures'
)
DMA_LOOKUP = PROJ_ROOT / 'shared' / 'reference' / 'dma_lookup.csv'


def load_panel() -> pd.DataFrame:
    files = sorted(glob.glob(
        str(PANEL_DIR / 'beer_dma_month_panel_*.csv')
    ))
    if not files:
        raise FileNotFoundError(
            f"No beer panel CSVs in {PANEL_DIR}; "
            "extract on Mercury first."
        )
    print(f"Loading {len(files)} year file(s):")
    for f in files:
        print(f"  {Path(f).name}")
    raw = pd.concat([
        pd.read_csv(
            f,
            dtype={'MarketCode': str, 'AdvParentCode': str,
                   'PCCSubCode': str},
        )
        for f in files
    ], ignore_index=True)
    raw['ym'] = pd.to_datetime(
        raw['year_month'], format='%Y-%m'
    )
    print(
        f"  raw: {len(raw):,} rows, "
        f"{raw['MarketCode'].nunique()} DMAs, "
        f"{raw['ym'].min().date()}..{raw['ym'].max().date()}"
    )
    return raw


def collapse_dma_month(raw: pd.DataFrame) -> pd.DataFrame:
    """Sum spend over all parents and PCCs per (DMA, month).
    Zero-fill the full DMA x month grid so absence of spend
    plots as zero, not as a missing line segment."""
    g = (
        raw.groupby(['MarketCode', 'ym'], as_index=False)
        ['spend_total'].sum()
    )
    dmas = sorted(raw['MarketCode'].unique())
    months = pd.date_range(
        raw['ym'].min(), raw['ym'].max(), freq='MS',
    )
    grid = pd.MultiIndex.from_product(
        [dmas, months], names=['MarketCode', 'ym'],
    ).to_frame(index=False)
    panel = grid.merge(g, on=['MarketCode', 'ym'], how='left')
    panel['spend_total'] = panel['spend_total'].fillna(0.0)
    return panel


def attach_dma_name(panel: pd.DataFrame) -> pd.DataFrame:
    lk = pd.read_csv(DMA_LOOKUP, dtype=str)
    return panel.merge(lk, on='MarketCode', how='left')


def plot_spaghetti(panel: pd.DataFrame, out: Path,
                   log: bool, title_suffix: str) -> None:
    fig, ax = plt.subplots(figsize=(13, 6))
    pivot = panel.pivot(
        index='ym', columns='MarketCode',
        values='spend_total',
    )
    for col in pivot.columns:
        ax.plot(
            pivot.index, pivot[col],
            color='gray', alpha=0.18, linewidth=0.6,
        )
    median = pivot.median(axis=1)
    ax.plot(
        median.index, median,
        color='C3', linewidth=2.0,
        label='Median DMA',
    )
    ax.set_title(
        f'Spot TV beer (PCC F310) ad spend by DMA, '
        f'2014-2023{title_suffix}'
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('Monthly spend ($)')
    if log:
        ax.set_yscale('symlog', linthresh=1e3)
    ax.legend(loc='upper right', frameon=False)
    fig.tight_layout()
    fig.savefig(out, dpi=180)
    plt.close(fig)
    print(f"Wrote {out}")


def plot_top10_labeled(panel: pd.DataFrame, out: Path) -> None:
    """Highlight the 10 DMAs with the largest cumulative
    beer spend; gray out the rest. Helps see whether the
    big-market DMAs dominate or whether mid-market DMAs
    also have meaningful local spend."""
    totals = (
        panel.groupby('MarketCode')['spend_total'].sum()
        .sort_values(ascending=False)
    )
    top10 = list(totals.head(10).index)
    pivot = panel.pivot(
        index='ym', columns='MarketCode',
        values='spend_total',
    )
    fig, ax = plt.subplots(figsize=(13, 6))
    for col in pivot.columns:
        if col in top10:
            continue
        ax.plot(
            pivot.index, pivot[col],
            color='gray', alpha=0.18, linewidth=0.6,
        )
    cmap = plt.get_cmap('tab10')
    for i, dma in enumerate(top10):
        name = (
            panel[panel['MarketCode'] == dma]
            ['DMAName_public'].iloc[0]
            if 'DMAName_public' in panel.columns else dma
        )
        ax.plot(
            pivot.index, pivot[dma],
            color=cmap(i), linewidth=1.5,
            label=f'{name} ({dma})',
        )
    ax.set_title(
        'Spot TV beer (PCC F310) ad spend by DMA, '
        '2014-2023 — top 10 DMAs labeled'
    )
    ax.set_xlabel('Month')
    ax.set_ylabel('Monthly spend ($)')
    ax.legend(loc='upper right', fontsize=8, frameon=False,
              ncol=2)
    fig.tight_layout()
    fig.savefig(out, dpi=180)
    plt.close(fig)
    print(f"Wrote {out}")


def print_summary(panel: pd.DataFrame) -> None:
    totals = panel.groupby('MarketCode')['spend_total'].sum()
    months = panel['ym'].nunique()
    n_dmas = panel['MarketCode'].nunique()
    print(
        f"\nPanel: {n_dmas} DMAs x {months} months "
        f"= {n_dmas * months:,} cells"
    )
    nonzero_share = (
        (panel['spend_total'] > 0).mean() * 100
    )
    print(
        f"  Cells with positive spend: {nonzero_share:.1f}%"
    )
    print(
        f"  Total category spend: "
        f"${panel['spend_total'].sum():,.0f}"
    )
    print(
        f"  Median DMA cumulative spend: "
        f"${totals.median():,.0f}"
    )
    print(
        f"  P10/P90 DMA cumulative spend: "
        f"${np.percentile(totals, 10):,.0f} / "
        f"${np.percentile(totals, 90):,.0f}"
    )
    print("\nTop 10 DMAs by cumulative beer spend:")
    top = totals.sort_values(ascending=False).head(10)
    lk = pd.read_csv(DMA_LOOKUP, dtype=str).set_index('MarketCode')
    for mc, sp in top.items():
        name = lk.loc[mc, 'DMAName_public'] if mc in lk.index else mc
        print(f"  {mc:>4}  {name:<25}  ${sp:>15,.0f}")


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_panel()
    panel = collapse_dma_month(raw)
    panel = attach_dma_name(panel)
    print_summary(panel)
    plot_spaghetti(
        panel, FIG_DIR / 'beer_dma_spend_spaghetti.png',
        log=False, title_suffix='',
    )
    plot_spaghetti(
        panel, FIG_DIR / 'beer_dma_spend_spaghetti_log.png',
        log=True, title_suffix=' (symlog)',
    )
    plot_top10_labeled(
        panel, FIG_DIR / 'beer_dma_spend_top10_labeled.png',
    )


if __name__ == '__main__':
    main()
