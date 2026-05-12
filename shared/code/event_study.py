#!/usr/bin/env python3
"""Canonical event-study analyzer for the taxes_ads
project. One script handles every registered event.

Spec (quarterly PPML, two-way FE, placebo permutation):

  spend_{d,q} ~ Poisson(exp(
      alpha_d + delta_q
      + sum_{k != -1} beta_k * 1{event_time_{d,q} = k}
                              * 1{d = treated_dma}
  ))

  alpha_d = DMA fixed effect
  delta_q = quarter fixed effect
  k       = event time in quarters (passage quarter = 0)

Inference: refit the same spec pretending each non-treated
DMA is treated, build the placebo distribution of beta_k.
Plot band = 5-95% percentile of placebos. Per-event-time
Fisher exact p-value = share of placebos with
|placebo coef| >= |treated coef|.

Usage:
    python3 shared/code/event_study.py --event philly_ssb
    python3 shared/code/event_study.py --event seattle_ssb

Outputs land in experiments/{event_label_full}/
analysis_outputs/. `event_label_full` is the per-event
folder name in experiments/ (e.g. philly_soda_tax,
seattle_ssb_tax). Set via --out-subdir if it doesn't
match the event label.
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyfixest as pf

# Project root = parent of shared/. Add shared/ to path so
# we can import events and categories without packaging.
PROJ_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJ_ROOT / 'shared'))
import categories as cat_mod  # noqa: E402
import events as evt_mod      # noqa: E402

PANEL_DIR_BY_CATEGORY = {
    'SSB': PROJ_ROOT / 'shared' / 'data' / 'ssb_panel',
    'BEER': PROJ_ROOT / 'experiments' / 'beer_descriptive'
            / 'data',
}

# Event-study window (quarters): 9 pre, 10 post.
WINDOW_PRE = 9
WINDOW_POST = 10
OMITTED_LAG = -1

def _regression_annotation(event: 'evt_mod.Event') -> str:
    """Equation + caption shown in the bottom-right of the
    plot. The reference quarter (omitted lag) shows as the
    excluded index in the sum so the spec reads cleanly."""
    ref_q = str(event.passage_quarter - 1)  # e.g. '2016Q1'
    eq = (
        r"$\log \mathcal{E}[\mathrm{spend}_{d,q}] = "
        r"\alpha_d + \delta_q + "
        r"\sum_{q' \neq " + ref_q + r"} "
        r"\gamma_{q'}^{(d^*)}\,"
        r"\mathbf{1}\{d = d^*\}\,"
        r"\mathbf{1}\{q = q'\}$"
    )
    caption = (
        r"PPML; refit for each $d^* \in \{$all 210 DMAs$\}$. "
        r"Red: $d^* = $" + event.dma_short
        + r"; band: 5-95% across the 209 placebo $d^*$."
    )
    return eq + "\n" + caption


def event_outdir(event: 'evt_mod.Event', out_subdir: str | None) -> Path:
    """Resolve the experiments/<folder>/analysis_outputs path."""
    folder = out_subdir or _default_outdir_for(event)
    out = PROJ_ROOT / 'experiments' / folder / 'analysis_outputs'
    out.mkdir(parents=True, exist_ok=True)
    return out


def _default_outdir_for(event: 'evt_mod.Event') -> str:
    # Map event.label -> existing folder names. Extend as
    # the events list grows.
    return {
        'philly_ssb': 'philly_soda_tax',
        'seattle_ssb': 'seattle_ssb_tax',
        'delaware_beer': 'delaware_beer_tax',
    }[event.label]


def load_quarterly_panel(
    event: 'evt_mod.Event',
) -> pd.DataFrame:
    category = cat_mod.get(event.category)
    pccs = (
        event.pcc_subcodes_override
        or category.pcc_subcodes_treated
    )
    parents = (
        event.parent_codes_use
        or category.parent_codes_major
    )

    panel_dir = PANEL_DIR_BY_CATEGORY[event.category]
    files = sorted(glob.glob(
        str(panel_dir / f'{event.category.lower()}_dma_month_panel_*.csv')
    ))
    if not files:
        raise FileNotFoundError(
            f"No panel files for category {event.category} "
            f"at {panel_dir}"
        )

    raw = pd.concat([
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
    raw['ym'] = pd.to_datetime(
        raw['year_month'], format='%Y-%m'
    )

    mask = raw['PCCSubCode'].isin(pccs)
    if parents:
        mask &= raw['AdvParentCode'].isin(parents)
    sub = raw[mask].copy()
    sub['quarter'] = sub['ym'].dt.to_period('Q')

    # Symmetric window in calendar quarters around passage.
    lo = event.passage_quarter - WINDOW_PRE
    hi = event.passage_quarter + WINDOW_POST
    sub = sub[
        (sub['quarter'] >= lo) & (sub['quarter'] <= hi)
    ]
    spend = (
        sub.groupby(['MarketCode', 'quarter'])['spend_total']
        .sum()
        .reset_index()
    )

    # Zero-fill full DMA x quarter grid using all DMAs that
    # appear anywhere in the underlying raw panel (not just
    # those with positive spend in the window).
    dmas = sorted(raw['MarketCode'].unique())
    qtrs = sorted(spend['quarter'].unique())
    grid = pd.MultiIndex.from_product(
        [dmas, qtrs], names=['MarketCode', 'quarter']
    ).to_frame(index=False)
    panel = grid.merge(
        spend, on=['MarketCode', 'quarter'], how='left'
    )
    panel['spend_total'] = panel['spend_total'].fillna(0.0)
    panel['event_time'] = (
        panel['quarter'].astype('int64')
        - event.passage_quarter.ordinal
    ).astype(int)
    panel['quarter_str'] = panel['quarter'].astype(str)
    return panel


def fit_one(
    panel: pd.DataFrame, treated_dma: str,
) -> pd.DataFrame:
    d = panel.copy()
    d['is_treated'] = (
        d['MarketCode'] == treated_dma
    ).astype(int)
    et_vals = sorted(d['event_time'].unique())
    et_keep = [k for k in et_vals if k != OMITTED_LAG]

    def col(k: int) -> str:
        return f'et_m{abs(k)}' if k < 0 else f'et_p{k}'

    for k in et_keep:
        d[col(k)] = (
            (d['event_time'] == k) & (d['is_treated'] == 1)
        ).astype(int)
    rhs = ' + '.join(col(k) for k in et_keep)
    fml = (
        f"spend_total ~ {rhs} | MarketCode + quarter_str"
    )
    fit = pf.fepois(fml=fml, data=d, vcov='iid')
    params = fit.coef()
    coefs = {OMITTED_LAG: 0.0}
    for k in et_keep:
        coefs[k] = float(params[col(k)])
    return pd.DataFrame(
        {'event_time': sorted(coefs),
         'coef': [coefs[k] for k in sorted(coefs)]}
    )


def run(event: 'evt_mod.Event', out_dir: Path) -> None:
    panel = load_quarterly_panel(event)
    print(
        f"[{event.label}] quarterly panel: "
        f"{len(panel):,} obs, "
        f"{panel['MarketCode'].nunique()} DMAs, "
        f"{panel['quarter'].nunique()} quarters, "
        f"zeros: {(panel['spend_total']==0).mean()*100:.1f}%"
    )
    treated_zeros = (
        panel[panel['MarketCode'] == event.treated_marketcode]
        ['spend_total'] == 0
    ).sum()
    print(
        f"  treated DMA ({event.dma_name}) quarters with "
        f"zero spend: {treated_zeros}"
    )

    print(f"Fitting {event.dma_name}...")
    treated = fit_one(panel, event.treated_marketcode)
    treated.to_csv(
        out_dir / f'event_study_ppml_quarters_{event.label}.csv',
        index=False,
    )

    other_dmas = [
        d for d in sorted(panel['MarketCode'].unique())
        if d != event.treated_marketcode
    ]
    print(f"Running {len(other_dmas)} placebo fits...")
    placebos, failed = [], []
    for i, d in enumerate(other_dmas, 1):
        try:
            est = fit_one(panel, d)
            est['placebo_dma'] = d
            placebos.append(est)
        except Exception as e:
            failed.append((d, str(e)[:80]))
        if i % 25 == 0:
            print(f"  {i}/{len(other_dmas)} done")
    print(
        f"placebos ok: {len(placebos)}, failed: {len(failed)}"
    )
    placebo_df = pd.concat(placebos, ignore_index=True)
    placebo_df.to_csv(
        out_dir / 'event_study_ppml_quarters_placebos.csv',
        index=False,
    )

    band = (
        placebo_df.groupby('event_time')['coef']
        .agg(p05=lambda s: np.percentile(s, 5),
             p50='median',
             p95=lambda s: np.percentile(s, 95))
        .reset_index()
    )
    s = treated.merge(band, on='event_time')
    fp = []
    for _, row in s.iterrows():
        ref = abs(row['coef'])
        pl = placebo_df.loc[
            placebo_df['event_time'] == row['event_time'],
            'coef',
        ]
        fp.append((pl.abs() >= ref).mean() if ref > 0 else np.nan)
    s['fisher_p'] = fp
    s.to_csv(
        out_dir / 'event_study_ppml_quarters_summary.csv',
        index=False,
    )

    _plot(s, event, out_dir, n_placebos=len(placebos))


def _plot(
    summary: pd.DataFrame,
    event: 'evt_mod.Event',
    out_dir: Path,
    n_placebos: int,
) -> None:
    s = summary.copy()
    s['calendar'] = s['event_time'].apply(
        lambda k: (event.passage_quarter + int(k)).to_timestamp()
    )

    fig, ax = plt.subplots(figsize=(11.5, 5.5))
    ax.axhline(0, color='black', linewidth=0.6)
    ax.fill_between(
        s['calendar'], s['p05'], s['p95'],
        color='gray', alpha=0.30,
        label=f'90% permutation CI ({n_placebos} placebo DMAs)',
    )
    ax.plot(
        s['calendar'], s['coef'],
        marker='o', markersize=6, linewidth=1.8,
        color='C3', label=f'{event.dma_name} (treated)',
    )

    pq = event.passage_quarter.to_timestamp()
    iq = event.impl_quarter.to_timestamp()
    ax.axvline(
        pq, color='orange', linestyle='--', linewidth=1,
        label=f'Tax passed ({event.passage_quarter}, t = 0)',
    )
    impl_offset = (
        event.impl_quarter.ordinal
        - event.passage_quarter.ordinal
    )
    ax.axvline(
        iq, color='red', linestyle='--', linewidth=1,
        label=(
            f'Tax effective ({event.impl_quarter}, '
            f't = +{impl_offset})'
        ),
    )

    ax.set_xlabel('Quarter')
    ax.set_ylabel(
        'PPML event-study coefficient (log scale),\n'
        f'relative to {event.passage_quarter - 1} (t = -1)'
    )

    # Title: tax name + "Quarterly Spot TV ad spend by
    # <parent firm names joined with Oxford comma>".
    parent_names = event.parent_names_use or ()
    if len(parent_names) >= 2:
        parents_str = (
            ', '.join(parent_names[:-1])
            + ', and ' + parent_names[-1]
        )
        ax.set_title(
            f'{event.tax_name}: '
            f'Quarterly Spot TV ad spend by {parents_str}'
        )
    elif parent_names:
        ax.set_title(
            f'{event.tax_name}: '
            f'Quarterly Spot TV ad spend by '
            f'{parent_names[0]}'
        )
    else:
        ax.set_title(
            f'{event.tax_name}: '
            f'Quarterly Spot TV {event.category.lower()} '
            f'ad spend (all brands)'
        )

    # Regression equation + caption in the lower-right.
    ax.text(
        0.985, 0.04, _regression_annotation(event),
        transform=ax.transAxes,
        ha='right', va='bottom',
        fontsize=10,
        bbox=dict(
            boxstyle='round,pad=0.5',
            facecolor='white',
            edgecolor='gray',
            alpha=0.95,
        ),
    )

    ax.legend(loc='upper left', fontsize=9, frameon=False)
    fig.tight_layout()
    out = out_dir / 'event_study_ppml_quarters.png'
    fig.savefig(out, dpi=180)
    plt.close(fig)
    print(f"Wrote {out}")
    print(
        s[['event_time', 'coef', 'p05', 'p95', 'fisher_p']]
        .round(3).to_string(index=False)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--event', required=True,
        choices=sorted(evt_mod.REGISTRY),
        help='Event label from shared/events.py',
    )
    parser.add_argument(
        '--out-subdir', default=None,
        help='experiments/<this>/analysis_outputs '
             '(default: mapped from event label)',
    )
    args = parser.parse_args()
    event = evt_mod.get(args.event)
    out_dir = event_outdir(event, args.out_subdir)
    run(event, out_dir)


if __name__ == '__main__':
    main()
