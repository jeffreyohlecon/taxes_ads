#!/usr/bin/env python3
"""
Two-way FE event study: Philly SSB Spot TV spend response
to the Philadelphia Beverage Tax.

Spec:
  spend_dt = alpha_d + delta_t
           + sum_k beta_k * 1{event_time_dt = k}
                          * 1{d = Philadelphia}
           + e_dt

  alpha_d   = DMA fixed effect
  delta_t   = year-month fixed effect
  event_t   = calendar month minus event month (in months)
  beta_k    = event-study coefficient at lag/lead k

Outcome: top-4-parent SSB Spot TV spend (Coca-Cola,
PepsiCo, Dr Pepper Snapple, Nestle), F221/F222/F223 only,
levels in $. DMA-months with no spend filled with zero
(per CLAUDE.md: no log(x+1)).

Donor pool: all non-Philadelphia DMAs (Pittsburgh and
the other 208).

Two specs:
  - Tax effective (Jan 2017)
  - Tax passed   (Jun 2016, anticipation)

No standard errors plotted. With one treated unit,
clustered SEs are not credible; follow-up should be
wild-cluster bootstrap or permutation inference.
"""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

PROJ_ROOT = Path(__file__).resolve().parents[1]
PANEL_DIR = PROJ_ROOT / 'extraction_outputs' / 'national_panel'
OUT_DIR = PROJ_ROOT / 'analysis_outputs'

TREATED_DMA = '104'  # Philadelphia
TREATED_PCC = {'F221', 'F222', 'F223'}
TOP_PARENTS = {'11440', '14842', '2531252', '1809'}

# Single treatment date: passage (Jun 2016). Implementation
# (Jan 2017) falls at event_time = +7 inside the post period
# and is drawn as a vertical reference line, not a second
# specification. The B-spec (Jan 2017 as cut) lives in
# _legacy/ for now.
EVENT_DATE = pd.Timestamp('2016-06-01')
EVENT_LABEL = 'A_passage_jun2016'
IMPLEMENTATION_DATE = pd.Timestamp('2017-01-01')
EVENT_WINDOW = range(-18, 19)  # 18 pre, 18 post
OMITTED_LAG = -1


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


def build_dma_month_panel(
    raw: pd.DataFrame,
    parent_filter: set[str] | None = None,
) -> pd.DataFrame:
    parents = parent_filter or TOP_PARENTS
    f = raw[
        raw['PCCSubCode'].isin(TREATED_PCC)
        & raw['AdvParentCode'].isin(parents)
    ]
    spend = (
        f.groupby(['MarketCode', 'ym'])['spend_total']
        .sum()
        .reset_index()
    )

    # Full cross-product DMA x year-month, zero-filled.
    dmas = sorted(raw['MarketCode'].unique())
    months = sorted(raw['ym'].unique())
    grid = pd.MultiIndex.from_product(
        [dmas, months], names=['MarketCode', 'ym']
    ).to_frame(index=False)
    panel = grid.merge(
        spend, on=['MarketCode', 'ym'], how='left'
    )
    panel['spend_total'] = panel['spend_total'].fillna(0.0)
    panel['treated'] = (
        panel['MarketCode'] == TREATED_DMA
    ).astype(int)
    return panel


def event_time_months(
    months: pd.Series, event_date: pd.Timestamp
) -> pd.Series:
    delta = (months.dt.year - event_date.year) * 12 + (
        months.dt.month - event_date.month
    )
    return delta.astype(int)


def run_event_study(
    panel: pd.DataFrame, event_date: pd.Timestamp,
) -> pd.DataFrame:
    p = panel.copy()
    p['event_time'] = event_time_months(p['ym'], event_date)
    # Restrict to the 18-month symmetric window (matches
    # the DiD spec in did_philly_18mo.py).
    lo, hi = min(EVENT_WINDOW), max(EVENT_WINDOW)
    p = p[(p['event_time'] >= lo) & (p['event_time'] <= hi)].copy()

    p['ym_str'] = p['ym'].dt.strftime('%Y-%m')
    p['et_factor'] = p['event_time'].astype(int)

    formula = (
        f"spend_total ~ "
        f"C(et_factor):treated + C(MarketCode) + C(ym_str)"
    )
    model = smf.ols(formula, data=p).fit()

    # Pull all event-time × treated coefficients, then
    # rebase so the OMITTED_LAG coefficient is 0 (the
    # reference) — this works regardless of which column
    # statsmodels dropped for collinearity.
    rows = []
    for k in sorted(p['et_factor'].unique()):
        name = f"C(et_factor)[{k}]:treated"
        if name in model.params.index:
            rows.append({
                'event_time': k,
                'coef_raw': model.params[name],
                'se': model.bse[name],
            })
        else:
            # Dropped due to collinearity; coef is 0 by
            # construction.
            rows.append({
                'event_time': k,
                'coef_raw': 0.0,
                'se': 0.0,
            })
    df = pd.DataFrame(rows).sort_values('event_time')
    ref_val = df.loc[
        df['event_time'] == OMITTED_LAG, 'coef_raw'
    ].iloc[0]
    df['coef'] = df['coef_raw'] - ref_val
    return df


def event_time_to_calendar(k: int) -> pd.Timestamp:
    """Map event-time month index back to calendar month."""
    return EVENT_DATE + pd.DateOffset(months=int(k))


def plot_event_study(
    coefs: pd.DataFrame, parent_label: str, out: Path,
) -> None:
    coefs = coefs.copy()
    coefs['calendar'] = coefs['event_time'].apply(
        event_time_to_calendar
    )

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.axhline(0, color='black', linewidth=0.6)
    ax.axvline(
        EVENT_DATE, color='orange', linestyle='--',
        linewidth=1, label='Tax passed (Jun 2016)',
    )
    ax.axvline(
        IMPLEMENTATION_DATE, color='red', linestyle='--',
        linewidth=1, label='Tax effective (Jan 2017)',
    )
    coef_k = coefs['coef'] / 1e3
    se_k = coefs['se'] / 1e3
    ax.errorbar(
        coefs['calendar'], coef_k, yerr=1.96 * se_k,
        fmt='o', markersize=4, linewidth=1.0,
        color='steelblue', ecolor='steelblue', alpha=0.85,
        capsize=2,
    )
    ax.set_xlabel('Month')
    ax.set_ylabel(
        'Philly SSB Spot TV spend, '
        'relative to t=−1 ($K, two-way FE)'
    )
    ax.set_title(
        f'Event study (calendar time): '
        f'passage = Jun 2016 [{parent_label}]\n'
        f'Bars = ±1.96 × OLS SE — NOT properly clustered '
        f'(single treated unit); for visual scale only'
    )
    ax.legend(loc='upper left', fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)
    print(f"Wrote {out}")


PARENT_SETS = {
    'top4': TOP_PARENTS,
    'coke': {'11440'},
    'pepsi': {'14842'},
}


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = load_panel()

    for parent_label, parent_set in PARENT_SETS.items():
        panel = build_dma_month_panel(raw, parent_set)
        share_pos = (panel['spend_total'] > 0).mean() * 100
        print(
            f"\n[{parent_label}] panel: {len(panel):,} obs, "
            f"{panel['MarketCode'].nunique()} DMAs, "
            f"{panel['ym'].nunique()} months, "
            f"spend>0: {share_pos:.1f}%"
        )

        tag = f"{EVENT_LABEL}_{parent_label}"
        print(f"\n=== {tag} (event={EVENT_DATE.date()}) ===")
        coefs = run_event_study(panel, EVENT_DATE)
        coefs['parent_set'] = parent_label
        coefs.to_csv(
            OUT_DIR / f'event_study_{tag}.csv',
            index=False,
        )
        print(
            coefs[['event_time', 'coef']]
            .to_string(index=False)
        )
        plot_event_study(
            coefs, parent_label,
            OUT_DIR / f'event_study_{tag}.png',
        )


if __name__ == '__main__':
    main()
