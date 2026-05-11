#!/usr/bin/env python3
"""
Event study (log scale via PPML) of Philly SSB Spot TV
spend response to the Philadelphia Beverage Tax, with
permutation/randomization inference instead of analytical
clustered SEs.

Why PPML, not log(spend):
  22% of DMA-months in the 5-yr window have zero SSB spend
  from the top-4 parents. Dropping zero-DMAs leaves only
  19 donors out of 210. PPML keeps all 210, handles zeros,
  and coefficients are still log-scale (exp(beta_k) is the
  multiplicative effect on E[spend]).

Why permutation, not clustered SEs:
  With one treated unit (Philadelphia), analytical
  clustered SEs are not valid even with 209 clusters --
  the cluster-robust asymptotics need variation across
  treated units. Permutation (Conley-Taber / Abadie):
  refit pretending each non-Philly DMA is treated, build
  the placebo distribution of beta_k, and compare Philly
  to it.

Spec:
  spend_{d,t} ~ Poisson(exp(alpha_d + delta_t
                             + sum_k beta_k 1{ET_{d,t}=k}
                                          1{d = TREATED})),
  ET_{d,t} = months since June 2016 (tax passage),
  omitted lag: k = -1.

Inference:
  Fisher exact p_k = share of placebos with |beta_k^c|
                     >= |beta_k^Philly|.
  Band: 5th-95th percentile of placebo beta_k distribution.
"""

from __future__ import annotations

import glob
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyfixest as pf

PROJ_ROOT = Path(__file__).resolve().parents[1]
PANEL_DIR = PROJ_ROOT / 'extraction_outputs' / 'national_panel'
OUT_DIR = PROJ_ROOT / 'analysis_outputs'

TREATED_DMA = '104'  # Philadelphia
TOP_PARENTS = {'11440', '14842', '2531252', '1809'}
TREATED_PCC = {'F221', 'F222', 'F223'}

EVENT_DATE = pd.Timestamp('2016-06-01')  # passage
IMPL_DATE = pd.Timestamp('2017-01-01')   # effective
EVENT_WINDOW = range(-18, 19)
OMITTED_LAG = -1


def load_dma_month_panel() -> pd.DataFrame:
    files = sorted(glob.glob(
        str(PANEL_DIR / 'ssb_dma_month_panel_*.csv')
    ))
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

    sub = raw[
        raw['PCCSubCode'].isin(TREATED_PCC)
        & raw['AdvParentCode'].isin(TOP_PARENTS)
    ]
    spend = (
        sub.groupby(['MarketCode', 'ym'])['spend_total']
        .sum()
        .reset_index()
    )

    spend = spend[
        (spend['ym'] >= '2014-01-01')
        & (spend['ym'] <= '2018-12-31')
    ]
    dmas = sorted(spend['MarketCode'].unique())
    months = sorted(spend['ym'].unique())
    grid = pd.MultiIndex.from_product(
        [dmas, months], names=['MarketCode', 'ym']
    ).to_frame(index=False)
    panel = grid.merge(
        spend, on=['MarketCode', 'ym'], how='left'
    )
    panel['spend_total'] = panel['spend_total'].fillna(0.0)
    panel['event_time'] = (
        (panel['ym'].dt.year - EVENT_DATE.year) * 12
        + (panel['ym'].dt.month - EVENT_DATE.month)
    ).astype(int)
    lo, hi = min(EVENT_WINDOW), max(EVENT_WINDOW)
    panel = panel[
        (panel['event_time'] >= lo)
        & (panel['event_time'] <= hi)
    ].copy()
    panel['ym_str'] = panel['ym'].dt.strftime('%Y-%m')
    return panel


def fit_one(
    panel: pd.DataFrame, treated_dma: str,
) -> pd.DataFrame:
    """Fit PPML event study with one DMA as treated.
    Returns dataframe of event_time -> coef (rebased so
    OMITTED_LAG = 0)."""
    d = panel.copy()
    d['is_treated'] = (
        d['MarketCode'] == treated_dma
    ).astype(int)
    # event_time × treated interactions
    et_vals = sorted(d['event_time'].unique())
    coefs = {}
    # Build interaction columns only for k != OMITTED_LAG
    et_keep = [k for k in et_vals if k != OMITTED_LAG]
    def col(k):
        return f'et_m{abs(k)}' if k < 0 else f'et_p{k}'
    for k in et_keep:
        d[col(k)] = (
            (d['event_time'] == k) & (d['is_treated'] == 1)
        ).astype(int)
    rhs = ' + '.join([col(k) for k in et_keep])
    fml = f"spend_total ~ {rhs} | MarketCode + ym_str"
    fit = pf.fepois(fml=fml, data=d, vcov='iid')
    params = fit.coef()
    for k in et_keep:
        coefs[k] = float(params[col(k)])
    coefs[OMITTED_LAG] = 0.0
    out = pd.DataFrame(
        {'event_time': sorted(coefs.keys()),
         'coef': [coefs[k] for k in sorted(coefs.keys())]}
    )
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    panel = load_dma_month_panel()
    print(
        f"panel: {len(panel):,} obs, "
        f"{panel['MarketCode'].nunique()} DMAs, "
        f"{panel['ym'].nunique()} months, "
        f"zeros: {(panel['spend_total']==0).mean()*100:.1f}%"
    )

    # 1) Philly point estimates
    print("\nFitting Philadelphia ...")
    philly = fit_one(panel, TREATED_DMA)
    philly.to_csv(
        OUT_DIR / 'event_study_ppml_philly.csv',
        index=False,
    )

    # 2) Permutation over all other DMAs
    other_dmas = [
        d for d in sorted(panel['MarketCode'].unique())
        if d != TREATED_DMA
    ]
    print(
        f"\nRunning {len(other_dmas)} placebo fits ..."
    )
    placebos = []
    failed = []
    for i, d in enumerate(other_dmas, 1):
        try:
            est = fit_one(panel, d)
            est['placebo_dma'] = d
            placebos.append(est)
        except Exception as e:
            failed.append((d, str(e)[:80]))
        if i % 25 == 0:
            print(f"  {i}/{len(other_dmas)} done "
                  f"(failed: {len(failed)})")
    print(f"placebo fits ok: {len(placebos)}, "
          f"failed: {len(failed)}")
    if failed:
        print("first failures:", failed[:3])

    placebo_df = pd.concat(placebos, ignore_index=True)
    placebo_df.to_csv(
        OUT_DIR / 'event_study_ppml_placebos.csv',
        index=False,
    )

    # 3) Build inference summary
    band = (
        placebo_df.groupby('event_time')['coef']
        .agg(p05=lambda s: np.percentile(s, 5),
             p25=lambda s: np.percentile(s, 25),
             p50='median',
             p75=lambda s: np.percentile(s, 75),
             p95=lambda s: np.percentile(s, 95))
        .reset_index()
    )
    summary = philly.merge(band, on='event_time')
    # Fisher exact two-sided p-value: share of placebos
    # with |placebo_coef| >= |philly_coef|
    fisher_p = []
    for _, row in philly.iterrows():
        k = row['event_time']
        ref = abs(row['coef'])
        pl_k = placebo_df.loc[
            placebo_df['event_time'] == k, 'coef'
        ]
        if len(pl_k) == 0 or ref == 0:
            fisher_p.append(np.nan)
        else:
            fisher_p.append((pl_k.abs() >= ref).mean())
    summary['fisher_p'] = fisher_p
    summary.to_csv(
        OUT_DIR / 'event_study_ppml_summary.csv',
        index=False,
    )

    # 4) Plot
    summary['calendar'] = summary['event_time'].apply(
        lambda k: EVENT_DATE + pd.DateOffset(months=int(k))
    )
    fig, ax = plt.subplots(figsize=(11.5, 5.5))
    ax.axhline(0, color='black', linewidth=0.6)
    ax.fill_between(
        summary['calendar'], summary['p05'], summary['p95'],
        color='gray', alpha=0.25,
        label='Placebo 5–95% band (209 DMAs)',
    )
    ax.fill_between(
        summary['calendar'], summary['p25'], summary['p75'],
        color='gray', alpha=0.35,
        label='Placebo 25–75% band',
    )
    ax.plot(
        summary['calendar'], summary['coef'],
        marker='o', markersize=4, linewidth=1.8,
        color='C3', label='Philadelphia (treated)',
    )
    ax.axvline(EVENT_DATE, color='orange',
               linestyle='--', linewidth=1,
               label='Tax passed (Jun 2016)')
    ax.axvline(IMPL_DATE, color='red',
               linestyle='--', linewidth=1,
               label='Tax effective (Jan 2017)')
    ax.set_xlabel('Month')
    ax.set_ylabel(
        'Event-study coefficient (log scale, PPML)\n'
        'relative to t = −1'
    )
    ax.set_title(
        'Philadelphia Beverage Tax: SSB Spot TV spend '
        'response, top-4 parents\n'
        'PPML two-way FE; bands = placebo distribution '
        'over 209 non-Philly DMAs'
    )
    ax.legend(loc='lower left', fontsize=9, frameon=False)
    fig.tight_layout()
    out = OUT_DIR / 'event_study_ppml_philly.png'
    fig.savefig(out, dpi=180)
    plt.close(fig)
    print(f"\nWrote {out}")
    print(f"Wrote {OUT_DIR / 'event_study_ppml_summary.csv'}")


if __name__ == '__main__':
    main()
