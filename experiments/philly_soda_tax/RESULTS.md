# Philly Soda Tax — Empirical Results

Single source of truth for what we learned from the
Philadelphia Beverage Tax event. For pipeline / how to
re-run, see [README.md](README.md). For the formal model
behind the sign question, see
[../../paper/model.tex](../../paper/model.tex).

## Event

- **Passed**: Jun 16, 2016
- **Effective**: Jan 1, 2017
- 1.5¢/oz on distributors. Covers sugar-sweetened AND
  artificially sweetened drinks (diet included).
- Philadelphia DMA = MarketCode 104. Philly proper is
  ~30-40% of DMA households.

## Headline result

**Philly Spot TV SSB ad spend rose ~$11K/month relative
to Pittsburgh after the tax** — opposite of the naive
"firms cut buys in treated market" prediction, and
consistent across two cut-date specifications.

Specification: 18-month symmetric pre/post window
around each cut date. Outcome: total Spot TV ad spend
from top-4 SSB advertisers (Coca-Cola, PepsiCo, Dr
Pepper Snapple, Nestle), F221/F222/F223 PCC subcodes
only. Comparison units: Pittsburgh (MarketCode 108) and
rest-of-US (DMA-level mean across non-Philly). Panel
zero-filled over the full DMA × month grid.

| Spec | Philly Δ | Pittsburgh Δ | Rest-of-US Δ | DiD vs Pitt | DiD vs ROU |
|---|---|---|---|---|---|
| A. Passage cut (Jun 2016) | +$24K | +$13K | +$6K | **+$11K** | +$18K |
| B. Effective cut (Jan 2017) | +$15K | +$4K | +$2K | **+$10K** | +$13K |

Source: [analysis_outputs/did_18mo_summary.csv](analysis_outputs/did_18mo_summary.csv).

## Plots

| Plot | What it shows |
|---|---|
| [did_18mo_A_passage_jun2016_monthly.png](analysis_outputs/did_18mo_A_passage_jun2016_monthly.png) | Monthly series in the 36-month window around Jun 2016 passage, three lines (Philly / Pittsburgh / rest-of-US) |
| [did_18mo_A_passage_jun2016_means.png](analysis_outputs/did_18mo_A_passage_jun2016_means.png) | Pre/post bar chart for passage spec |
| [event_study_A_passage_jun2016_top4.png](analysis_outputs/event_study_A_passage_jun2016_top4.png) | Two-way FE event study (calendar time), top-4 parents combined |
| [event_study_A_passage_jun2016_coke.png](analysis_outputs/event_study_A_passage_jun2016_coke.png) | Same, Coke only |
| [event_study_A_passage_jun2016_pepsi.png](analysis_outputs/event_study_A_passage_jun2016_pepsi.png) | Same, Pepsi only |
| [national_panel_three_series.png](analysis_outputs/national_panel_three_series.png) | Full 2010-2023 series (long-window overview) |

Plots from earlier specs (full-post-period DiD, Jan 2017
cut as a separate spec, legacy single-DMA outputs) are
in [analysis_outputs/_legacy/](analysis_outputs/_legacy/).

## Caveats (in plain English)

1. **Single treated DMA.** Analytical clustered SEs are
   not credible. Plot bars show ±1.96 × OLS SE for visual
   scale only. Inference needs wild-cluster bootstrap or
   permutation — not done.
2. **Spot TV only.** A null (or wrong-sign result) in
   Spot TV is consistent with reallocation to channels
   we can't measure (geo-targeted digital, CTV,
   sponsorships, anti-tax political ads booked under the
   brand's ad spend). The prior Baltimore paper found
   internet ads-per-100-HH **fell** 0.42 post-tax — a
   substitution signal Spot TV can't see.
3. **DMA-tax mismatch.** Philly DMA spans PA/NJ/DE;
   taxed area is 30-40% of DMA HHs. Firms can't
   surgically cut Philly-proper exposure without
   dropping the whole DMA, so a Spot TV response is *a
   priori* unlikely on geographic grounds alone.
4. **Wrong-sign possibilities.** The +$11K/month could
   be: anti-tax counter-campaigns (real and documented),
   pricing-promotion ads to offset the consumer price
   hike, Becker-Murphy persuasive ads rotating demand
   flatter to support pass-through, or noise.
5. **2011-2013 data hole.** Permission denied on
   `/kilts/adintel/{2011,2012,2013}/Occurrences/SpotTV.tsv`
   from the compute node, despite 2010 and 2014+ being
   readable. Pre window in our DiD spans 2014-2016;
   2011-2013 not used.

## Theory connection

[paper/model.tex](../../paper/model.tex) derives the
sign condition for a per-unit excise tax:

$$\frac{dA^*}{dt} > 0 \;\iff\;
D_{pA} > \frac{(1-\rho)\, D_A}{m\, \rho}$$

Multiplicatively separable demand $D(p, A) = f(p)g(A)$
(sb_incidence sportsbook setup) has $D_{pA} = f'g' < 0$,
fails the condition: $dA^*/dt < 0$.

Persuasive demand ($D_{pA} > 0$, ads rotate demand
flatter at higher prices) satisfies it: $dA^*/dt > 0$.

The Philly point estimate is in the second regime. With
empirically full pass-through ($\rho \approx 1$, Seiler-
Tuchman-Yao), the sign condition collapses to
$D_{pA} > 0$ — i.e., the SSB result reads off a positive
cross-partial, consistent with persuasive over
informative advertising for beverages.

## Prior literature (discovered post-hoc)

Independent team (Philly vs Baltimore, Jan 2016 - Dec
2019) using a controlled interrupted time series with
segmented linear regression on quarterly beverage ad
expenditures reports **no significant differences in
taxed beverage advertising expenditures**. Decomposed by
channel: Spot TV null at all stages; internet
ads-per-100-HH fall by 0.42 immediately post-tax;
baseline Spot TV share of taxed beverages 28 pp higher
in Philly than Baltimore.

Full citation: hunt outstanding. Pattern matches 2024
Prev Med / AJPM. Acknowledge in paper lit review as
prior null replication.

## Implications for paper

Single-event Philly is now a **replication, not a
contribution**. Project contribution lives in:

1. **Stacked SSB events the prior paper doesn't cover**:
   Cook County (Aug 2017 impl, Dec 2017 repeal —
   uniquely clean), Boulder (Jul 2017), Seattle
   (Jan 2018), Bay Area (multiple events in DMA 407).
2. **Cross-category sign tests** of $D_{pA}$: SSB
   vs sportsbook vs liquor vs beer vs tobacco. Different
   categories likely sit on different sides of the
   threshold.
3. **The theory framing** in paper/model.tex: the sign
   of $dA^*/dt$ is informative about the persuasive-vs-
   informative ad debate; stacked events give a panel
   of sign tests.
