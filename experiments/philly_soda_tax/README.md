# Philadelphia Soda Tax — Ad Spend Exploration

First concrete event in the framework. Question: did the
Philadelphia Beverage Tax cause sweetened-beverage
advertisers to cut Spot TV buys in the Philadelphia DMA?

## What the tax actually did

- **Passed**: Jun 16, 2016
- **Effective**: Jan 1, 2017
- 1.5¢ per fluid ounce, on distributors
- Covers sugar-sweetened AND artificially sweetened drinks
  (diet sodas are included — rarely so elsewhere)
- Most-studied US sugar tax (Cawley et al., Roberto et al.)

The 6-month gap between "passed" and "effective" gives an
anticipation window worth treating as its own period in
the event study.

## Caveat the user flagged

- Philadelphia DMA (MarketCode 104) spans PA / NJ / DE
- Philly proper is only ~30–40% of DMA households
- Advertisers may not cut DMA-wide buys for a 30–40% shock
- Prior on detecting a clean response is low; document the
  null carefully if it lands that way

## Pipeline (current)

1. **Discovery (one-off, done)** —
   `mercury/discover_soda_brands.py` confirmed the SSB
   PCC subcodes (F221/F222/F223 treated, F224 placebo)
   and the Philadelphia MarketCode (104).
2. **National-panel extraction (the real one)** —
   `../../shared/mercury/extract_category_panel.py`
   (called via `run_category_panel.sh` SLURM array).
   Pulls all DMAs, all years 2010-2023, filtered only on
   SSB PCC subcodes. Emits a collapsed
   `(MarketCode × year_month × AdvParentCode × PCCSubCode)`
   panel — one CSV per year. **Per project policy
   ([../../CLAUDE.md](../../CLAUDE.md)) this replaces the
   one-event single-DMA extraction below.**
3. **Local analysis** — see "Analyzers" below.

### Deprecated: one-event single-DMA extractor

`mercury/_legacy/extract_philly_spend.py` was the
original first-pass extractor (job 261336, May 2026). It
filters at extraction time to `MarketCode == 104` and
emits disaggregated occurrence rows for 2014-2018 only.
Superseded by the national-panel extractor — pulling all
DMAs once costs the same wall-time and yields
counterfactuals for free. Parked under `_legacy/` for
reference; do not run for new events. See
[mercury/_legacy/README.md](mercury/_legacy/README.md).

## Analyzers

Four scripts in `code/`, each with a specific purpose:

| Script | Input | Output | Use when |
|---|---|---|---|
| `analyze_philly_diff.py` | single-DMA occurrence CSVs from the legacy extractor | monthly plot with Jan 2017 cut line, F221-F223 vs F224 (bottled water) placebo, pre/post means | first-pass within-DMA sanity check; legacy |
| `analyze_philly_top4.py` | single-DMA occurrence CSVs | top-4 parents (Coke/Pepsi/DPS/Nestle), monthly plot by parent, two cut dates (Jun 2016 vs Jan 2017) | check anticipation window vs effective date; reduce noise from tail advertisers |
| `analyze_philly_seasonal.py` | single-DMA occurrence CSVs | Coca-Cola and PepsiCo, two-panel month-of-year view with one line per year | spot seasonality and whether 2017 line departs from the seasonal envelope |
| `analyze_national_panel.py` | **collapsed national panel CSVs** from the new extractor | Philly vs Pittsburgh vs rest-of-US monthly, Philly−Pittsburgh difference bars, DiD summary with anticipation handling | the live answer to the research question; produces the figure that goes in the paper |

`analyze_national_panel.py` is the one the rest of the
pipeline points at; the others are historical / diagnostic.

## Comparison DMAs

- **Pittsburgh (108)** — best single comparison: same
  state, no tax, similar Rust Belt demographics.
- **New York (101)** — considered, dropped: Bloomberg-era
  anti-soda regulation lingers in ad-market behavior.
- Rest of US (mean across DMAs not in PA) — donor pool
  for synthetic-control style robustness.

## Status (2026-05-11)

- Legacy single-DMA extraction done (job 261336): the
  five `philly_soda_dma_spend_{year}.csv` files for 2014-
  2018 are in `extraction_outputs/`. Within-Philly result:
  treated SSB spend rose ~50-60% in 2017+ vs 2014-pre.
  Sign opposite of the simple hypothesis, but **no
  comparison DMA in that data** — the rise could be
  national.
- National-panel extraction in flight (job 261414,
  pyarrow array 2010-2023). On completion, scp the
  per-year `ssb_dma_month_panel_*.csv` into
  `extraction_outputs/national_panel/` and run
  `analyze_national_panel.py`.

## Open items

- After national panel lands, look at Philly minus
  Pittsburgh and Philly minus rest-of-US, both with the
  Jun-2016 anticipation cut.
- Also useful: check whether F223 (sweetened non-
  carbonated) is reclassified across reference years —
  it disappears in the 2023 panel.
- Filter out the cross-PCC contamination in the panel:
  the candidate-brand mask matches any of Prim/Scnd/Ter
  but the PCC merge is on PrimBrandCode only, so rows
  with a SSB secondary brand and a non-SSB primary leak
  in. Local filter: `PCCSubCode ∈ {F221, F222, F223, F224}`.
