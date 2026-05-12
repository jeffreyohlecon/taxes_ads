# Seattle SSB Tax

## Event

- **Passed**: Jun 5, 2017 (City Council 7-1; 2017-Q2,
  t = 0)
- **Effective**: Jan 1, 2018 (2018-Q1, t = +3)
- $0.0175/oz on distributors (17% larger than Philly's
  $0.015/oz).
- Excludes diet drinks and 100% juice; treated PCC set
  matches Philly (F221/F222/F223).
- Still in effect (>1yr duration; in sample under the
  short-tax exclusion rule).

## Treated unit

Seattle-Tacoma DMA = panel `MarketCode` **419** (public
DMA 819). See [../../shared/reference/dma_lookup.csv](../../shared/reference/dma_lookup.csv).
Seattle proper is a much larger share of the
Seattle-Tacoma DMA than Philly proper is of the Philly
DMA, so the geographic dilution concern is weaker here.

## Pipeline

1. **National-panel extraction** — already done by the
   Philly event. The per-year SSB CSVs at
   [../philly_soda_tax/extraction_outputs/national_panel/](../philly_soda_tax/extraction_outputs/national_panel/)
   are the canonical national panel; this experiment
   reaches them via the
   [national_panel](national_panel) symlink.
2. **Canonical analyzer** —
   [code/event_study_quarters_permutation.py](code/event_study_quarters_permutation.py).
   Same spec as Philly (quarterly PPML two-way FE with
   placebo permutation across 209 non-Seattle DMAs). For
   spec and inference protocol see
   [../../CLAUDE.md](../../CLAUDE.md).
