# Philadelphia Soda Tax

## Event

- **Passed**: Jun 16, 2016 (2016-Q2, t = 0)
- **Effective**: Jan 1, 2017 (2017-Q1, t = +3)
- 1.5¢ per fluid ounce, on distributors. Covers both
  sugar-sweetened and artificially sweetened drinks
  (diet included — rarely so elsewhere).

## Treated unit

Philadelphia DMA = panel `MarketCode` **104** (public
DMA 504). See [../../shared/reference/dma_lookup.csv](../../shared/reference/dma_lookup.csv)
for the lookup. Philadelphia DMA spans PA / NJ / DE;
Philly proper is ~30-40% of DMA households, so a clean
DMA-level Spot TV response is *a priori* unlikely.

## Pipeline

1. **Discovery (one-off, done)** —
   [mercury/discover_soda_brands.py](mercury/discover_soda_brands.py)
   confirmed the SSB PCC subcodes and the Philadelphia
   MarketCode.
2. **National-panel extraction** —
   [../../shared/mercury/extract_category_panel.py](../../shared/mercury/extract_category_panel.py)
   via the `run_category_panel.sh` SLURM array. Per-year
   CSVs land in
   [extraction_outputs/national_panel/](extraction_outputs/national_panel/).
3. **Canonical analyzer** —
   [code/event_study_quarters_permutation.py](code/event_study_quarters_permutation.py).
   Quarterly PPML two-way FE event study with placebo
   permutation across all 209 non-Philly DMAs. Outputs in
   [analysis_outputs/](analysis_outputs/). For the spec
   in detail and inference protocol, see
   [../../CLAUDE.md](../../CLAUDE.md).
