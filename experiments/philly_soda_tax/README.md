# Philadelphia Soda Tax — Ad Spend Exploration

Self-contained mini-project. Question: did the Philadelphia
Beverage Tax (effective Jan 1, 2017; 1.5¢/oz on sugar-
and artificially sweetened drinks) cause sweetened-beverage
advertisers to cut Spot TV buys in the Philadelphia DMA?

## What the tax actually did

- Effective Jan 1, 2017
- 1.5¢ per fluid ounce, on distributors
- Covers sugar-sweetened AND artificially sweetened drinks
  (diet Coke etc. are included — diet is rarely taxed
  elsewhere)
- Most-studied US sugar tax (Cawley et al., Roberto et al.)

## Caveat the user flagged

- Philadelphia DMA spans PA / NJ / DE
- Philly proper is only ~30–40% of DMA households
- Advertisers may not cut DMA-wide buys for a 30–40% shock
- So expect a noisy / small effect at best; the prior on
  detecting anything is low

## Pipeline

1. **Discovery (Mercury)** — `mercury/discover_soda_brands.py`
   - Confirms Spot TV file access from compute node for years
     2014–2018 (login-node access blocked for pre-2018 files;
     SLURM may have elevated read)
   - Dumps Spot TV column layouts for those years
   - Finds Philadelphia DMA `MarketCode` from
     `/kilts/adintel/Master_Files/Latest/Market.tsv`
   - Filters Brand reference for SSB / soda brands and parent
     beverage companies, writes `soda_brand_candidates.csv`
2. **Extraction (Mercury)** — TBD after discovery
   - Mirror the v2 sportsbook extractor in
     `code/data_pipeline/ads_elasticity_aggregation/`
   - Filter Spot TV occurrences to candidate brand codes and
     the Philadelphia `MarketCode`
3. **Local analysis** — `code/analyze_philly_diff.py`
   - Monthly Spot TV ad-spend series, Jan 2017 cut line
   - Pre/post means, simple within-DMA comparison

## Status

Only discovery runs first. Do not submit extraction until
discovery confirms (a) Spot TV access from compute nodes,
(b) which brand codes to pull, and (c) the Philadelphia
`MarketCode`.
