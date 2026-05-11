# Tax-Change × Ad-Spend Natural Experiments

A reusable framework for asking "did this tax change cause
local advertisers to cut Spot TV buys in the treated DMA(s)?"
across many tax events.

## Why

Most US sin / commodity taxes are state- or city-level
events. Nielsen Ad Intel Spot TV data is DMA × month and
goes back to 2010 on Mercury. If a tax shifts the marginal
return to advertising in a treated DMA, the response — or
non-response — is a moderately cheap test of how the
tax changes industry behavior. Stacking many events
gives at least *some* power even if each event is
underpowered on its own.

## Status

- `philly_soda_tax/` is the first concrete experiment,
  prototyped one-off. After it produces a sensible output
  this folder absorbs the working scaffolding.
- `shared/categories.py` holds the category registry —
  the meta-naming layer the user asked for. New events
  declare a category name (`SSB`, `LIQUOR`, `BEER`, ...)
  and the registry resolves to PCC subcodes, placebo
  PCCs, and large-firm parent codes.

## Anatomy of an experiment

Each experiment is one folder under `experiments/`. The
config is small:

```python
from tax_ads_experiments.shared import categories

EVENT = {
    'name':            'philly_ssb_2017',
    'category':        categories.SSB,
    'event_date':      '2017-01-01',
    'treated_market_codes':    [104],     # Nielsen MarketCode
    'comparison_market_codes': None,      # default: rest of US
    'years':           ['2014','2015','2016','2017','2018','2019'],
    # Optional overrides (e.g., Cook County also taxed
    # flavored bottled water):
    'placebo_pcc_override': None,
}
```

The shared Mercury scripts read the config and produce:

1. **Discovery** — candidate brand list and Philadelphia
   MarketCode confirmation
2. **Extraction** — disaggregated occurrence rows for the
   treated DMA(s) across `years`
3. **Local analysis** — monthly time series, pre/post
   diff, within-firm placebo, plot

## Open items

- Refactor `philly_soda_tax/` into this structure once
  the extraction returns a sensible monthly series.
- Confirm whether pre-2014 SpotTV files are readable
  from SLURM compute nodes (yes for 2014-2018; unclear
  for 2011-2013 — the WA-liquor probe didn't get that
  far before pivoting).
- Populate `BEER`, `WINE`, `TOBACCO` PCC codes via
  discovery probes.
