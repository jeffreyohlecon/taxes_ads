# Tax-Change × Ad-Spend Natural Experiments

A reusable framework for asking "did this tax change cause
national advertisers to cut Spot TV buys in the treated
DMA(s)?" across many tax events.

## Why

Most US sin / commodity taxes are state- or city-level
events. Nielsen Ad Intel Spot TV data on Mercury goes
back to 2010 and is DMA × month. If a tax shifts the
marginal return to advertising in a treated DMA, the
response — or non-response — is a moderately cheap test
of how the tax changes industry behavior. Stacking many
events gives some power even if each one is underpowered.

## Repo shape

```
shared/
  categories.py                  # PCC subcodes + parents per category
  mercury/
    extract_category_panel.py    # the extractor — see "Extraction policy"
    run_category_panel.sh        # SLURM array wrapper

experiments/
  philly_soda_tax/               # first concrete event; see its README
    code/                        # local analyzers
    mercury/                     # (legacy, superseded) one-off extractors
    extraction_outputs/          # CSVs from Mercury (gitignored)
    analysis_outputs/            # plots and summary tables (gitignored)
```

## Extraction policy

**Brand is the only selection filter at extraction time.
Never pre-filter on year or DMA.** Each Mercury pull for
a category scans 2010-2023, all 210 DMAs, filtered only
on PCC subcodes from `shared/categories.py`, and emits a
collapsed panel:

  `(MarketCode × year_month × AdvParentCode × PCCSubCode)`
  → `(spend_total, units_total, n_spots)`

One CSV per year. Wall-time is dominated by the SpotTV
file scan, not the row filter, so pulling all DMAs costs
the same as pulling one but yields a national panel
reusable for every event in that category. Per-event
slicing, comparison-DMA selection, synthetic-control
donor pools, and rest-of-US aggregates all happen
locally — no second Mercury job. Full rationale lives
in [CLAUDE.md](CLAUDE.md).

## Standard workflow for a new event

1. **Pick a category** (or add one to `shared/categories.py`
   with its PCC subcodes — run a discovery probe first to
   confirm the PCCs are right).
2. **National-panel extraction on Mercury** (one-time per
   category — skip if already pulled):

   ```bash
   # On the local machine: sync repo to Mercury home.
   rsync -av --exclude='.git' \
       --exclude='extraction_outputs' \
       --exclude='analysis_outputs' \
       . johl@mercury:~/taxes_ads/

   # On Mercury, 14 years in parallel as a SLURM array:
   sbatch --array=0-13 --export=CATEGORY=SSB \
       ~/taxes_ads/shared/mercury/run_category_panel.sh
   # task idx i → year 2010+i (Mercury caps array IDs at 1001).
   ```

3. **scp outputs back**:

   ```bash
   mkdir -p experiments/<event>/extraction_outputs/national_panel
   scp 'johl@mercury:~/ssb_dma_month_panel_*.csv' \
       experiments/<event>/extraction_outputs/national_panel/
   ```

4. **Local event analysis**: each event folder owns its
   own analyzer that loads the panel, picks treated +
   comparison DMA(s), computes the event-study /
   pre-post / DiD, and writes plots and summary tables.
   See `experiments/philly_soda_tax/code/` for the
   first working example.

## Parallelism on Mercury

Default to a SLURM array: one task per year. Per-task
`--mem=64G`, `--cpus-per-task=8`, `--partition=highmem`.
PyArrow CSV reader is multithreaded and uses those cores.
The full 2010-2023 SSB pull runs in ~max(slowest year)
wall-time, not sum.

Mercury limits (verified 2026-05-11): `MaxArraySize=1001`,
`MaxJobCount=10000`, `qos_highmem MaxJobs=360` concurrent.
Details in [CLAUDE.md](CLAUDE.md).

## Status

- **`shared/mercury/extract_category_panel.py`** (v1) —
  spend + units + n_spots. Handles both Spot TV file
  layouts: ≤2021 legacy 23-col single file, ≥2022
  standard 18-col monthly files.
- **GRPs follow-up** in [issue #2](https://github.com/jeffreyohlecon/taxes_ads/issues/2) —
  v2 would join impressions and universe to emit
  `grp_total`. v1 carries `units_total` as a partial proxy.
- **`experiments/philly_soda_tax/`** — first event,
  in flight as of 2026-05-11. See its
  [README](experiments/philly_soda_tax/README.md).
- **`SSB`** is the only fully populated category.
  `LIQUOR`, `BEER`, `WINE`, `TOBACCO` need discovery
  probes to lock PCCs and parent codes.
