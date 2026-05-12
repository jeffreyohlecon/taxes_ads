# Agent Operating Notes — `taxes_ads`

For the next Claude landing in this repo. Object-level
state, not admin trivia.

## What this project is

Stacked natural experiments asking: **when a city or state
taxes a category (sweetened beverages, liquor, beer, ...),
do the major national advertisers cut Spot TV buys in the
treated DMA, relative to itself and to comparison DMAs?**

Data: Nielsen Ad Intel on Mercury at `/kilts/adintel/`,
2010-2023. Spot TV occurrences are DMA × month and large
enough to potentially detect a DMA-level advertiser
response if one exists.

## Extraction policy (project-wide)

**Brand is the only selection filter at extraction time.
Never pre-filter on year or DMA.** Each Mercury pull for a
product category scans the full SpotTV file 2010-2023, all
210 DMAs, filtered only on PCC subcodes for that category,
and emits a collapsed panel:
`(MarketCode × year_month × AdvParentCode × PCCSubCode → Spend, n_spots)`.

Rationale: wall-time is dominated by the file scan, not
the row filter. Pulling all years/DMAs costs the same as
pulling one DMA but yields a national panel reusable for
every event in that category (Philly, Cook County, Boulder,
Berkeley, SF, Seattle, Oakland, ...). Per-event slicing,
comparison-DMA selection, synthetic-control donor pools,
and rest-of-US aggregates all happen locally on the
collapsed panel — no second Mercury job.

The collapsed grain (year-month × DMA × parent × PCC) is
~MB-scale per year and trivial to scp back. Occurrence-
level granularity is not needed for the DMA-response
question; if it ever is, re-pull is cheap.

This rule supersedes the per-event one-DMA extraction
pattern used for the first Philly pass (job 261336).

## Parallelism on Mercury

We pull lots of data; default to maximum parallelism the
scheduler will grant. The sb_incidence Mercury scripts
process years serially in one job — match or beat that.

For per-year-independent scans (which all SpotTV/Network
extractions are): submit as a SLURM job array, one task
per year. `SLURM_ARRAY_TASK_ID` carries the year. Memory
ask per task should be sized to one year (~32-64G is
plenty for SpotTV), not the inflated 128G that single-job
serial loops use.

Example: `sbatch --array=0-13 --export=CATEGORY=SSB
taxes_ads/shared/mercury/run_category_panel.sh` runs 14
tasks in parallel as nodes free up; wall-time drops from
several hours serial to ~one-task duration.

### Mercury array-job limits (verified 2026-05-11)

- **`MaxArraySize = 1001`** — array task IDs MUST be in
  `[0, 1000]`. `--array=2010-2023` fails with "Invalid
  job array specification". Use small indices and map
  to a real label (year, month, DMA) inside the wrapper
  via `YEAR_BASE + SLURM_ARRAY_TASK_ID`, etc.
- **`MaxJobCount = 10000`** — total queued jobs cap.
- **`qos_highmem MaxJobs = 360`** — per-user concurrent
  highmem job cap. We can run up to 360 tasks at once.
  All current extraction shapes fit comfortably (14
  years × ~12 months = ~168 tasks worst case).

### Memory sizing

Per-task `--mem=64G` is enough on the pandas path (peak
RSS ~4 GB). The pyarrow path uses much more (~27 GB peak
on 2017) because of multithreaded read-ahead + the
to_pandas() conversion per batch. Keep at 64 GB for now.

### PyArrow CSV reader regression (2026-05-11)

The extractor was rewritten to use `pyarrow.csv.open_csv`
with `include_columns=NEEDED_COLS` and 256 MB blocks,
expecting a speedup from multithreaded parsing + column
projection. **Result: ~2× slower than pandas + 7× more
memory.** Pandas baseline on 2017: ~5-6 min wall, 4 GB RSS.
PyArrow on same year: ~11 min wall, 27 GB RSS. The
parallel SLURM array still gives the big win; the
within-task pyarrow change is a regression.

Likely causes (not yet diagnosed): block_size too large
for the workload, per-batch `to_pandas()` cost, multi-
threaded read-ahead holding multiple in-flight batches.
If revisiting, try smaller blocks (32-64 MB), keep data
in arrow until after the filter+groupby, or just revert
to pandas — pandas is fine and already keeps up with the
disk on `/kilts`.

Future extensions can add intra-year parallelism (one task
per month/file for the 2022+ monthly-file layout), but
per-year is enough for now.

## GRPs (follow-up, not yet wired in)

`spend_total` in dollars is what the current category
panel emits. Gross Rating Points are the more standard
ad-exposure measure and what a v2 panel should also carry:

  grp = 100 * (impressions_tv_hh / universe_tv_hh) * Units

Computing GRPs requires joining three files:
  - Occurrences (`SpotTV.tsv`) - current source
  - Impressions (`SpotTVImpressions.tsv` or daily files) -
    TV_HH per DistributorID × DayOfWeek × TimeInterval
  - Universe estimates (`MarketUniverse...tsv`) -
    total TV_HH per DMA × time period

The join keys (`DistributorID`, `DayOfWeek`,
`TimeIntervalNumber`, `HispanicFlag`) are NOT in the
current collapsed panel, so GRPs cannot be back-computed
from the v1 output — adding GRPs means a v2 extractor
that does the impressions+universe join upstream and
emits both `spend_total` and `grp_total`.

Template: `sb_incidence/code/data_pipeline/GRPs/
build_spot_tv_sportsbook_grps_v1.py`. Port that join into
the category panel extractor when we commit to GRPs as a
metric. The v1 extractor already carries `units_total` as
a coarse proxy.

## What's done (as of 2026-05-11)

- **National-panel extractor live and run for SSB**. See
  [shared/mercury/extract_category_panel.py](shared/mercury/extract_category_panel.py)
  + [run_category_panel.sh](shared/mercury/run_category_panel.sh).
  Pulls all DMAs × all years; handles both legacy
  (2010-2021) and standard (2022+) Spot TV layouts.
  Runs as a SLURM array (one task per year). See the
  "Extraction policy" and "Parallelism" sections above
  for details.
- **SSB national pull complete** (job 261414): 11 of 14
  years usable — **2010, 2014-2023**. **2011, 2012, 2013
  failed with permission denied** on
  `/kilts/adintel/{year}/Occurrences/SpotTV.tsv` from the
  compute node, despite 2010 and 2014+ being accessible.
  This is a permanent data hole until/unless the data
  steward changes ACLs.
- **First (superseded) Philly-only extraction** (job
  261336, single-DMA 2014-2018) also done. CSVs in
  [experiments/philly_soda_tax/extraction_outputs/](experiments/philly_soda_tax/extraction_outputs/).
  Kept for reference; **do not run for new events** — the
  national-panel extractor replaces it.
- **Category registry** at [shared/categories.py](shared/categories.py).
  `SSB` fully populated (F221/F222/F223 treated, F224
  placebo, top-4 parents Coca-Cola/PepsiCo/DPS/Nestle).
  `LIQUOR` partial. `BEER`/`WINE`/`TOBACCO` skeletons.
- **Canonical analyzer** is
  `event_study_quarters_permutation.py` (one copy per
  event, identical structure). Quarterly PPML two-way FE
  event study with treated × event-time interactions,
  refit pretending each non-treated DMA is treated to
  build the placebo distribution. The 90% band on the
  output PNG is the 5-95% percentile of placebo
  coefficients; Fisher exact p-values are per event-time.
  All other specs (monthly OLS, monthly PPML, DiD) have
  been retired; do not reintroduce them.
- **Philly headline**: null. Post-period quarterly coefs
  sit inside the placebo band at every event-time;
  Fisher p > 0.10 in every post quarter. Outputs in
  [experiments/philly_soda_tax/analysis_outputs/](experiments/philly_soda_tax/analysis_outputs/).
- **Seattle headline**: also null. Fisher p > 0.18 in
  every post quarter. Outputs in
  [experiments/seattle_ssb_tax/analysis_outputs/](experiments/seattle_ssb_tax/analysis_outputs/).
- **Prior paper found post-hoc** — independent team
  (Philly vs Baltimore, Jan 2016 - Dec 2019, segmented
  linear regression) reports the **same Spot TV
  expenditure null** plus additional internet and radio
  channels we can't see in Nielsen Ad Intel. Citation
  hunt outstanding (likely 2024 Prev Med or AJPM). The
  single-event Philly result is therefore a replication,
  not a contribution — see "Framing pivot" below.
- **Baby theory section** at [paper/model.tex](paper/model.tex)
  with the central result: $dA^*/dt > 0$ iff
  $D_{pA} > (1-\rho)D_A / (m\rho)$. Separable demand
  (sb_incidence sportsbook setup) fails the condition →
  ads fall with tax; persuasive demand (Becker-Murphy
  rotation) passes → ads rise. The empirical sign of
  $dA^*/dt$ becomes the test.

## Framing pivot (2026-05-11; updated 2026-05-12)

Single-event Philly is a replication, not a contribution.

**Headline category is BEER, not SSB.** The case for
beer is per-event power, not event count — Butters et al
have only ~4 usable beer events too, so the count is
similar to SSB. What's better with beer is:

- **State-level treated unit**: a state beer-tax hike
  treats *every DMA mostly inside that state* — typically
  3-8 DMAs per event — versus one fractional DMA for an
  SSB city tax.
- **Bigger nominal-rate shocks as % of retail**: state
  beer excise hikes are typically a larger fraction of
  the retail price than SSB local taxes.
- **Less geographic dilution**: SSB treated cities are
  small slices of their DMAs (Boulder ≈ 15% of Denver
  DMA, Berkeley ≈ 3% of SF DMA), mechanically
  attenuating any DMA-level Spot TV response. Beer
  state-level treatment doesn't have this problem.

SSB drawbacks that motivate the pivot (kept for context):
- ~4 distinct stacked DMA-event pairs after exclusions
  (Philly, Seattle, Boulder, Bay-Area-407; Cook County
  out as a short-lived repeal).
- Single-treated-DMA per event, so per-event inference is
  weak.

Project structure under the pivot:
1. **BEER as headline**: state-level excise tax changes,
   2010-2023. Treated unit = state, which maps to many
   DMAs (use sb_incidence
   `generate_market_allocations.py` patterns for the
   state-DMA assignment).
2. **SSB as a chapter, not the headline**: Philly +
   Seattle done; Boulder + Bay Area as additional events
   if time. Frame as a precise-null replication that
   establishes the methodology.
3. **TOBACCO is out**: cigarettes have been banned from
   US broadcast TV since Jan 2, 1971; smokeless since
   1986. No Spot TV signal to measure (except possibly
   pre-2016 e-cig/vape buys — narrow scope; do not
   prioritize). See [shared/categories.py](shared/categories.py).
4. **Sports betting is OUT** — that's
   [sb_incidence](https://github.com/jeffreyohlecon/sb_incidence)
   territory, not taxes_ads. Do not propose it as a
   category for this project.
5. **LIQUOR / WINE / cannabis**: additional categories
   if scope allows.
6. **Theory: $D_{pA}$ is the test**. The sign and
   magnitude of the ad response to a per-unit tax reads
   off whether demand is persuasive (rotates) or
   separable (doesn't). Stacked events give a panel of
   sign tests.

## What's next, in priority order

### 1. BEER pivot (the new headline)

a. **Discovery probe** on Mercury for BEER PCCs and top
   parent firms (suspect F31x family; major parents are
   ABI, MolsonCoors, Constellation, Heineken USA, Boston
   Beer). Mirror
   [shared/mercury/_legacy_philly_discovery/discover_soda_brands.py](shared/mercury/_legacy_philly_discovery/discover_soda_brands.py).
b. **Tax-change history** from NIAAA APIS or Beer
   Institute annual data: state-year excise rates,
   identify hikes ≥ X% (cutoff TBD).
c. **Beer panel extraction** —
   `sbatch --array=0-13 --export=CATEGORY=BEER
    shared/mercury/run_category_panel.sh`
   once BEER is populated in
   [shared/categories.py](shared/categories.py).
d. **Spec refactor for state-level treatment**: the
   current canonical analyzer
   ([shared/code/event_study.py](shared/code/event_study.py))
   assumes one treated DMA per event. State-level beer
   events have many treated DMAs. Refactor to accept a
   `treated_marketcodes: tuple[str, ...]` and a
   state-DMA allocation (port the pattern from
   `sb_incidence/code/generate_market_allocations.py`).

### 2. Permissions push for 2011-2013

Three years of AdIntel data are silently missing on
SpotTV. Worth a ticket to the data steward to confirm
whether `/kilts/adintel/{2011,2012,2013}/Occurrences/SpotTV.tsv`
can be made readable from compute nodes (the 2010 and
2014+ files are; the 2011-2013 ones aren't, no
documented reason). Especially relevant for beer since
state beer-tax changes happen across the whole window
and three missing years matters.

### 3. GRPs v2

Tracked in [issue #2](https://github.com/jeffreyohlecon/taxes_ads/issues/2).
Spend in dollars is what v1 emits; GRPs require joining
impressions and universe files in the Mercury job. Port
from `sb_incidence/code/data_pipeline/GRPs/`.

### 4. Additional SSB events (lower priority)

Boulder, Bay Area events as bonus replications. Treat as
a chapter, not the headline.

### 5. LIQUOR / WINE / cannabis

If scope allows. TOBACCO is out (broadcast ad ban; see
Framing pivot above). Sports betting is also out — that's
[sb_incidence](https://github.com/jeffreyohlecon/sb_incidence),
not taxes_ads.

## Mercury operating particulars

- `module load python/booth/...` no longer works post-RHEL 9
  upgrade. Always `source ~/venv/bin/activate`.
- Standard SLURM header: `--account=phd --partition=highmem
  --mem=128G`. Other partitions stall on
  `QOSMaxBillingPerUser`.
- Pre-2018 SpotTV files are `-rwx------` on the login node.
  **Readable from SLURM compute nodes for 2010 and 2014+
  but NOT for 2011-2013** (permission denied even on
  compute). Three-year hole in our SSB panel as of
  2026-05-11. Don't try to `head` any pre-2018 file via
  SSH; submit a job to test, and expect 2011-2013 to fail.
- **Maintenance window: 2026-05-14 07:00-15:00 CDT.** Jobs
  with `--time` that overlaps will be pended with
  `ReqNodeNotAvail,Reserved_for_maintenance`. Use
  `--time=12:00:00` for SpotTV scans (actual runtime
  ~2-4 h) until after the window.
- Submit from `~` so `${SLURM_SUBMIT_DIR}` resolves to home
  and the SLURM wrappers find the Python entrypoint via
  `${SLURM_SUBMIT_DIR}/taxes_ads/experiments/.../mercury/...`.

## Conventions worth keeping

- **PCC + parent codes, not keyword regex**, for brand
  selection. Keywords are noisy beyond saving.
- **Match Mercury jobs to one event at a time**, output
  per-year CSVs, scp back, analyze locally.
- **Compile-check before pushing to Mercury**:
  `python3 -m py_compile path/to/script.py` and
  `bash -n path/to/wrapper.sh`.
- **Test access on the compute node** with a tiny probe
  before assuming a year's files are readable.

## Nielsen Ad Intel `MarketCode` vs public DMA code

These are *not* the same number system. The panel column
`MarketCode` is Ad Intel's internal code; the public
Nielsen DMA code (the one in the ZIP-to-DMA reference
file) is +400 from it. Verified across all 210 DMAs in
the panel.

| Market | Public DMA | Panel `MarketCode` |
|---|---|---|
| Philadelphia | 504 | 104 |
| Pittsburgh | 508 | 108 |
| Chicago | 602 | 202 |
| Seattle-Tacoma | 819 | 419 |
| Portland OR | 820 | 420 |
| San Francisco | 807 | 407 |

Canonical lookup is persisted at
[shared/reference/dma_lookup.csv](shared/reference/dma_lookup.csv)
(columns: `MarketCode, DMACode, DMAName_public`). It is
derived from the bulletproof source
`/Users/jeffreyohl/Dropbox/Gambling Papers and Data/Data
Docs/Nielsen docs/Reference_Documentation/Zip to DMA
Mappings 2023/Zip to DMA 2023.XLS` (sheet "ZIP by DMA").
**Always look up new events here before hardcoding a
`MarketCode`.** Do not trust a number you got from
context, only from the lookup table or the source XLS.

## sb_incidence as the "daddy" repo for ads/DMA knowledge

`sb_incidence` is the upstream project for everything we
know about Nielsen Ad Intel, DMAs, and ad-spend pipelines.
When stuck on data-layout, DMA-mapping, market-allocation,
or county-population questions, check `sb_incidence` first:

- `sb_incidence/code/generate_market_allocations.py` —
  the canonical DMA → state/county allocation routine,
  using county-population weights from Census.
- `sb_incidence/code/ad_spend_analysis.py` — full
  ad-spend analysis pipeline; the `MAPPING_FILE` constant
  points to the bulletproof Zip-to-DMA reference.
- `sb_incidence/code/nh_dma_breakdown.py`,
  `generate_dma_border_drop_table.py`,
  `nj_zip_county_maps.py` — examples of sub-DMA and
  cross-border ZIP-level work using the same source.
- `sb_incidence/code/data_pipeline/GRPs/` — the GRP join
  pattern (impressions + universe) we'll port when we
  graduate to GRPs.
- `sb_incidence/philly_soda_tax/mercury/` — the original
  one-DMA extractor pattern that `taxes_ads`
  generalized.

If a `taxes_ads` data question has any equivalent in
`sb_incidence`, the `sb_incidence` answer is canonical.
Do not reinvent the wheel here; port the pattern.

## Manuscript prose

- **Ask before making discretionary writing edits.** If the
  user asks a question about wording or quotes manuscript
  text for discussion, do not rewrite the source unless
  they explicitly ask for an edit.
- **When asked for feedback on draft text, default to point
  edits, not a full rewrite.** Prefer short bullets or
  line-level substitutions. If a full rewrite is necessary,
  summarize the specific changes made.
- **Preserve user wording in `.tex` files by default.** Do
  not polish, shorten, or paraphrase existing user-written
  text. Structural edits (pacing, overlays, bullet
  hierarchy, spacing) are allowed; wording edits are not.
- **Propose wording separately.** Suggest in chat first
  instead of silently changing the source.
- **Avoid stale academic filler.** No `depend critically`,
  `hinges on`, `hinges critically on`, `it bears noting
  that`, `it is worth emphasizing that`, `which has long
  highlighted`. State the claim directly.

## LaTeX / Beamer syntax safety

- **No digits in `\newcommand` names.** TeX control
  sequences can only contain letters. `\HetF1OwnNew`
  silently breaks — TeX reads `\HetF` then literal
  `1OwnNew`. Use word prefixes (`\HetExtMarg`,
  `\HetNewVsEx`). Brand names with digits get spelled or
  abbreviated: `bet365` → `BetTSF`. Applies to Python
  generators too — sanitize before emitting.
- **Never add empty optional argument blocks.** No
  `\begin{frame}[]{...}`, `\begin{frame}[]`, `\section[]{}`.
  Omit the brackets entirely if there are no options.
- **Beamer frames:** use `\begin{frame}{Title}` for ordinary
  titled slides; use `[]` only for real options like
  `[plain]`, `[fragile]`, `[noframenumbering]`.
- **Compile-check after structural LaTeX edits.** If you
  touch frame declarations, overlays, appendix navigation,
  or `\input{}` structure, run a compile so malformed
  syntax surfaces immediately.
- **"Button" means an appendix frame with
  `\beamergotobutton` / `\beamerreturnbutton`.** Separate
  appendix frame with content; link via `\hyperlink` +
  `\beamergotobutton` from the main slide; add
  `\beamerreturnbutton{Back}` on the appendix frame. Do not
  add extra overlays to the main slide.

## Figures

- **Keep annotation boxes inside the plotting area** unless
  the user explicitly asks otherwise. Default to short
  arrows and nearby callouts rather than labels outside the
  axes.
- **Show outputs before compiling slides after analytic
  changes.** If a slide edit adds or changes analysis,
  figures, or tables, show the generated outputs and a
  short summary first. Do not immediately run a full slide
  compile unless asked.

## Inference and reporting

- **Report all test results, not just significant ones.**
  When multiple tests are run (e.g., the same comparison
  for two outcomes), report both p-values. Never
  selectively cite only the significant result.
- **Before producing any regression output with stars,
  stop:** what are we clustering on? How many clusters?
  Is this the right level? If unsure, ask.
- **Never put analytical clustered p-values or stars in
  any output** (tables, slides, paper, macros) when the
  number of clusters is below ~40. SEs have to be right
  or they're useless. If wild-cluster bootstrap hasn't
  been run, show coefficients without stars rather than
  fall back to analytical p-values.
- **Never plot ±1.96 × OLS SE bars on event studies.**
  Banned regardless of any "not properly clustered" or
  "for visual scale only" caveat in the title — readers
  see error bars and read significance no matter what
  the subtitle says. Default: points/lines with no bars.
  If a band is needed, run a permutation / wild-cluster
  bootstrap and plot that distribution. Do not fall back
  to OLS SEs as a visual placeholder.

## Never hardcode what should be computed

- **If a task says to compute a value, compute it from the
  data/macros.** Do not approximate by hand or plug in
  numbers from memory. If pipeline macros exist, read
  them. If they don't, say so and ask — don't improvise.
- **Flag it when something feels off.** If instructions
  would produce a wrong-looking number, or you're tempted
  to shortcut ("approximate range" instead of computing
  from stored coefficients), stop and tell the user.
- **Applies to slides, paper text, and any generated
  content.** Never insert a hand-computed number into
  LaTeX when the pipeline can produce the real one.
