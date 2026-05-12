# Legacy / superseded Mercury scripts

`extract_philly_spend.py` and `run_extract.sh` were the
first-pass single-DMA extractor for the Philly Beverage
Tax (job 261336, May 2026). They filter at extraction
time to `MarketCode == 104` and emit disaggregated
occurrence rows for 2014-2018.

**Superseded by** `shared/mercury/extract_category_panel.py`
+ `run_category_panel.sh` per the project-wide
"brand-only at extraction time" policy in
[../../../../CLAUDE.md](../../../../CLAUDE.md).

Kept here for reference. **Do not run for new events.**
Use the parameterized extractor in `shared/mercury/`.

`discover_soda_brands.py` and `run_discover.sh` stay in
the parent `mercury/` folder — they document how we
locked in the SSB PCC subcodes (F221/F222/F223/F224) and
remain useful as a template for new-category discovery.
