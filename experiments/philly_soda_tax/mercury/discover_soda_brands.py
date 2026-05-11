#!/usr/bin/env python3
"""
Discovery v2 for the Philadelphia Beverage Tax ad-spend
exploration. Runs on Mercury (compute node).

What it does:
  - list /kilts/adintel/ top-level entries
  - per probed year, walk the directory and head SpotTV.tsv
    (compute nodes can read pre-2018 SpotTV files; login
    nodes cannot)
  - load the Brand reference
        /kilts/adintel/{year}/References/Brand.tsv
  - filter to Product Class Codes for sweetened beverages
        F221 REGULAR CARBONATED  (taxed by Philly)
        F222 DIETARY CARBONATED  (taxed)
        F223 NON-CARBONATED      (taxed if sweetened)
        F224 BOTTLED WATERS      (UNTAXED -> placebo)
  - attach Advertiser parent names via AdvParentCode join
  - rank by candidate-brand count per parent (proxy for size)
  - load Market.tsv from Master_Files/Latest/ and find
    Philadelphia MarketCode -> philadelphia_market_code.txt

Outputs (in submit dir):
  - adintel_year_inventory.txt
  - spot_tv_head_{year}.tsv
  - soda_brand_candidates.csv      (PCC-filtered, joined to
                                    advertiser parent names)
  - parent_brand_counts.csv        (top advertisers among
                                    candidates)
  - philadelphia_market_code.txt
"""

import glob
import os
import sys
from datetime import datetime

import pandas as pd


PROBE_YEARS = ['2014', '2015', '2016', '2017', '2018']
REF_YEAR = '2017'
ADINTEL_ROOT = '/kilts/adintel'
MASTER_MARKET_TSV = (
    '/kilts/adintel/Master_Files/Latest/Market.tsv'
)

# Sweetened-beverage Product Class subcodes.
# F221 / F222 / F223 are taxed under Philly Beverage Tax.
# F224 BOTTLED WATERS is untaxed and useful as a within-
# firm placebo (Coke sells Dasani, Pepsi sells Aquafina).
TARGET_PCC_SUBCODES = ['F221', 'F222', 'F223', 'F224']

# Sanity-check list of large national beverage parents.
# Used only to flag the candidate list, not to filter.
KNOWN_BEVERAGE_PARENTS = {
    '11440':   'COCA-COLA CO',
    '14842':   'PEPSICO INC',
    '1568215': 'DR PEPPER SNAPPLE GROUP INC',
    '1693':    'KEURIG GREEN MOUNTAIN INC',
    '20790':   'HANSEN BEVERAGE CO (Monster)',
    '1110474': 'RED BULL GMBH',
    '551289':  'COTT CORP',
    '1809':    'NESTLE SA',
    '2475043': 'AMERICAN BEVERAGE CORP',
}


def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def list_adintel_years():
    log(f"Listing {ADINTEL_ROOT} ...")
    return sorted(os.listdir(ADINTEL_ROOT))


def spot_tv_path(year):
    legacy = os.path.join(
        ADINTEL_ROOT, year, 'Occurrences', 'SpotTV.tsv',
    )
    if os.path.exists(legacy):
        return legacy
    occ_dir = os.path.join(
        ADINTEL_ROOT, year,
        'Occurrence Data File Formats', 'Spot TV',
    )
    if os.path.isdir(occ_dir):
        files = sorted(glob.glob(os.path.join(occ_dir, '*')))
        if files:
            return files[0]
    return None


def probe_year_layout(year, fh):
    ydir = os.path.join(ADINTEL_ROOT, year)
    fh.write(f"\n===== {ydir} =====\n")
    if not os.path.isdir(ydir):
        fh.write("  (missing)\n")
        return

    for root, _dirs, files in os.walk(ydir):
        depth = root.replace(ydir, '').count(os.sep)
        if depth > 2:
            continue
        indent = '  ' * depth
        fh.write(f"{indent}{os.path.basename(root)}/\n")
        for f in sorted(files)[:6]:
            try:
                size = os.path.getsize(
                    os.path.join(root, f)
                )
            except OSError:
                size = -1
            fh.write(f"{indent}  {f}  ({size} bytes)\n")
        if len(files) > 6:
            fh.write(f"{indent}  ... +{len(files) - 6} more\n")

    spot = spot_tv_path(year)
    if not spot:
        fh.write("  [no Spot TV file located]\n")
        return
    fh.write(f"  Spot TV sample: {spot}\n")
    fh.write(f"  R_OK: {os.access(spot, os.R_OK)}\n")
    head_path = f"spot_tv_head_{year}.tsv"
    try:
        with open(spot, 'r', errors='replace') as fin:
            with open(head_path, 'w') as fout:
                for i, line in enumerate(fin):
                    if i >= 20:
                        break
                    fout.write(line)
        fh.write(f"  Wrote head -> {head_path}\n")
    except Exception as exc:
        fh.write(f"  [head failed: {exc}]\n")


def load_brand_reference(year):
    path = os.path.join(
        ADINTEL_ROOT, year, 'References', 'Brand.tsv',
    )
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    log(f"Loading Brand reference {path}")
    df = pd.read_csv(path, sep='\t', dtype=str)
    df.columns = [c.strip() for c in df.columns]
    log(f"  {len(df):,} brand rows.")
    return df


def load_advertiser_reference(year):
    path = os.path.join(
        ADINTEL_ROOT, year, 'References', 'Advertiser.tsv',
    )
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    log(f"Loading Advertiser reference {path}")
    df = pd.read_csv(path, sep='\t', dtype=str)
    df.columns = [c.strip() for c in df.columns]
    # Keep parent-level only (one row per AdvParentCode).
    parents = (
        df[['AdvParentCode', 'AdvParentDesc']]
        .drop_duplicates(subset=['AdvParentCode'])
        .copy()
    )
    parents['AdvParentDesc'] = (
        parents['AdvParentDesc'].fillna('').str.strip()
    )
    return parents


def load_pcc_reference(year):
    path = os.path.join(
        ADINTEL_ROOT, year, 'References',
        'ProductCategories.tsv',
    )
    if not os.path.isfile(path):
        log(f"  [no PCC reference at {path}]")
        return None
    df = pd.read_csv(path, sep='\t', dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df


def discover_brands():
    brand_df = load_brand_reference(REF_YEAR)
    parents = load_advertiser_reference(REF_YEAR)
    pcc = load_pcc_reference(REF_YEAR)

    for col in (
        'BrandCode', 'BrandDesc', 'AdvParentCode',
        'PCCSubCode',
    ):
        if col not in brand_df.columns:
            log(f"ERROR: Brand.tsv missing {col}")
            return
        brand_df[col] = (
            brand_df[col].fillna('').astype(str).str.strip()
        )

    mask = brand_df['PCCSubCode'].isin(TARGET_PCC_SUBCODES)
    cand = brand_df.loc[mask].copy()
    log(
        f"PCC filter ({TARGET_PCC_SUBCODES}) matched "
        f"{len(cand):,} brand rows."
    )

    cand = cand.merge(
        parents, on='AdvParentCode', how='left',
    )
    if pcc is not None:
        pcc_sub = (
            pcc[['PCCSubCode', 'PCCSubDesc']]
            .drop_duplicates()
        )
        cand = cand.merge(
            pcc_sub, on='PCCSubCode', how='left',
        )

    cand['_known_major_parent'] = (
        cand['AdvParentCode']
        .isin(KNOWN_BEVERAGE_PARENTS.keys())
    )

    out_cols = [
        'BrandCode', 'BrandDesc',
        'AdvParentCode', 'AdvParentDesc',
        'PCCSubCode',
    ]
    if 'PCCSubDesc' in cand.columns:
        out_cols.append('PCCSubDesc')
    out_cols += ['_known_major_parent']
    cand[out_cols].to_csv(
        'soda_brand_candidates.csv', index=False,
    )
    log("Wrote soda_brand_candidates.csv")

    # Top parents by candidate-brand count.
    by_parent = (
        cand.groupby(['AdvParentCode', 'AdvParentDesc'])
        .size()
        .reset_index(name='n_candidate_brands')
        .sort_values('n_candidate_brands', ascending=False)
    )
    by_parent.to_csv('parent_brand_counts.csv', index=False)
    log("Wrote parent_brand_counts.csv")

    log("\nTop 20 advertisers among candidates:")
    for _, row in by_parent.head(20).iterrows():
        marker = (
            '*' if row['AdvParentCode']
            in KNOWN_BEVERAGE_PARENTS else ' '
        )
        log(
            f"  {marker} {row['AdvParentCode']:>10} "
            f"{row['AdvParentDesc']:<40} "
            f"{row['n_candidate_brands']:>6}"
        )


def discover_philadelphia_market():
    if not os.path.isfile(MASTER_MARKET_TSV):
        log(f"ERROR: missing {MASTER_MARKET_TSV}")
        return
    market_df = pd.read_csv(
        MASTER_MARKET_TSV, sep='\t', dtype=str,
    )
    market_df.columns = [c.strip() for c in market_df.columns]
    name_col = 'MarketName' if 'MarketName' in market_df.columns \
        else market_df.columns[1]
    market_df[name_col] = (
        market_df[name_col].fillna('').str.strip()
    )
    philly = market_df[
        market_df[name_col].str.contains(
            'philadelphia', case=False, na=False,
        )
    ]
    with open('philadelphia_market_code.txt', 'w') as fh:
        fh.write(f"# Source: {MASTER_MARKET_TSV}\n")
        fh.write(philly.to_string(index=False))
        fh.write('\n')
    log("Wrote philadelphia_market_code.txt")


def main():
    log("Discovery v2 start.")
    years = list_adintel_years()
    with open('adintel_year_inventory.txt', 'w') as fh:
        fh.write("== /kilts/adintel/ top-level ==\n")
        fh.write('\n'.join(years))
        fh.write('\n')
        for y in PROBE_YEARS:
            probe_year_layout(y, fh)
    log("Wrote adintel_year_inventory.txt")

    discover_brands()
    discover_philadelphia_market()
    log("Discovery done.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
