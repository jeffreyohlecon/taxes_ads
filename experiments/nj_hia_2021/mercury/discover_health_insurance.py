#!/usr/bin/env python3
"""
Discovery probe for health-insurance advertisers in
Nielsen Ad Intel. Runs on Mercury compute node.

Goal: confirm Horizon BCBS NJ is in the data and identify
the PCCSubCodes used for health-insurance advertising, so
we can populate shared/categories.py with a HEALTH_INS
entry.

What it does:
  - load Brand.tsv and Advertiser.tsv for REF_YEAR
  - name-search the Advertiser parent list for known
    health-insurance parents (Horizon, BCBS variants,
    UnitedHealth, Elevance/Anthem, Aetna, Cigna, Humana,
    Centene, Kaiser, Molina, etc.)
  - for matched parents, list their PCCSubCodes and the
    associated brand descriptions
  - also do the reverse: rank PCCSubCodes by how many of
    the matched parents use them, to identify the canonical
    "health insurance" PCC(s)
  - emit a Horizon-specific report: every BrandCode +
    BrandDesc + PCCSubCode associated with any parent whose
    name contains HORIZON

Outputs (in submit dir):
  - health_insurance_parents.csv      parent matches w/ kw
  - health_insurance_brands.csv       brands under matched parents
  - health_insurance_pcc_ranks.csv    PCCSubCode -> n parents
  - horizon_report.csv                Horizon-specific brand list
  - horizon_summary.txt               human-readable summary
"""

import csv
import os
import sys
from datetime import datetime

import pandas as pd


REF_YEAR = '2021'
ADINTEL_ROOT = '/kilts/adintel'

# Case-insensitive substring matches against AdvParentDesc.
# Each tuple is (label, keyword). A parent matching ANY
# keyword is included.
PARENT_KEYWORDS = [
    ('horizon',          'HORIZON'),
    ('bcbs',             'BLUE CROSS'),
    ('bcbs_alt',         'BLUE SHIELD'),
    ('unitedhealth',     'UNITEDHEALTH'),
    ('unitedhealth_alt', 'UNITED HEALTH'),
    ('anthem',           'ANTHEM'),
    ('elevance',         'ELEVANCE'),
    ('aetna',            'AETNA'),
    ('cvs',              'CVS HEALTH'),
    ('cigna',            'CIGNA'),
    ('humana',           'HUMANA'),
    ('centene',          'CENTENE'),
    ('kaiser',           'KAISER'),
    ('molina',           'MOLINA'),
    ('wellpoint',        'WELLPOINT'),
    ('amerigroup',       'AMERIGROUP'),
    ('emblem',           'EMBLEMHEALTH'),
    ('healthnet',        'HEALTH NET'),
    ('oscar',            'OSCAR HEALTH'),
    ('bright',           'BRIGHT HEALTH'),
    ('uhc',              'UHC'),
]


def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def load_tsv(path):
    log(f"Loading {path}")
    df = pd.read_csv(
        path, sep='\t', dtype=str,
        quoting=csv.QUOTE_NONE, on_bad_lines='warn',
    )
    df.columns = [c.strip() for c in df.columns]
    log(f"  {len(df):,} rows")
    return df


def main():
    log("Health-insurance discovery start.")

    brand_path = os.path.join(
        ADINTEL_ROOT, REF_YEAR, 'References', 'Brand.tsv',
    )
    adv_path = os.path.join(
        ADINTEL_ROOT, REF_YEAR, 'References', 'Advertiser.tsv',
    )
    pcc_path = os.path.join(
        ADINTEL_ROOT, REF_YEAR, 'References',
        'ProductCategories.tsv',
    )

    brand = load_tsv(brand_path)
    adv = load_tsv(adv_path)
    pcc = load_tsv(pcc_path) if os.path.isfile(pcc_path) else None

    # ---- 1. parent name search --------------------------------
    parents = (
        adv[['AdvParentCode', 'AdvParentDesc']]
        .drop_duplicates(subset=['AdvParentCode'])
        .copy()
    )
    parents['AdvParentDesc'] = (
        parents['AdvParentDesc'].fillna('').str.strip()
    )
    pdesc_up = parents['AdvParentDesc'].str.upper()

    keep = pd.Series(False, index=parents.index)
    label_col = pd.Series('', index=parents.index)
    for label, kw in PARENT_KEYWORDS:
        hit = pdesc_up.str.contains(kw, na=False)
        keep = keep | hit
        # tag each row with the FIRST matching label
        label_col = label_col.where(
            label_col.ne(''), other=label_col,
        )
        label_col.loc[hit & label_col.eq('')] = label
    matched = parents.loc[keep].copy()
    matched['kw_label'] = label_col.loc[keep].values
    matched = matched.sort_values('AdvParentDesc')
    matched.to_csv(
        'health_insurance_parents.csv', index=False,
    )
    log(
        f"Matched {len(matched):,} parents on keywords. "
        f"Wrote health_insurance_parents.csv"
    )

    # ---- 2. brands under matched parents ----------------------
    for col in (
        'BrandCode', 'BrandDesc',
        'AdvParentCode', 'PCCSubCode',
    ):
        if col not in brand.columns:
            log(f"ERROR: Brand.tsv missing {col}")
            return 1
        brand[col] = (
            brand[col].fillna('').astype(str).str.strip()
        )

    cand_brands = brand[
        brand['AdvParentCode'].isin(matched['AdvParentCode'])
    ].copy()
    cand_brands = cand_brands.merge(
        matched[['AdvParentCode', 'AdvParentDesc', 'kw_label']],
        on='AdvParentCode', how='left',
    )
    if pcc is not None:
        pcc_sub = (
            pcc[['PCCSubCode', 'PCCSubDesc']]
            .drop_duplicates()
        )
        cand_brands = cand_brands.merge(
            pcc_sub, on='PCCSubCode', how='left',
        )
    cand_brands.to_csv(
        'health_insurance_brands.csv', index=False,
    )
    log(
        f"Wrote health_insurance_brands.csv "
        f"({len(cand_brands):,} brand rows)"
    )

    # ---- 3. PCCSubCode rank by # matched parents --------------
    pcc_rank = (
        cand_brands
        .drop_duplicates(['PCCSubCode', 'AdvParentCode'])
        .groupby('PCCSubCode')
        .size()
        .reset_index(name='n_parents')
        .sort_values('n_parents', ascending=False)
    )
    if pcc is not None:
        pcc_rank = pcc_rank.merge(
            pcc[['PCCSubCode', 'PCCSubDesc']]
            .drop_duplicates(),
            on='PCCSubCode', how='left',
        )
    pcc_rank.to_csv(
        'health_insurance_pcc_ranks.csv', index=False,
    )
    log("Wrote health_insurance_pcc_ranks.csv")

    # ---- 4. Horizon-specific report ---------------------------
    horizon_parents = matched[
        matched['AdvParentDesc'].str.upper().str.contains(
            'HORIZON', na=False,
        )
    ]
    horizon_brands = cand_brands[
        cand_brands['AdvParentCode'].isin(
            horizon_parents['AdvParentCode']
        )
    ].copy()
    horizon_brands.to_csv('horizon_report.csv', index=False)

    with open('horizon_summary.txt', 'w') as fh:
        fh.write(f"Ref year: {REF_YEAR}\n\n")
        fh.write(
            f"Parents matching 'HORIZON' "
            f"({len(horizon_parents)}):\n"
        )
        for _, r in horizon_parents.iterrows():
            fh.write(
                f"  {r['AdvParentCode']:>10}  "
                f"{r['AdvParentDesc']}\n"
            )
        fh.write(
            f"\nBrands under those parents "
            f"({len(horizon_brands)}):\n"
        )
        for _, r in horizon_brands.iterrows():
            pcc_desc = r.get('PCCSubDesc', '') or ''
            fh.write(
                f"  parent={r['AdvParentCode']:>10}  "
                f"brand={r['BrandCode']:>10}  "
                f"pcc={r['PCCSubCode']:<6}  "
                f"{r['BrandDesc']}  "
                f"[{pcc_desc}]\n"
            )
        fh.write("\nTop 20 PCCSubCodes by # matched parents:\n")
        for _, r in pcc_rank.head(20).iterrows():
            desc = r.get('PCCSubDesc', '') if pcc is not None else ''
            fh.write(
                f"  {r['PCCSubCode']:<6}  "
                f"{r['n_parents']:>4}  {desc}\n"
            )

    log("Wrote horizon_summary.txt and horizon_report.csv")
    log("Done.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
