"""
Category registry for stacked tax-change × ad-spend
natural experiments.

Each entry maps a human-readable category name (e.g.,
"SSB", "BEER", "LIQUOR") to the Nielsen Ad Intel
identifiers that select the relevant brands:

  - pcc_subcodes_treated:  PCC subcodes of products
        directly hit by the typical tax in this category
  - pcc_subcodes_placebo:  same-firm, same-channel
        products that are NOT taxed — used as a
        within-firm placebo arm. Caveat: which products
        are exempt depends on the specific tax, so
        each experiment can override this list (e.g.,
        Chicago/Cook County's 2017 SSB tax DID hit
        flavored bottled water, so for that event the
        placebo list shrinks).
  - parent_codes_major:    a small handful of "large
        national firms" AdvParentCodes. Used to define
        a focused sample of advertisers when we want
        to avoid noise from tiny regional players.
        Codes pulled from
        /kilts/adintel/2017/References/Advertiser.tsv.

Sources of truth on Mercury:
  /kilts/adintel/2017/References/Brand.tsv
  /kilts/adintel/2017/References/Advertiser.tsv
  /kilts/adintel/2017/References/ProductCategories.tsv

When adding a new category, run the discovery script with
the proposed PCC subcodes and verify the candidate brand
list looks right before locking the codes in here.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Category:
    name: str
    description: str
    pcc_subcodes_treated: tuple[str, ...]
    pcc_subcodes_placebo: tuple[str, ...] = ()
    parent_codes_major: tuple[str, ...] = field(default=())
    notes: str = ''


# F221 REG SOFT DRINK, F222 DIET SOFT DRINK,
# F223 DRINKS-NON CARBONATED (sweetened), F224 BOTTLED
# WATERS (placebo for Philly; treated for Cook County).
SSB = Category(
    name='SSB',
    description='Sweetened beverages (CSDs + sweetened '
                'non-carbonated). Philly-style tax base.',
    pcc_subcodes_treated=('F221', 'F222', 'F223'),
    pcc_subcodes_placebo=('F224',),  # bottled water
    parent_codes_major=(
        '11440',    # Coca-Cola Co
        '14842',    # PepsiCo Inc
        '2531252',  # JAB Holding (Dr Pepper Snapple
                    # mapped here in 2017 ref)
        '1568215',  # Dr Pepper Snapple Group Inc
                    # (older code; mostly empty in 2017)
        '1693',     # Keurig Green Mountain
        '20790',    # Hansen Beverage Co (Monster)
        '1110474',  # Red Bull GmbH
        '1809',     # Nestle SA (waters)
        '551289',   # Cott Corp
        '14335',    # National Beverage Corp (La Croix)
    ),
    notes=(
        'Chicago / Cook County 2017 sweetened-beverage '
        'tax also covered flavored bottled waters; for '
        'that event drop F224 from placebo.'
    ),
)

# F330 LIQUOR (per ProductCategories.tsv). Confirmed via
# discovery probe.
LIQUOR = Category(
    name='LIQUOR',
    description='Distilled spirits.',
    pcc_subcodes_treated=('F330',),
    pcc_subcodes_placebo=(),  # within-firm placebo TBD
    parent_codes_major=(
        # Fill in via discovery probe (Diageo, Pernod,
        # Bacardi, Brown-Forman, Beam Suntory, Sazerac,
        # Constellation, etc.). Not locked yet.
    ),
    notes=(
        'WA I-1183 (2012) is a candidate event but '
        'pre-2014 AdIntel layout / permissions still '
        'unverified; treat as an open thread.'
    ),
)

# Placeholder skeletons; need discovery passes to
# confirm PCC subcodes and parent firms.
BEER = Category(
    name='BEER',
    description='Beer. Per Nielsen ProductCategories.tsv '
                '(2017 ref), F310 is the single beer subcode '
                '(ProductIDs 140 BEER, 141 BEER PDTS, 142 '
                'BEER-NON ALCOHOLIC).',
    pcc_subcodes_treated=('F310',),
    pcc_subcodes_placebo=(),  # wine F320, liquor F330 are
                              # separate categories, not
                              # within-firm placebos
    parent_codes_major=(),    # left empty: spaghetti plot
                              # uses all parents; analyzer
                              # events override per event
    notes='F310 covers regular beer + non-alcoholic beer; '
          'PCC-level filter cannot exclude non-alc, but it '
          'is a tiny share of category Spot TV. Drop non-alc '
          'brands post-extraction via Brand.tsv ProductID '
          '142 if needed. Wine (F320) and liquor (F330) are '
          'separate categories.',
)

WINE = Category(
    name='WINE',
    description='Wine (placeholder).',
    pcc_subcodes_treated=(),
    notes='Run discovery to populate.',
)

TOBACCO = Category(
    name='TOBACCO',
    description='Tobacco products. Effectively NOT a Spot '
                'TV category: cigarettes are banned from US '
                'broadcast since Jan 2, 1971 (Public Health '
                'Cigarette Smoking Act); smokeless tobacco '
                'banned from broadcast since 1986. Only '
                'feasible sub-segments are e-cigarettes / '
                'vapes (pre-2016 TV buys by Blu, etc.) and '
                'cigars (limited).',
    pcc_subcodes_treated=(),
    notes='Do not treat tobacco as a stacked-event '
          'category for Spot TV ad-response analysis. '
          'If revisited, scope narrowly to e-cig / vape '
          'TV spend pre-FDA-restrictions, and confirm any '
          'taxable PCC subcode exists in AdIntel before '
          'extracting.',
)


REGISTRY: dict[str, Category] = {
    c.name: c for c in (SSB, LIQUOR, BEER, WINE, TOBACCO)
}


def get(name: str) -> Category:
    if name not in REGISTRY:
        raise KeyError(
            f"Category {name!r} not in registry. "
            f"Known: {sorted(REGISTRY)}"
        )
    return REGISTRY[name]
