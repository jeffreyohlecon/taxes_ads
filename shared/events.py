"""
Event registry for stacked tax-change x ad-spend natural
experiments.

Each event maps a short label (e.g. 'philly_ssb',
'seattle_ssb') to the parameters needed to run the
canonical event study:

  - category:           key into shared.categories.REGISTRY
  - treated_marketcode: Nielsen Ad Intel MarketCode for
                        the treated DMA (= public DMA - 400;
                        see shared/reference/dma_lookup.csv)
  - dma_name:           human-readable DMA label
  - passage_quarter:    pd.Period (Q) when the tax was
                        legislatively passed; this is the
                        treatment date in the canonical
                        spec (t = 0)
  - impl_quarter:       pd.Period (Q) when the tax became
                        effective; shown as a secondary
                        vertical line on the plot
  - parent_codes_use:   subset of category.parent_codes_major
                        actually used for this event's
                        analysis (defaults to top-4 SSB if
                        None and category is SSB)
  - pcc_subcodes_override: per-event override of the
                        treated PCC subcodes (None = use
                        category default)
  - rate_description:   short string for caption / context

Add new events here. Do not duplicate analyzer code per
event; the shared analyzer in shared/code/event_study.py
runs against any registered event.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class Event:
    label: str
    category: str
    treated_marketcode: str
    dma_name: str                # used in plot legend
    dma_short: str               # used in caption ("Philly", "Seattle")
    tax_name: str                # used in plot title
    passage_quarter: pd.Period
    impl_quarter: pd.Period
    rate_description: str
    parent_codes_use: Optional[tuple[str, ...]] = None
    parent_names_use: Optional[tuple[str, ...]] = None
    pcc_subcodes_override: Optional[tuple[str, ...]] = None


# Top-4 SSB parents actually used in the canonical SSB
# analyses. Subset of the broader
# categories.SSB.parent_codes_major.
_SSB_TOP4_CODES = ('11440', '14842', '2531252', '1809')
_SSB_TOP4_NAMES = ('Coca-Cola', 'PepsiCo',
                   'Dr Pepper Snapple', 'Nestlé')


PHILLY_SSB = Event(
    label='philly_ssb',
    category='SSB',
    treated_marketcode='104',  # public DMA 504
    dma_name='Philadelphia',
    dma_short='Philly',
    tax_name='Philadelphia Beverage Tax',
    passage_quarter=pd.Period('2016Q2'),
    impl_quarter=pd.Period('2017Q1'),
    rate_description='1.5 cents/oz; covers sugar- and '
                     'artificially-sweetened drinks',
    parent_codes_use=_SSB_TOP4_CODES,
    parent_names_use=_SSB_TOP4_NAMES,
)

SEATTLE_SSB = Event(
    label='seattle_ssb',
    category='SSB',
    treated_marketcode='419',  # public DMA 819
    dma_name='Seattle',
    dma_short='Seattle',
    tax_name='Seattle Sweetened Beverage Tax',
    passage_quarter=pd.Period('2017Q2'),
    impl_quarter=pd.Period('2018Q1'),
    rate_description='1.75 cents/oz; excludes diet drinks '
                     'and 100% juice',
    parent_codes_use=_SSB_TOP4_CODES,
    parent_names_use=_SSB_TOP4_NAMES,
)

# Delaware HB 241, signed June 2017, effective Sept 1, 2017.
# Beer excise: $0.156/gal -> $0.262/gal (+$0.106/gal,
# +68%, +~2c per 12 oz can). Treated DMA = Salisbury (576),
# the only DMA where DE is the dominant state (54.8% pop).
# Salisbury also covers eastern shore MD + a sliver of VA;
# this is a fractional-DMA event analogous to Philly SSB.
# Caveat: Salisbury beer Spot TV averages ~$11K/year
# total, so the analysis is severely underpowered against
# any plausible effect size.
DELAWARE_BEER = Event(
    label='delaware_beer',
    category='BEER',
    treated_marketcode='176',  # public DMA 576
    dma_name='Salisbury',
    dma_short='Salisbury',
    tax_name='Delaware Beer Excise Tax (HB 241)',
    passage_quarter=pd.Period('2017Q3'),  # signed Jul 1 2017
    impl_quarter=pd.Period('2017Q3'),     # effective Sep 1
    rate_description=(
        '$0.156/gal -> $0.262/gal (+$0.106/gal, +68%)'
    ),
    parent_codes_use=None,
    parent_names_use=None,
)


REGISTRY: dict[str, Event] = {
    e.label: e for e in (PHILLY_SSB, SEATTLE_SSB,
                         DELAWARE_BEER)
}


def get(label: str) -> Event:
    if label not in REGISTRY:
        raise KeyError(
            f"Event {label!r} not in registry. "
            f"Known: {sorted(REGISTRY)}"
        )
    return REGISTRY[label]
