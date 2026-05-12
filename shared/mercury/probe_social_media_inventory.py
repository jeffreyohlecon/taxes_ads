#!/usr/bin/env python3
"""
Inventory probe: what's in /kilts social media (2022+)?

Pure discovery — no parsing, no aggregation. For each year
2022 forward and each platform subdirectory, emit:
  - full directory tree (files + sizes)
  - header row of each .tsv file
  - first 3 data rows (truncated to 500 chars per row)
  - reference-table dumps (full content, capped at 100 rows
    each)

Goal is to answer: which platforms are present, what
columns each carries, and — critically — whether any geo
column (DMA, state, ZIP, city, country) exists. That
determines whether social media data can support a
treated-vs-control geographic excise-tax test the way
Spot TV does.

Output:
  social_media_inventory.txt  (in submit dir)

Run on Mercury under SLURM (see
run_probe_social_media_inventory.sh).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

ADINTEL_ROOT = '/kilts/adintel'
YEARS = ['2022', '2023']
OUT_PATH = 'social_media_inventory.txt'

# Candidate directory names to look for under each year.
# We don't know the exact layout yet — list everything and
# flag anything that looks platform-named.
SOCIAL_HINTS = (
    'social', 'facebook', 'instagram', 'twitter', 'x_',
    'reddit', 'tiktok', 'pinterest', 'meta', 'digital',
)


def log(out, msg=''):
    print(msg, flush=True)
    out.write(msg + '\n')


def list_tree(out, root):
    """Walk root and print every file with size."""
    if not os.path.exists(root):
        log(out, f'  [missing] {root}')
        return
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        log(out, f'  DIR: {rel}/')
        for fn in sorted(filenames):
            full = os.path.join(dirpath, fn)
            try:
                sz = os.path.getsize(full)
            except OSError as e:
                log(out, f'    {fn}  [stat failed: {e}]')
                continue
            log(out, f'    {fn:60s}  {sz:>14,d} bytes')


def peek_tsv(out, path, n_data=3, max_chars=500):
    """Dump header + first n_data rows of a .tsv file."""
    log(out, f'\n  --- peek: {path}')
    try:
        with open(path, 'r', encoding='utf-8',
                  errors='replace') as f:
            header = f.readline().rstrip('\n')
            log(out, f'    HEADER ({header.count(chr(9))+1} cols):')
            for col in header.split('\t'):
                log(out, f'      - {col}')
            log(out, '    SAMPLE ROWS:')
            for i in range(n_data):
                row = f.readline()
                if not row:
                    break
                row = row.rstrip('\n')
                if len(row) > max_chars:
                    row = row[:max_chars] + ' ...[truncated]'
                log(out, f'      [{i}] {row}')
    except PermissionError as e:
        log(out, f'    [permission denied: {e}]')
    except OSError as e:
        log(out, f'    [open failed: {e}]')


def dump_reference(out, path, max_rows=100):
    """Dump up to max_rows of a small reference table."""
    log(out, f'\n  --- reference dump: {path}')
    try:
        with open(path, 'r', encoding='utf-8',
                  errors='replace') as f:
            for i, row in enumerate(f):
                if i >= max_rows:
                    log(out, f'    ...[truncated at '
                             f'{max_rows} rows]')
                    break
                log(out, f'    {row.rstrip(chr(10))}')
    except PermissionError as e:
        log(out, f'    [permission denied: {e}]')
    except OSError as e:
        log(out, f'    [open failed: {e}]')


def main():
    with open(OUT_PATH, 'w') as out:
        log(out, '=' * 60)
        log(out, 'Social media inventory probe')
        log(out, f'Started: {datetime.now().isoformat()}')
        log(out, f'Root:    {ADINTEL_ROOT}')
        log(out, '=' * 60)

        # First: top-level listing of /kilts/adintel to see
        # what year directories exist and what's adjacent.
        log(out, '\n## Top-level /kilts/adintel listing')
        try:
            entries = sorted(os.listdir(ADINTEL_ROOT))
            for e in entries:
                full = os.path.join(ADINTEL_ROOT, e)
                kind = 'DIR ' if os.path.isdir(full) \
                    else 'FILE'
                log(out, f'  {kind} {e}')
        except OSError as e:
            log(out, f'  [listdir failed: {e}]')

        for year in YEARS:
            log(out, '\n' + '=' * 60)
            log(out, f'## YEAR {year}')
            log(out, '=' * 60)
            ydir = os.path.join(ADINTEL_ROOT, year)
            if not os.path.isdir(ydir):
                log(out, f'  [missing] {ydir}')
                continue

            # Year-level subdirectory listing.
            log(out, f'\n### Year-level subdirs of {year}:')
            try:
                subs = sorted(os.listdir(ydir))
            except OSError as e:
                log(out, f'  [listdir failed: {e}]')
                continue
            for sub in subs:
                full = os.path.join(ydir, sub)
                kind = 'DIR ' if os.path.isdir(full) \
                    else 'FILE'
                flag = ''
                lo = sub.lower()
                if any(h in lo for h in SOCIAL_HINTS):
                    flag = '  <-- social-hint'
                log(out, f'  {kind} {sub}{flag}')

            # Walk any social-flavored subdirectory and any
            # "Occurrences"-like one (in case social ads
            # live under the standard schema).
            for sub in subs:
                full = os.path.join(ydir, sub)
                if not os.path.isdir(full):
                    continue
                lo = sub.lower()
                is_social = any(h in lo for h in
                                SOCIAL_HINTS)
                if not is_social:
                    continue
                log(out, f'\n### Tree of {year}/{sub}:')
                list_tree(out, full)

                # Peek every .tsv we find under social dirs.
                for dp, _, fns in os.walk(full):
                    for fn in sorted(fns):
                        if fn.lower().endswith('.tsv'):
                            peek_tsv(out,
                                     os.path.join(dp, fn))

            # Also: check for a Reference subdirectory and
            # dump anything that smells like a media-type
            # or platform reference table (small files only).
            for sub in subs:
                full = os.path.join(ydir, sub)
                if not os.path.isdir(full):
                    continue
                if 'reference' not in sub.lower():
                    continue
                log(out, f'\n### Reference tables in '
                         f'{year}/{sub}:')
                for dp, _, fns in os.walk(full):
                    for fn in sorted(fns):
                        if not fn.lower().endswith('.tsv'):
                            continue
                        path = os.path.join(dp, fn)
                        try:
                            sz = os.path.getsize(path)
                        except OSError:
                            continue
                        # Only dump small ref tables; flag
                        # any that mention media or social.
                        lo_fn = fn.lower()
                        looks_relevant = (
                            sz < 5_000_000 and (
                                'media' in lo_fn or
                                'source' in lo_fn or
                                'social' in lo_fn or
                                'platform' in lo_fn or
                                'distributor' in lo_fn
                            )
                        )
                        if looks_relevant:
                            dump_reference(out, path)

        log(out, '\n' + '=' * 60)
        log(out, f'Finished: {datetime.now().isoformat()}')
        log(out, '=' * 60)

    print(f'Wrote {OUT_PATH}')


if __name__ == '__main__':
    main()
