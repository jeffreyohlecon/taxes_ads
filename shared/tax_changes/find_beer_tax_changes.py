"""
Find state-level beer excise tax changes from the Tax Policy
Center "State Alcohol Excise Tax Rates, 1982-2023" file.

Source workbook is "as of January 1" of each year, dollars per
gallon. A change between year Y-1 and year Y means the new rate
took effect at some point during calendar Y-1 (or on Jan 1 of Y).

Outputs:
  - beer_panel_long.csv: state-year beer rate panel
  - beer_changes_all.csv: every state-year where the beer rate
    moved (any direction, any magnitude)
  - beer_changes_post2018.csv: subset with year >= 2019
"""

from pathlib import Path
import pandas as pd

HERE = Path(__file__).parent
SRC = HERE / "state_alcohol_rates_1982_2023.xlsx"
OUT_PANEL = HERE / "beer_panel_long.csv"
OUT_ALL = HERE / "beer_changes_all.csv"
OUT_POST = HERE / "beer_changes_post2018.csv"


def load_panel() -> pd.DataFrame:
    raw = pd.read_excel(SRC, sheet_name="1982-2023", header=None)
    # Row 3 has the headers in the workbook.
    header_row = 3
    cols = raw.iloc[header_row].tolist()
    df = raw.iloc[header_row + 1 :].copy()
    df.columns = cols
    df = df[["State name", "State abbreviation", "Year", "Beer"]]
    df = df.rename(
        columns={
            "State name": "state",
            "State abbreviation": "state_abbr",
            "Year": "year",
            "Beer": "beer_rate_raw",
        }
    )
    df = df.dropna(subset=["state", "year"])
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    # Many cells carry an asterisk to flag "see footnote / additional
    # sales tax." Strip it for the numeric column but keep a flag.
    raw_str = df["beer_rate_raw"].astype(str)
    df["has_footnote"] = raw_str.str.contains(r"\*")
    cleaned = raw_str.str.replace(r"[*\s]", "", regex=True)
    df["beer_rate"] = pd.to_numeric(cleaned, errors="coerce")
    df = df.sort_values(["state_abbr", "year"]).reset_index(drop=True)
    return df


def find_changes(panel: pd.DataFrame) -> pd.DataFrame:
    p = panel.copy()
    p["beer_rate_prev"] = p.groupby("state_abbr")["beer_rate"].shift(1)
    p["delta"] = p["beer_rate"] - p["beer_rate_prev"]
    p["pct_change"] = p["delta"] / p["beer_rate_prev"]
    changed = p[p["delta"].abs() > 1e-9].copy()
    changed = changed[
        [
            "state",
            "state_abbr",
            "year",
            "beer_rate_prev",
            "beer_rate",
            "delta",
            "pct_change",
            "has_footnote",
        ]
    ]
    return changed.sort_values(["year", "state_abbr"]).reset_index(
        drop=True
    )


def main() -> None:
    panel = load_panel()
    panel.to_csv(OUT_PANEL, index=False)

    changes = find_changes(panel)
    changes.to_csv(OUT_ALL, index=False)

    post = changes[changes["year"] >= 2019].copy()
    post.to_csv(OUT_POST, index=False)

    print(f"Wrote {OUT_PANEL.name}: {len(panel)} state-years")
    print(f"Wrote {OUT_ALL.name}: {len(changes)} changes 1982-2023")
    print(f"Wrote {OUT_POST.name}: {len(post)} changes 2019-2023")
    print()
    print("=== All beer-rate changes, 2019-2023 ===")
    if post.empty:
        print("(none)")
    else:
        with pd.option_context(
            "display.max_rows", None, "display.width", 120
        ):
            print(
                post[
                    [
                        "year",
                        "state_abbr",
                        "state",
                        "beer_rate_prev",
                        "beer_rate",
                        "delta",
                        "pct_change",
                    ]
                ].to_string(index=False)
            )


if __name__ == "__main__":
    main()
