"""
Download employment status data from SCB Statistikdatabasen.

DeSO  table: AM/AM0210/AM0210G/ArRegDesoStatusN   (2020–2024, labour market status)
Municipality: AM/AM0210/AM0210D (sub-tables auto-detected at runtime)
              Target: annual employment by municipality, ideally back to 1985.

Outputs:
    ../output/employment_deso.csv
    ../output/employment_municipality.csv
"""

import os
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "../output")

EXTERNAL_PATH = os.environ.get("EXTERNAL_PATHS", "").split()[0] if os.environ.get("EXTERNAL_PATHS") else ""

sys.path.insert(0, SCRIPT_DIR)
from scb_utils import (
    fetch_metadata,
    fetch_table,
    parse_to_df,
    save_raw,
    all_values_query,
    standardise_region_cols,
    standardise_year_col,
)

DESO_PATH  = "AM/AM0210/AM0210G/ArRegDesoStatusN"
MUNI_BASE  = "AM/AM0210/AM0210D"


def _raw_parent():
    return os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "scb") if EXTERNAL_PATH else ""


def download_deso():
    print("Fetching employment metadata (DeSO)...")
    meta = fetch_metadata(DESO_PATH)
    query = all_values_query(meta)
    print("  Downloading DeSO employment data...")
    raw = fetch_table(DESO_PATH, query)
    parent = _raw_parent()
    if parent:
        save_raw(raw, "employment", "deso", parent)
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "deso")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"] != "00"].copy()
    out = os.path.join(OUTPUT_DIR, "employment_deso.csv")
    df.to_csv(out, index=False)
    print(f"  Written: employment_deso.csv ({len(df)} rows)")


def _detect_best_muni_table():
    """
    Inspect AM0210D sub-tables and return the one with the longest year coverage.
    Warns if coverage doesn't reach 1985.
    """
    print("  Inspecting AM0210D sub-tables for best municipality coverage...")
    try:
        meta = fetch_metadata(MUNI_BASE)
    except Exception as e:
        print(f"  Warning: could not fetch AM0210D metadata: {e}")
        return None, None

    subtables = meta if isinstance(meta, list) else meta.get("table", [])
    if not subtables:
        print("  Warning: no sub-tables found under AM0210D")
        return None, None

    best_path = None
    best_meta = None
    best_years = 0

    for item in subtables:
        tbl_id = item.get("id", "")
        tbl_path = f"{MUNI_BASE}/{tbl_id}"
        try:
            tbl_meta = fetch_metadata(tbl_path)
        except Exception:
            continue
        # Count available years
        n_years = 0
        min_year = 9999
        for var in tbl_meta.get("variables", []):
            if var.get("code", "").lower() in ("tid", "år"):
                vals = var.get("values", [])
                n_years = len(vals)
                years_int = [int(v) for v in vals if v.isdigit()]
                if years_int:
                    min_year = min(years_int)
                break
        print(f"    {tbl_id}: {n_years} years, earliest={min_year}")
        if n_years > best_years:
            best_years = n_years
            best_path = tbl_path
            best_meta = tbl_meta

    if best_path and min_year > 1985:
        print(f"  Warning: best municipality table ({best_path}) only reaches {min_year}, not 1985.")

    return best_path, best_meta


def download_municipality():
    print("Fetching employment data (municipality)...")
    best_path, best_meta = _detect_best_muni_table()

    if best_path is None:
        print("  Warning: could not identify a valid AM0210D sub-table. Skipping municipality employment.")
        # Write empty placeholder so pipeline doesn't break
        pd.DataFrame(columns=["region_code", "region_name", "region_type", "year", "value"]).to_csv(
            os.path.join(OUTPUT_DIR, "employment_municipality.csv"), index=False
        )
        return

    query = all_values_query(best_meta)
    print(f"  Downloading municipality employment data from {best_path}...")
    raw = fetch_table(best_path, query)
    parent = _raw_parent()
    if parent:
        save_raw(raw, "employment", "municipality", parent)
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "municipality")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"].str.match(r"^\d{4}$", na=False)].copy()
    df["source_table"] = best_path
    out = os.path.join(OUTPUT_DIR, "employment_municipality.csv")
    df.to_csv(out, index=False)
    print(f"  Written: employment_municipality.csv ({len(df)} rows)")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_deso()
    download_municipality()
    print("download_employment.py done.")


if __name__ == "__main__":
    main()
