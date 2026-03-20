"""
Download household type and size data from SCB Statistikdatabasen.

DeSO  table: BE/BE0101/BE0101Y/HushallDesoTyp    (2011–2024, household type)
             NOTE: household *size* is only available at municipality level.
Municipality: BE/BE0101/BE0101S/HushallT05       (2011–2024, type + num children)
Municipality: BE/BE0101/BE0101S/HushallT03       (2011–2024, household size)

Outputs:
    ../output/households_deso.csv
    ../output/households_municipality.csv
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

DESO_PATH   = "BE/BE0101/BE0101Y/HushallDesoTyp"
MUNI_TYPE   = "BE/BE0101/BE0101S/HushallT05"
MUNI_SIZE   = "BE/BE0101/BE0101S/HushallT03"


def _raw_parent():
    return os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "scb") if EXTERNAL_PATH else ""


def download_deso():
    print("Fetching households metadata (DeSO)...")
    meta = fetch_metadata(DESO_PATH)
    query = all_values_query(meta)
    print("  Downloading DeSO household data (type only — size unavailable at DeSO level)...")
    raw = fetch_table(DESO_PATH, query)
    parent = _raw_parent()
    if parent:
        save_raw(raw, "households", "deso", parent)
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "deso")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["note"] = "household_type_only_deso_level"
    df = df[df["region_code"] != "00"].copy()
    out = os.path.join(OUTPUT_DIR, "households_deso.csv")
    df.to_csv(out, index=False)
    print(f"  Written: households_deso.csv ({len(df)} rows)")


def download_municipality():
    print("Fetching households metadata (municipality — HushallT05)...")
    meta_type = fetch_metadata(MUNI_TYPE)
    raw_type = fetch_table(MUNI_TYPE, all_values_query(meta_type))
    parent = _raw_parent()
    if parent:
        save_raw(raw_type, "households", "municipality_type", parent)
    df_type = parse_to_df(raw_type)
    df_type["source_table"] = "HushallT05"

    print("  Downloading municipality household size data (HushallT03)...")
    meta_size = fetch_metadata(MUNI_SIZE)
    raw_size = fetch_table(MUNI_SIZE, all_values_query(meta_size))
    if parent:
        save_raw(raw_size, "households", "municipality_size", parent)
    df_size = parse_to_df(raw_size)
    df_size["source_table"] = "HushallT03"

    df = pd.concat([df_type, df_size], ignore_index=True)
    df = standardise_region_cols(df, "Region", "municipality")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"].str.match(r"^\d{4}$", na=False)].copy()
    out = os.path.join(OUTPUT_DIR, "households_municipality.csv")
    df.to_csv(out, index=False)
    print(f"  Written: households_municipality.csv ({len(df)} rows)")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_deso()
    download_municipality()
    print("download_households.py done.")


if __name__ == "__main__":
    main()
