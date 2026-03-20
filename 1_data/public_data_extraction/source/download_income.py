"""
Download income / economic standard data from SCB Statistikdatabasen.

DeSO  table: HE/HE0110/HE0110I/Tab4InkDesoRegso     (2011–2024, low/high econ standard)
Municipality: HE/HE0110/HE0110A/NetInk02             (2000–2024, net income)
Municipality: HE/HE0110/HE0110A/SamForvInk1          (1999–2024, total earned income)

Outputs:
    ../output/income_deso.csv
    ../output/income_municipality.csv
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

DESO_PATH   = "HE/HE0110/HE0110I/Tab4InkDesoRegso"
MUNI_NET    = "HE/HE0110/HE0110A/NetInk02"
MUNI_EARNED = "HE/HE0110/HE0110A/SamForvInk1"


def _get_raw_dir(subdir):
    if EXTERNAL_PATH:
        return os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "scb", subdir)
    return ""


def download_deso():
    print("Fetching income metadata (DeSO)...")
    meta = fetch_metadata(DESO_PATH)
    query = all_values_query(meta)
    print("  Downloading DeSO income data...")
    raw = fetch_table(DESO_PATH, query)
    raw_dir = _get_raw_dir("income")
    if raw_dir:
        save_raw(raw, "income", "deso", os.path.dirname(raw_dir))
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "deso")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"] != "00"].copy()
    out = os.path.join(OUTPUT_DIR, "income_deso.csv")
    df.to_csv(out, index=False)
    print(f"  Written: income_deso.csv ({len(df)} rows)")


def download_municipality():
    print("Fetching income metadata (municipality — NetInk02)...")
    meta_net = fetch_metadata(MUNI_NET)
    raw_net = fetch_table(MUNI_NET, all_values_query(meta_net))
    raw_dir = _get_raw_dir("income")
    if raw_dir:
        save_raw(raw_net, "income", "municipality_net", os.path.dirname(raw_dir))
    df_net = parse_to_df(raw_net)
    df_net["source_table"] = "NetInk02"

    print("  Downloading municipality income data (SamForvInk1)...")
    meta_earn = fetch_metadata(MUNI_EARNED)
    raw_earn = fetch_table(MUNI_EARNED, all_values_query(meta_earn))
    if raw_dir:
        save_raw(raw_earn, "income", "municipality_earned", os.path.dirname(raw_dir))
    df_earn = parse_to_df(raw_earn)
    df_earn["source_table"] = "SamForvInk1"

    df = pd.concat([df_net, df_earn], ignore_index=True)
    df = standardise_region_cols(df, "Region", "municipality")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"].str.match(r"^\d{4}$", na=False)].copy()
    out = os.path.join(OUTPUT_DIR, "income_municipality.csv")
    df.to_csv(out, index=False)
    print(f"  Written: income_municipality.csv ({len(df)} rows)")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_deso()
    download_municipality()
    print("download_income.py done.")


if __name__ == "__main__":
    main()
