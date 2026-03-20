"""
Download educational attainment data from SCB Statistikdatabasen.

DeSO  table: UF/UF0506/UF0506D/UtbSUNBefDesoRegso  (2015–2023)
Municipality: UF/UF0506/UF0506B/Utbildning           (1985–2024)

Outputs:
    ../output/education_deso.csv
    ../output/education_municipality.csv
Raw JSON saved under {EXTERNAL_PATHS[0]}/0_raw/public_data/scb/education/
"""

import os
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "../output")

# Load external path from environment (set by local_env.sh)
EXTERNAL_PATH = os.environ.get("EXTERNAL_PATHS", "").split()[0] if os.environ.get("EXTERNAL_PATHS") else ""
RAW_DIR = os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "scb", "education") if EXTERNAL_PATH else ""

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

DESO_PATH  = "UF/UF0506/UF0506D/UtbSUNBefDesoRegso"
MUNI_PATH  = "UF/UF0506/UF0506B/Utbildning"


def download_deso():
    print("Fetching education metadata (DeSO)...")
    meta = fetch_metadata(DESO_PATH)
    query = all_values_query(meta)
    print("  Downloading DeSO education data...")
    raw = fetch_table(DESO_PATH, query)
    if RAW_DIR:
        save_raw(raw, "education", "deso", os.path.dirname(RAW_DIR))
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "deso")
    df = standardise_year_col(df)
    # Rename education level dimension
    for col in ["UtbildningsNiva", "UtbNiva", "SUN2000Niva"]:
        if col in df.columns:
            df = df.rename(columns={col: "education_level", f"{col}_label": "education_level_label"})
            break
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"] != "00"].copy()  # drop national total if present
    out = os.path.join(OUTPUT_DIR, "education_deso.csv")
    df.to_csv(out, index=False)
    print(f"  Written: education_deso.csv ({len(df)} rows)")
    return df


def download_municipality():
    print("Fetching education metadata (municipality)...")
    meta = fetch_metadata(MUNI_PATH)
    query = all_values_query(meta)
    print("  Downloading municipality education data...")
    raw = fetch_table(MUNI_PATH, query)
    if RAW_DIR:
        save_raw(raw, "education", "municipality", os.path.dirname(RAW_DIR))
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "municipality")
    df = standardise_year_col(df)
    for col in ["UtbildningsNiva", "UtbNiva", "SUN2000Niva"]:
        if col in df.columns:
            df = df.rename(columns={col: "education_level", f"{col}_label": "education_level_label"})
            break
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # Keep only 4-digit municipality codes (drop county/national aggregates)
    df = df[df["region_code"].str.match(r"^\d{4}$", na=False)].copy()
    out = os.path.join(OUTPUT_DIR, "education_municipality.csv")
    df.to_csv(out, index=False)
    print(f"  Written: education_municipality.csv ({len(df)} rows)")
    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_deso()
    download_municipality()
    print("download_education.py done.")


if __name__ == "__main__":
    main()
