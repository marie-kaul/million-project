"""
Download housing tenure data from SCB Statistikdatabasen.

DeSO  table: HE/HE0111/HE0111YDeSo/HushallT33Deso  (2012–2023, persons by tenure, DeSO 2018)
             NOTE: uses DeSO 2018 boundaries only. Flag DeSO 2025 tables if encountered.
Municipality (persons): HE/HE0111/HE0111A (sub-tables auto-detected)
Municipality (dwellings): BO/BO0104/BO0104D/BO0104T04 (1990–2024)

Outputs:
    ../output/housing_tenure_deso.csv           (persons by tenure, DeSO level)
    ../output/housing_tenure_municipality.csv   (persons + dwellings, municipality level)
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

DESO_PATH      = "HE/HE0111/HE0111YDeSo/HushallT33Deso"
MUNI_PERSONS_BASE = "HE/HE0111/HE0111A"
MUNI_DWELLINGS = "BO/BO0104/BO0104D/BO0104T04"


def _raw_parent():
    return os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "scb") if EXTERNAL_PATH else ""


def download_deso():
    print("Fetching housing tenure metadata (DeSO 2018)...")
    meta = fetch_metadata(DESO_PATH)
    # Sanity check: warn if this looks like a DeSO 2025 table
    for var in meta.get("variables", []):
        if "2025" in var.get("code", "") or "2025" in var.get("text", ""):
            print("  WARNING: DeSO 2025 variable detected — expected DeSO 2018 table. Check table path.")
    query = all_values_query(meta)
    print("  Downloading DeSO housing tenure data (persons by tenure)...")
    raw = fetch_table(DESO_PATH, query)
    parent = _raw_parent()
    if parent:
        save_raw(raw, "housing_tenure", "deso", parent)
    df = parse_to_df(raw)
    df = standardise_region_cols(df, "Region", "deso")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["measure"] = "persons"
    df = df[df["region_code"] != "00"].copy()
    out = os.path.join(OUTPUT_DIR, "housing_tenure_deso.csv")
    df.to_csv(out, index=False)
    print(f"  Written: housing_tenure_deso.csv ({len(df)} rows)")


def _detect_best_muni_persons_table():
    """Auto-detect best sub-table under HE0111A for municipality-level persons by tenure."""
    print("  Inspecting HE0111A sub-tables for municipality persons by tenure...")
    try:
        meta = fetch_metadata(MUNI_PERSONS_BASE)
    except Exception as e:
        print(f"  Warning: could not fetch HE0111A metadata: {e}")
        return None, None

    subtables = meta if isinstance(meta, list) else meta.get("table", [])
    if not subtables:
        return None, None

    best_path = None
    best_meta = None
    best_years = 0

    for item in subtables:
        tbl_id = item.get("id", "")
        tbl_path = f"{MUNI_PERSONS_BASE}/{tbl_id}"
        try:
            tbl_meta = fetch_metadata(tbl_path)
        except Exception:
            continue
        n_years = 0
        for var in tbl_meta.get("variables", []):
            if var.get("code", "").lower() in ("tid", "år"):
                n_years = len(var.get("values", []))
                break
        print(f"    {tbl_id}: {n_years} years")
        if n_years > best_years:
            best_years = n_years
            best_path = tbl_path
            best_meta = tbl_meta

    return best_path, best_meta


def download_municipality():
    print("Fetching housing tenure data (municipality)...")

    # --- Persons by tenure ---
    best_path, best_meta = _detect_best_muni_persons_table()
    dfs = []
    if best_path and best_meta:
        raw_p = fetch_table(best_path, all_values_query(best_meta))
        parent = _raw_parent()
        if parent:
            save_raw(raw_p, "housing_tenure", "municipality_persons", parent)
        df_p = parse_to_df(raw_p)
        df_p["measure"] = "persons"
        df_p["source_table"] = best_path
        dfs.append(df_p)
    else:
        print("  Warning: no persons-by-tenure table found under HE0111A. Skipping.")

    # --- Dwelling count ---
    print("  Downloading municipality dwelling count data (BO0104T04)...")
    meta_d = fetch_metadata(MUNI_DWELLINGS)
    raw_d = fetch_table(MUNI_DWELLINGS, all_values_query(meta_d))
    parent = _raw_parent()
    if parent:
        save_raw(raw_d, "housing_tenure", "municipality_dwellings", parent)
    df_d = parse_to_df(raw_d)
    df_d["measure"] = "dwellings"
    df_d["source_table"] = MUNI_DWELLINGS
    dfs.append(df_d)

    df = pd.concat(dfs, ignore_index=True)
    df = standardise_region_cols(df, "Region", "municipality")
    df = standardise_year_col(df)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["region_code"].str.match(r"^\d{4}$", na=False)].copy()
    out = os.path.join(OUTPUT_DIR, "housing_tenure_municipality.csv")
    df.to_csv(out, index=False)
    print(f"  Written: housing_tenure_municipality.csv ({len(df)} rows)")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    download_deso()
    download_municipality()
    print("download_housing_tenure.py done.")


if __name__ == "__main__":
    main()
