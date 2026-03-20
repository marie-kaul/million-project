"""
Process crime data from raw XLS files in 0_raw/public_data/crime/.

Source files:
    total_crime_kommun_1996to2022.xls
    crime_against_life_1996to2022_kommun.xls

Both files:
    - Single sheet "Resultat"
    - Rows 0–4: title / header (year labels in row 3, "Antal" labels in row 4)
    - Data: municipality name rows alternating with data rows (or 3-level hierarchy)
    - Footer: administrative footnotes (rows without numeric data)

File 1: one row per municipality with total crime counts per year.
File 2: 3-level hierarchy (municipality → group → specific crime type).
         Extract "3 kap." (crimes against life and health) rows only.

Municipality codes matched from SCB API (BE/BE0101/BE0101A or similar).

Outputs:
    ../output/crime_total_municipality.csv
    ../output/crime_against_life_municipality.csv
"""

import os
import re
import sys
import time
import urllib.request
import json

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "../output")

EXTERNAL_PATH = os.environ.get("EXTERNAL_PATHS", "").split()[0] if os.environ.get("EXTERNAL_PATHS") else ""
CRIME_DIR = os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "crime") if EXTERNAL_PATH else ""

TOTAL_FILE = "total_crime_kommun_1996to2022.xls"
LIFE_FILE  = "crime_against_life_1996to2022_kommun.xls"

YEARS = list(range(1996, 2023))  # 1996–2022 inclusive


# ── Municipality code lookup ──────────────────────────────────────────────────

def _fetch_municipality_codes() -> dict:
    """
    Fetch current municipality list from SCB API and return name→code dict.
    Tries BE/BE0101/BE0101A/BefolkningNy as a known municipality-level table.
    Falls back to a minimal hard-coded Swedish normalisation if API fails.
    """
    url = "https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101/BE0101A/BefolkningNy"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "python/prepare_crime"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            meta = json.loads(resp.read().decode("utf-8"))
        time.sleep(0.5)
        for var in meta.get("variables", []):
            if var.get("code") == "Region":
                code_list = var.get("values", [])
                name_list = var.get("valueTexts", [])
                # SCB region codes for municipalities are 4-digit
                result = {}
                for code, name in zip(code_list, name_list):
                    if re.match(r"^\d{4}$", code):
                        result[_normalise(name)] = code
                print(f"  Loaded {len(result)} municipality codes from SCB API")
                return result
    except Exception as e:
        print(f"  Warning: could not fetch municipality codes from SCB API: {e}")
    return {}


def _normalise(name: str) -> str:
    """Lowercase and strip whitespace/punctuation for fuzzy matching."""
    return re.sub(r"\s+", " ", name.lower().strip())


# ── XLS parsing ───────────────────────────────────────────────────────────────

def _read_xls(filepath: str) -> pd.DataFrame:
    """Read XLS file, skipping the first 5 header rows. Returns raw DataFrame."""
    return pd.read_excel(filepath, sheet_name="Resultat", header=None, skiprows=5, engine="xlrd")


def _is_footnote_row(row: pd.Series) -> bool:
    """Detect footer/footnote rows: text in first cell, all others NaN or non-numeric."""
    first = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ""
    if not first.strip():
        return True
    # If numeric values exist in year columns, it's a data row
    numeric_count = sum(1 for v in row.iloc[1:] if isinstance(v, (int, float)) and not pd.isna(v))
    return numeric_count == 0


def _parse_total_crime(df_raw: pd.DataFrame, muni_codes: dict) -> pd.DataFrame:
    """
    Parse total crime file: each row is a municipality with counts for 1996–2022.
    Columns: [municipality_name, 1996, 1997, ..., 2022]  (27 year columns + name)
    """
    rows = []
    for _, row in df_raw.iterrows():
        if _is_footnote_row(row):
            continue
        name = str(row.iloc[0]).strip()
        if not name or name.lower() in ("nan", "none"):
            continue
        code = muni_codes.get(_normalise(name), "")
        # Year values start at column index 1
        year_vals = list(row.iloc[1: 1 + len(YEARS)])
        for year, val in zip(YEARS, year_vals):
            rows.append({
                "municipality_code": code,
                "municipality_name": name,
                "year": year,
                "total_crimes": pd.to_numeric(val, errors="coerce"),
            })
    return pd.DataFrame(rows)


def _parse_crime_against_life(df_raw: pd.DataFrame, muni_codes: dict) -> pd.DataFrame:
    """
    Parse crime-against-life file: 3-level hierarchy.
    Identify rows whose name cell matches "3 kap." (crimes against life and health).
    Track current municipality from preceding name rows.
    """
    rows = []
    current_muni = ""
    current_code = ""

    for _, row in df_raw.iterrows():
        if _is_footnote_row(row):
            continue
        name = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ""
        if not name:
            continue

        # Check if this looks like a municipality row (no leading spaces, no "kap.")
        is_chapter = bool(re.match(r"^\d+\s*kap\.", name, re.IGNORECASE))
        is_subtype = name.startswith(" ") or re.match(r"^\s", name)

        if not is_chapter and not is_subtype:
            # Likely a municipality name
            current_muni = name.strip()
            current_code = muni_codes.get(_normalise(current_muni), "")
            continue

        # We only want "3 kap." rows (crimes against life and health)
        if is_chapter and re.match(r"^3\s*kap\.", name, re.IGNORECASE):
            year_vals = list(row.iloc[1: 1 + len(YEARS)])
            for year, val in zip(YEARS, year_vals):
                rows.append({
                    "municipality_code": current_code,
                    "municipality_name": current_muni,
                    "year": year,
                    "crimes_against_life": pd.to_numeric(val, errors="coerce"),
                })

    return pd.DataFrame(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not CRIME_DIR:
        print("Warning: EXTERNAL_PATHS not set. Cannot locate crime data files.")
        sys.exit(1)

    total_path = os.path.join(CRIME_DIR, TOTAL_FILE)
    life_path  = os.path.join(CRIME_DIR, LIFE_FILE)

    for p in (total_path, life_path):
        if not os.path.exists(p):
            print(f"Error: crime file not found: {p}", file=sys.stderr)
            sys.exit(1)

    print("Fetching municipality codes for name→code matching...")
    muni_codes = _fetch_municipality_codes()

    # --- Total crime ---
    print(f"\nParsing total crime file: {TOTAL_FILE}")
    df_raw_total = _read_xls(total_path)
    df_total = _parse_total_crime(df_raw_total, muni_codes)
    unmatched = df_total[df_total["municipality_code"] == ""]["municipality_name"].unique()
    if len(unmatched):
        print(f"  Warning: {len(unmatched)} municipalities without code match: {list(unmatched[:5])}")
    out1 = os.path.join(OUTPUT_DIR, "crime_total_municipality.csv")
    df_total.to_csv(out1, index=False)
    print(f"  Written: crime_total_municipality.csv ({len(df_total)} rows)")

    # --- Crime against life ---
    print(f"\nParsing crime against life file: {LIFE_FILE}")
    df_raw_life = _read_xls(life_path)
    df_life = _parse_crime_against_life(df_raw_life, muni_codes)
    unmatched2 = df_life[df_life["municipality_code"] == ""]["municipality_name"].unique()
    if len(unmatched2):
        print(f"  Warning: {len(unmatched2)} municipalities without code match: {list(unmatched2[:5])}")
    out2 = os.path.join(OUTPUT_DIR, "crime_against_life_municipality.csv")
    df_life.to_csv(out2, index=False)
    print(f"  Written: crime_against_life_municipality.csv ({len(df_life)} rows)")

    print("\nprepare_crime.py done.")


if __name__ == "__main__":
    main()
