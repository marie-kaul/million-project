"""
Download SCB panel data: education, income, households, employment, housing tenure.

Parameterized downloader for multiple SCB Statistikdatabasen tables.
Each table spec defines DeSO and municipality paths; the script handles
fetching, chunking, parsing, and standardizing all of them.

Outputs: {name}_deso.csv, {name}_municipality.csv in ../output/
Raw JSON saved under {EXTERNAL_PATHS[0]}/0_raw/public_data/scb/{name}/
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
    fetch_table_all,
    parse_to_df,
    save_raw,
    standardise_region_cols,
    standardise_year_col,
)


# ── Table specifications ──────────────────────────────────────────────────────

TABLES = [
    {
        "name": "education",
        "deso": {"path": "UF/UF0506/UF0506D/UtbSUNBefDesoRegso"},
        "muni": {"path": "UF/UF0506/UF0506B/Utbildning"},
        "education_level_cols": ["UtbildningsNiva", "UtbNiva", "SUN2000Niva"],
    },
    {
        "name": "income",
        "deso": {"path": "HE/HE0110/HE0110I/Tab4InkDesoRegso"},
        "muni": [
            {"path": "HE/HE0110/HE0110A/NetInk02", "label": "net_income"},
            {"path": "HE/HE0110/HE0110A/SamForvInk1", "label": "earned_income"},
        ],
    },
    {
        "name": "households",
        "deso": {"path": "BE/BE0101/BE0101Y/HushallDesoTyp", "note": "type_only_deso"},
        "muni": [
            {"path": "BE/BE0101/BE0101S/HushallT05", "label": "type"},
            {"path": "BE/BE0101/BE0101S/HushallT03", "label": "size"},
        ],
    },
    {
        "name": "employment",
        "deso": {"path": "AM/AM0210/AM0210G/ArRegDesoStatusN"},
        "muni_auto_detect": {"base": "AM/AM0210/AM0210D", "warn_before": 1985},
    },
    {
        "name": "housing_tenure",
        "deso": {"path": "HE/HE0111/HE0111YDeSo/HushallT33Deso", "measure": "persons"},
        "muni": [
            {"path": "HE/HE0111/HE0111A", "auto_detect": True, "measure": "persons"},
            {"path": "BO/BO0104/BO0104D/BO0104T04", "measure": "dwellings"},
        ],
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _raw_dir(name: str) -> str:
    """Path to 0_raw/public_data/scb/{name}"""
    if EXTERNAL_PATH:
        return os.path.join(EXTERNAL_PATH, "0_raw", "public_data", "scb", name)
    return ""


def _output_path(name: str, level: str) -> str:
    """Output CSV path for a table."""
    return os.path.join(OUTPUT_DIR, f"{name}_{level}.csv")


def _fetch_and_parse(path: str, meta: dict, var_name: str = "", table_label: str = ""):
    """Fetch, parse, and return standardised DataFrame."""
    raw = fetch_table_all(path, meta)
    if var_name and EXTERNAL_PATH:
        save_raw(raw, var_name, table_label, os.path.dirname(_raw_dir(var_name)))
    return parse_to_df(raw)


def _download_level(name: str, table_specs, level: str):
    """
    Download and standardise data for one level (deso or muni).

    table_specs: dict or list of dicts, each with 'path' and optional 'label', 'measure', etc.
    """
    if isinstance(table_specs, dict):
        table_specs = [table_specs]

    dfs = []
    for spec in table_specs:
        path = spec["path"]
        label = spec.get("label", level)

        print(f"  Fetching {name} metadata ({level}/{label})...")
        meta = fetch_metadata(path)
        raw = fetch_table_all(path, meta)
        if EXTERNAL_PATH:
            save_raw(raw, name, f"{level}_{label}" if label != level else level,
                    os.path.dirname(_raw_dir(name)))

        df = parse_to_df(raw)
        df = standardise_region_cols(df, "Region", level)
        df = standardise_year_col(df)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Add measure column if specified
        if "measure" in spec:
            df["measure"] = spec["measure"]

        # Add source table if multiple
        if len(table_specs) > 1:
            df["source_table"] = path

        dfs.append(df)

    # Combine if multiple tables
    df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

    # Filter to valid region codes (4-digit for municipality, DeSO format for deso)
    if level == "municipality":
        df = df[df["region_code"].str.match(r"^\d{4}$", na=False)].copy()
    elif level == "deso":
        df = df[df["region_code"] != "00"].copy()  # drop national total

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check all outputs exist for checkpoint
    all_outputs = []
    for table in TABLES:
        name = table["name"]
        all_outputs.extend([_output_path(name, level) for level in ("deso", "municipality")])

    if all(os.path.exists(p) for p in all_outputs):
        print("Checkpoint: all SCB panel outputs exist — skipping.")
        return

    # Download each table
    for table in TABLES:
        name = table["name"]
        print(f"\n{'='*60}")
        print(f"Downloading: {name.upper()}")
        print(f"{'='*60}")

        # --- DeSO ---
        deso_spec = table.get("deso")
        if deso_spec:
            out_deso = _output_path(name, "deso")
            if not os.path.exists(out_deso):
                print(f"\n{name} (DeSO)")
                df_deso = _download_level(name, deso_spec, "deso")
                if "education_level_cols" in table:
                    for col in table["education_level_cols"]:
                        if col in df_deso.columns:
                            df_deso = df_deso.rename(
                                columns={col: "education_level", f"{col}_label": "education_level_label"}
                            )
                            break
                df_deso.to_csv(out_deso, index=False)
                print(f"  Written: {os.path.basename(out_deso)} ({len(df_deso)} rows)")
            else:
                print(f"{name} (DeSO) - output exists, skipping")

        # --- Municipality ---
        out_muni = _output_path(name, "municipality")
        if not os.path.exists(out_muni):
            print(f"\n{name} (Municipality)")

            # Handle auto-detect (e.g., employment, housing tenure)
            if "muni_auto_detect" in table:
                spec = table["muni_auto_detect"]
                base = spec["base"]
                print(f"  Inspecting {base} sub-tables for best coverage...")
                try:
                    meta = fetch_metadata(base)
                    subtables = meta if isinstance(meta, list) else meta.get("table", [])
                    best_path, best_meta = None, None
                    best_years = 0
                    for item in subtables:
                        tbl_id = item.get("id", "")
                        tbl_path = f"{base}/{tbl_id}"
                        try:
                            tbl_meta = fetch_metadata(tbl_path)
                        except Exception:
                            continue
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
                        if n_years > best_years:
                            best_years = n_years
                            best_path = tbl_path
                            best_meta = tbl_meta
                        print(f"    {tbl_id}: {n_years} years, earliest={min_year}")

                    if best_path is None:
                        print(f"  Warning: no valid sub-table found under {base}. Writing empty output.")
                        pd.DataFrame(columns=["region_code", "region_name", "region_type", "year", "value"]).to_csv(
                            out_muni, index=False
                        )
                    else:
                        if min_year > spec.get("warn_before", 1900):
                            print(f"  Warning: best table only reaches {min_year}, not {spec.get('warn_before')}")
                        df_muni = _download_level(name, {"path": best_path}, "municipality")
                        df_muni.to_csv(out_muni, index=False)
                        print(f"  Written: {os.path.basename(out_muni)} ({len(df_muni)} rows)")
                except Exception as e:
                    print(f"  Error inspecting {base}: {e}. Writing empty output.")
                    pd.DataFrame(columns=["region_code", "region_name", "region_type", "year", "value"]).to_csv(
                        out_muni, index=False
                    )

            elif "muni" in table:
                muni_spec = table["muni"]
                df_muni = _download_level(name, muni_spec, "municipality")
                if "education_level_cols" in table:
                    for col in table["education_level_cols"]:
                        if col in df_muni.columns:
                            df_muni = df_muni.rename(
                                columns={col: "education_level", f"{col}_label": "education_level_label"}
                            )
                            break
                df_muni.to_csv(out_muni, index=False)
                print(f"  Written: {os.path.basename(out_muni)} ({len(df_muni)} rows)")
        else:
            print(f"{name} (Municipality) - output exists, skipping")

    print(f"\n{'='*60}")
    print("download_scb_panel.py done.")


if __name__ == "__main__":
    main()
