"""
Shared utilities for downloading data from SCB Statistikdatabasen API.

SCB API base: https://api.scb.se/OV0104/v1/doris/sv/ssd/
Rate limit: ~30 calls / 10 seconds — we add a 0.5s delay between requests.
"""

import json
import os
import time
import urllib.request
import urllib.error
from typing import Any

import pandas as pd

SCB_BASE = "https://api.scb.se/OV0104/v1/doris/sv/ssd/"
_last_request_time: float = 0.0
MIN_INTERVAL = 0.5  # seconds between requests


def _throttle():
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)
    _last_request_time = time.time()


def fetch_metadata(path: str) -> dict:
    """GET the SCB table metadata / variable list for a given table path."""
    url = SCB_BASE + path.lstrip("/")
    _throttle()
    req = urllib.request.Request(url, headers={"User-Agent": "python/scb_utils"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_table(path: str, query: dict) -> dict:
    """POST a selection query to an SCB table and return the JSON response."""
    url = SCB_BASE + path.lstrip("/")
    payload = json.dumps(query).encode("utf-8")
    _throttle()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "python/scb_utils"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} for {url}: {body[:500]}") from e


def parse_to_df(response: dict) -> pd.DataFrame:
    """
    Convert an SCB JSON-stat response to a tidy long-format DataFrame.

    SCB returns either:
      - "dataset" format (JSON-stat)
      - "data" format (rows of {"key": [...], "values": [...]})

    We handle both. The resulting DataFrame always has columns for each
    dimension (the last of which is typically 'year' / 'tid') plus 'value'.
    """
    if "dataset" in response:
        return _parse_jsonstat(response["dataset"])
    elif "data" in response:
        return _parse_data_format(response)
    else:
        raise ValueError(f"Unrecognised SCB response keys: {list(response.keys())}")


def _parse_jsonstat(ds: dict) -> pd.DataFrame:
    """Parse JSON-stat dataset format."""
    dimension_ids = ds["id"]  # ordered list of dimension names
    dimension_info = ds["dimension"]
    values = ds["value"]

    # Build list of (label, code) for each dimension
    dim_categories = []
    for dim_id in dimension_ids:
        cat = dimension_info[dim_id]["category"]
        # 'index' maps code -> position; 'label' maps code -> human label
        index = cat.get("index", {})
        label = cat.get("label", {})
        if isinstance(index, list):
            codes = index
        else:
            codes = sorted(index.keys(), key=lambda k: index[k])
        dim_categories.append((dim_id, codes, label))

    # Compute strides for each dimension
    sizes = [len(cats) for _, cats, _ in dim_categories]
    rows = []
    for flat_idx, val in enumerate(values):
        row = {}
        remainder = flat_idx
        for i, (dim_id, codes, label) in enumerate(dim_categories):
            stride = 1
            for s in sizes[i + 1 :]:
                stride *= s
            pos = remainder // stride
            remainder %= stride
            code = codes[pos]
            row[dim_id] = code
            row[f"{dim_id}_label"] = label.get(code, code)
        row["value"] = val
        rows.append(row)

    return pd.DataFrame(rows)


def _parse_data_format(response: dict) -> pd.DataFrame:
    """Parse SCB 'data' list format with parallel 'columns' metadata."""
    columns = response.get("columns", [])
    col_names = [c["code"] for c in columns]
    rows = []
    for entry in response["data"]:
        row = dict(zip(col_names, entry["key"]))
        vals = entry["values"]
        if len(vals) == 1:
            row["value"] = vals[0]
        else:
            for i, v in enumerate(vals):
                row[f"value_{i}"] = v
        rows.append(row)
    return pd.DataFrame(rows)


def save_raw(data: Any, var_name: str, level: str, raw_dir: str) -> None:
    """Save raw JSON response to 0_raw/public_data/scb/{var_name}/raw_{level}.json."""
    dest = os.path.join(raw_dir, var_name)
    os.makedirs(dest, exist_ok=True)
    path = os.path.join(dest, f"raw_{level}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved raw JSON → {path}")


def all_values_query(metadata: dict) -> dict:
    """
    Build a query dict that selects all values for every variable in a table.
    Uses filter="all" which is valid for SCB API v1.
    """
    selection = []
    for var in metadata.get("variables", []):
        selection.append(
            {"code": var["code"], "selection": {"filter": "all", "values": []}}
        )
    return {"query": selection, "response": {"format": "json"}}


def standardise_region_cols(df: pd.DataFrame, region_col: str, level: str) -> pd.DataFrame:
    """
    Ensure df has 'region_code', 'region_name', 'region_type' columns.

    SCB dimension codes are typically 'Region' or 'Tid'.
    The region column contains codes like '0114' (municipality) or '1486A0010' (DeSO).
    """
    df = df.copy()
    if region_col in df.columns:
        df["region_code"] = df[region_col]
        label_col = f"{region_col}_label"
        df["region_name"] = df[label_col] if label_col in df.columns else df[region_col]
    df["region_type"] = level
    return df


def standardise_year_col(df: pd.DataFrame) -> pd.DataFrame:
    """Rename whichever column holds the year ('Tid', 'tid', 'År') to 'year'."""
    df = df.copy()
    for candidate in ["Tid", "tid", "År", "ar", "year"]:
        if candidate in df.columns:
            df = df.rename(columns={candidate: "year"})
            break
    return df
