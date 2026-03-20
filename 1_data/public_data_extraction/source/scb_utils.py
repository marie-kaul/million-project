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


def fetch_table(path: str, query: dict, max_retries: int = 4) -> dict:
    """
    POST a selection query to an SCB table and return the JSON response.
    Retries up to max_retries times on timeout or 429/5xx errors with exponential backoff.
    """
    url = SCB_BASE + path.lstrip("/")
    payload = json.dumps(query).encode("utf-8")
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        if attempt > 0:
            wait = min(2 ** attempt, 30)
            print(f"  Retry {attempt}/{max_retries} after {wait}s (prev: {last_exc})")
            time.sleep(wait)
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
            if e.code in (429, 500, 502, 503, 504):
                last_exc = RuntimeError(f"HTTP {e.code}")
                continue  # retry
            raise RuntimeError(f"HTTP {e.code} for {url}: {body[:500]}") from e
        except (TimeoutError, OSError) as e:
            last_exc = e
            continue  # retry on timeout/network errors
    raise RuntimeError(f"Failed after {max_retries} retries for {url}: {last_exc}")


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

    Uses filter="item" with the explicit value list from the metadata.
    ContentsCode is omitted from the query — the API returns all content codes
    automatically when it's absent, and including it causes HTTP 400 errors.
    """
    selection = []
    for var in metadata.get("variables", []):
        if var["code"] == "ContentsCode":
            continue  # omitting ContentsCode lets the API return all measures
        values = var.get("values", [])
        selection.append(
            {"code": var["code"], "selection": {"filter": "item", "values": values}}
        )
    return {"query": selection, "response": {"format": "json"}}


MAX_CELLS = 100_000  # conservative limit (SCB hard limit is ~150k)


def fetch_table_all(path: str, metadata: dict) -> dict:
    """
    Fetch an entire SCB table, automatically chunking by region values if needed
    to stay under the per-request cell limit. Returns a merged 'data' format response.
    """
    variables = metadata.get("variables", [])

    # Find the region variable and compute cells-per-region
    region_var = next((v for v in variables if v["code"] == "Region"), None)
    if region_var is None:
        # No region dimension — just fetch directly
        return fetch_table(path, all_values_query(metadata))

    region_values = region_var["values"]
    # Cells per region = product of all other non-ContentsCode dimensions
    cells_per_region = 1
    for v in variables:
        if v["code"] not in ("Region", "ContentsCode"):
            cells_per_region *= max(len(v.get("values", [])), 1)

    chunk_size = max(1, MAX_CELLS // cells_per_region)
    chunks = [region_values[i: i + chunk_size] for i in range(0, len(region_values), chunk_size)]

    if len(chunks) == 1:
        return fetch_table(path, all_values_query(metadata))

    print(f"  Table has {len(region_values)} regions × {cells_per_region} cells each"
          f" → fetching in {len(chunks)} chunks of ≤{chunk_size} regions")

    all_rows = []
    columns = None
    for i, chunk in enumerate(chunks):
        print(f"    Chunk {i + 1}/{len(chunks)} ({len(chunk)} regions)...")
        # Build query for this chunk
        selection = []
        for var in variables:
            if var["code"] == "ContentsCode":
                continue
            if var["code"] == "Region":
                selection.append({"code": "Region", "selection": {"filter": "item", "values": chunk}})
            else:
                selection.append({"code": var["code"], "selection": {"filter": "item", "values": var.get("values", [])}})
        query = {"query": selection, "response": {"format": "json"}}
        resp = fetch_table(path, query)
        if columns is None:
            columns = resp.get("columns", [])
        all_rows.extend(resp.get("data", []))

    return {"columns": columns, "data": all_rows, "comments": []}


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
