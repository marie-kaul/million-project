# GIS Extraction: Million Programme Housing Data

## Overview

This module extracts Sweden's Million Programme housing data from a geodatabase and links it to demographic statistical areas (DeSO). The result is two CSV files:
- **Grid-level data**: one row per grid cell with DeSO code and centroid coordinates
- **DeSO-level aggregates**: dwelling counts summed by DeSO area and tenure type

## What it does

1. **Fetches DeSO 2018 boundaries** from SCB's open WFS service (Statistics Sweden)
2. **Reads 3 grid layers** from the geodatabase:
   - Multi-family rental housing (`FlerBo_HyrRatt_region`)
   - Multi-family tenant-owned housing (`FlerBo_BoRatt_region`)
   - Single-family owner-occupied housing (`SmaHus_AgandeRatt_region`)
3. **Computes grid cell centroids** from geometries
4. **Spatial join**: assigns each centroid to a DeSO polygon using point-in-polygon
5. **Exports two CSVs**:
   - Grid-level data with DeSO codes
   - DeSO-level aggregates (sum of dwelling counts by DeSO × tenure)

## How to run

```bash
bash 1_data/gis_extraction/make.sh
```

**Requirements:**
- Internet connection (to download DeSO boundaries from SCB WFS)
- `python3` with GDAL/OGR support (should be pre-installed)
- ~3–5 minutes runtime

**Output location:** `1_data/gis_extraction/output/`

## Input

- **Source:** `../../0_raw/Miljonprogrammet.gdb`
- **Format:** ESRI File Geodatabase (EPSG:3006, SWEREF 99 TM)
- **Contents:** 7 layers (this module uses the 3 grid layers)

## Outputs

### 1. `miljonprogrammet_grid.csv`

One row per grid cell. ~220,000 rows total.

| Column | Type | Description |
|--------|------|-------------|
| `tenure_type` | string | `"rental"`, `"tenant_owned"`, or `"owner_occupied"` |
| `deso_kod` | string | DeSO code (e.g., `1486A0010`); `null` if unmatched |
| `centroid_x` | float | Grid cell centroid X-coordinate (SWEREF 99 TM, metres) |
| `centroid_y` | float | Grid cell centroid Y-coordinate (SWEREF 99 TM, metres) |
| `Ruta` | string | Grid cell ID (combination of coordinates) |
| `RutStorl` | float | Grid cell size (1 or 2, representing 250m or 1km cells) |
| `Ar30_34`, `Ar35_39`, ... | int | Dwelling count built in period (e.g., 1930–1934) |
| `Ar65`, `Ar66`, ..., `Ar74` | int | **Million Programme era (1965–1974)** — dwelling count by year |
| `Ar75_plus` (if present) | int | Dwellings built 1975 onwards |
| `Ar65_74` (if present) | int | Total Million Programme dwellings (pre-summed) |
| Other columns | mixed | Additional fields from geodatabase (e.g., construction year variants) |

**Notes:**
- Centroid coordinates are rounded to 1 decimal place (nearest ~10 cm)
- ~99–100% of grid cells matched to DeSO; unmatched cells are edge cases (outside DeSO coverage or on borders)
- Use `Ar65_74` or sum `Ar65` through `Ar74` for Million Programme dwelling counts

### 2. `miljonprogrammet_deso.csv`

Dwelling counts aggregated to DeSO level. ~14,000 rows (combinations of ~6,000 DeSO codes × 3 tenure types).

| Column | Type | Description |
|--------|------|-------------|
| `deso_kod` | string | DeSO code (demographic statistical area identifier) |
| `tenure_type` | string | `"rental"`, `"tenant_owned"`, or `"owner_occupied"` |
| `n_grid_cells` | int | Number of grid cells in this DeSO × tenure combination |
| `Ar30_34`, `Ar35_39`, ... | int | Sum of dwellings built in period across all grid cells |
| `Ar65`, `Ar66`, ..., `Ar74` | int | **Million Programme (1965–1974)** — total dwellings by year |
| Other columns | int | Sums of all numeric fields from grid data |

**Notes:**
- Each row represents a unique DeSO × tenure combination
- Values are sums (aggregates) from all grid cells in that DeSO
- Use for statistical analysis: link to register data by DeSO code and tenure type
- DeSO codes follow SCB format (e.g., `0114A0010`)

## Column naming convention

- **Year ranges** (e.g., `Ar30_34`): dwelling count for 1930–1934
- **Single years** (e.g., `Ar65`, `Ar66`): dwelling count for that specific year
- **`Ar65_74`**: sum of all Million Programme years (1965–1974) — pre-computed
- **`w_30`**: likely a weighting or indicator column (consult original geodatabase documentation)

## Spatial reference

All coordinates are in **EPSG:3006** (SWEREF 99 TM):
- X-axis: east-west (roughly 270,000–930,000 m)
- Y-axis: north-south (roughly 6,150,000–7,700,000 m)
- Unit: metres

## DeSO reference

DeSO (Demografiska Statistiska Områden) are small demographic statistical areas used by SCB. Features:
- ~6,000 areas in Sweden
- Designed for statistical reporting
- Updated regularly (this module uses 2018 version)
- Non-overlapping, complete coverage of Sweden

For more information: [SCB DeSO](https://www.scb.se/en/services/open-data-api/open-geodata/open-data-for-deso--demographic-statistical-areas/)

## Usage example

Link to other register data:

```python
import pandas as pd

# Load aggregated DeSO data
deso = pd.read_csv("1_data/gis_extraction/output/miljonprogrammet_deso.csv")

# Filter to Million Programme years
mp_cols = [f"Ar{y}" for y in range(65, 75)]
deso_mp = deso[["deso_kod", "tenure_type"] + mp_cols].copy()
deso_mp["total_mp"] = deso_mp[mp_cols].sum(axis=1)

# Merge with other DeSO-level register data
# register = pd.read_csv("path/to/register_data.csv")
# result = deso_mp.merge(register, on="deso_kod")
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Script fails with network error | Check internet connection; SCB WFS may be temporarily unavailable |
| 0 rows in output | Check that geodatabase path is correct and `.gdb` file exists |
| High number of unmatched grid cells | Expected for grid cells on Denmark border or in unpopulated areas |

## Files

- `make.sh` — main execution script
- `get_inputs.sh` — symlinks the geodatabase
- `source/extract_miljonprogrammet.py` — Python extraction logic
- `input/` — symlinked input files (populated by `get_inputs.sh`)
- `output/` — generated CSV files and log
