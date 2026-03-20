# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a research project studying Sweden's Million Programme housing, built on the [GentzkowLabTemplate](https://github.com/gentzkowlab/GentzkowLabTemplate). 

## Running the project

### Setup (first time)
```sh
bash setup.sh
```
This creates `local_env.sh` from `lib/setup/local_env_template.sh` (gitignored) and populates module inputs.

### Run everything
```sh
bash run_all.sh
```

### Run a single module
```sh
bash 1_data/make.sh
bash 1_data/gis_extraction/make.sh
bash 1_data/public_data_extraction/make.sh
bash 2_analysis/make.sh
# etc.
```

Each `make.sh` clears its `output/` directory, re-links inputs via `get_inputs.sh`, then runs scripts in `source/`.

## Architecture

The repo follows a numbered pipeline: `0_raw/ → 1_data/ → 2_analysis/ → 3_slides/ → 4_paper/`.

**Module structure** — every module (`1_data/`, `2_analysis/`, etc.) has:
- `make.sh` — orchestrates the module; sources `local_env.sh` and `lib/shell/run_<lang>.sh` helpers, then calls scripts
- `get_inputs.sh` — creates symlinks in `input/` pointing to upstream outputs
- `source/` — actual code (shell, Python, R, Stata, etc.)
- `input/` — symlinks only (regenerated on each run)
- `output/` — all outputs including `make.log`

**`lib/`** contains shared shell utilities:
- `lib/shell/run_python.sh`, `run_R.sh`, `run_stata.sh`, etc. — wrappers that run a script and tee stdout/stderr to `make.log`
- `lib/shell/check_setup.sh` — verifies `local_env.sh` exists
- `lib/setup/local_env_template.sh` — template for `local_env.sh`

## Local configuration

`local_env.sh` (gitignored, at repo root) sets executable names and external paths:
```sh
export stataCmd="StataMP"
export pythonCmd="python3"
export rCmd="Rscript"
EXTERNAL_NAMES=("dropbox")
EXTERNAL_PATHS=("/path/to/external/data")
```

## Git workflow

Commit all changes to GitHub with clear, descriptive commit messages that explain *what* changed and *why*. Use the imperative mood (e.g. "Add DeSO spatial join step" not "Added..."). Keep commits focused — one logical change per commit. Push after committing unless told otherwise.

## Key data notes

- Million Programme era columns: `Ar65` through `Ar74` (years 1965–1974); `Ar65_74` is their pre-computed sum
- DeSO codes follow SCB format, e.g. `0114A0010`; GIS extraction fetches 2018 boundaries live from SCB's WFS
- Large GIS files tracked via Git LFS

## `1_data/public_data_extraction/` — SCB panel data + crime data

Downloads panel data from the SCB Statistikdatabasen API and processes raw crime XLS files.

**SCB table paths used:**

| Variable | Level | Table path | Years |
|----------|-------|-----------|-------|
| Education | DeSO | `UF/UF0506/UF0506D/UtbSUNBefDesoRegso` | 2015–2023 |
| Education | Municipality | `UF/UF0506/UF0506B/Utbildning` | 1985–2024 |
| Income (econ. standard) | DeSO | `HE/HE0110/HE0110I/Tab4InkDesoRegso` | 2011–2024 |
| Income (net) | Municipality | `HE/HE0110/HE0110A/NetInk02` | 2000–2024 |
| Income (earned) | Municipality | `HE/HE0110/HE0110A/SamForvInk1` | 1999–2024 |
| Households (type) | DeSO | `BE/BE0101/BE0101Y/HushallDesoTyp` | 2011–2024 |
| Households (type+children) | Municipality | `BE/BE0101/BE0101S/HushallT05` | 2011–2024 |
| Households (size) | Municipality | `BE/BE0101/BE0101S/HushallT03` | 2011–2024 |
| Employment | DeSO | `AM/AM0210/AM0210G/ArRegDesoStatusN` | 2020–2024 |
| Employment | Municipality | `AM/AM0210/AM0210D` (auto-detect sub-table) | varies |
| Housing tenure (persons) | DeSO 2018 | `HE/HE0111/HE0111YDeSo/HushallT33Deso` | 2012–2023 |
| Housing tenure (persons) | Municipality | `HE/HE0111/HE0111A` (auto-detect sub-table) | varies |
| Housing tenure (dwellings) | Municipality | `BO/BO0104/BO0104D/BO0104T04` | 1990–2024 |

**Crime files** (from `{EXTERNAL_PATHS[0]}/0_raw/public_data/crime/`):
- `total_crime_kommun_1996to2022.xls` → `crime_total_municipality.csv`
- `crime_against_life_1996to2022_kommun.xls` → `crime_against_life_municipality.csv`

**Notes:**
- All output CSVs are long-format with `region_code`, `region_type`, `year`, `value` columns
- Raw API responses saved to `{EXTERNAL_PATHS[0]}/0_raw/public_data/scb/{variable}/`
- DeSO housing tenure uses DeSO 2018 boundaries only; the script flags any DeSO 2025 tables
- Household *size* is only available at municipality level (not DeSO)
