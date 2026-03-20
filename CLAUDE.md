# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a research project studying Sweden's Million Programme housing, built on the [GentzkowLabTemplate](https://github.com/gentzkowlab/GentzkowLabTemplate). The project processes GIS data from a geodatabase (`0_raw/Miljonprogrammet.gdb`) to extract housing data linked to Swedish DeSO demographic statistical areas.

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

**Key sub-module:** `1_data/gis_extraction/` — Python pipeline that reads the geodatabase, fetches DeSO 2018 boundaries from SCB's WFS API, spatial-joins grid centroids to DeSO polygons, and outputs:
- `miljonprogrammet_grid.csv` — ~220k rows, one per grid cell with DeSO code and centroid coords
- `miljonprogrammet_deso.csv` — ~14k rows aggregated by DeSO × tenure type

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

- Raw geodatabase: `0_raw/Miljonprogrammet.gdb` (EPSG:3006, SWEREF 99 TM)
- Million Programme era columns: `Ar65` through `Ar74` (years 1965–1974); `Ar65_74` is their pre-computed sum
- DeSO codes follow SCB format, e.g. `0114A0010`; GIS extraction fetches 2018 boundaries live from SCB's WFS
- Large GIS files tracked via Git LFS
