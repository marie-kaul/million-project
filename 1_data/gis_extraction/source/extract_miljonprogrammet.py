#!/usr/bin/env python3
"""
Extract Million Programme housing data from geodatabase and aggregate to DeSO level.

Inputs:
    ../input/Miljonprogrammet.gdb  - geodatabase with grid layers (EPSG:3006)

Outputs:
    ../output/miljonprogrammet_grid.csv  - one row per grid cell, with DeSO code
    ../output/miljonprogrammet_deso.csv  - dwelling counts aggregated to DeSO level
"""

import sys
import os
import csv
import json
import urllib.request
from osgeo import ogr, osr

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR  = os.path.join(SCRIPT_DIR, "../input")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "../output")
GDB_PATH   = os.path.join(INPUT_DIR, "Miljonprogrammet.gdb")

# ── Grid layers and their tenure labels ───────────────────────────────────────
GRID_LAYERS = {
    "FlerBo_HyrRatt_region":    "rental",
    "FlerBo_BoRatt_region":     "tenant_owned",
    "SmaHus_AgandeRatt_region": "owner_occupied",
}

# ── WFS endpoint for DeSO 2018 ────────────────────────────────────────────────
# Requesting EPSG:3006 (SWEREF 99 TM) to match the grid data
WFS_URL = (
    "https://geodata.scb.se/geoserver/stat/wfs"
    "?service=WFS&version=2.0.0&request=GetFeature"
    "&typeNames=stat:DeSO_2018"
    "&outputFormat=application/json"
    "&srsName=EPSG:3006"
    "&count=10000"
)


# ── DeSO fetching and indexing ────────────────────────────────────────────────

def fetch_deso_polygons():
    """Download DeSO 2018 polygons from SCB WFS. Returns list of (deso_kod, geom)."""
    print("Fetching DeSO 2018 boundaries from SCB WFS...")
    req = urllib.request.Request(WFS_URL, headers={"User-Agent": "python-ogr/extract_miljonprogrammet"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    features = data.get("features", [])
    print(f"  Received {len(features)} features")

    # Detect coordinate system from first feature
    # EPSG:3006 (SWEREF 99 TM): X ≈ 270000–920000, Y ≈ 6130000–7700000
    # WGS84: X ≈ 10–25 (lon), Y ≈ 55–70 (lat)
    needs_reproject = False
    if features:
        coords = features[0]["geometry"]["coordinates"]
        # Flatten to get a sample coordinate
        while isinstance(coords[0], list):
            coords = coords[0]
        sample_x = coords[0]
        if abs(sample_x) < 200:  # clearly WGS84 longitude
            needs_reproject = True
            print("  Coordinates appear to be WGS84 — will reproject to EPSG:3006")
        else:
            print("  Coordinates appear to be EPSG:3006 — no reprojection needed")

    transform = None
    if needs_reproject:
        srs_wgs84 = osr.SpatialReference()
        srs_wgs84.ImportFromEPSG(4326)
        srs_wgs84.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        srs_sweref = osr.SpatialReference()
        srs_sweref.ImportFromEPSG(3006)
        transform = osr.CoordinateTransformation(srs_wgs84, srs_sweref)

    # Try common field names for the DeSO code
    DESO_FIELD_CANDIDATES = ["deso", "DeSO", "deso_kod", "DESO", "desokod"]

    polygons = []
    missing_code = 0
    for feat in features:
        props = feat["properties"]
        deso_kod = None
        for candidate in DESO_FIELD_CANDIDATES:
            if candidate in props and props[candidate]:
                deso_kod = str(props[candidate])
                break

        if deso_kod is None:
            missing_code += 1
            continue

        geom = ogr.CreateGeometryFromJson(json.dumps(feat["geometry"]))
        if geom is None:
            continue

        if transform is not None:
            geom.Transform(transform)

        polygons.append((deso_kod, geom))

    if missing_code:
        print(f"  Warning: {missing_code} features had no DeSO code (tried: {DESO_FIELD_CANDIDATES})")
        if polygons:
            sample_props = list(features[0]["properties"].keys())
            print(f"  Available property fields: {sample_props}")

    print(f"  Parsed {len(polygons)} DeSO polygons")
    return polygons


def build_spatial_index(polygons, cell_size=50000):
    """
    Build a grid-based spatial index for fast point-in-polygon lookup.
    cell_size is in EPSG:3006 units (metres). 50 km works well for Sweden.
    Returns (index_dict, origin_x, origin_y, cell_size).
    """
    envs = [geom.GetEnvelope() for _, geom in polygons]  # (minX, maxX, minY, maxY)
    origin_x = min(e[0] for e in envs)
    origin_y = min(e[2] for e in envs)

    index = {}
    for (deso_kod, geom), env in zip(polygons, envs):
        xi_lo = int((env[0] - origin_x) / cell_size)
        xi_hi = int((env[1] - origin_x) / cell_size)
        yi_lo = int((env[2] - origin_y) / cell_size)
        yi_hi = int((env[3] - origin_y) / cell_size)
        entry = (deso_kod, geom, env[0], env[1], env[2], env[3])
        for xi in range(xi_lo, xi_hi + 1):
            for yi in range(yi_lo, yi_hi + 1):
                index.setdefault((xi, yi), []).append(entry)

    return index, origin_x, origin_y, cell_size


def find_deso(x, y, index, origin_x, origin_y, cell_size):
    """Return the DeSO code for point (x, y), or None if not matched."""
    xi = int((x - origin_x) / cell_size)
    yi = int((y - origin_y) / cell_size)
    candidates = index.get((xi, yi), [])

    pt = ogr.Geometry(ogr.wkbPoint)
    pt.AddPoint(x, y)

    for deso_kod, geom, minX, maxX, minY, maxY in candidates:
        if minX <= x <= maxX and minY <= y <= maxY:
            if geom.Contains(pt):
                return deso_kod
    return None


# ── Grid extraction ───────────────────────────────────────────────────────────

def list_layers(ds):
    """Print all layer names in the datasource."""
    print("Layers in geodatabase:")
    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)
        print(f"  [{i}] {layer.GetName()} ({layer.GetFeatureCount()} features)")


def extract_layer(ds, layer_name, tenure_type, deso_index, deso_origin_x, deso_origin_y, cell_size):
    """
    Extract all features from a grid layer, compute centroids, assign DeSO codes.
    Returns (rows, field_names).
    """
    layer = ds.GetLayerByName(layer_name)
    if layer is None:
        raise RuntimeError(f"Layer '{layer_name}' not found in geodatabase")

    defn = layer.GetLayerDefn()
    field_names = [defn.GetFieldDefn(i).GetName() for i in range(defn.GetFieldCount())]

    total = layer.GetFeatureCount()
    print(f"  {layer_name}: {total} features")

    rows = []
    unmatched = 0
    for i, feat in enumerate(layer):
        if i % 25000 == 0 and i > 0:
            print(f"    {i}/{total}...")

        row = {"tenure_type": tenure_type}
        for field in field_names:
            row[field] = feat.GetField(field)

        geom = feat.GetGeometryRef()
        if geom:
            centroid = geom.Centroid()
            cx, cy = centroid.GetX(), centroid.GetY()
            row["centroid_x"] = round(cx, 1)
            row["centroid_y"] = round(cy, 1)
            deso_kod = find_deso(cx, cy, deso_index, deso_origin_x, deso_origin_y, cell_size)
            row["deso_kod"] = deso_kod
            if deso_kod is None:
                unmatched += 1
        else:
            row["centroid_x"] = None
            row["centroid_y"] = None
            row["deso_kod"] = None
            unmatched += 1

        rows.append(row)

    if unmatched:
        print(f"  Warning: {unmatched}/{total} features had no DeSO match")

    return rows, field_names


# ── DeSO aggregation ──────────────────────────────────────────────────────────

def aggregate_to_deso(rows, numeric_fields):
    """Sum numeric fields by (deso_kod, tenure_type)."""
    agg = {}
    for row in rows:
        key = (row.get("deso_kod"), row["tenure_type"])
        if key not in agg:
            agg[key] = {
                "deso_kod":    row.get("deso_kod"),
                "tenure_type": row["tenure_type"],
                "n_grid_cells": 0,
                **{f: 0 for f in numeric_fields},
            }
        agg[key]["n_grid_cells"] += 1
        for f in numeric_fields:
            val = row.get(f)
            if isinstance(val, (int, float)):
                agg[key][f] += val
    return list(agg.values())


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Fetch DeSO 2018 boundaries
    deso_polygons = fetch_deso_polygons()
    if not deso_polygons:
        print("Error: no DeSO polygons loaded", file=sys.stderr)
        sys.exit(1)
    deso_index, origin_x, origin_y, cell_size = build_spatial_index(deso_polygons)
    print(f"  Spatial index built ({len(deso_index)} grid cells)")

    # 2. Open geodatabase
    print(f"\nOpening: {GDB_PATH}")
    driver = ogr.GetDriverByName("OpenFileGDB")
    ds = driver.Open(GDB_PATH, 0)
    if ds is None:
        print(f"Error: could not open {GDB_PATH}", file=sys.stderr)
        sys.exit(1)

    list_layers(ds)

    # 3. Extract grid layers
    all_rows = []
    all_field_names = []  # preserves order, no duplicates
    for layer_name, tenure_type in GRID_LAYERS.items():
        print(f"\nExtracting: {layer_name} → tenure_type='{tenure_type}'")
        rows, fields = extract_layer(
            ds, layer_name, tenure_type,
            deso_index, origin_x, origin_y, cell_size
        )
        all_rows.extend(rows)
        for f in fields:
            if f not in all_field_names:
                all_field_names.append(f)

    ds = None
    print(f"\nTotal grid rows: {len(all_rows)}")

    # 4. Write grid-level CSV
    fixed_cols = ["tenure_type", "deso_kod", "centroid_x", "centroid_y"]
    data_cols  = [f for f in all_field_names if f not in fixed_cols]
    grid_cols  = fixed_cols + data_cols

    grid_path = os.path.join(OUTPUT_DIR, "miljonprogrammet_grid.csv")
    with open(grid_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=grid_cols, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Written: miljonprogrammet_grid.csv ({len(all_rows)} rows)")

    # 5. Detect numeric fields from a sample
    sample = all_rows[:200]
    numeric_fields = [
        f for f in data_cols
        if any(isinstance(r.get(f), (int, float)) for r in sample)
    ]

    # 6. Aggregate to DeSO and write
    deso_rows = aggregate_to_deso(all_rows, numeric_fields)
    deso_cols = ["deso_kod", "tenure_type", "n_grid_cells"] + numeric_fields

    deso_path = os.path.join(OUTPUT_DIR, "miljonprogrammet_deso.csv")
    with open(deso_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=deso_cols, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(deso_rows)
    print(f"Written: miljonprogrammet_deso.csv ({len(deso_rows)} rows)")

    # 7. Quick summary
    print("\n── Summary ──────────────────────────────────────────────")
    for tenure in set(r["tenure_type"] for r in all_rows):
        n = sum(1 for r in all_rows if r["tenure_type"] == tenure)
        matched = sum(1 for r in all_rows if r["tenure_type"] == tenure and r.get("deso_kod"))
        print(f"  {tenure}: {n} grid cells, {matched} matched to DeSO ({100*matched//n}%)")
    print("Done!")


if __name__ == "__main__":
    main()
