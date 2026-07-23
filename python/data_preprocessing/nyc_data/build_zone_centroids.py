"""
One-shot helper: build taxi_zone_lookup_with_latlon.csv from the TLC zone polygons.

TLC trips (since 2015) carry only PULocationID, not coordinates. This computes one
(lat, lon) centroid per taxi zone so the Spark preprocessing can join spatial features.

Run ONCE on a machine with geopandas (e.g. Ares login node), in the folder holding the
downloaded files:
    taxi_zones.zip             https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip
    taxi_zone_lookup.csv       https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

    pip install geopandas   # or: conda install -c conda-forge geopandas
    python build_zone_centroids.py
"""

import geopandas as gpd
import pandas as pd

# geopandas reads the shapefile straight out of the zip. The TLC shapefile is in
# EPSG:2263 (NY Long Island, US ft) — a projected CRS, so centroids are already valid;
# we reproject those centroid POINTS to 4326 for lat/lon output. (to_crs(2263) is a
# no-op if the source is already 2263, but keeps this correct for other sources too.)
# The .shp sits in a taxi_zones/ subfolder inside the zip, so point GDAL at it directly.
zones = gpd.read_file("zip://taxi_zones.zip!taxi_zones/taxi_zones.shp")
if zones.crs is None:
    zones = zones.set_crs(2263)

centroids = zones.to_crs(2263).geometry.centroid.to_crs(4326)
zones["lat"] = centroids.y
zones["lon"] = centroids.x

lookup = pd.read_csv("taxi_zone_lookup.csv")
result = lookup.merge(zones[["LocationID", "lat", "lon"]], on="LocationID", how="left")
result.to_csv("taxi_zone_lookup_with_latlon.csv", index=False)

n_total = len(result)
n_geo = int(result["lat"].notna().sum())
print(f"Wrote taxi_zone_lookup_with_latlon.csv: {n_total} zones, {n_geo} with centroids.")
