# test_sgis.py — SGIS full integration test
import os, sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")

from src.sgis_client import is_sgis_available, get_sgis_grid_data

print(f"SGIS available: {is_sgis_available()}")
if not is_sgis_available():
    print("Set SGIS_CONSUMER_KEY and SGIS_CONSUMER_SECRET first.")
    sys.exit(1)

# Create a dummy boundary for Ilsandong-gu (simple bbox)
import geopandas as gpd
from shapely.geometry import box

# Ilsandong-gu approximate bounds (WGS84)
boundary = gpd.GeoDataFrame(
    {"adm_name": ["ilsandong"]},
    geometry=[box(126.72, 37.64, 126.82, 37.70)],
    crs="EPSG:4326",
)

print("\nRunning SGIS data collection for Ilsandong-gu...")
for name in ["일산동구", "고양시 일산동구", "일산동"]:
    print(f"\n  Trying region='{name}'...")
    result = get_sgis_grid_data(boundary, region=name, cell_size_m=500)
    if result:
        break

if result:
    pop = result["population"]
    work = result["workplace"]
    print(f"\nSUCCESS!")
    print(f"  Population points: {len(pop)}, total: {pop['population'].sum():,.0f}")
    print(f"  Workplace points:  {len(work)}, total: {work['workplace'].sum():,.0f}")
    print(f"\n  Pop sample:\n{pop.head()}")
else:
    print("\nAll attempts failed.")
