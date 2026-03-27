# src/grid.py — 격자 생성 및 지표 집계 (AA Agent)
# 분석 연산은 EPSG:5179(미터 단위) 기준, 반환은 EPSG:4326

import logging
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_WGS84, CRS_KOREA, GRID_SIZE_OPTIONS, GRID_SIZE_DEFAULT

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# 격자 생성
# ─────────────────────────────────────────────────────────

def make_grid(boundary_gdf: gpd.GeoDataFrame, cell_size_m: int = GRID_SIZE_DEFAULT) -> gpd.GeoDataFrame:
    """
    행정경계를 덮는 정사각형 격자 생성.

    Args:
        boundary_gdf: 행정경계 GeoDataFrame (어떤 CRS든 내부에서 변환)
        cell_size_m:  셀 크기 (미터). 허용값: 250, 500, 1000, 2000

    Returns:
        격자 GeoDataFrame (EPSG:4326), 컬럼: grid_id, geometry
    """
    if cell_size_m not in GRID_SIZE_OPTIONS:
        raise ValueError(
            f"허용되지 않은 셀 크기: {cell_size_m}m. "
            f"허용값: {GRID_SIZE_OPTIONS}"
        )

    log.info(f"격자 생성 시작: cell_size={cell_size_m}m")

    # 미터 단위 연산을 위해 EPSG:5179로 변환
    boundary_tm = boundary_gdf.to_crs(CRS_KOREA)
    unified = boundary_tm.unary_union   # 전체 경계 합치기

    minx, miny, maxx, maxy = unified.bounds

    # x, y 축 격자 좌표 생성
    xs = np.arange(minx, maxx, cell_size_m)
    ys = np.arange(miny, maxy, cell_size_m)

    cells = []
    for i, x in enumerate(xs):
        for j, y in enumerate(ys):
            cell = box(x, y, x + cell_size_m, y + cell_size_m)
            # 경계와 교차하는 셀만 보존
            if unified.intersects(cell):
                cells.append({
                    "grid_id":  f"{i:04d}_{j:04d}",
                    "geometry": cell,
                })

    grid_tm = gpd.GeoDataFrame(cells, crs=CRS_KOREA)
    grid = grid_tm.to_crs(CRS_WGS84)

    log.info(f"격자 생성 완료: {len(grid)}셀 (cell_size={cell_size_m}m)")
    return grid


# ─────────────────────────────────────────────────────────
# 지표 집계
# ─────────────────────────────────────────────────────────

def aggregate_to_grid(
    grid_gdf: gpd.GeoDataFrame,
    point_gdf: gpd.GeoDataFrame,
    value_col: str,
    result_col: str,
    agg: str = "sum",
) -> gpd.GeoDataFrame:
    """
    포인트 데이터를 격자에 집계 (공간 조인).

    Args:
        grid_gdf:   격자 GeoDataFrame
        point_gdf:  포인트 GeoDataFrame
        value_col:  집계할 컬럼명 (없으면 포인트 개수만 카운트)
        result_col: 결과 컬럼명
        agg:        집계 방식 ("sum" | "mean" | "count")

    Returns:
        grid_gdf에 result_col 컬럼이 추가된 GeoDataFrame
    """
    # 공간 조인을 위해 동일 CRS로 통일
    grid_tm   = grid_gdf.to_crs(CRS_KOREA)
    points_tm = point_gdf.to_crs(CRS_KOREA)

    joined = gpd.sjoin(points_tm, grid_tm, how="left", predicate="within")

    if value_col and value_col in joined.columns and agg != "count":
        grouped = joined.groupby("index_right")[value_col].agg(agg)
    else:
        # 포인트 개수 카운트
        grouped = joined.groupby("index_right").size()

    grid_out = grid_gdf.copy()
    grid_out[result_col] = grid_out.index.map(grouped).fillna(0)

    log.info(f"집계 완료: '{result_col}' ({agg}) — 비어있는 셀: {(grid_out[result_col] == 0).sum()}")
    return grid_out


def _aggregate_diversity(
    grid_gdf: gpd.GeoDataFrame,
    diversity_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    격자별 상권 다양성 지수 집계.
    격자 내 존재하는 고유 업종 카테고리 수(0~6)를 diversity 컬럼으로 추가.
    """
    grid_tm = grid_gdf.to_crs(CRS_KOREA)
    div_tm  = diversity_gdf.to_crs(CRS_KOREA)

    joined = gpd.sjoin(div_tm, grid_tm, how="left", predicate="within")
    # 격자별 고유 카테고리 수
    diversity_count = joined.groupby("index_right")["cat_code"].nunique()

    grid_out = grid_gdf.copy()
    grid_out["diversity"] = grid_out.index.map(diversity_count).fillna(0)

    log.info(f"다양성 집계 완료: 평균 {grid_out['diversity'].mean():.1f}개 카테고리/셀")
    return grid_out


def build_grid_features(
    boundary_gdf: gpd.GeoDataFrame,
    cell_size_m: int = GRID_SIZE_DEFAULT,
    population_gdf: gpd.GeoDataFrame | None = None,
    floating_gdf:   gpd.GeoDataFrame | None = None,
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
    workplace_gdf:  gpd.GeoDataFrame | None = None,
    parking_gdf:    gpd.GeoDataFrame | None = None,
    diversity_gdf:  gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    """
    격자 생성 후 모든 지표를 한 번에 집계하는 편의 함수.

    Returns:
        컬럼: grid_id, population, floating, workplace,
              competitor_cnt, transport_score, parking_cnt, diversity,
              avg_age, juv_suprt_per, oldage_suprt_per, geometry
    """
    grid = make_grid(boundary_gdf, cell_size_m)

    if population_gdf is not None:
        grid = aggregate_to_grid(grid, population_gdf, "population", "population", agg="sum")
        for demo_col in ["avg_age", "juv_suprt_per", "oldage_suprt_per"]:
            if demo_col in population_gdf.columns:
                grid = aggregate_to_grid(grid, population_gdf, demo_col, demo_col, agg="mean")
            else:
                grid[demo_col] = 0.0
    else:
        grid["population"] = 0.0
        grid["avg_age"] = 0.0
        grid["juv_suprt_per"] = 0.0
        grid["oldage_suprt_per"] = 0.0

    if floating_gdf is not None:
        grid = aggregate_to_grid(grid, floating_gdf, "floating", "floating", agg="sum")
    else:
        grid["floating"] = 0.0

    if workplace_gdf is not None:
        grid = aggregate_to_grid(grid, workplace_gdf, "workplace", "workplace", agg="sum")
    else:
        grid["workplace"] = 0.0

    if competitor_gdf is not None:
        grid = aggregate_to_grid(grid, competitor_gdf, None, "competitor_cnt", agg="count")
    else:
        grid["competitor_cnt"] = 0

    # Why: 지하철역(weight=3)이 버스(weight=1)보다 집객 효과가 크므로
    #      단순 개수가 아닌 가중합으로 교통 접근성 산출
    if transport_gdf is not None and "weight" in transport_gdf.columns:
        grid = aggregate_to_grid(grid, transport_gdf, "weight", "transport_score", agg="sum")
    elif transport_gdf is not None:
        grid = aggregate_to_grid(grid, transport_gdf, None, "transport_score", agg="count")
    else:
        grid["transport_score"] = 0

    if parking_gdf is not None:
        grid = aggregate_to_grid(grid, parking_gdf, None, "parking_cnt", agg="count")
    else:
        grid["parking_cnt"] = 0

    if diversity_gdf is not None and len(diversity_gdf) > 0:
        grid = _aggregate_diversity(grid, diversity_gdf)
    else:
        grid["diversity"] = 0

    log.info(
        f"격자 피처 빌드 완료: {len(grid)}셀 | "
        f"인구합={grid['population'].sum():.0f} | "
        f"종사자합={grid['workplace'].sum():.0f} | "
        f"교통점수합={grid['transport_score'].sum():.0f} | "
        f"주차장총={grid['parking_cnt'].sum():.0f} | "
        f"다양성평균={grid['diversity'].mean():.1f} | "
        f"경쟁업체총={grid['competitor_cnt'].sum():.0f}"
    )
    return grid
