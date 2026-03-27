# src/buffer.py — 반경 분석 (AA Agent)
# 특정 좌표 기준 반경 내 데이터 집계 / 복수 반경 동시 지원

import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_WGS84, CRS_KOREA, BUFFER_RADIUS_DEFAULT

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# 버퍼 생성
# ─────────────────────────────────────────────────────────

def make_buffer(lat: float, lng: float, radius_m: int | float) -> gpd.GeoDataFrame:
    """
    좌표 기준 원형 버퍼 GeoDataFrame 생성 (EPSG:4326).

    Args:
        lat:      위도
        lng:      경도
        radius_m: 반경 (미터)

    Returns:
        버퍼 폴리곤 GeoDataFrame (EPSG:4326)
    """
    # 미터 단위 버퍼 생성을 위해 EPSG:5179로 변환
    point_wgs = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs=CRS_WGS84)
    point_tm  = point_wgs.to_crs(CRS_KOREA)

    buffer_tm = point_tm.copy()
    buffer_tm["geometry"] = point_tm.buffer(radius_m)

    return buffer_tm.to_crs(CRS_WGS84)


def query_within_buffer(
    buffer_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    버퍼 내에 포함된 target_gdf 행 반환 (공간 조인).

    Returns:
        버퍼 내 포인트/폴리곤 GeoDataFrame
    """
    target_tm = target_gdf.to_crs(CRS_KOREA)
    buffer_tm = buffer_gdf.to_crs(CRS_KOREA)

    joined = gpd.sjoin(target_tm, buffer_tm, how="inner", predicate="within")
    result = target_gdf.loc[joined.index].copy()
    return result


# ─────────────────────────────────────────────────────────
# 단일 반경 요약
# ─────────────────────────────────────────────────────────

def summarize_buffer(
    lat: float,
    lng: float,
    radius_m: int | float,
    population_gdf: gpd.GeoDataFrame | None = None,
    floating_gdf:   gpd.GeoDataFrame | None = None,
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
) -> dict:
    """
    단일 좌표 기준 반경 내 지표 집계 결과 반환.

    Returns:
        {radius_m, population, floating, competitor_cnt, transport_cnt}
    """
    buf = make_buffer(lat, lng, radius_m)

    def _count(gdf):
        if gdf is None or len(gdf) == 0:
            return 0
        return len(query_within_buffer(buf, gdf))

    def _sum(gdf, col):
        if gdf is None or len(gdf) == 0:
            return 0
        inside = query_within_buffer(buf, gdf)
        return inside[col].sum() if col in inside.columns else len(inside)

    result = {
        "lat":            lat,
        "lng":            lng,
        "radius_m":       radius_m,
        "population":     _sum(population_gdf, "population"),
        "floating":       _sum(floating_gdf,   "floating"),
        "competitor_cnt": _count(competitor_gdf),
        "transport_cnt":  _count(transport_gdf),
    }

    log.info(
        f"반경 분석 완료 ({radius_m}m): "
        f"인구={result['population']:.0f}, "
        f"유동={result['floating']:.0f}, "
        f"경쟁={result['competitor_cnt']}, "
        f"교통={result['transport_cnt']}"
    )
    return result


# ─────────────────────────────────────────────────────────
# 복수 반경 동시 분석
# ─────────────────────────────────────────────────────────

def analyze_multi_radius(
    lat: float,
    lng: float,
    radii: list[int] | None = None,
    population_gdf: gpd.GeoDataFrame | None = None,
    floating_gdf:   gpd.GeoDataFrame | None = None,
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
) -> pd.DataFrame:
    """
    복수 반경에 대해 일괄 분석.

    Args:
        radii: 반경 리스트 (미터). 기본값: [BUFFER_RADIUS_DEFAULT]

    Returns:
        반경별 지표 DataFrame (행: 반경, 열: 지표)
    """
    if radii is None:
        radii = [BUFFER_RADIUS_DEFAULT]

    rows = []
    for r in sorted(radii):
        row = summarize_buffer(
            lat, lng, r,
            population_gdf=population_gdf,
            floating_gdf=floating_gdf,
            competitor_gdf=competitor_gdf,
            transport_gdf=transport_gdf,
        )
        rows.append(row)

    df = pd.DataFrame(rows)
    log.info(f"복수 반경 분석 완료: {radii}")
    return df


# ─────────────────────────────────────────────────────────
# 복수 후보지 일괄 비교
# ─────────────────────────────────────────────────────────

def compare_candidates(
    candidates: list[dict],
    radius_m: int | float = BUFFER_RADIUS_DEFAULT,
    population_gdf: gpd.GeoDataFrame | None = None,
    floating_gdf:   gpd.GeoDataFrame | None = None,
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
) -> pd.DataFrame:
    """
    복수 후보지를 동일 반경으로 비교.

    Args:
        candidates: [{"name": "후보A", "lat": 37.5, "lng": 126.9}, ...]
        radius_m:   분석 반경 (미터)

    Returns:
        후보지별 지표 + 정규화 점수 DataFrame
    """
    rows = []
    for c in candidates:
        row = summarize_buffer(
            c["lat"], c["lng"], radius_m,
            population_gdf=population_gdf,
            floating_gdf=floating_gdf,
            competitor_gdf=competitor_gdf,
            transport_gdf=transport_gdf,
        )
        row["name"] = c.get("name", f"{c['lat']},{c['lng']}")
        rows.append(row)

    df = pd.DataFrame(rows)
    log.info(f"후보지 {len(candidates)}곳 비교 완료 (반경 {radius_m}m)")
    return df
