# src/vworld_client.py — Vworld 용도지역 API 클라이언트
# 용도지역 조회 → 업종 입점 가능 여부 판단 (하드 필터)

import logging
import requests
import geopandas as gpd
from shapely.geometry import shape

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_WGS84

log = logging.getLogger(__name__)

# ── 용도지역 상업 적합도 점수 (0.0 ~ 1.0) ──────────────
# 0.0 = 상업 활동 불가 (하드 필터로 제거)
# 1.0 = 모든 상업 업종 허용
ZONE_COMMERCIAL_SCORE = {
    "중심상업지역":     1.0,
    "일반상업지역":     1.0,
    "근린상업지역":     1.0,
    "유통상업지역":     1.0,
    "준주거지역":       0.9,
    "준공업지역":       0.8,
    "제3종일반주거지역": 0.6,
    "제2종일반주거지역": 0.4,
    "제1종일반주거지역": 0.2,
    "제1종전용주거지역": 0.0,
    "제2종전용주거지역": 0.0,
    "전용공업지역":     0.0,
    "일반공업지역":     0.3,
    "보전녹지지역":     0.0,
    "생산녹지지역":     0.0,
    "자연녹지지역":     0.1,
    "보전관리지역":     0.0,
    "생산관리지역":     0.1,
    "계획관리지역":     0.3,
    "농림지역":         0.0,
    "자연환경보전지역": 0.0,
}


def _match_zone_score(zone_name: str) -> float:
    """용도지역명에서 상업 적합도 점수 추출 (부분 매칭)."""
    if not zone_name:
        return 0.5
    for key, score in ZONE_COMMERCIAL_SCORE.items():
        if key in zone_name:
            return score
    # Why: 매칭 실패는 데이터 이상이므로 보수적 0.5
    log.warning(f"알 수 없는 용도지역: '{zone_name}' → 0.5 기본값")
    return 0.5


def get_land_use_zones(
    boundary_gdf: gpd.GeoDataFrame,
    api_key: str,
) -> gpd.GeoDataFrame | None:
    """
    Vworld 2D데이터 API로 용도지역 폴리곤 조회.

    Args:
        boundary_gdf: 분석 영역 경계 (EPSG:4326)
        api_key:      Vworld API 키

    Returns:
        용도지역 GeoDataFrame (zone_name, zone_score, geometry) 또는 None
    """
    if not api_key:
        log.warning("Vworld API 키 없음 → 용도지역 필터 건너뜀")
        return None

    bounds = boundary_gdf.total_bounds  # [minx, miny, maxx, maxy]
    bbox_str = f"BOX({bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]})"

    url = "https://api.vworld.kr/req/data"
    all_features = []
    page = 1
    max_pages = 50

    log.info(f"Vworld 용도지역 조회 시작")

    while page <= max_pages:
        params = {
            "service": "data",
            "request": "GetFeature",
            "data": "LT_C_UQ111",
            "key": api_key,
            "geomFilter": bbox_str,
            "crs": "EPSG:4326",
            "size": 1000,
            "page": page,
            "format": "json",
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.error(f"Vworld API 오류: {e}")
            return None

        response_obj = data.get("response", {})
        status = response_obj.get("status")

        if status != "OK":
            error_text = response_obj.get("error", {}).get("text", "알 수 없는 오류")
            log.error(f"Vworld API 오류 응답: {status} — {error_text}")
            if not all_features:
                return None
            break

        result = response_obj.get("result", {})
        fc = result.get("featureCollection", {})
        features = fc.get("features", [])

        if not features:
            break

        all_features.extend(features)

        total = int(result.get("totalcount", 0))
        if page * 1000 >= total:
            break
        page += 1

    if not all_features:
        log.warning("Vworld 용도지역 데이터 없음")
        return None

    # GeoDataFrame 변환
    records = []
    for feat in all_features:
        props = feat.get("properties", {})
        geom_dict = feat.get("geometry")
        if not geom_dict:
            continue
        try:
            geom = shape(geom_dict)
            zone_name = props.get("UNAME", "")
            records.append({
                "zone_name": zone_name,
                "zone_score": _match_zone_score(zone_name),
                "geometry": geom,
            })
        except Exception:
            continue

    if not records:
        return None

    gdf = gpd.GeoDataFrame(records, crs=CRS_WGS84)

    # 경계 내 클리핑
    boundary_union = boundary_gdf.unary_union
    gdf = gdf[gdf.geometry.intersects(boundary_union)].copy()

    log.info(f"용도지역 조회 완료: {len(gdf)}개 폴리곤")
    for zn, cnt in gdf["zone_name"].value_counts().head(5).items():
        log.info(f"  {zn}: {cnt}개")

    return gdf
