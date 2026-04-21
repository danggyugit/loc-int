# src/geocoding.py — 격자 중심점 → 주소 역지오코딩 (카카오 Local)
#
# Why: 사용자에게 'G0234' 같은 격자 ID보다 '강남구 역삼동' 같은
#      자연 주소가 훨씬 직관적. 격자 수천 개 전부 도로명까지 조회하면
#      쿼터 부담이 크므로, 두 단계로 나눔:
#        - 전체 격자: coord2regioncode (법정동명만, 1회/셀)
#        - 상위 N개:   coord2address    (도로명 추가, 1회/셀)

import logging
import requests
import pandas as pd
import geopandas as gpd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src import session_keys

log = logging.getLogger(__name__)

_REGION_URL  = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
_ADDRESS_URL = "https://dapi.kakao.com/v2/local/geo/coord2address.json"


def _reverse_region(lat: float, lng: float) -> str:
    """좌표 → '강남구 역삼동' 형태 법정동 주소. 실패 시 빈 문자열."""
    kakao_key = session_keys.get("KAKAO_API_KEY")
    if not kakao_key:
        return ""
    try:
        resp = requests.get(
            _REGION_URL,
            headers={"Authorization": f"KakaoAK {kakao_key}"},
            params={"x": lng, "y": lat},
            timeout=5,
        )
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
        # 법정동(B) 우선, 없으면 행정동(H)
        for doc in docs:
            if doc.get("region_type") == "B":
                gu = doc.get("region_2depth_name", "")
                dong = doc.get("region_3depth_name", "")
                return f"{gu} {dong}".strip()
        for doc in docs:
            if doc.get("region_type") == "H":
                gu = doc.get("region_2depth_name", "")
                dong = doc.get("region_3depth_name", "")
                return f"{gu} {dong}".strip()
    except Exception as e:
        log.debug(f"coord2regioncode 실패 ({lat:.4f},{lng:.4f}): {e}")
    return ""


def _reverse_road_address(lat: float, lng: float) -> str:
    """좌표 → '테헤란로 152' 형태 도로명 주소. 실패 시 빈 문자열."""
    kakao_key = session_keys.get("KAKAO_API_KEY")
    if not kakao_key:
        return ""
    try:
        resp = requests.get(
            _ADDRESS_URL,
            headers={"Authorization": f"KakaoAK {kakao_key}"},
            params={"x": lng, "y": lat},
            timeout=5,
        )
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
        if not docs:
            return ""
        road = docs[0].get("road_address")
        if road:
            # 도로명 + 건물번호 정도만
            rn = road.get("road_name", "")
            bn = road.get("main_building_no", "")
            if rn and bn:
                return f"{rn} {bn}".strip()
            if rn:
                return rn
    except Exception as e:
        log.debug(f"coord2address 실패 ({lat:.4f},{lng:.4f}): {e}")
    return ""


def annotate_addresses(
    scored_gdf: gpd.GeoDataFrame,
    top_gdf: gpd.GeoDataFrame,
    top_with_road: int = 10,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    scored와 top 양쪽에 `address` 컬럼 추가.
      - 전체 scored: 법정동만 (grid_id 당 1회 호출)
      - top 상위 top_with_road개: 도로명 추가 → "역삼동 (테헤란로 152 일대)"

    Returns:
        (scored_with_address, top_with_address)

    주의: 격자 수가 많으면 호출 수가 많아짐(셀당 1회). 필요 시 상위 격자만
         호출하거나 cell_size를 키울 것. 카카오 좌표→주소는 일반적으로 빠름.
    """
    if len(scored_gdf) == 0:
        return scored_gdf, top_gdf

    log.info(f"역지오코딩 시작: {len(scored_gdf)}개 셀 (법정동) + 상위 {top_with_road}개 (도로명)")

    scored_out = scored_gdf.copy()
    centroids = scored_out.geometry.centroid
    # 중복 좌표 제거 + 캐시
    cache: dict[tuple[float, float], str] = {}
    regions = []
    for pt in centroids:
        key = (round(pt.y, 5), round(pt.x, 5))  # ~1m 정밀도로 캐시 키
        if key not in cache:
            cache[key] = _reverse_region(pt.y, pt.x)
        regions.append(cache[key])
    scored_out["address"] = regions

    # top에 도로명 추가
    top_out = top_gdf.copy()
    # 먼저 법정동을 top에도 매핑 (scored의 grid_id 기준)
    if "grid_id" in top_out.columns and "grid_id" in scored_out.columns:
        addr_map = scored_out.set_index("grid_id")["address"]
        top_out["address"] = top_out["grid_id"].map(addr_map).fillna("")

    top_ctr = top_out.geometry.centroid
    roads = []
    limit = min(top_with_road, len(top_out))
    for i, pt in enumerate(top_ctr):
        if i < limit:
            roads.append(_reverse_road_address(pt.y, pt.x))
        else:
            roads.append("")
    top_out["road_address"] = roads

    # 최종 표시용 주소: 도로명 있으면 "법정동 (도로명)", 없으면 법정동만
    def _fmt(row):
        region = row.get("address", "")
        road   = row.get("road_address", "")
        if road:
            return f"{region} · {road}" if region else road
        return region
    top_out["display_address"] = top_out.apply(_fmt, axis=1)

    log.info(f"역지오코딩 완료: 고유 좌표 {len(cache)}개, 상위 {limit}개 도로명 추가")
    return scored_out, top_out
