# src/geocoding.py — 격자 중심점 → 주소 역지오코딩 (카카오 Local)
#
# Why: 사용자에게 'G0234' 같은 격자 ID보다 '강남구 역삼동' 같은
#      자연 주소가 훨씬 직관적.
#
# 성능 설계:
#   - 격자 수천 개 전부 순차 호출하면 분당 수백 회 → 수 분 block
#   - 대부분의 사용자는 상위 N개만 확인하므로 top_with_region(기본 100)까지만 법정동
#   - top_with_road(기본 10)까지만 도로명 추가
#   - ThreadPoolExecutor로 병렬화 (max_workers=20) → 호출 시간 10~20배 단축
#   - worker 스레드엔 session_keys가 안 들어가므로 snapshot/apply로 전파

import logging
import requests
import pandas as pd
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor

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
            rn = road.get("road_name", "")
            bn = road.get("main_building_no", "")
            if rn and bn:
                return f"{rn} {bn}".strip()
            if rn:
                return rn
    except Exception as e:
        log.debug(f"coord2address 실패 ({lat:.4f},{lng:.4f}): {e}")
    return ""


def _batch_reverse(coords: list[tuple[float, float]], fn, max_workers: int = 20) -> list[str]:
    """좌표 리스트에 대해 역지오코딩을 병렬 실행.
    worker 스레드엔 session_keys(threading.local)가 상속되지 않으므로 snapshot/apply로 전파."""
    if not coords:
        return []
    keys_snap = session_keys.snapshot()

    def _worker(lat_lng):
        session_keys.apply(keys_snap)
        return fn(lat_lng[0], lat_lng[1])

    results = [""] * len(coords)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # 순서 보존: submit + index 매핑
        futures = {ex.submit(_worker, c): i for i, c in enumerate(coords)}
        for fut in futures:
            idx = futures[fut]
            try:
                results[idx] = fut.result()
            except Exception:
                results[idx] = ""
    return results


def annotate_addresses(
    scored_gdf: gpd.GeoDataFrame,
    top_gdf: gpd.GeoDataFrame,
    top_with_region: int = 100,
    top_with_road: int = 10,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    상위 N개 격자에만 주소 부여 (전체 격자 역지오코딩은 비용 과다).

    Args:
        scored_gdf:       전체 격자 GeoDataFrame
        top_gdf:          상위 후보지 (rank별 정렬)
        top_with_region:  법정동 주소를 부여할 상위 격자 수 (기본 100)
        top_with_road:    도로명 주소를 추가할 상위 격자 수 (기본 10)

    Returns:
        (scored_with_address, top_with_address)

    성능: 100건 × 병렬 20 = 약 2~5초 (순차 호출 대비 10~20배 단축).
    """
    scored_out = scored_gdf.copy()
    scored_out["address"] = ""

    top_out = top_gdf.copy()
    top_out["address"] = ""
    top_out["road_address"] = ""
    top_out["display_address"] = ""

    if len(scored_out) == 0 or len(top_out) == 0:
        return scored_out, top_out

    # 점수 순 정렬된 상위 N개만 선택
    scored_sorted = scored_out.sort_values("score", ascending=False).reset_index(drop=True)
    n_region = min(top_with_region, len(scored_sorted))
    top_cells = scored_sorted.head(n_region)

    log.info(f"역지오코딩 시작: 법정동 {n_region}개 + 도로명 상위 {top_with_road}개 (병렬 20 workers)")

    # 법정동 병렬 호출
    coords = [(pt.y, pt.x) for pt in top_cells.geometry.centroid]
    regions = _batch_reverse(coords, _reverse_region, max_workers=20)

    # scored에 grid_id 기준 매핑
    addr_by_id = dict(zip(top_cells["grid_id"], regions))
    scored_out["address"] = scored_out["grid_id"].map(addr_by_id).fillna("")

    # top에도 동일 매핑
    top_out["address"] = top_out["grid_id"].map(addr_by_id).fillna("")

    # 상위 top_with_road개에 도로명 추가
    limit = min(top_with_road, len(top_out))
    road_coords = [(pt.y, pt.x) for pt in top_out.head(limit).geometry.centroid]
    road_results = _batch_reverse(road_coords, _reverse_road_address, max_workers=10)
    for i, road in enumerate(road_results):
        top_out.at[top_out.index[i], "road_address"] = road

    # display_address: 도로명 있으면 "법정동 · 도로명", 없으면 법정동만
    def _fmt(row):
        region = row.get("address", "")
        road   = row.get("road_address", "")
        if road:
            return f"{region} · {road}" if region else road
        return region
    top_out["display_address"] = top_out.apply(_fmt, axis=1)

    n_success = (scored_out["address"].str.len() > 0).sum()
    log.info(f"역지오코딩 완료: 법정동 {n_success}개, 도로명 {limit}개")
    return scored_out, top_out
