# src/collector.py — 자동 데이터 수집 (DA Agent)
# 지역명 + 업종만으로 분석에 필요한 모든 데이터를 자동 수집

import os
import time
import logging
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CRS_WGS84, CRS_KOREA,
    KAKAO_API_KEY, KAKAO_LOCAL_URL, KAKAO_CATEGORY_URL,
    KAKAO_MAX_PAGE, KAKAO_PAGE_SIZE, KAKAO_CATEGORY,
    KAKAO_RADIUS_MAX,
)

log = logging.getLogger(__name__)

# 카카오 교통 카테고리 (None = 키워드 검색)
KAKAO_TRANSPORT_CATEGORY = {
    "subway": "SW8",   # 지하철역 (카테고리 코드 존재)
    "bus":    None,    # 버스정류장 (키워드 검색으로 대체)
}


# ─────────────────────────────────────────────────────────
# 1. 행정경계 자동 수집 (OpenStreetMap)
# ─────────────────────────────────────────────────────────

def get_boundary(region: str) -> gpd.GeoDataFrame:
    """
    지역명으로 행정경계 폴리곤 자동 수집 (OpenStreetMap 기반).

    Args:
        region: 시·도·구 이름 (예: '서울특별시', '강남구', '부산광역시 해운대구')

    Returns:
        행정경계 GeoDataFrame (EPSG:4326)
    """
    try:
        import osmnx as ox
        log.info(f"행정경계 수집 중: {region} (OpenStreetMap)")
        gdf = ox.geocode_to_gdf(f"{region}, 대한민국")
        gdf = gdf[["geometry"]].copy()
        gdf["adm_name"] = region
        gdf["adm_code"] = "auto"
        gdf = gdf.to_crs(CRS_WGS84)
        log.info(f"행정경계 수집 완료: {region}")
        return gdf
    except Exception as e:
        log.warning(f"OSM 수집 실패 ({e}) → 카카오 좌표 기반 bbox 사용")
        return _get_boundary_bbox(region)


def _get_boundary_bbox(region: str) -> gpd.GeoDataFrame:
    """카카오 주소 검색으로 중심 좌표를 얻고 근사 bounding box 생성 (OSM 폴백)."""
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    resp = requests.get(url, headers=headers, params={"query": region}, timeout=10)
    resp.raise_for_status()
    docs = resp.json().get("documents", [])
    if not docs:
        raise ValueError(f"지역을 찾을 수 없습니다: {region}")

    lng = float(docs[0]["x"])
    lat = float(docs[0]["y"])

    # 시·도급 ≈ ±0.5도, 구·군급 ≈ ±0.1도
    delta = 0.5 if any(k in region for k in ["특별시","광역시","도"]) else 0.12
    boundary_poly = box(lng - delta, lat - delta, lng + delta, lat + delta)

    gdf = gpd.GeoDataFrame(
        {"adm_name": [region], "adm_code": ["auto"]},
        geometry=[boundary_poly], crs=CRS_WGS84,
    )
    log.info(f"bbox 행정경계 생성 완료: {region}")
    return gdf


# ─────────────────────────────────────────────────────────
# 2. 경쟁업체 자동 수집 (카카오 로컬 API — 격자 기반 전수조사)
# ─────────────────────────────────────────────────────────

def get_competitors(category: str, boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    카카오 로컬 API로 경쟁업체 위치 전수 수집.

    단순 키워드 검색(최대 1,350건)의 한계를 극복하기 위해
    경계를 격자로 나눠 각 격자 중심에서 반경 검색 → 중복 제거.

    Args:
        category:     업종 코드 (config의 KAKAO_CATEGORY 키)
        boundary_gdf: 분석 영역 경계

    Returns:
        경쟁업체 GeoDataFrame (EPSG:4326)
    """
    cat_code = KAKAO_CATEGORY.get(category)
    if cat_code is None:
        raise ValueError(f"알 수 없는 업종: '{category}'. 허용값: {list(KAKAO_CATEGORY.keys())}")

    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    search_points = _make_search_grid(boundary_gdf, step_m=5000)
    log.info(f"경쟁업체 수집 시작: {category} ({len(search_points)}개 격자 중심)")

    records = []
    seen_ids = set()

    for i, (slng, slat) in enumerate(search_points):
        for page in range(1, KAKAO_MAX_PAGE + 1):
            params = {
                "category_group_code": cat_code,
                "x":      slng,
                "y":      slat,
                "radius": KAKAO_RADIUS_MAX,
                "page":   page,
                "size":   KAKAO_PAGE_SIZE,
            }
            try:
                # 카테고리 반경 검색은 category.json 엔드포인트 사용
                resp = requests.get(KAKAO_CATEGORY_URL, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                log.warning(f"API 오류 (격자 {i}, page {page}): {e}")
                break

            for d in data.get("documents", []):
                bid = d.get("id", "")
                if bid and bid not in seen_ids:
                    seen_ids.add(bid)
                    records.append({
                        "biz_id":   bid,
                        "name":     d.get("place_name", ""),
                        "category": category,
                        "lat":      float(d.get("y", 0)),
                        "lng":      float(d.get("x", 0)),
                        "address":  d.get("address_name", ""),
                    })

            if data.get("meta", {}).get("is_end", True):
                break
            time.sleep(0.05)

        if (i + 1) % 10 == 0:
            log.info(f"  진행: {i+1}/{len(search_points)} 격자 | 누적 {len(records)}건")

    if not records:
        log.warning(f"수집된 경쟁업체 없음: {category}")
        return gpd.GeoDataFrame(
            columns=["biz_id","name","category","lat","lng","address","geometry"],
            crs=CRS_WGS84,
        )

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )

    # 경계 내 포인트만 보존
    gdf = _clip_to_boundary(gdf, boundary_gdf)
    log.info(f"경쟁업체 수집 완료: {len(gdf)}건 (경계 내)")
    return gdf[["biz_id","name","category","lat","lng","address","geometry"]]


# ─────────────────────────────────────────────────────────
# 3. 교통 인프라 자동 수집 (지하철 + 버스)
# ─────────────────────────────────────────────────────────

def get_transport(boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    카카오 로컬 API로 지하철역 + 버스정류장 수집.

    Returns:
        교통 인프라 GeoDataFrame (EPSG:4326)
    """
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    search_points = _make_search_grid(boundary_gdf, step_m=8000)
    records = []
    seen_ids = set()

    for type_name, cat_code in KAKAO_TRANSPORT_CATEGORY.items():
        log.info(f"교통 수집: {type_name} ({len(search_points)}개 격자)")
        for slng, slat in search_points:
            for page in range(1, 10):
                if cat_code:
                    # 카테고리 코드 있으면 category.json
                    params = {
                        "category_group_code": cat_code,
                        "x": slng, "y": slat,
                        "radius": KAKAO_RADIUS_MAX,
                        "page": page, "size": KAKAO_PAGE_SIZE,
                    }
                    url = KAKAO_CATEGORY_URL
                else:
                    # 버스정류장은 키워드 검색
                    params = {
                        "query": "버스정류장",
                        "x": slng, "y": slat,
                        "radius": KAKAO_RADIUS_MAX,
                        "page": page, "size": KAKAO_PAGE_SIZE,
                    }
                    url = KAKAO_LOCAL_URL
                try:
                    resp = requests.get(url, headers=headers, params=params, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                except requests.RequestException:
                    break

                for d in data.get("documents", []):
                    bid = d.get("id", "")
                    if bid and bid not in seen_ids:
                        seen_ids.add(bid)
                        # Why: 지하철역은 버스 정류장 대비 집객 효과가 훨씬 크므로
                        #      가중치를 차등 부여 (subway=3, bus=1)
                        weight = 3 if type_name == "subway" else 1
                        records.append({
                            "stop_id":   bid,
                            "stop_name": d.get("place_name", ""),
                            "type":      type_name,
                            "weight":    weight,
                            "lat":       float(d.get("y", 0)),
                            "lng":       float(d.get("x", 0)),
                        })

                if data.get("meta", {}).get("is_end", True):
                    break
                time.sleep(0.05)

    if not records:
        log.warning("교통 인프라 데이터 없음")
        return gpd.GeoDataFrame(
            columns=["stop_id","stop_name","type","weight","lat","lng","geometry"],
            crs=CRS_WGS84,
        )

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )
    gdf = _clip_to_boundary(gdf, boundary_gdf)
    n_sub = (gdf["type"] == "subway").sum()
    n_bus = (gdf["type"] == "bus").sum()
    log.info(f"교통 인프라 수집 완료: {len(gdf)}건 (지하철 {n_sub} × 3 + 버스 {n_bus} × 1)")
    return gdf[["stop_id","stop_name","type","weight","lat","lng","geometry"]]


# ─────────────────────────────────────────────────────────
# 4. 인구 추정 (아파트 밀도 proxy)
# ─────────────────────────────────────────────────────────

def get_population_proxy(boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame | None:
    """
    카카오 API로 아파트 검색 → 격자당 아파트 수를 인구 proxy로 활용.
    실제 인구 데이터가 없을 때 합리적인 근사값 제공.

    Returns:
        아파트 위치 GeoDataFrame (인구 proxy, population 컬럼 = 1)
    """
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    search_points = _make_search_grid(boundary_gdf, step_m=6000)
    records = []
    seen_ids = set()

    log.info(f"인구 proxy 수집 (아파트): {len(search_points)}개 격자")

    for slng, slat in search_points:
        for page in range(1, 20):
            params = {
                "query":  "아파트",
                "x": slng, "y": slat,
                "radius": KAKAO_RADIUS_MAX,
                "page": page, "size": KAKAO_PAGE_SIZE,
            }
            try:
                resp = requests.get(KAKAO_LOCAL_URL, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException:
                break

            for d in data.get("documents", []):
                bid = d.get("id", "")
                if bid and bid not in seen_ids:
                    seen_ids.add(bid)
                    records.append({
                        "biz_id":     bid,
                        "name":       d.get("place_name", ""),
                        "lat":        float(d.get("y", 0)),
                        "lng":        float(d.get("x", 0)),
                        "population": 1,     # 아파트 1건 = 인구 1단위
                    })

            if data.get("meta", {}).get("is_end", True):
                break
            time.sleep(0.05)

    if not records:
        return None

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )
    gdf = _clip_to_boundary(gdf, boundary_gdf)
    log.info(f"인구 proxy 수집 완료: 아파트 {len(gdf)}건")
    return gdf


# ─────────────────────────────────────────────────────────
# 5. 주차 인프라 수집 (카카오 키워드 검색)
# ─────────────────────────────────────────────────────────

def get_parking(boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    카카오 로컬 키워드 검색으로 주차장 위치 수집.

    Returns:
        주차장 GeoDataFrame (EPSG:4326)
    """
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    search_points = _make_search_grid(boundary_gdf, step_m=6000)
    records = []
    seen_ids = set()

    log.info(f"주차장 수집 시작: {len(search_points)}개 격자")

    for slng, slat in search_points:
        for page in range(1, 20):
            params = {
                "query":  "주차장",
                "x": slng, "y": slat,
                "radius": KAKAO_RADIUS_MAX,
                "page": page, "size": KAKAO_PAGE_SIZE,
            }
            try:
                resp = requests.get(KAKAO_LOCAL_URL, headers=headers,
                                    params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException:
                break

            for d in data.get("documents", []):
                bid = d.get("id", "")
                if bid and bid not in seen_ids:
                    seen_ids.add(bid)
                    records.append({
                        "park_id":   bid,
                        "name":      d.get("place_name", ""),
                        "lat":       float(d.get("y", 0)),
                        "lng":       float(d.get("x", 0)),
                        "parking":   1,
                    })

            if data.get("meta", {}).get("is_end", True):
                break
            time.sleep(0.05)

    if not records:
        log.warning("주차장 데이터 없음")
        return gpd.GeoDataFrame(
            columns=["park_id", "name", "lat", "lng", "parking", "geometry"],
            crs=CRS_WGS84,
        )

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )
    gdf = _clip_to_boundary(gdf, boundary_gdf)
    log.info(f"주차장 수집 완료: {len(gdf)}건")
    return gdf


# ─────────────────────────────────────────────────────────
# 6. 상권 다양성 수집 (카카오 카테고리 다중 검색)
# ─────────────────────────────────────────────────────────

# 다양성 지표용 업종 카테고리 (경쟁업체와 별도로 상권 활성도 측정)
_DIVERSITY_CATEGORIES = {
    "CE7": "cafe",
    "FD6": "restaurant",
    "CS2": "convenience",
    "PM9": "pharmacy",
    "MT1": "mart",
    "HP8": "hospital",
}


def get_commercial_diversity(boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    카카오 카테고리 검색으로 주요 업종 6종 동시 수집 → 상권 다양성 지수 산출용.

    격자별 존재하는 업종 카테고리 수(0~6)를 통해 상권 활성도를 측정.
    다양한 업종이 골고루 분포할수록 상업적으로 성숙한 지역.

    Returns:
        GeoDataFrame (EPSG:4326) — cat_code, cat_name 컬럼 포함
    """
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    search_points = _make_search_grid(boundary_gdf, step_m=8000)
    records = []
    seen_ids = set()

    log.info(f"상권 다양성 수집 시작: {len(_DIVERSITY_CATEGORIES)}개 카테고리 × "
             f"{len(search_points)}개 격자")

    for cat_code, cat_name in _DIVERSITY_CATEGORIES.items():
        for slng, slat in search_points:
            for page in range(1, 4):  # 다양성 확인용이므로 3페이지로 제한
                params = {
                    "category_group_code": cat_code,
                    "x": slng, "y": slat,
                    "radius": KAKAO_RADIUS_MAX,
                    "page": page, "size": KAKAO_PAGE_SIZE,
                }
                try:
                    resp = requests.get(KAKAO_CATEGORY_URL, headers=headers,
                                        params=params, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                except requests.RequestException:
                    break

                for d in data.get("documents", []):
                    bid = d.get("id", "")
                    if bid and bid not in seen_ids:
                        seen_ids.add(bid)
                        records.append({
                            "biz_id":    bid,
                            "name":      d.get("place_name", ""),
                            "cat_code":  cat_code,
                            "cat_name":  cat_name,
                            "lat":       float(d.get("y", 0)),
                            "lng":       float(d.get("x", 0)),
                        })

                if data.get("meta", {}).get("is_end", True):
                    break
                time.sleep(0.05)

    if not records:
        log.warning("상권 다양성 데이터 없음")
        return gpd.GeoDataFrame(
            columns=["biz_id", "name", "cat_code", "cat_name",
                     "lat", "lng", "geometry"],
            crs=CRS_WGS84,
        )

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )
    gdf = _clip_to_boundary(gdf, boundary_gdf)

    cat_counts = gdf["cat_name"].value_counts()
    log.info(f"상권 다양성 수집 완료: 총 {len(gdf)}건 | "
             + " | ".join(f"{k}={v}" for k, v in cat_counts.items()))
    return gdf


# ─────────────────────────────────────────────────────────
# 7. 키워드 기반 경쟁업체 수집 (자유 검색어)
# ─────────────────────────────────────────────────────────

def get_competitors_by_keyword(keyword: str, boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    카카오 로컬 키워드 검색으로 경쟁업체 수집.
    카테고리 코드가 없는 업종(공방, 스튜디오 등)에 사용.

    Args:
        keyword:      검색 키워드 (예: '도자기 공방', '필라테스', '네일샵')
        boundary_gdf: 분석 영역 경계

    Returns:
        경쟁업체 GeoDataFrame (EPSG:4326)
    """
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    search_points = _make_search_grid(boundary_gdf, step_m=5000)
    log.info(f"키워드 검색 시작: '{keyword}' ({len(search_points)}개 격자)")

    records = []
    seen_ids = set()

    for i, (slng, slat) in enumerate(search_points):
        for page in range(1, KAKAO_MAX_PAGE + 1):
            params = {
                "query":  keyword,
                "x":      slng,
                "y":      slat,
                "radius": KAKAO_RADIUS_MAX,
                "page":   page,
                "size":   KAKAO_PAGE_SIZE,
            }
            try:
                resp = requests.get(KAKAO_LOCAL_URL, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                log.warning(f"API 오류 (격자 {i}, page {page}): {e}")
                break

            for d in data.get("documents", []):
                bid = d.get("id", "")
                if bid and bid not in seen_ids:
                    seen_ids.add(bid)
                    records.append({
                        "biz_id":   bid,
                        "name":     d.get("place_name", ""),
                        "category": keyword,
                        "lat":      float(d.get("y", 0)),
                        "lng":      float(d.get("x", 0)),
                        "address":  d.get("address_name", ""),
                    })

            if data.get("meta", {}).get("is_end", True):
                break
            time.sleep(0.05)

        if (i + 1) % 10 == 0:
            log.info(f"  진행: {i+1}/{len(search_points)} 격자 | 누적 {len(records)}건")

    if not records:
        log.warning(f"키워드 검색 결과 없음: '{keyword}'")
        return gpd.GeoDataFrame(
            columns=["biz_id","name","category","lat","lng","address","geometry"],
            crs=CRS_WGS84,
        )

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )
    gdf = _clip_to_boundary(gdf, boundary_gdf)
    log.info(f"키워드 검색 완료: '{keyword}' {len(gdf)}건 (경계 내)")
    return gdf[["biz_id","name","category","lat","lng","address","geometry"]]


# ─────────────────────────────────────────────────────────
# 8. 전체 수집 파이프라인
# ─────────────────────────────────────────────────────────

def collect_all(region: str, category: str = None, keyword: str = None,
                cell_size_m: int = 500,
                vworld_key: str = None, building_key: str = None) -> dict:
    """
    지역명 + 업종(또는 키워드)으로 분석에 필요한 모든 데이터를 자동 수집.

    boundary 수집 후 나머지 8개 데이터 소스를 ThreadPoolExecutor로 병렬 수집.
    Why: 각 API 호출은 I/O 바운드(네트워크 대기)이므로 병렬화로 3~4배 단축.

    인구 데이터 우선순위:
      1. SGIS 격자 통계 (실인구 + 종사자수 + 인구통계) — SGIS_CONSUMER_KEY 설정 시
      2. 아파트 밀도 proxy (카카오 API) — 폴백

    Args:
        region:       분석 지역 (예: '강남구', '서울특별시 마포구')
        category:     프리셋 업종 코드 (예: 'cafe', 'hospital') — preset 방식
        keyword:      자유 검색어 (예: '도자기 공방', '네일샵') — keyword 방식
        cell_size_m:  격자 셀 크기 (SGIS 레벨 자동 선택에 사용)
        vworld_key:   Vworld API 키 (용도지역 조회)
        building_key: 건축물대장 API 키 (상가건물 조회)

    Returns:
        {boundary, competitor, transport, parking, diversity,
         population, workplace, pop_source,
         land_use, buildings, roads}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    label = keyword if keyword else category
    log.info(f"=== 데이터 자동 수집 시작: {region} / {label} (병렬) ===")

    # Step 0. boundary는 모든 작업의 선행 조건 — 순차 실행
    boundary = get_boundary(region)

    # ── 병렬 수집 태스크 정의 ──────────────────────────────
    # Why: boundary만 있으면 나머지 8개 소스는 서로 독립적
    def _task_competitor():
        if keyword:
            return get_competitors_by_keyword(keyword, boundary)
        return get_competitors(category, boundary)

    def _task_transport():
        return get_transport(boundary)

    def _task_parking():
        return get_parking(boundary)

    def _task_diversity():
        return get_commercial_diversity(boundary)

    def _task_population():
        """인구·종사자: SGIS 우선, 실패 시 아파트 proxy 폴백."""
        from src.sgis_client import is_sgis_available
        if is_sgis_available():
            try:
                from src.sgis_client import get_sgis_grid_data
                sgis_data = get_sgis_grid_data(boundary, region=region,
                                               cell_size_m=cell_size_m)
                if sgis_data is not None:
                    log.info("인구 데이터: SGIS 격자 통계 사용")
                    return sgis_data["population"], sgis_data["workplace"], "sgis"
            except Exception as e:
                log.warning(f"SGIS 수집 실패 → 아파트 proxy 폴백: {e}")
        pop = get_population_proxy(boundary)
        src = "apartment_proxy" if pop is not None else "none"
        if pop is not None:
            log.info("인구 데이터: 아파트 proxy 사용 (SGIS 미설정 또는 실패)")
        return pop, None, src

    def _task_land_use():
        try:
            from src.vworld_client import get_land_use_zones
            return get_land_use_zones(boundary, vworld_key or "")
        except Exception as e:
            log.warning(f"용도지역 수집 실패: {e}")
            return None

    def _task_buildings():
        kakao_key_runtime = os.environ.get("KAKAO_API_KEY", "")
        if building_key and kakao_key_runtime:
            try:
                from src.building_client import get_commercial_buildings
                return get_commercial_buildings(
                    region, boundary, building_key, kakao_key_runtime,
                )
            except Exception as e:
                log.warning(f"상가건물 수집 실패: {e}")
        return None

    def _task_roads():
        try:
            from src.building_client import get_road_network
            return get_road_network(boundary)
        except Exception as e:
            log.warning(f"도로 네트워크 수집 실패: {e}")
            return None

    # ── 병렬 실행 (max_workers=4: API rate limit 고려) ────
    task_map = {
        "competitor": _task_competitor,
        "transport":  _task_transport,
        "parking":    _task_parking,
        "diversity":  _task_diversity,
        "population": _task_population,
        "land_use":   _task_land_use,
        "buildings":  _task_buildings,
        "roads":      _task_roads,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fn): key for key, fn in task_map.items()}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
                log.info(f"  ✓ {key} 수집 완료")
            except Exception as e:
                log.warning(f"  ✗ {key} 수집 실패: {e}")
                results[key] = None

    # ── 결과 추출 ─────────────────────────────────────────
    def _gdf_or_empty(val):
        return val if val is not None else gpd.GeoDataFrame()

    competitor = _gdf_or_empty(results.get("competitor"))
    transport  = _gdf_or_empty(results.get("transport"))
    parking    = _gdf_or_empty(results.get("parking"))
    diversity  = _gdf_or_empty(results.get("diversity"))
    land_use   = results.get("land_use")
    buildings  = results.get("buildings")
    roads      = results.get("roads")

    pop_result = results.get("population", (None, None, "none"))
    if isinstance(pop_result, tuple) and len(pop_result) == 3:
        population, workplace, pop_source = pop_result
    else:
        population, workplace, pop_source = None, None, "none"

    log.info(
        f"=== 수집 완료 === "
        f"경쟁업체 {len(competitor)}건 | "
        f"교통 {len(transport)}건 | "
        f"주차장 {len(parking)}건 | "
        f"상권 다양성 {len(diversity)}건 | "
        f"인구 {len(population) if population is not None else 0}건 ({pop_source}) | "
        f"종사자 {len(workplace) if workplace is not None else 0}건 | "
        f"용도지역 {len(land_use) if land_use is not None else 0}건 | "
        f"상가건물 {len(buildings) if buildings is not None else 0}건 | "
        f"도로 {len(roads) if roads is not None else 0}건"
    )
    return {
        "boundary":   boundary,
        "competitor": competitor,
        "transport":  transport,
        "parking":    parking,
        "diversity":  diversity,
        "population": population,
        "workplace":  workplace,
        "pop_source": pop_source,
        "land_use":   land_use,
        "buildings":  buildings,
        "roads":      roads,
    }


# ─────────────────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────────────────

def _make_search_grid(boundary_gdf: gpd.GeoDataFrame, step_m: int) -> list[tuple]:
    """경계 내 격자 중심 좌표 리스트 생성 (API 검색 거점용)."""
    boundary_tm = boundary_gdf.to_crs(CRS_KOREA)
    unified     = boundary_tm.unary_union
    minx, miny, maxx, maxy = unified.bounds

    xs = np.arange(minx + step_m / 2, maxx, step_m)
    ys = np.arange(miny + step_m / 2, maxy, step_m)

    # EPSG:5179 → EPSG:4326 변환
    points_tm = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x in xs for y in ys],
        crs=CRS_KOREA,
    )
    # 경계 내 포인트만
    joined = gpd.sjoin(points_tm, boundary_tm[["geometry"]], how="inner", predicate="within")
    points_wgs = joined.to_crs(CRS_WGS84)
    return [(p.x, p.y) for p in points_wgs.geometry]


def _clip_to_boundary(gdf: gpd.GeoDataFrame, boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """경계 폴리곤 내 포인트만 반환."""
    boundary_union = boundary_gdf.unary_union
    return gdf[gdf.geometry.within(boundary_union)].copy()
