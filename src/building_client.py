# src/building_client.py — 건축물대장 API + 도로 접근성 분석
# 상가건물 밀도 (건축물대장 API) + 도로 등급 (OSM) → 소프트 팩터 2개

import logging
import time
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_WGS84, CRS_KOREA

log = logging.getLogger(__name__)

# 상업용 건축물 용도 키워드
COMMERCIAL_USAGE_KEYWORDS = [
    "근린생활시설", "판매시설", "업무시설", "숙박시설",
    "위락시설", "관광휴게시설",
]

# OSM 도로 등급별 입지 접근성 점수
# Why: 대로변일수록 가시성·유동인구 유리, 이면도로는 임대료 저렴하지만 노출 불리
ROAD_SCORE_MAP = {
    "primary":       1.0,
    "primary_link":  0.9,
    "trunk":         0.8,
    "trunk_link":    0.7,
    "secondary":     0.8,
    "secondary_link": 0.7,
    "tertiary":      0.6,
    "tertiary_link": 0.5,
    "residential":   0.3,
    "living_street": 0.3,
    "unclassified":  0.3,
    "service":       0.2,
}


# ──────────────────────────────────────────────────────────
# 1. 건축물대장 — 상가건물 위치 수집
# ──────────────────────────────────────────────────────────

def _get_sigungu_code(region: str, kakao_key: str) -> str | None:
    """카카오 API로 지역명 → 시군구코드(5자리) 조회."""
    headers = {"Authorization": f"KakaoAK {kakao_key}"}

    # 1) 지역 중심 좌표 얻기 (구청/시청 검색)
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    for query in [f"{region}청", region]:
        try:
            resp = requests.get(url, headers=headers,
                                params={"query": query}, timeout=10)
            resp.raise_for_status()
            docs = resp.json().get("documents", [])
            if docs:
                lng, lat = docs[0]["x"], docs[0]["y"]
                break
        except Exception:
            continue
    else:
        return None

    # 2) 좌표 → 법정동코드 → 시군구코드
    url2 = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
    try:
        resp2 = requests.get(url2, headers=headers,
                             params={"x": lng, "y": lat}, timeout=10)
        resp2.raise_for_status()
        for doc in resp2.json().get("documents", []):
            if doc.get("region_type") == "B":
                return doc["code"][:5]
    except Exception as e:
        log.warning(f"시군구코드 조회 실패: {e}")
    return None


def _geocode_address(address: str, kakao_key: str) -> tuple | None:
    """카카오 주소 검색으로 좌표 반환 (lng, lat)."""
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {kakao_key}"}
    try:
        resp = requests.get(url, headers=headers,
                            params={"query": address}, timeout=5)
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
        if docs:
            return float(docs[0]["x"]), float(docs[0]["y"])
    except Exception:
        pass
    return None


def get_commercial_buildings(
    region: str,
    boundary_gdf: gpd.GeoDataFrame,
    building_api_key: str,
    kakao_key: str,
    max_buildings: int = 300,
) -> gpd.GeoDataFrame | None:
    """
    건축물대장 API로 상가건물 위치 수집.

    1. 시군구코드 조회 (카카오 API)
    2. 건축물대장 표제부 조회 (건축HUB API)
    3. 상업용 건축물 필터링
    4. 주소 → 좌표 변환 (카카오 지오코딩)

    Returns:
        상가건물 GeoDataFrame 또는 None
    """
    if not building_api_key:
        log.warning("건축물대장 API 키 없음 → 건너뜀")
        return None
    if not kakao_key:
        log.warning("카카오 API 키 없음 → 건축물 지오코딩 불가")
        return None

    # 1) 시군구코드 조회
    sigungu_cd = _get_sigungu_code(region, kakao_key)
    if not sigungu_cd:
        log.warning(f"시군구코드 조회 실패: {region}")
        return None
    log.info(f"시군구코드: {sigungu_cd} ({region})")

    # 2) 건축물대장 표제부 조회
    url = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
    all_items = []
    page = 1
    num_rows = 100
    # Why: 필터링 전이므로 상업용 비율 감안해 3배수 조회
    fetch_limit = max_buildings * 3

    log.info(f"건축물대장 조회 시작: 시군구={sigungu_cd}")

    while len(all_items) < fetch_limit:
        params = {
            "serviceKey": building_api_key,
            "sigunguCd": sigungu_cd,
            "numOfRows": num_rows,
            "pageNo": page,
            "_type": "json",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning(f"건축물대장 API 오류 (page {page}): {e}")
            break

        body = data.get("response", {}).get("body", {})
        items = body.get("items", {})

        if isinstance(items, dict):
            item_list = items.get("item", [])
        elif isinstance(items, list):
            item_list = items
        else:
            item_list = []

        if isinstance(item_list, dict):
            item_list = [item_list]

        if not item_list:
            break

        all_items.extend(item_list)

        total = int(body.get("totalCount", 0))
        if page * num_rows >= total or page * num_rows >= fetch_limit:
            break
        page += 1
        time.sleep(0.1)

    if not all_items:
        log.warning("건축물대장 데이터 없음")
        return None

    log.info(f"건축물대장 원본: {len(all_items)}건 조회")

    # 3) 상업용 건축물 필터링
    commercial = []
    for item in all_items:
        main_purps = item.get("mainPurpsCdNm", "") or ""
        etc_purps = item.get("etcPurps", "") or ""
        purps_text = f"{main_purps} {etc_purps}"
        if any(kw in purps_text for kw in COMMERCIAL_USAGE_KEYWORDS):
            addr = item.get("newPlatPlc") or item.get("platPlc") or ""
            if addr:
                commercial.append({
                    "address": addr,
                    "usage": main_purps,
                    "floors": int(item.get("grndFlrCnt", 0) or 0),
                })
        if len(commercial) >= max_buildings:
            break

    if not commercial:
        log.warning("상업용 건축물 없음")
        return None

    log.info(f"상업용 건축물 필터: {len(commercial)}건")

    # 4) 지오코딩 (주소 → 좌표) — 중복 주소 제거 후 변환
    unique_addrs = list({b["address"] for b in commercial})
    addr_coords = {}
    geocode_fail = 0

    for i, addr in enumerate(unique_addrs):
        coords = _geocode_address(addr, kakao_key)
        if coords:
            addr_coords[addr] = coords
        else:
            geocode_fail += 1

        if (i + 1) % 50 == 0:
            log.info(f"  지오코딩 진행: {i+1}/{len(unique_addrs)}")
        time.sleep(0.03)

    records = []
    for bld in commercial:
        coords = addr_coords.get(bld["address"])
        if coords:
            bld["lng"], bld["lat"] = coords
            records.append(bld)

    if not records:
        log.warning("지오코딩 성공 건물 없음")
        return None

    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(
        df,
        geometry=[Point(r["lng"], r["lat"]) for _, r in df.iterrows()],
        crs=CRS_WGS84,
    )

    # 경계 내 클리핑
    boundary_union = boundary_gdf.unary_union
    gdf = gdf[gdf.geometry.within(boundary_union)].copy()

    log.info(f"상가건물 수집 완료: {len(gdf)}건 (지오코딩 실패 {geocode_fail}건)")
    return gdf


# ──────────────────────────────────────────────────────────
# 2. 도로 접근성 — OSM 도로 등급 분석
# ──────────────────────────────────────────────────────────

def get_road_network(boundary_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame | None:
    """
    OSM 도로 네트워크 조회 → 도로 등급별 접근성 점수 GeoDataFrame.

    Returns:
        도로 GeoDataFrame (highway_type, road_score, geometry) 또는 None
    """
    try:
        import osmnx as ox
    except ImportError:
        log.warning("osmnx 미설치 → 도로 접근성 분석 건너뜀")
        return None

    boundary_union = boundary_gdf.to_crs(CRS_WGS84).unary_union

    try:
        log.info("OSM 도로 네트워크 조회 시작")
        G = ox.graph_from_polygon(boundary_union, network_type="drive")
        edges = ox.graph_to_gdfs(G, nodes=False)
    except Exception as e:
        log.warning(f"OSM 도로 조회 실패: {e}")
        return None

    if edges.empty:
        log.warning("도로 데이터 없음")
        return None

    # highway 컬럼이 리스트인 경우 첫 번째 값 사용
    def _first_val(val):
        return val[0] if isinstance(val, list) else val

    edges = edges.copy()
    edges["highway_type"] = edges["highway"].apply(_first_val)
    edges["road_score"] = edges["highway_type"].map(ROAD_SCORE_MAP).fillna(0.2)

    result = edges[["highway_type", "road_score", "geometry"]].copy()
    result = result.to_crs(CRS_WGS84)

    road_counts = result["highway_type"].value_counts()
    log.info(f"도로 네트워크 조회 완료: {len(result)}개 세그먼트")
    for rt, cnt in road_counts.head(5).items():
        log.info(f"  {rt}: {cnt}개")

    return result
