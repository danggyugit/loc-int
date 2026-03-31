# src/building_client.py — 건축물대장 API + 도로 접근성 분석
# 상가건물 밀도 (건축물대장 API) + 도로 등급 (OSM) → 소프트 팩터 2개
#
# 건축물대장 API 주의사항:
#   - getBrTitleInfo는 sigunguCd + bjdongCd를 모두 필수로 요구
#   - sigunguCd만 전송 시 빈 body({}) 반환 (에러가 아닌 빈 결과)
#   - bjdongCd는 법정동코드 하위 5자리 (b_code[5:])

import logging
import time
import requests
import numpy as np
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

def _get_bjdong_codes(region: str, boundary_gdf: gpd.GeoDataFrame,
                      kakao_key: str) -> list[dict]:
    """
    분석 영역 내 법정동 코드 목록 조회.

    1차: 경계 내 격자 포인트에서 coord2regioncode로 법정동 수집
    2차: 지역명 주소 검색으로 보완

    Returns:
        [{"sigungu_cd": "41285", "bjdong_cd": "10500", "name": "마두동"}, ...]
    """
    headers = {"Authorization": f"KakaoAK {kakao_key}"}
    seen_codes = set()
    result = []

    # 경계 내 격자 포인트 생성 → coord2regioncode로 법정동 수집
    boundary_tm = boundary_gdf.to_crs(CRS_KOREA)
    unified = boundary_tm.unary_union
    minx, miny, maxx, maxy = unified.bounds

    # 2km 간격으로 샘플 포인트 생성 (법정동 경계를 충분히 커버)
    step = 2000
    xs = np.arange(minx + step / 2, maxx, step)
    ys = np.arange(miny + step / 2, maxy, step)

    points_tm = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x in xs for y in ys],
        crs=CRS_KOREA,
    )
    # 경계 내 포인트만
    points_tm = points_tm[points_tm.geometry.within(unified)].copy()
    points_wgs = points_tm.to_crs(CRS_WGS84)

    log.info(f"법정동 코드 수집: {len(points_wgs)}개 격자 포인트 사용")

    for pt in points_wgs.geometry:
        try:
            resp = requests.get(
                "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json",
                headers=headers,
                params={"x": pt.x, "y": pt.y},
                timeout=5,
            )
            resp.raise_for_status()
            docs = resp.json().get("documents", [])
            for doc in docs:
                if doc.get("region_type") == "B":  # 법정동
                    code = doc.get("code", "")
                    if len(code) >= 10:
                        sigungu_cd = code[:5]
                        bjdong_cd = code[5:10]
                        key = f"{sigungu_cd}_{bjdong_cd}"
                        if key not in seen_codes:
                            seen_codes.add(key)
                            name = doc.get("region_3depth_name", "")
                            result.append({
                                "sigungu_cd": sigungu_cd,
                                "bjdong_cd": bjdong_cd,
                                "name": name,
                            })
            time.sleep(0.03)
        except Exception as e:
            log.debug(f"coord2regioncode 오류: {e}")
            continue

    if not result:
        # 폴백: 주소 검색으로 시군구 코드만이라도 확보
        log.warning("격자 기반 법정동 수집 실패 → 주소 검색 폴백")
        for query in [region, f"{region} 청사", f"고양시 {region}"]:
            try:
                resp = requests.get(
                    "https://dapi.kakao.com/v2/local/search/address.json",
                    headers=headers,
                    params={"query": query},
                    timeout=10,
                )
                resp.raise_for_status()
                docs = resp.json().get("documents", [])
                for doc in docs:
                    addr = doc.get("address")
                    if addr and addr.get("b_code"):
                        code = addr["b_code"]
                        if len(code) >= 10:
                            sigungu_cd = code[:5]
                            bjdong_cd = code[5:10]
                            key = f"{sigungu_cd}_{bjdong_cd}"
                            if key not in seen_codes:
                                seen_codes.add(key)
                                name = addr.get("region_3depth_name", "")
                                result.append({
                                    "sigungu_cd": sigungu_cd,
                                    "bjdong_cd": bjdong_cd,
                                    "name": name,
                                })
            except Exception:
                continue

    log.info(f"법정동 코드 수집 완료: {len(result)}개 동")
    for r in result[:5]:
        log.info(f"  {r['sigungu_cd']}-{r['bjdong_cd']} ({r['name']})")
    if len(result) > 5:
        log.info(f"  ... 외 {len(result) - 5}개")

    return result


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

    1. 법정동 코드 목록 조회 (카카오 coord2regioncode)
    2. 법정동별 건축물대장 표제부 조회 (건축HUB API)
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

    # 1) 법정동 코드 목록 조회
    bjdong_list = _get_bjdong_codes(region, boundary_gdf, kakao_key)
    if not bjdong_list:
        log.warning(f"법정동 코드 조회 실패: {region}")
        return None

    # 2) 법정동별 건축물대장 표제부 조회
    url = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
    all_items = []
    # Why: 법정동별로 조회하므로 동당 조회량 제한
    per_dong_limit = max(50, max_buildings * 3 // len(bjdong_list))

    log.info(f"건축물대장 조회 시작: {len(bjdong_list)}개 법정동")

    for dong_info in bjdong_list:
        sigungu_cd = dong_info["sigungu_cd"]
        bjdong_cd = dong_info["bjdong_cd"]
        dong_name = dong_info["name"]
        dong_items = []
        page = 1
        num_rows = 100

        while len(dong_items) < per_dong_limit:
            params = {
                "serviceKey": building_api_key,
                "sigunguCd": sigungu_cd,
                "bjdongCd": bjdong_cd,
                "numOfRows": num_rows,
                "pageNo": page,
                "_type": "json",
            }
            try:
                resp = requests.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.warning(f"건축물대장 API 오류 ({dong_name}, page {page}): {e}")
                break

            # Why: bjdongCd 포함 시 response wrapper 구조가 다름
            body = data.get("response", {}).get("body", {})
            if not body:
                # bjdongCd 없는 경우의 구조 시도
                body = data.get("body", {})
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

            dong_items.extend(item_list)

            total = int(body.get("totalCount", 0))
            if page * num_rows >= total or page * num_rows >= per_dong_limit:
                break
            page += 1
            time.sleep(0.1)

        all_items.extend(dong_items)

        if len(all_items) >= max_buildings * 3:
            break

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

    # 4) 지오코딩 (주소 → 좌표) — 병렬 처리
    unique_addrs = list({b["address"] for b in commercial})
    addr_coords = {}
    geocode_fail = 0

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_geocode_address, addr, kakao_key): addr
            for addr in unique_addrs
        }
        for i, future in enumerate(as_completed(futures)):
            addr = futures[future]
            coords = future.result()
            if coords:
                addr_coords[addr] = coords
            else:
                geocode_fail += 1
            if (i + 1) % 50 == 0:
                log.info(f"  지오코딩 진행: {i+1}/{len(unique_addrs)}")

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
