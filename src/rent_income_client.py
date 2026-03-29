# src/rent_income_client.py — 공공데이터포털 API 클라이언트 (소득 proxy + 상가 월세)
#
# 데이터 소스:
#   1. 국토교통부 아파트매매 실거래 상세 자료 → 읍면동별 평균 매매가 (소득 proxy)
#   2. 국토교통부 상업업무용 부동산 임대 실거래자료 → 읍면동별 평균 월세 (임대 비용)
#      - 상업용 부동산 데이터 미제공 시 아파트 전월세로 폴백
#
# 사전 준비:
#   1. https://www.data.go.kr 회원가입
#   2. "국토교통부 아파트매매 실거래 상세 자료" API 활용 신청
#   3. "국토교통부 상업업무용 부동산 임대동향 실거래 자료" API 활용 신청
#   4. 환경변수:
#      $env:DATA_GO_KR_API_KEY = "발급받은_인증키"

import os
import time
import logging
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from shapely.geometry import Point

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_WGS84, CRS_KOREA, KAKAO_API_KEY

log = logging.getLogger(__name__)

DATA_GO_KR_API_KEY = os.environ.get("DATA_GO_KR_API_KEY", "")

# API 엔드포인트
_APT_TRADE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
_APT_RENT_URL  = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"

# 카카오 역지오코딩
_KAKAO_COORD2REGION_URL = "https://dapi.kakao.com/v2/local/geo/coord2regioncode.json"
_KAKAO_ADDRESS_URL      = "https://dapi.kakao.com/v2/local/search/address.json"


def is_data_api_available() -> bool:
    """공공데이터포털 API 키 설정 여부 확인."""
    return bool(DATA_GO_KR_API_KEY)


# ─────────────────────────────────────────────────────────
# 법정동 코드 조회
# ─────────────────────────────────────────────────────────

def _get_lawd_code(boundary_gdf: gpd.GeoDataFrame) -> str | None:
    """
    분석 영역 중심점 → 카카오 역지오코딩 → 법정동 시군구코드(5자리) 반환.
    """
    kakao_key = os.environ.get("KAKAO_API_KEY", KAKAO_API_KEY or "")
    if not kakao_key:
        log.warning("카카오 API 키 미설정 → 법정동코드 조회 불가")
        return None

    centroid = boundary_gdf.to_crs(CRS_WGS84).unary_union.centroid
    lng, lat = centroid.x, centroid.y

    try:
        resp = requests.get(
            _KAKAO_COORD2REGION_URL,
            headers={"Authorization": f"KakaoAK {kakao_key}"},
            params={"x": lng, "y": lat},
            timeout=10,
        )
        resp.raise_for_status()
        docs = resp.json().get("documents", [])

        # 법정동(region_type=B) 우선
        for doc in docs:
            if doc.get("region_type") == "B":
                code = doc.get("code", "")
                if len(code) >= 5:
                    lawd_cd = code[:5]
                    log.info(f"법정동 시군구코드: {lawd_cd} ({doc.get('address_name', '')})")
                    return lawd_cd

        # B타입 없으면 H타입 사용
        for doc in docs:
            code = doc.get("code", "")
            if len(code) >= 5:
                return code[:5]

    except Exception as e:
        log.warning(f"법정동코드 조회 실패: {e}")

    return None


def _get_region_name(boundary_gdf: gpd.GeoDataFrame) -> str:
    """분석 영역 중심점 → 카카오 역지오코딩 → 시군구명 반환."""
    kakao_key = os.environ.get("KAKAO_API_KEY", KAKAO_API_KEY or "")
    if not kakao_key:
        return ""

    centroid = boundary_gdf.to_crs(CRS_WGS84).unary_union.centroid
    try:
        resp = requests.get(
            _KAKAO_COORD2REGION_URL,
            headers={"Authorization": f"KakaoAK {kakao_key}"},
            params={"x": centroid.x, "y": centroid.y},
            timeout=10,
        )
        docs = resp.json().get("documents", [])
        for doc in docs:
            if doc.get("region_type") == "B":
                return doc.get("address_name", "").strip()
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────
# 법정동 지오코딩 (동 이름 → 좌표)
# ─────────────────────────────────────────────────────────

_geocode_cache: dict[str, tuple[float, float] | None] = {}


def _geocode_dong(region_prefix: str, dong_name: str) -> tuple[float, float] | None:
    """
    '{시군구} {법정동}' → (lat, lng) 좌표 변환.
    카카오 주소검색 API 사용, 결과 캐싱.
    """
    cache_key = f"{region_prefix} {dong_name}"
    if cache_key in _geocode_cache:
        return _geocode_cache[cache_key]

    kakao_key = os.environ.get("KAKAO_API_KEY", KAKAO_API_KEY or "")
    if not kakao_key:
        return None

    try:
        resp = requests.get(
            _KAKAO_ADDRESS_URL,
            headers={"Authorization": f"KakaoAK {kakao_key}"},
            params={"query": cache_key},
            timeout=10,
        )
        docs = resp.json().get("documents", [])
        if docs:
            lat = float(docs[0]["y"])
            lng = float(docs[0]["x"])
            _geocode_cache[cache_key] = (lat, lng)
            return (lat, lng)
    except Exception:
        pass

    _geocode_cache[cache_key] = None
    return None


# ─────────────────────────────────────────────────────────
# 공공데이터포털 API 호출 공통
# ─────────────────────────────────────────────────────────

def _call_data_api(url: str, lawd_cd: str, deal_ymd: str) -> list[dict]:
    """공공데이터포털 API 호출 → 아이템 리스트 반환."""
    params = {
        "serviceKey": DATA_GO_KR_API_KEY,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": "1",
        "numOfRows": "9999",
        "_type": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        # XML 응답인 경우 (인증 실패 등)
        try:
            data = resp.json()
        except ValueError:
            text = resp.text[:500]
            log.warning(f"JSON 파싱 실패 (XML?): {text}")
            return []

        # 에러 응답 확인
        header = data.get("response", {}).get("header", {})
        result_code = str(header.get("resultCode", ""))
        # data.go.kr 성공 코드: "00" 또는 "000"
        if result_code and result_code not in ("00", "000"):
            api_name = url.split("/")[-1]
            log.warning(
                f"[{api_name}] API 에러 ({deal_ymd}): "
                f"code={result_code}, msg={header.get('resultMsg', '')}"
            )
            return []

        body = data.get("response", {}).get("body", {})
        items = body.get("items", {})

        if not items or (isinstance(items, str) and items.strip() == ""):
            return []

        item_list = items.get("item", [])
        if isinstance(item_list, dict):
            item_list = [item_list]
        return item_list

    except Exception as e:
        api_name = url.split("/")[-1]
        log.warning(f"[{api_name}] 호출 실패 ({deal_ymd}): {e}")
        return []


def diagnose_api(lawd_cd: str = "11680", deal_ymd: str = "202501") -> dict:
    """
    API 연결 진단 함수. Streamlit 앱 또는 터미널에서 직접 호출 가능.

    사용법:
        from src.rent_income_client import diagnose_api
        result = diagnose_api("11680", "202501")  # 강남구, 2025년 1월

    Returns:
        {"trade": {status, count, error}, "rent": {status, count, error}}
    """
    import urllib.parse

    results = {}
    for name, url in [("trade", _APT_TRADE_URL), ("rent", _APT_RENT_URL)]:
        params = {
            "serviceKey": DATA_GO_KR_API_KEY,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "pageNo": "1",
            "numOfRows": "10",
            "_type": "json",
        }
        try:
            resp = requests.get(url, params=params, timeout=30)
            # 실제 요청 URL 로깅 (디버깅용)
            actual_url = resp.url
            log.info(f"[진단:{name}] URL: {actual_url[:150]}...")
            log.info(f"[진단:{name}] HTTP {resp.status_code}")
            log.info(f"[진단:{name}] 응답 앞부분: {resp.text[:300]}")

            try:
                data = resp.json()
                header = data.get("response", {}).get("header", {})
                body = data.get("response", {}).get("body", {})
                total = body.get("totalCount", 0)
                results[name] = {
                    "status": header.get("resultCode"),
                    "msg": header.get("resultMsg"),
                    "count": total,
                    "error": None,
                }
            except ValueError:
                results[name] = {
                    "status": "XML",
                    "msg": resp.text[:200],
                    "count": 0,
                    "error": "JSON 파싱 실패 (XML 응답)",
                }
        except Exception as e:
            results[name] = {
                "status": "FAIL",
                "msg": str(e),
                "count": 0,
                "error": str(e),
            }

    return results


def _get_recent_months(n: int = 6) -> list[str]:
    """최근 n개월 YYYYMM 리스트 (실거래가 2~3개월 지연 감안하여 3개월 전부터)."""
    now = datetime.now()
    # 실거래가 데이터는 보통 2~3개월 지연
    start = now - timedelta(days=90)
    months = []
    for i in range(n):
        dt = start - timedelta(days=30 * i)
        ym = dt.strftime("%Y%m")
        if ym not in months:
            months.append(ym)
    return months[:n]


# ─────────────────────────────────────────────────────────
# 소득 proxy: 아파트매매 실거래가
# ─────────────────────────────────────────────────────────

def get_income_data(
    boundary_gdf: gpd.GeoDataFrame,
    region: str = "",
) -> gpd.GeoDataFrame | None:
    """
    아파트매매 실거래가 → 읍면동별 평균 매매가(만원/㎡) 수집.
    높은 매매가 = 높은 소득수준 지역 proxy.

    Returns:
        GeoDataFrame with columns: dong, avg_price, geometry (Point, EPSG:4326)
        또는 데이터 없으면 None
    """
    if not is_data_api_available():
        log.info("공공데이터포털 API 키 미설정 → 소득 데이터 건너뜀")
        return None

    lawd_cd = _get_lawd_code(boundary_gdf)
    if not lawd_cd:
        return None

    region_name = _get_region_name(boundary_gdf) or region
    # "경기도 고양시 일산동구" → "고양시 일산동구" (시군구 부분)
    region_parts = region_name.split()
    region_prefix = " ".join(region_parts[:3]) if len(region_parts) >= 3 else region_name

    log.info(f"소득 proxy 수집 시작: LAWD_CD={lawd_cd}, 지역={region_prefix}")

    # 최근 6개월 데이터 수집
    all_items = []
    for ym in _get_recent_months(6):
        items = _call_data_api(_APT_TRADE_URL, lawd_cd, ym)
        all_items.extend(items)
        time.sleep(0.1)

    if not all_items:
        log.warning("아파트매매 실거래 데이터 없음")
        return None

    log.info(f"아파트매매 실거래 {len(all_items)}건 수집")

    # 파싱: 읍면동별 평균 매매가(만원/㎡)
    records = []
    for item in all_items:
        dong = (item.get("umdNm") or item.get("dong") or
                item.get("법정동") or "").strip()
        price_str = str(item.get("dealAmount") or item.get("거래금액") or "0")
        area_str = str(item.get("excluUseAr") or item.get("전용면적") or "0")

        try:
            price = int(price_str.replace(",", "").strip())
            area = float(area_str.strip())
            if dong and price > 0 and area > 0:
                records.append({"dong": dong, "price_per_m2": price / area})
        except (ValueError, TypeError):
            continue

    if not records:
        return None

    df = pd.DataFrame(records)
    dong_avg = df.groupby("dong")["price_per_m2"].mean().reset_index()
    dong_avg.columns = ["dong", "avg_price"]

    # 지오코딩
    rows = []
    for _, r in dong_avg.iterrows():
        coords = _geocode_dong(region_prefix, r["dong"])
        if coords:
            rows.append({
                "dong": r["dong"],
                "avg_price": r["avg_price"],
                "geometry": Point(coords[1], coords[0]),  # (lng, lat)
            })
        time.sleep(0.05)

    if not rows:
        return None

    gdf = gpd.GeoDataFrame(rows, crs=CRS_WGS84)

    # 경계 내 포인트만 유지
    boundary_union = boundary_gdf.to_crs(CRS_WGS84).unary_union
    gdf = gdf[gdf.geometry.within(boundary_union.buffer(0.01))].copy()

    log.info(
        f"소득 proxy 수집 완료: {len(gdf)}개 동, "
        f"평균 매매가={gdf['avg_price'].mean():.0f}만원/㎡"
    )
    return gdf


# ─────────────────────────────────────────────────────────
# 월세: 아파트 전월세 실거래가 (상가 데이터 미제공 시 proxy)
# ─────────────────────────────────────────────────────────

def get_rent_data(
    boundary_gdf: gpd.GeoDataFrame,
    region: str = "",
) -> gpd.GeoDataFrame | None:
    """
    아파트 전월세 실거래 → 읍면동별 평균 월세(만원) 수집.
    지역 임대료 수준 지표로 활용.

    Returns:
        GeoDataFrame with columns: dong, monthly_rent, deposit, geometry (Point, EPSG:4326)
        또는 데이터 없으면 None
    """
    if not is_data_api_available():
        log.info("공공데이터포털 API 키 미설정 → 월세 데이터 건너뜀")
        return None

    lawd_cd = _get_lawd_code(boundary_gdf)
    if not lawd_cd:
        return None

    region_name = _get_region_name(boundary_gdf) or region
    region_parts = region_name.split()
    region_prefix = " ".join(region_parts[:3]) if len(region_parts) >= 3 else region_name

    log.info(f"월세 데이터 수집 시작: LAWD_CD={lawd_cd}, 지역={region_prefix}")

    # 최근 6개월 수집
    all_items = []
    for ym in _get_recent_months(6):
        items = _call_data_api(_APT_RENT_URL, lawd_cd, ym)
        all_items.extend(items)
        time.sleep(0.1)

    if not all_items:
        log.warning("아파트 전월세 실거래 데이터 없음")
        return None

    log.info(f"아파트 전월세 실거래 {len(all_items)}건 수집")

    # 파싱: 읍면동별 평균 월세·보증금
    records = []
    for item in all_items:
        dong = (item.get("umdNm") or item.get("dong") or
                item.get("법정동") or "").strip()
        rent_str = str(item.get("monthlyRent") or item.get("월세금액") or
                       item.get("monthlyAmount") or "0")
        deposit_str = str(item.get("deposit") or item.get("보증금액") or "0")

        try:
            rent = int(rent_str.replace(",", "").strip())
            deposit = int(deposit_str.replace(",", "").strip())
            # 월세가 0인 전세 거래는 제외 (월세 수준 분석이므로)
            if dong and rent > 0:
                records.append({
                    "dong": dong,
                    "monthly_rent": rent,
                    "deposit": deposit,
                })
        except (ValueError, TypeError):
            continue

    if not records:
        # 전세만 있는 경우: 보증금 기반으로 월세 환산 (연 5% 기준)
        log.info("월세 거래 없음 → 전세 보증금으로 월세 환산 시도")
        for item in all_items:
            dong = (item.get("umdNm") or item.get("dong") or
                    item.get("법정동") or "").strip()
            deposit_str = str(item.get("deposit") or item.get("보증금액") or "0")
            try:
                deposit = int(deposit_str.replace(",", "").strip())
                if dong and deposit > 0:
                    # Why: 전세보증금의 연 5%를 12로 나누면 월세 환산액
                    estimated_rent = int(deposit * 0.05 / 12)
                    records.append({
                        "dong": dong,
                        "monthly_rent": estimated_rent,
                        "deposit": deposit,
                    })
            except (ValueError, TypeError):
                continue

    if not records:
        return None

    df = pd.DataFrame(records)
    dong_avg = df.groupby("dong").agg(
        monthly_rent=("monthly_rent", "mean"),
        deposit=("deposit", "mean"),
    ).reset_index()

    # 지오코딩
    rows = []
    for _, r in dong_avg.iterrows():
        coords = _geocode_dong(region_prefix, r["dong"])
        if coords:
            rows.append({
                "dong": r["dong"],
                "monthly_rent": r["monthly_rent"],
                "deposit": r["deposit"],
                "geometry": Point(coords[1], coords[0]),
            })
        time.sleep(0.05)

    if not rows:
        return None

    gdf = gpd.GeoDataFrame(rows, crs=CRS_WGS84)

    # 경계 내 포인트만 유지
    boundary_union = boundary_gdf.to_crs(CRS_WGS84).unary_union
    gdf = gdf[gdf.geometry.within(boundary_union.buffer(0.01))].copy()

    log.info(
        f"월세 수집 완료: {len(gdf)}개 동, "
        f"평균 월세={gdf['monthly_rent'].mean():.0f}만원, "
        f"평균 보증금={gdf['deposit'].mean():.0f}만원"
    )
    return gdf


# ─────────────────────────────────────────────────────────
# 격자 집계 유틸 (nearest neighbor)
# ─────────────────────────────────────────────────────────

def assign_nearest_to_grid(
    grid_gdf: gpd.GeoDataFrame,
    point_gdf: gpd.GeoDataFrame,
    value_col: str,
    out_col: str,
    max_distance_m: int = 3000,
) -> gpd.GeoDataFrame:
    """
    포인트 데이터를 가장 가까운 격자 셀에 할당 (nearest neighbor).
    읍면동 단위 데이터를 격자에 매핑할 때 사용.

    일반 sjoin은 격자 내부의 포인트만 매칭하므로,
    동 중심점 1개만 있는 경우 대부분의 격자가 비어버림.
    sjoin_nearest로 최근접 동의 값을 모든 격자에 할당.

    Args:
        grid_gdf:       격자 GeoDataFrame (EPSG:4326)
        point_gdf:      동별 집계 포인트 GeoDataFrame (EPSG:4326)
        value_col:      할당할 값 컬럼명
        out_col:        결과 컬럼명
        max_distance_m: 최대 매칭 거리 (미터)

    Returns:
        out_col 컬럼이 추가된 grid_gdf
    """
    grid_out = grid_gdf.copy()

    if point_gdf is None or len(point_gdf) == 0:
        grid_out[out_col] = 0.0
        return grid_out

    # EPSG:5179 (미터 단위)에서 거리 기반 매칭
    grid_5179 = grid_out.to_crs(CRS_KOREA).copy()
    grid_5179["_centroid"] = grid_5179.geometry.centroid
    grid_centers = grid_5179.set_geometry("_centroid")[["_centroid"]].rename_geometry("geometry")

    pts_5179 = point_gdf.to_crs(CRS_KOREA)

    try:
        joined = gpd.sjoin_nearest(
            grid_centers,
            pts_5179[[value_col, "geometry"]],
            how="left",
            max_distance=max_distance_m,
        )
        grid_out[out_col] = joined[value_col].values
    except Exception as e:
        # sjoin_nearest 미지원 시 수동 최근접 매칭
        log.debug(f"sjoin_nearest 실패, 수동 매칭: {e}")
        from shapely.ops import nearest_points
        pts_union = pts_5179.geometry.unary_union
        values = []
        for _, row in grid_5179.iterrows():
            center = row.geometry.centroid
            nearest = nearest_points(center, pts_union)[1]
            dist = center.distance(nearest)
            if dist <= max_distance_m:
                idx = pts_5179.geometry.distance(nearest).idxmin()
                values.append(pts_5179.loc[idx, value_col])
            else:
                values.append(0.0)
        grid_out[out_col] = values

    grid_out[out_col] = grid_out[out_col].fillna(0.0)

    nonzero = (grid_out[out_col] > 0).sum()
    log.info(f"격자 할당 완료: '{out_col}' — {nonzero}/{len(grid_out)}셀 매칭")
    return grid_out
