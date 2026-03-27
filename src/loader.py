# src/loader.py — 데이터 로드 및 전처리 (DA Agent)
# 모든 함수는 EPSG:4326 기준 GeoDataFrame 반환

import time
import logging
import requests
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CRS_WGS84, SIDO_CODE,
    KAKAO_API_KEY, KAKAO_LOCAL_URL,
    KAKAO_MAX_PAGE, KAKAO_PAGE_SIZE, KAKAO_CATEGORY,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────────────────

def _filter_region(gdf: gpd.GeoDataFrame, region: str | None, code_col: str = "adm_code") -> gpd.GeoDataFrame:
    """시·도 코드 앞 2자리로 지역 필터링. region=None 이면 전국 반환."""
    if region is None:
        return gdf
    code = SIDO_CODE.get(region)
    if code is None:
        raise ValueError(f"알 수 없는 시·도명: '{region}'. config.py의 SIDO_CODE 참조.")
    mask = gdf[code_col].astype(str).str.startswith(code)
    result = gdf[mask].copy()
    log.info(f"지역 필터 '{region}': {len(gdf)} → {len(result)}행")
    return result


def _drop_invalid_coords(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """결측·무효 좌표 행 제거 후 경고 로그 출력."""
    before = len(gdf)
    gdf = gdf[gdf.geometry.notnull() & gdf.geometry.is_valid].copy()
    dropped = before - len(gdf)
    if dropped > 0:
        log.warning(f"WARNING: {dropped}행 제거됨 (결측 또는 무효 좌표)")
    return gdf


def _to_geodataframe(df: pd.DataFrame, lat_col: str, lng_col: str) -> gpd.GeoDataFrame:
    """위경도 컬럼을 가진 DataFrame → GeoDataFrame (EPSG:4326)."""
    df = df.dropna(subset=[lat_col, lng_col]).copy()
    geometry = [Point(lng, lat) for lng, lat in zip(df[lng_col], df[lat_col])]
    return gpd.GeoDataFrame(df, geometry=geometry, crs=CRS_WGS84)


# ─────────────────────────────────────────────────────────
# 인구 데이터 (통계청 KOSIS CSV)
# ─────────────────────────────────────────────────────────

def load_population(path: str, region: str | None = None) -> gpd.GeoDataFrame:
    """
    통계청 행정동별 인구 CSV 로드.

    기대 컬럼: adm_code, adm_name, population, lat, lng
    (또는 geometry 컬럼이 있는 GeoJSON/SHP)
    """
    path = Path(path)
    log.info(f"인구 데이터 로드: {path}")

    suffix = path.suffix.lower()
    if suffix in (".shp", ".geojson", ".json"):
        gdf = gpd.read_file(path)
        if gdf.crs is None:
            gdf = gdf.set_crs(CRS_WGS84)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(CRS_WGS84)
    elif suffix == ".csv":
        df = pd.read_csv(path, dtype={"adm_code": str})
        gdf = _to_geodataframe(df, lat_col="lat", lng_col="lng")
    else:
        raise ValueError(f"지원하지 않는 파일 형식: {suffix}")

    gdf = _drop_invalid_coords(gdf)

    if region:
        gdf = _filter_region(gdf, region)

    log.info(f"인구 데이터 로드 완료: {len(gdf)}행")
    return gdf[["adm_code", "adm_name", "population", "geometry"]]


# ─────────────────────────────────────────────────────────
# 유동인구 데이터 (공공데이터포털 CSV)
# ─────────────────────────────────────────────────────────

def load_floating(path: str, region: str | None = None) -> gpd.GeoDataFrame:
    """
    유동인구 CSV 로드.

    기대 컬럼: adm_code, adm_name, floating, lat, lng
    """
    path = Path(path)
    log.info(f"유동인구 데이터 로드: {path}")

    df = pd.read_csv(path, dtype={"adm_code": str})
    gdf = _to_geodataframe(df, lat_col="lat", lng_col="lng")
    gdf = _drop_invalid_coords(gdf)

    if region:
        gdf = _filter_region(gdf, region)

    log.info(f"유동인구 데이터 로드 완료: {len(gdf)}행")
    return gdf[["adm_code", "adm_name", "floating", "geometry"]]


# ─────────────────────────────────────────────────────────
# 경쟁업체 데이터 (카카오 API 우선, CSV 폴백)
# ─────────────────────────────────────────────────────────

def load_competitor(
    category: str,
    region: str | None = None,
    csv_path: str | None = None,
) -> gpd.GeoDataFrame:
    """
    경쟁업체 위치 데이터 로드.

    카카오 API 키 존재 시 → 카카오 로컬 API 사용
    없으면 → csv_path CSV 폴백
    """
    if KAKAO_API_KEY:
        log.info(f"카카오 API로 경쟁업체 로드: category={category}, region={region}")
        return _load_from_kakao(category, region)
    else:
        log.warning("KAKAO_API_KEY 없음 → CSV 폴백")
        if csv_path is None:
            raise ValueError("API 키가 없으면 csv_path를 지정해야 합니다.")
        return _load_competitor_csv(csv_path, category, region)


def _load_from_kakao(category: str, region: str | None) -> gpd.GeoDataFrame:
    """카카오 로컬 키워드 검색 API로 업체 목록 수집."""
    cat_code = KAKAO_CATEGORY.get(category)
    if cat_code is None:
        raise ValueError(f"알 수 없는 업종: '{category}'. 허용값: {list(KAKAO_CATEGORY.keys())}")

    # 전국 검색: 시·도별로 순회하여 수집
    # region 지정 시: 해당 시·도 이름을 쿼리에 포함
    regions_to_search = [region] if region else list(SIDO_CODE.keys())

    records = []
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}

    for sido in regions_to_search:
        log.info(f"  카카오 검색: {sido} / {category}")
        for page in range(1, KAKAO_MAX_PAGE + 1):
            params = {
                "query":          sido,           # 지역명으로 검색 범위 한정
                "category_group_code": cat_code,
                "page":           page,
                "size":           KAKAO_PAGE_SIZE,
            }
            try:
                resp = requests.get(KAKAO_LOCAL_URL, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                log.warning(f"카카오 API 오류 ({sido}, page={page}): {e} → 스킵")
                break

            docs = data.get("documents", [])
            for d in docs:
                records.append({
                    "biz_id":   d.get("id", ""),
                    "name":     d.get("place_name", ""),
                    "category": category,
                    "lat":      float(d.get("y", 0)),
                    "lng":      float(d.get("x", 0)),
                    "address":  d.get("address_name", ""),
                })

            # 마지막 페이지 확인
            if data.get("meta", {}).get("is_end", True):
                break

            time.sleep(0.1)  # API 호출 간격 (rate limit 방지)

    if not records:
        log.warning(f"카카오 API 결과 없음: category={category}, region={region}")
        return gpd.GeoDataFrame(columns=["biz_id", "name", "category", "lat", "lng", "address", "geometry"], crs=CRS_WGS84)

    df = pd.DataFrame(records).drop_duplicates(subset=["biz_id"])
    gdf = _to_geodataframe(df, lat_col="lat", lng_col="lng")
    gdf = _drop_invalid_coords(gdf)
    log.info(f"카카오 API 수집 완료: {len(gdf)}건")
    return gdf[["biz_id", "name", "category", "lat", "lng", "address", "geometry"]]


def _load_competitor_csv(path: str, category: str, region: str | None) -> gpd.GeoDataFrame:
    """공공데이터포털 사업자 등록 CSV 로드 (카카오 API 폴백)."""
    df = pd.read_csv(path, dtype={"adm_code": str})
    if "category" in df.columns:
        df = df[df["category"] == category]
    gdf = _to_geodataframe(df, lat_col="lat", lng_col="lng")
    gdf = _drop_invalid_coords(gdf)
    if region:
        gdf = _filter_region(gdf, region)
    log.info(f"경쟁업체 CSV 로드 완료: {len(gdf)}행")
    return gdf


# ─────────────────────────────────────────────────────────
# 교통 인프라 데이터 (지하철·버스 정류장 CSV)
# ─────────────────────────────────────────────────────────

def load_transport(path: str, region: str | None = None) -> gpd.GeoDataFrame:
    """
    교통 인프라 (지하철역 / 버스 정류장) CSV 로드.

    기대 컬럼: stop_id, stop_name, type(subway/bus), lat, lng, adm_code
    """
    path = Path(path)
    log.info(f"교통 데이터 로드: {path}")

    df = pd.read_csv(path, dtype={"adm_code": str, "stop_id": str})
    gdf = _to_geodataframe(df, lat_col="lat", lng_col="lng")
    gdf = _drop_invalid_coords(gdf)

    if region and "adm_code" in gdf.columns:
        gdf = _filter_region(gdf, region)

    log.info(f"교통 데이터 로드 완료: {len(gdf)}행")
    return gdf[["stop_id", "stop_name", "type", "lat", "lng", "geometry"]]


# ─────────────────────────────────────────────────────────
# 행정경계 데이터 (NGII SHP / GeoJSON)
# ─────────────────────────────────────────────────────────

def load_boundary(path: str, region: str | None = None) -> gpd.GeoDataFrame:
    """
    행정경계 GIS 데이터 로드 (국토지리정보원 NGII).

    기대 컬럼: adm_code, adm_name, geometry (Polygon)
    """
    path = Path(path)
    log.info(f"행정경계 데이터 로드: {path}")

    gdf = gpd.read_file(path)

    # 좌표계 표준화
    if gdf.crs is None:
        log.warning("좌표계 미설정 → EPSG:4326 강제 지정")
        gdf = gdf.set_crs(CRS_WGS84)
    elif gdf.crs.to_epsg() != 4326:
        log.info(f"좌표계 변환: {gdf.crs} → {CRS_WGS84}")
        gdf = gdf.to_crs(CRS_WGS84)

    gdf = _drop_invalid_coords(gdf)

    if region and "adm_code" in gdf.columns:
        gdf = _filter_region(gdf, region)

    log.info(f"행정경계 로드 완료: {len(gdf)}행")
    return gdf[["adm_code", "adm_name", "geometry"]]
