# src/sgis_client.py — 통계청 SGIS API 클라이언트 (v2)
#
# SGIS 제공 데이터:
#   - 읍면동별 인구(tot_ppltn), 종사자(employee_cnt), 사업체(corp_cnt)
#   - 읍면동 경계 폴리곤 (UTMK/EPSG:5179 좌표)
#
# 데이터 배분 방식:
#   읍면동 폴리곤 내부에 200m 간격 샘플 포인트 생성 →
#   인구/종사자를 균등 배분 → aggregate_to_grid()에서 격자 집계
#
# 사전 준비:
#   1. https://sgis.kostat.go.kr/developer/ 회원가입
#   2. 서비스 키 발급 (서비스ID, 보안KEY)
#   3. 환경변수:
#      $env:SGIS_CONSUMER_KEY = "서비스ID"
#      $env:SGIS_CONSUMER_SECRET = "보안KEY"

import logging
import requests
import urllib3
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkt as shapely_wkt

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CRS_WGS84, CRS_KOREA,
    SGIS_BASE_URL,
)
from src import session_keys

log = logging.getLogger(__name__)

# SSL 인증서 문제 대응
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 읍면동 내부 샘플 포인트 간격 (미터, UTMK 기준)
_SAMPLE_STEP = 200


# ─────────────────────────────────────────────────────────
# 인증
# ─────────────────────────────────────────────────────────

def _get_sgis_keys() -> tuple[str, str]:
    """세션별 격리된 SGIS API 키 조회 (process os.environ 오염 방지)."""
    return (
        session_keys.get("SGIS_CONSUMER_KEY"),
        session_keys.get("SGIS_CONSUMER_SECRET"),
    )


def _get_access_token() -> str:
    """SGIS API 인증 토큰 발급."""
    consumer_key, consumer_secret = _get_sgis_keys()
    resp = requests.get(
        f"{SGIS_BASE_URL}/auth/authentication.json",
        params={"consumer_key": consumer_key,
                "consumer_secret": consumer_secret},
        timeout=15, verify=False,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errCd") != 0:
        raise RuntimeError(f"SGIS 인증 실패: {data.get('errMsg', '')}")
    token = data["result"]["accessToken"]
    log.info("SGIS 인증 토큰 발급 완료")
    return token


def _api(token: str, endpoint: str, params: dict) -> dict:
    """SGIS API 공통 호출."""
    params["accessToken"] = token
    resp = requests.get(
        f"{SGIS_BASE_URL}{endpoint}",
        params=params, timeout=30, verify=False,
    )
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────────────────
# 행정코드 탐색: 지역명 → SGIS adm_cd
# ─────────────────────────────────────────────────────────

def _find_sgis_adm_cd(token: str, region: str) -> dict | None:
    """
    사용자 입력 지역명에서 SGIS 행정코드를 찾는다.

    '강남구' → 시도 순회 → 시군구 목록에서 이름 매칭
    '서울특별시 강남구' → 시도 이름 먼저 매칭 후 시군구 탐색

    Returns:
        {"adm_cd": "11230", "adm_nm": "서울특별시 강남구", "x": ..., "y": ...}
        또는 None
    """
    # 시도 목록
    sido_data = _api(token, "/boundary/hadmarea.json", {"year": "2022"})
    sidos = sido_data.get("result", [])
    if not isinstance(sidos, list):
        return None

    # 시도 이름으로 범위 축소 시도
    target_sidos = sidos
    for sido in sidos:
        sido_nm = sido.get("adm_nm", "")
        if sido_nm and sido_nm in region:
            target_sidos = [sido]
            break

    # 각 시도의 시군구를 순회하며 매칭
    for sido in target_sidos:
        sido_cd = sido.get("adm_cd")
        sgg_data = _api(token, "/boundary/hadmarea.json",
                        {"year": "2022", "adm_cd": sido_cd})
        sggs = sgg_data.get("result", [])
        if not isinstance(sggs, list):
            continue

        for sgg in sggs:
            sgg_nm = sgg.get("adm_nm", "")
            # "일산동구" in "경기도 고양시 일산동구" → True
            if region in sgg_nm or sgg_nm.endswith(region):
                log.info(f"SGIS 행정코드 매칭: '{region}' → {sgg.get('adm_cd')} ({sgg_nm})")
                return sgg

    log.warning(f"SGIS 행정코드 매칭 실패: '{region}'")
    return None


# ─────────────────────────────────────────────────────────
# 읍면동 데이터 수집: 경계 + 인구 + 사업체
# ─────────────────────────────────────────────────────────

def _get_dong_stats(token: str, adm_cd: str) -> list[dict]:
    """
    시군구 코드로 하위 읍면동의 경계·인구·사업체 데이터를 수집.

    Returns:
        [{"adm_cd", "adm_nm", "x", "y", "geometry",
          "population", "workplace"}, ...]
    """
    # 읍면동 경계 + 중심좌표
    dong_boundary = _api(token, "/boundary/hadmarea.json",
                         {"year": "2022", "adm_cd": adm_cd})
    dongs = dong_boundary.get("result", [])
    if not isinstance(dongs, list) or not dongs:
        log.warning(f"읍면동 경계 조회 실패: adm_cd={adm_cd}")
        return []

    # 읍면동별 인구
    pop_map = {}
    for dong in dongs:
        dong_cd = dong.get("adm_cd")
        pop_data = _api(token, "/stats/population.json",
                        {"year": "2022", "adm_cd": dong_cd})
        pop_result = pop_data.get("result", [])
        if isinstance(pop_result, list) and pop_result:
            r = pop_result[0]
            pop_map[dong_cd] = {
                "population": _safe_int(r.get("tot_ppltn", 0)),
                "workplace":  _safe_int(r.get("employee_cnt", 0)),
                "avg_age":         _safe_float(r.get("avg_age", 0)),
                "juv_suprt_per":   _safe_float(r.get("juv_suprt_per", 0)),
                "oldage_suprt_per": _safe_float(r.get("oldage_suprt_per", 0)),
            }

    # 사업체(company) API도 읍면동별 종사자 제공 → 보완용
    comp_data = _api(token, "/stats/company.json",
                     {"year": "2022", "adm_cd": adm_cd})
    comp_result = comp_data.get("result", [])
    comp_map = {}
    if isinstance(comp_result, list):
        for c in comp_result:
            comp_map[c.get("adm_cd")] = _safe_int(c.get("tot_worker", 0))

    # 결합
    result = []
    for dong in dongs:
        dong_cd = dong.get("adm_cd")
        stats = pop_map.get(dong_cd, {})
        # company API의 종사자 수가 더 세밀한 경우 사용
        workplace = stats.get("workplace", 0)
        if dong_cd in comp_map and comp_map[dong_cd] > 0:
            workplace = comp_map[dong_cd]

        result.append({
            "adm_cd":     dong_cd,
            "adm_nm":     dong.get("adm_nm", ""),
            "x":          float(dong.get("x", 0)),
            "y":          float(dong.get("y", 0)),
            "geometry":   dong.get("geometry", ""),
            "population": stats.get("population", 0),
            "workplace":  workplace,
            "avg_age":         stats.get("avg_age", 0.0),
            "juv_suprt_per":   stats.get("juv_suprt_per", 0.0),
            "oldage_suprt_per": stats.get("oldage_suprt_per", 0.0),
        })

    total_pop = sum(d["population"] for d in result)
    total_work = sum(d["workplace"] for d in result)
    log.info(f"읍면동 {len(result)}개 수집: 인구합={total_pop:,}, 종사자합={total_work:,}")
    return result


# ─────────────────────────────────────────────────────────
# 공간 배분: 읍면동 폴리곤 → 샘플 포인트
# ─────────────────────────────────────────────────────────

def _distribute_to_points(dong_stats: list[dict]) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    각 읍면동 폴리곤 내부에 샘플 포인트를 생성하고
    인구·종사자를 균등 배분한 포인트 GeoDataFrame 반환.
    인구통계 지표(avg_age, juv_suprt_per, oldage_suprt_per)도 함께 전달.

    Returns:
        (population_gdf, workplace_gdf) — 둘 다 EPSG:4326
    """
    pop_rows = []
    work_rows = []

    for dong in dong_stats:
        geom_wkt = dong.get("geometry", "")
        population = dong["population"]
        workplace = dong["workplace"]
        # 인구통계 지표 (읍면동 내 모든 포인트에 동일 값 부여)
        avg_age = dong.get("avg_age", 0.0)
        juv_suprt_per = dong.get("juv_suprt_per", 0.0)
        oldage_suprt_per = dong.get("oldage_suprt_per", 0.0)

        if not geom_wkt or (population == 0 and workplace == 0):
            if population > 0 or workplace > 0:
                pop_rows.append({"x": dong["x"], "y": dong["y"],
                                 "population": population,
                                 "avg_age": avg_age,
                                 "juv_suprt_per": juv_suprt_per,
                                 "oldage_suprt_per": oldage_suprt_per})
                work_rows.append({"x": dong["x"], "y": dong["y"],
                                  "workplace": workplace})
            continue

        try:
            polygon = shapely_wkt.loads(geom_wkt)
        except Exception:
            pop_rows.append({"x": dong["x"], "y": dong["y"],
                             "population": population,
                             "avg_age": avg_age,
                             "juv_suprt_per": juv_suprt_per,
                             "oldage_suprt_per": oldage_suprt_per})
            work_rows.append({"x": dong["x"], "y": dong["y"],
                              "workplace": workplace})
            continue

        # 폴리곤 내부에 격자 포인트 생성 (UTMK 좌표)
        minx, miny, maxx, maxy = polygon.bounds
        xs = np.arange(minx + _SAMPLE_STEP / 2, maxx, _SAMPLE_STEP)
        ys = np.arange(miny + _SAMPLE_STEP / 2, maxy, _SAMPLE_STEP)

        inside_points = []
        for px in xs:
            for py in ys:
                pt = Point(px, py)
                if polygon.contains(pt):
                    inside_points.append((px, py))

        if not inside_points:
            inside_points = [(dong["x"], dong["y"])]

        n = len(inside_points)
        pop_per_pt = population / n
        work_per_pt = workplace / n

        for px, py in inside_points:
            pop_rows.append({"x": px, "y": py, "population": pop_per_pt,
                             "avg_age": avg_age,
                             "juv_suprt_per": juv_suprt_per,
                             "oldage_suprt_per": oldage_suprt_per})
            work_rows.append({"x": px, "y": py, "workplace": work_per_pt})

    if not pop_rows:
        return None, None

    # UTMK(EPSG:5179) → EPSG:4326 변환
    pop_df = pd.DataFrame(pop_rows)
    pop_gdf = gpd.GeoDataFrame(
        pop_df,
        geometry=[Point(r["x"], r["y"]) for _, r in pop_df.iterrows()],
        crs=CRS_KOREA,
    ).to_crs(CRS_WGS84)
    pop_gdf["lat"] = pop_gdf.geometry.y
    pop_gdf["lng"] = pop_gdf.geometry.x

    work_df = pd.DataFrame(work_rows)
    work_gdf = gpd.GeoDataFrame(
        work_df,
        geometry=[Point(r["x"], r["y"]) for _, r in work_df.iterrows()],
        crs=CRS_KOREA,
    ).to_crs(CRS_WGS84)
    work_gdf["lat"] = work_gdf.geometry.y
    work_gdf["lng"] = work_gdf.geometry.x

    log.info(f"샘플 포인트 생성: 인구 {len(pop_gdf)}포인트, 종사자 {len(work_gdf)}포인트")
    return pop_gdf, work_gdf


def _safe_int(val) -> int:
    if val is None or val == "" or val == "-" or val == "N/A":
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def _safe_float(val) -> float:
    if val is None or val == "" or val == "-" or val == "N/A":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────────────────
# 메인 함수
# ─────────────────────────────────────────────────────────

def get_sgis_grid_data(
    boundary_gdf: gpd.GeoDataFrame,
    region: str = "",
    cell_size_m: int = 500,
) -> dict | None:
    """
    통계청 SGIS 읍면동별 인구·종사자 통계 수집 → 격자 배분용 포인트 변환.

    Args:
        boundary_gdf: 분석 영역 경계 (EPSG:4326)
        region:       지역명 (SGIS 행정코드 매칭용)
        cell_size_m:  사용자 격자 셀 크기

    Returns:
        {"population": GeoDataFrame, "workplace": GeoDataFrame}
        또는 실패 시 None
    """
    consumer_key, consumer_secret = _get_sgis_keys()
    if not consumer_key or not consumer_secret:
        log.info("SGIS API 키 미설정 → SGIS 수집 건너뜀")
        return None

    try:
        token = _get_access_token()
    except Exception as e:
        log.warning(f"SGIS 인증 실패: {e}")
        return None

    # 지역명으로 SGIS 행정코드 검색
    matched = _find_sgis_adm_cd(token, region)
    if matched is None:
        log.warning(f"SGIS 행정코드 매칭 실패: '{region}'")
        return None

    adm_cd = matched["adm_cd"]
    log.info(f"SGIS 데이터 수집: {matched.get('adm_nm', '')} (adm_cd={adm_cd})")

    # 읍면동 통계 수집
    dong_stats = _get_dong_stats(token, adm_cd)
    if not dong_stats:
        return None

    # 폴리곤 내 샘플 포인트로 배분
    pop_gdf, work_gdf = _distribute_to_points(dong_stats)
    if pop_gdf is None:
        return None

    # 경계 내 포인트만 유지
    boundary_union = boundary_gdf.unary_union
    pop_gdf = pop_gdf[pop_gdf.geometry.within(boundary_union)].copy()
    work_gdf = work_gdf[work_gdf.geometry.within(boundary_union)].copy()

    log.info(
        f"SGIS 수집 완료: "
        f"인구 포인트={len(pop_gdf)} (합계={pop_gdf['population'].sum():,.0f}) | "
        f"종사자 포인트={len(work_gdf)} (합계={work_gdf['workplace'].sum():,.0f})"
    )

    return {
        "population": pop_gdf[["lat", "lng", "population", "geometry"]].copy(),
        "workplace":  work_gdf[["lat", "lng", "workplace", "geometry"]].copy(),
    }


def is_sgis_available() -> bool:
    """SGIS API 키가 설정되어 있는지 확인 (런타임 체크)."""
    consumer_key, consumer_secret = _get_sgis_keys()
    return bool(consumer_key and consumer_secret)
