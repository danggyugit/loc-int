# scripts/collect_sgis_national.py — SGIS API 전국 사전수집
#
# 17개 시·도 × 시·군·구(약 250개) × 읍면동(약 3,500개)의 인구·종사자·
# 사업체·연령통계를 수집하여 data/national/{sido_code}_*.parquet에 저장.
#
# 실행:
#   $env:SGIS_CONSUMER_KEY = "..."; $env:SGIS_CONSUMER_SECRET = "..."
#   python scripts/collect_sgis_national.py
#   python scripts/collect_sgis_national.py --sido 11   # 특정 시·도만
#   python scripts/collect_sgis_national.py --resume    # 누락된 시·도만
#
# Idempotent: 이미 존재하는 시·도 parquet은 --resume 시 건너뜀.

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import urllib3
import geopandas as gpd
import pandas as pd
from shapely import wkt as shapely_wkt
from shapely.geometry import shape

# 프로젝트 루트
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import CRS_WGS84, CRS_KOREA, SGIS_BASE_URL  # noqa: E402

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = ROOT / "data" / "national"
MANIFEST_PATH = DATA_DIR / "manifest.json"
YEAR = "2022"  # SGIS 가용 최신 연도

# SGIS가 채우는 컬럼 → manifest의 metric 키와 1:1
SGIS_METRICS = ["population", "workplace", "corp_cnt", "avg_age", "juv_suprt_per", "oldage_suprt_per"]


# ─────────────────────────────────────────────────────────
# SGIS API 호출
# ─────────────────────────────────────────────────────────

def get_token() -> str:
    key = os.environ.get("SGIS_CONSUMER_KEY", "").strip()
    secret = os.environ.get("SGIS_CONSUMER_SECRET", "").strip()
    if not key or not secret:
        sys.exit("❌ SGIS_CONSUMER_KEY / SGIS_CONSUMER_SECRET 환경변수가 필요합니다.")
    resp = requests.get(
        f"{SGIS_BASE_URL}/auth/authentication.json",
        params={"consumer_key": key, "consumer_secret": secret},
        timeout=15, verify=False,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("errCd") != 0:
        sys.exit(f"❌ SGIS 인증 실패: {data.get('errMsg', '')}")
    return data["result"]["accessToken"]


def api(token: str, endpoint: str, params: dict, retry: int = 3) -> dict:
    """공통 호출 + 간단 재시도."""
    params = {**params, "accessToken": token}
    last_err = None
    for attempt in range(retry):
        try:
            resp = requests.get(
                f"{SGIS_BASE_URL}{endpoint}",
                params=params, timeout=30, verify=False,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt < retry - 1:
                time.sleep(1 + attempt)
    raise RuntimeError(f"SGIS API 실패 ({endpoint}): {last_err}")


def safe_int(v) -> int:
    if v in (None, "", "-", "N/A"):
        return 0
    try:
        return int(float(v))
    except Exception:
        return 0


def safe_float(v) -> float:
    if v in (None, "", "-", "N/A"):
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def parse_geometry(g):
    """SGIS는 WKT 또는 GeoJSON-like dict로 주는 경우가 있어 둘 다 처리."""
    if not g:
        return None
    if isinstance(g, str):
        try:
            return shapely_wkt.loads(g)
        except Exception:
            return None
    if isinstance(g, dict):
        try:
            return shape(g)
        except Exception:
            return None
    return None


# ─────────────────────────────────────────────────────────
# 행정구역 트리 + 통계 수집
# ─────────────────────────────────────────────────────────

def list_sido(token: str) -> list[dict]:
    """전국 시·도 목록."""
    data = api(token, "/boundary/hadmarea.json", {"year": YEAR})
    return data.get("result", []) or []


def list_children(token: str, parent_adm_cd: str) -> list[dict]:
    """parent의 직계 하위 행정구역 (시·도→시·군·구, 시·군·구→읍면동)."""
    data = api(token, "/boundary/hadmarea.json",
               {"year": YEAR, "adm_cd": parent_adm_cd})
    return data.get("result", []) or []


def get_dong_stats(token: str, adm_cd: str) -> dict:
    """단일 읍면동(또는 시·군·구) 인구·사업체·연령 통계."""
    out = {k: 0 for k in ["population", "workplace", "corp_cnt"]}
    out.update({k: 0.0 for k in ["avg_age", "juv_suprt_per", "oldage_suprt_per"]})

    # 인구통계 (population.json)
    try:
        pop = api(token, "/stats/population.json",
                  {"year": YEAR, "adm_cd": adm_cd})
        rows = pop.get("result", [])
        if isinstance(rows, list) and rows:
            r = rows[0]
            out["population"]      = safe_int(r.get("tot_ppltn"))
            out["workplace"]       = safe_int(r.get("employee_cnt"))
            out["avg_age"]         = safe_float(r.get("avg_age"))
            out["juv_suprt_per"]   = safe_float(r.get("juv_suprt_per"))
            out["oldage_suprt_per"] = safe_float(r.get("oldage_suprt_per"))
    except Exception as e:
        log.debug(f"population.json 실패 ({adm_cd}): {e}")

    # 사업체 통계 (company.json)
    try:
        comp = api(token, "/stats/company.json",
                   {"year": YEAR, "adm_cd": adm_cd})
        rows = comp.get("result", [])
        if isinstance(rows, list) and rows:
            r = rows[0]
            out["corp_cnt"] = safe_int(r.get("corp_cnt"))
            # company API의 종사자수가 더 정확한 경우 덮어씀
            tot_worker = safe_int(r.get("tot_worker"))
            if tot_worker > 0:
                out["workplace"] = tot_worker
    except Exception as e:
        log.debug(f"company.json 실패 ({adm_cd}): {e}")

    return out


# ─────────────────────────────────────────────────────────
# 시·도 단위 처리
# ─────────────────────────────────────────────────────────

def collect_one_sido(token: str, sido: dict) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """한 개 시·도의 시·군·구 + 읍면동 데이터 수집."""
    sido_cd = sido["adm_cd"]
    sido_nm = sido["adm_nm"]
    log.info(f"=== {sido_nm} ({sido_cd}) 시·군·구 목록 조회 ===")

    sgg_list = list_children(token, sido_cd)
    log.info(f"  시·군·구 {len(sgg_list)}개")

    sgg_rows = []
    dong_rows = []

    for i, sgg in enumerate(sgg_list, 1):
        sgg_cd = sgg["adm_cd"]
        sgg_nm = sgg["adm_nm"]
        log.info(f"  [{i}/{len(sgg_list)}] {sgg_nm} ({sgg_cd})")

        # 시·군·구 자체 통계
        sgg_stats = get_dong_stats(token, sgg_cd)
        sgg_geom = parse_geometry(sgg.get("geometry"))
        sgg_rows.append({
            "adm_cd":   sgg_cd,
            "adm_nm":   sgg_nm,
            "level":    "sigungu",
            "sido_cd":  sido_cd,
            "geometry": sgg_geom,
            **sgg_stats,
        })

        # 읍면동
        try:
            dongs = list_children(token, sgg_cd)
        except Exception as e:
            log.warning(f"    읍면동 목록 실패: {e}")
            continue

        for dong in dongs:
            dong_cd = dong["adm_cd"]
            dong_nm = dong["adm_nm"]
            stats = get_dong_stats(token, dong_cd)
            dong_geom = parse_geometry(dong.get("geometry"))
            dong_rows.append({
                "adm_cd":     dong_cd,
                "adm_nm":     dong_nm,
                "level":      "eupmyeondong",
                "sido_cd":    sido_cd,
                "sgg_cd":     sgg_cd,
                "geometry":   dong_geom,
                **stats,
            })

        log.info(f"    읍면동 {len(dongs)}개 수집 완료 (누적 {len(dong_rows)}개)")

    # GeoDataFrame 생성 (UTMK → WGS84)
    sgg_gdf = gpd.GeoDataFrame(sgg_rows, geometry="geometry", crs=CRS_KOREA)
    sgg_gdf = sgg_gdf.dropna(subset=["geometry"])
    sgg_gdf = sgg_gdf.to_crs(CRS_WGS84) if len(sgg_gdf) > 0 else sgg_gdf

    dong_gdf = gpd.GeoDataFrame(dong_rows, geometry="geometry", crs=CRS_KOREA)
    dong_gdf = dong_gdf.dropna(subset=["geometry"])
    dong_gdf = dong_gdf.to_crs(CRS_WGS84) if len(dong_gdf) > 0 else dong_gdf

    return sgg_gdf, dong_gdf


def save_sido(sido_cd: str, sgg_gdf: gpd.GeoDataFrame, dong_gdf: gpd.GeoDataFrame) -> None:
    """parquet 저장."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    sgg_path = DATA_DIR / f"{sido_cd}_sigungu.parquet"
    dong_path = DATA_DIR / f"{sido_cd}_eupmyeondong.parquet"
    sgg_gdf.to_parquet(sgg_path, index=False)
    dong_gdf.to_parquet(dong_path, index=False)
    log.info(f"  저장: {sgg_path.name} ({len(sgg_gdf)}건), {dong_path.name} ({len(dong_gdf)}건)")


# ─────────────────────────────────────────────────────────
# manifest.json 갱신
# ─────────────────────────────────────────────────────────

def update_manifest(completed_sido_codes: list[str]) -> None:
    """수집 완료된 시·도 코드를 manifest의 각 SGIS 메트릭 available 배열에 등록."""
    if not MANIFEST_PATH.exists():
        log.warning("manifest.json 없음 — 갱신 건너뜀")
        return
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)
    for metric in SGIS_METRICS:
        if metric in manifest.get("metrics", {}):
            existing = set(manifest["metrics"][metric].get("available", []))
            existing.update(completed_sido_codes)
            manifest["metrics"][metric]["available"] = sorted(existing)
    manifest["last_updated"] = datetime.now().isoformat(timespec="seconds")
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info(f"manifest.json 갱신: SGIS 메트릭 {len(SGIS_METRICS)}개에 시·도 {len(completed_sido_codes)}개 등록")


# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="SGIS 전국 사전수집")
    ap.add_argument("--sido", help="특정 시·도 코드만 수집 (예: 11). 미지정 시 전체.")
    ap.add_argument("--resume", action="store_true",
                    help="이미 parquet이 있는 시·도는 건너뜀")
    args = ap.parse_args()

    log.info("SGIS 인증 토큰 발급 중...")
    token = get_token()

    log.info("전국 시·도 목록 조회 중...")
    sidos = list_sido(token)
    log.info(f"  {len(sidos)}개 시·도 발견")

    if args.sido:
        sidos = [s for s in sidos if s["adm_cd"] == args.sido]
        if not sidos:
            sys.exit(f"❌ 시·도 코드 {args.sido} 없음")

    completed_now = []
    skipped = []
    for sido in sidos:
        sido_cd = sido["adm_cd"]
        sgg_path = DATA_DIR / f"{sido_cd}_sigungu.parquet"
        dong_path = DATA_DIR / f"{sido_cd}_eupmyeondong.parquet"

        if args.resume and sgg_path.exists() and dong_path.exists():
            log.info(f"⏭  {sido['adm_nm']} ({sido_cd}) — 이미 존재 (--resume)")
            skipped.append(sido_cd)
            continue

        try:
            sgg_gdf, dong_gdf = collect_one_sido(token, sido)
            save_sido(sido_cd, sgg_gdf, dong_gdf)
            completed_now.append(sido_cd)
        except KeyboardInterrupt:
            log.warning("⚠ 사용자 중단 — 지금까지 완료된 시·도만 manifest 업데이트")
            break
        except Exception as e:
            log.error(f"❌ {sido['adm_nm']} 수집 실패: {e}")
            continue

    # manifest 업데이트는 새로 완료된 것 + 기존 skip된 것 모두 포함
    all_done = completed_now + skipped
    if all_done:
        update_manifest(all_done)

    log.info(f"=== 완료 === 신규 {len(completed_now)}, 건너뜀 {len(skipped)}")


if __name__ == "__main__":
    main()
