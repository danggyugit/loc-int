# scripts/collect_kakao_general.py — 카카오 일반 지표 전국 사전수집
#
# 업종 무관 일반 인프라 지표(지하철역·버스정류장·주차장·전체 업종 다양성)를
# 시·도별 검색 격자로 수집한 후 읍·면·동 폴리곤에 spatial join → 카운트.
#
# 시·군·구·읍·면·동 polygon은 SGIS 수집 결과(parquet)에서 재사용.
# → SGIS 수집(scripts/collect_sgis_national.py) 먼저 실행 필수.
#
# 실행:
#   $env:KAKAO_API_KEY = "..."
#   python scripts/collect_kakao_general.py
#   python scripts/collect_kakao_general.py --sido 11 --metric subway
#   python scripts/collect_kakao_general.py --resume
#
# 카카오 호출량 (시·도별):
#   - 시·도 면적 / 격자(~8km) ≈ 50~300개 검색 격자
#   - × 4 지표 × 5 페이지 = 약 1,000~6,000회/시·도
#   - 전국 합계 약 30,000~50,000회 (일일 쿼터 10만)
#
# Idempotent: 컬럼이 이미 있으면 --force 없이는 건너뜀.

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from config import (  # noqa: E402
    CRS_WGS84, CRS_KOREA,
    KAKAO_LOCAL_URL, KAKAO_CATEGORY_URL,
    KAKAO_PAGE_SIZE, KAKAO_RADIUS_MAX,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = ROOT / "data" / "national"
MANIFEST_PATH = DATA_DIR / "manifest.json"

SEARCH_STEP_M = 8000   # 검색 격자 간격 (지하철·버스 등 sparse 데이터용)


# 지표 정의
# kind == "category" → category_group_code, "keyword" → query
METRICS = {
    "subway_cnt": {
        "label":  "지하철역 수",
        "unit":   "개",
        "cmap":   "Blues",
        "kind":   "category",
        "param":  "SW8",
        "format": "{:,.0f}",
        "step_m": 8000,
    },
    "bus_cnt": {
        "label":  "버스정류장 수",
        "unit":   "개",
        "cmap":   "Purples",
        "kind":   "keyword",
        "param":  "버스정류장",
        "format": "{:,.0f}",
        "step_m": 5000,
    },
    "parking_cnt": {
        "label":  "주차장 수",
        "unit":   "개",
        "cmap":   "Greys",
        "kind":   "keyword",
        "param":  "주차장",
        "format": "{:,.0f}",
        "step_m": 6000,
    },
    "diversity_cnt": {
        "label":  "상권 업종 수",
        "unit":   "개",
        "cmap":   "Reds",
        "kind":   "diversity",
        "param":  ["CE7", "FD6", "HP8", "CS2", "MT1", "PM9"],
        "format": "{:,.0f}",
        "step_m": 8000,
    },
}


# ─────────────────────────────────────────────────────────
# 카카오 호출
# ─────────────────────────────────────────────────────────

def kakao_headers() -> dict:
    key = os.environ.get("KAKAO_API_KEY", "").strip()
    if not key:
        sys.exit("❌ KAKAO_API_KEY 환경변수가 필요합니다.")
    return {"Authorization": f"KakaoAK {key}"}


def search_one_grid(headers: dict, kind: str, param, lng: float, lat: float, radius: int) -> list[dict]:
    """단일 격자 중심에서 페이지네이션 수집 (is_end까지)."""
    out = []
    seen = set()

    if kind == "diversity":
        # 6개 카테고리 묶어 호출
        for code in param:
            out.extend(_search_pages(headers, KAKAO_CATEGORY_URL,
                                      {"category_group_code": code,
                                       "x": lng, "y": lat, "radius": radius},
                                      seen))
        return out

    if kind == "category":
        return _search_pages(headers, KAKAO_CATEGORY_URL,
                             {"category_group_code": param,
                              "x": lng, "y": lat, "radius": radius},
                             seen)

    # keyword
    return _search_pages(headers, KAKAO_LOCAL_URL,
                         {"query": param, "x": lng, "y": lat, "radius": radius},
                         seen)


def _search_pages(headers, url, base_params, seen) -> list[dict]:
    out = []
    for page in range(1, 46):  # 카카오 max 45페이지
        params = {**base_params, "page": page, "size": KAKAO_PAGE_SIZE}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.debug(f"페이지 {page} 실패: {e}")
            break
        for d in data.get("documents", []):
            bid = d.get("id", "")
            if bid and bid not in seen:
                seen.add(bid)
                try:
                    out.append({"lat": float(d["y"]), "lng": float(d["x"])})
                except Exception:
                    pass
        if data.get("meta", {}).get("is_end", True):
            break
        time.sleep(0.05)
    return out


# ─────────────────────────────────────────────────────────
# 시·도 boundary → 검색 격자
# ─────────────────────────────────────────────────────────

def make_search_grid(sigungu_gdf: gpd.GeoDataFrame, step_m: int) -> list[tuple[float, float]]:
    """시·도 union의 bbox에 step_m 간격 격자, 폴리곤 내부만."""
    tm = sigungu_gdf.to_crs(CRS_KOREA)
    union = tm.unary_union
    minx, miny, maxx, maxy = union.bounds
    xs = np.arange(minx + step_m / 2, maxx, step_m)
    ys = np.arange(miny + step_m / 2, maxy, step_m)
    pts_tm = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x in xs for y in ys],
        crs=CRS_KOREA,
    )
    inside = gpd.sjoin(pts_tm, tm[["geometry"]], how="inner", predicate="within")
    inside_wgs = inside.to_crs(CRS_WGS84)
    return [(p.x, p.y) for p in inside_wgs.geometry]


def points_to_dong_count(points: list[dict], dong_gdf: gpd.GeoDataFrame) -> pd.Series:
    """수집된 점들을 읍·면·동 폴리곤에 spatial join → adm_cd별 카운트."""
    if not points:
        return pd.Series(0, index=dong_gdf["adm_cd"], dtype=int)
    pts = gpd.GeoDataFrame(
        points,
        geometry=[Point(p["lng"], p["lat"]) for p in points],
        crs=CRS_WGS84,
    )
    j = gpd.sjoin(pts, dong_gdf[["adm_cd", "geometry"]], how="inner", predicate="within")
    counts = j.groupby("adm_cd").size()
    return counts.reindex(dong_gdf["adm_cd"], fill_value=0).astype(int)


# ─────────────────────────────────────────────────────────
# 시·도별 처리
# ─────────────────────────────────────────────────────────

def collect_one_metric_for_sido(
    sido_code: str,
    metric_key: str,
    headers: dict,
    force: bool = False,
) -> bool:
    """한 시·도의 한 지표를 수집해 parquet에 컬럼 추가. 성공 시 True."""
    spec = METRICS[metric_key]
    sgg_path  = DATA_DIR / f"{sido_code}_sigungu.parquet"
    dong_path = DATA_DIR / f"{sido_code}_eupmyeondong.parquet"
    if not (sgg_path.exists() and dong_path.exists()):
        log.warning(f"  ⏭ {sido_code} — SGIS 데이터 없음, 먼저 collect_sgis_national.py 실행")
        return False

    sgg_gdf = gpd.read_parquet(sgg_path)
    dong_gdf = gpd.read_parquet(dong_path)

    if metric_key in dong_gdf.columns and not force:
        log.info(f"  ⏭ {sido_code}/{metric_key} 이미 존재 (--force로 재수집)")
        return True

    log.info(f"  🔍 {sido_code}/{metric_key} 검색 격자 생성 중 (step={spec['step_m']}m)")
    grid = make_search_grid(sgg_gdf, step_m=spec["step_m"])
    log.info(f"     격자 {len(grid)}개 — 카카오 호출 시작")

    all_points = []
    seen_ids = set()  # 격자 간 중복 제거
    for i, (lng, lat) in enumerate(grid, 1):
        pts = search_one_grid(headers, spec["kind"], spec["param"],
                              lng, lat, KAKAO_RADIUS_MAX)
        for p in pts:
            key = (round(p["lat"], 6), round(p["lng"], 6))
            if key not in seen_ids:
                seen_ids.add(key)
                all_points.append(p)
        if i % 20 == 0:
            log.info(f"     [{i}/{len(grid)}] 격자 처리, 누적 {len(all_points):,}건")

    log.info(f"     수집 완료: 고유 {len(all_points):,}건 → 읍면동 매칭 중")

    # 읍면동·시·군·구 카운트 모두 갱신
    dong_counts = points_to_dong_count(all_points, dong_gdf)
    dong_gdf[metric_key] = dong_counts.values

    # 시·군·구 카운트는 읍면동 카운트의 합
    if "sgg_cd" in dong_gdf.columns:
        sgg_counts = dong_gdf.groupby("sgg_cd")[metric_key].sum()
        sgg_gdf[metric_key] = sgg_gdf["adm_cd"].map(sgg_counts).fillna(0).astype(int)
    else:
        sgg_gdf[metric_key] = 0

    sgg_gdf.to_parquet(sgg_path, index=False)
    dong_gdf.to_parquet(dong_path, index=False)

    log.info(f"  ✅ {sido_code}/{metric_key} 저장: 시·군·구 합계 {int(sgg_gdf[metric_key].sum()):,}, "
             f"읍면동 평균 {dong_gdf[metric_key].mean():.1f}")
    return True


# ─────────────────────────────────────────────────────────
# manifest 갱신
# ─────────────────────────────────────────────────────────

def update_manifest(metric_key: str, sido_codes: list[str]) -> None:
    if not MANIFEST_PATH.exists():
        log.warning("manifest.json 없음")
        return
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)
    spec = METRICS[metric_key]
    metrics = manifest.setdefault("metrics", {})
    if metric_key not in metrics:
        metrics[metric_key] = {
            "label":  spec["label"],
            "unit":   spec["unit"],
            "source": "Kakao",
            "cmap":   spec["cmap"],
            "format": spec["format"],
            "available": [],
        }
    existing = set(metrics[metric_key].get("available", []))
    existing.update(sido_codes)
    metrics[metric_key]["available"] = sorted(existing)
    manifest["last_updated"] = datetime.now().isoformat(timespec="seconds")
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info(f"manifest.json 갱신: {metric_key} ← 시·도 {sido_codes}")


# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="카카오 일반 지표 전국 사전수집")
    ap.add_argument("--sido", help="특정 시·도만")
    ap.add_argument("--metric", choices=list(METRICS.keys()) + ["all"], default="all",
                    help="수집할 지표 (기본 all)")
    ap.add_argument("--resume", action="store_true",
                    help="컬럼이 이미 있으면 건너뜀 (기본 동작)")
    ap.add_argument("--force", action="store_true",
                    help="이미 있는 컬럼도 재수집")
    args = ap.parse_args()

    headers = kakao_headers()

    # 대상 시·도: SGIS parquet 있는 것만
    if args.sido:
        sido_codes = [args.sido]
    else:
        sido_codes = sorted({p.name.split("_")[0] for p in DATA_DIR.glob("*_sigungu.parquet")})
    if not sido_codes:
        sys.exit("❌ data/national/ 에 SGIS parquet 없음. collect_sgis_national.py 먼저 실행.")

    metric_keys = list(METRICS.keys()) if args.metric == "all" else [args.metric]
    log.info(f"대상 시·도 {len(sido_codes)}개 × 지표 {len(metric_keys)}개")

    for metric_key in metric_keys:
        log.info(f"\n=== 지표: {metric_key} ({METRICS[metric_key]['label']}) ===")
        success = []
        for sido_code in sido_codes:
            log.info(f" 시·도 {sido_code}")
            try:
                ok = collect_one_metric_for_sido(
                    sido_code, metric_key, headers,
                    force=args.force,
                )
                if ok:
                    success.append(sido_code)
            except KeyboardInterrupt:
                log.warning("⚠ 중단 — manifest 부분 갱신")
                if success:
                    update_manifest(metric_key, success)
                sys.exit(0)
            except Exception as e:
                log.error(f"  ❌ 실패: {e}")
        if success:
            update_manifest(metric_key, success)

    log.info("\n=== 모든 지표 수집 완료 ===")


if __name__ == "__main__":
    main()
