# scripts/collect_competitors_by_preset.py — 7개 프리셋 업종별 전국 경쟁업체 수
#
# 7 프리셋(cafe, restaurant, hospital, convenience, mart, pharmacy, pottery) ×
# 17 시·도 × 검색 격자(약 200개) × 페이지 5 = 호출량 매우 큼.
# 일일 카카오 쿼터(10만) 한 번에 다 쓰기 부담 → 업종/시·도 분할 실행 권장.
#
# 결과: 시·군·구·읍·면·동 parquet에 {preset}_cnt 컬럼(예: cafe_cnt) 추가.
#
# 실행:
#   $env:KAKAO_API_KEY = "..."
#   python scripts/collect_competitors_by_preset.py --preset cafe --sido 11
#   python scripts/collect_competitors_by_preset.py --preset cafe   # 전국
#   python scripts/collect_competitors_by_preset.py --resume       # 누락 시·도×업종만

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
    KAKAO_PAGE_SIZE, KAKAO_RADIUS_MAX, KAKAO_CATEGORY,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DATA_DIR = ROOT / "data" / "national"
MANIFEST_PATH = DATA_DIR / "manifest.json"

SEARCH_STEP_M = 5000

# preset → (kind, param). 카테고리 코드 없는 업종은 키워드.
PRESET_SPEC = {
    "cafe":        ("category", "CE7"),
    "restaurant":  ("category", "FD6"),
    "hospital":    ("category", "HP8"),
    "convenience": ("category", "CS2"),
    "mart":        ("category", "MT1"),
    "pharmacy":    ("category", "PM9"),
    "pottery":     ("keyword",  "도자기 공방"),
}

PRESET_LABEL = {
    "cafe": "카페", "restaurant": "음식점", "hospital": "병원",
    "convenience": "편의점", "mart": "대형마트", "pharmacy": "약국",
    "pottery": "도자기 공방",
}


def headers() -> dict:
    k = os.environ.get("KAKAO_API_KEY", "").strip()
    if not k:
        sys.exit("❌ KAKAO_API_KEY 환경변수 필요")
    return {"Authorization": f"KakaoAK {k}"}


def search_grid(headers, kind: str, param: str, lng: float, lat: float, seen: set) -> list[dict]:
    out = []
    if kind == "category":
        url = KAKAO_CATEGORY_URL
        base = {"category_group_code": param, "x": lng, "y": lat, "radius": KAKAO_RADIUS_MAX}
    else:
        url = KAKAO_LOCAL_URL
        base = {"query": param, "x": lng, "y": lat, "radius": KAKAO_RADIUS_MAX}
    for page in range(1, 46):
        params = {**base, "page": page, "size": KAKAO_PAGE_SIZE}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
        except Exception:
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


def make_search_grid(sigungu_gdf: gpd.GeoDataFrame, step_m: int) -> list[tuple[float, float]]:
    tm = sigungu_gdf.to_crs(CRS_KOREA)
    union = tm.unary_union
    minx, miny, maxx, maxy = union.bounds
    xs = np.arange(minx + step_m / 2, maxx, step_m)
    ys = np.arange(miny + step_m / 2, maxy, step_m)
    pts = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x in xs for y in ys],
        crs=CRS_KOREA,
    )
    inside = gpd.sjoin(pts, tm[["geometry"]], how="inner", predicate="within").to_crs(CRS_WGS84)
    return [(p.x, p.y) for p in inside.geometry]


def points_to_count(points: list[dict], gdf: gpd.GeoDataFrame) -> pd.Series:
    if not points:
        return pd.Series(0, index=gdf["adm_cd"], dtype=int)
    pts = gpd.GeoDataFrame(
        points,
        geometry=[Point(p["lng"], p["lat"]) for p in points],
        crs=CRS_WGS84,
    )
    j = gpd.sjoin(pts, gdf[["adm_cd", "geometry"]], how="inner", predicate="within")
    counts = j.groupby("adm_cd").size()
    return counts.reindex(gdf["adm_cd"], fill_value=0).astype(int)


def collect(preset: str, sido_code: str, hdr: dict, force: bool) -> bool:
    col = f"{preset}_cnt"
    sgg_path  = DATA_DIR / f"{sido_code}_sigungu.parquet"
    dong_path = DATA_DIR / f"{sido_code}_eupmyeondong.parquet"
    if not (sgg_path.exists() and dong_path.exists()):
        log.warning(f"  ⏭ {sido_code} — SGIS 데이터 없음")
        return False

    sgg_gdf = gpd.read_parquet(sgg_path)
    dong_gdf = gpd.read_parquet(dong_path)
    if col in dong_gdf.columns and not force:
        log.info(f"  ⏭ {sido_code}/{preset} 이미 존재")
        return True

    kind, param = PRESET_SPEC[preset]
    grid = make_search_grid(sgg_gdf, step_m=SEARCH_STEP_M)
    log.info(f"     격자 {len(grid)}개 — 카카오 호출 시작 ({kind}: {param})")

    pts = []
    seen = set()
    for i, (lng, lat) in enumerate(grid, 1):
        new = search_grid(hdr, kind, param, lng, lat, seen)
        pts.extend(new)
        if i % 20 == 0:
            log.info(f"     [{i}/{len(grid)}] 누적 {len(pts):,}건")
    log.info(f"     총 {len(pts):,}건 → 매칭 중")

    dong_counts = points_to_count(pts, dong_gdf)
    dong_gdf[col] = dong_counts.values
    if "sgg_cd" in dong_gdf.columns:
        sgg_counts = dong_gdf.groupby("sgg_cd")[col].sum()
        sgg_gdf[col] = sgg_gdf["adm_cd"].map(sgg_counts).fillna(0).astype(int)
    else:
        sgg_gdf[col] = 0

    sgg_gdf.to_parquet(sgg_path, index=False)
    dong_gdf.to_parquet(dong_path, index=False)
    log.info(f"  ✅ {sido_code}/{preset} 저장: 시·도 합계 {int(sgg_gdf[col].sum()):,}")
    return True


def update_manifest(preset: str, sido_codes: list[str]) -> None:
    if not MANIFEST_PATH.exists():
        return
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)
    key = f"{preset}_cnt"
    metrics = manifest.setdefault("metrics", {})
    if key not in metrics:
        metrics[key] = {
            "label":  f"{PRESET_LABEL[preset]} 매장 수",
            "unit":   "개",
            "source": f"Kakao ({preset})",
            "cmap":   "Reds",
            "format": "{:,.0f}",
            "available": [],
        }
    existing = set(metrics[key].get("available", []))
    existing.update(sido_codes)
    metrics[key]["available"] = sorted(existing)
    manifest["last_updated"] = datetime.now().isoformat(timespec="seconds")
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info(f"manifest 갱신: {key} ← {sido_codes}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preset", choices=list(PRESET_SPEC.keys()) + ["all"], default="all",
                    help="수집할 프리셋 (기본 all)")
    ap.add_argument("--sido", help="특정 시·도만")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    hdr = headers()

    if args.sido:
        codes = [args.sido]
    else:
        codes = sorted({p.name.split("_")[0] for p in DATA_DIR.glob("*_sigungu.parquet")})
    if not codes:
        sys.exit("❌ data/national/ 에 SGIS parquet 없음")

    presets = list(PRESET_SPEC.keys()) if args.preset == "all" else [args.preset]
    log.info(f"대상 시·도 {len(codes)}개 × 프리셋 {len(presets)}개")

    for preset in presets:
        log.info(f"\n=== 프리셋: {preset} ({PRESET_LABEL[preset]}) ===")
        success = []
        for c in codes:
            log.info(f" 시·도 {c}")
            try:
                if collect(preset, c, hdr, args.force):
                    success.append(c)
            except KeyboardInterrupt:
                log.warning("⚠ 중단")
                if success:
                    update_manifest(preset, success)
                sys.exit(0)
            except Exception as e:
                log.error(f"  ❌ {e}")
        if success:
            update_manifest(preset, success)


if __name__ == "__main__":
    main()
