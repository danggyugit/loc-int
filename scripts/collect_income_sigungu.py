# scripts/collect_income_sigungu.py — data.go.kr 시·군·구 평균 소득·월세 수집
#
# 시·군·구별 boundary로 rent_income_client를 호출 → 평균 매매가·월세 계산 →
# {sido}_sigungu.parquet의 income_avg / rent_avg 컬럼에 저장.
# (읍·면·동 단위 데이터는 출처 부재로 제공 안 함)
#
# 실행:
#   $env:DATA_GO_KR_API_KEY = "..."; $env:KAKAO_API_KEY = "..."
#   python scripts/collect_income_sigungu.py
#   python scripts/collect_income_sigungu.py --sido 11 --resume

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import geopandas as gpd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src import session_keys  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DATA_DIR = ROOT / "data" / "national"
MANIFEST_PATH = DATA_DIR / "manifest.json"

INCOME_COL = "income_avg"
RENT_COL   = "rent_avg"


def setup_keys() -> None:
    dk = os.environ.get("DATA_GO_KR_API_KEY", "").strip()
    kk = os.environ.get("KAKAO_API_KEY", "").strip()
    if not dk or not kk:
        sys.exit("❌ DATA_GO_KR_API_KEY, KAKAO_API_KEY 환경변수 필요")
    session_keys.set_keys(DATA_GO_KR_API_KEY=dk, KAKAO_API_KEY=kk)


def collect_one_sido(sido_code: str, resume: bool) -> bool:
    sgg_path = DATA_DIR / f"{sido_code}_sigungu.parquet"
    if not sgg_path.exists():
        log.warning(f"  ⏭ {sido_code} — SGIS 데이터 없음")
        return False

    sgg_gdf = gpd.read_parquet(sgg_path)
    if resume and INCOME_COL in sgg_gdf.columns and RENT_COL in sgg_gdf.columns:
        log.info(f"  ⏭ {sido_code} — 이미 수집됨")
        return True

    from src.rent_income_client import get_income_data, get_rent_data

    incomes, rents = [], []
    n = len(sgg_gdf)
    for i, (_, row) in enumerate(sgg_gdf.iterrows(), 1):
        single = gpd.GeoDataFrame([row], geometry="geometry", crs=sgg_gdf.crs)
        # 소득 (아파트 평균 매매가)
        try:
            inc = get_income_data(single, region=row["adm_nm"])
            v = float(inc["avg_price"].mean()) if inc is not None and len(inc) else 0.0
        except Exception as e:
            log.debug(f"income 실패 {row['adm_nm']}: {e}")
            v = 0.0
        incomes.append(v)
        # 월세
        try:
            rent = get_rent_data(single, region=row["adm_nm"])
            r = float(rent["monthly_rent"].mean()) if rent is not None and len(rent) else 0.0
        except Exception as e:
            log.debug(f"rent 실패 {row['adm_nm']}: {e}")
            r = 0.0
        rents.append(r)
        log.info(f"     [{i}/{n}] {row['adm_nm']} — 소득 {v:.0f} / 월세 {r:.1f}")

    sgg_gdf[INCOME_COL] = incomes
    sgg_gdf[RENT_COL]   = rents
    sgg_gdf.to_parquet(sgg_path, index=False)
    n_ok_inc = sum(1 for v in incomes if v > 0)
    n_ok_rent = sum(1 for v in rents if v > 0)
    log.info(f"  ✅ {sido_code} 저장: 소득 데이터 있는 시·군·구 {n_ok_inc}/{n}, 월세 {n_ok_rent}/{n}")
    return True


def update_manifest(sido_codes: list[str]) -> None:
    if not MANIFEST_PATH.exists():
        return
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        manifest = json.load(f)
    metrics = manifest.setdefault("metrics", {})
    for key, label, unit, cmap in [
        (INCOME_COL, "평균 소득(아파트 매매가)", "만원/㎡", "Greens"),
        (RENT_COL,   "평균 월세",                "만원",     "Oranges"),
    ]:
        if key not in metrics:
            metrics[key] = {
                "label": label, "unit": unit, "source": "data.go.kr",
                "cmap": cmap, "format": "{:,.1f}", "available": [],
            }
        existing = set(metrics[key].get("available", []))
        existing.update(sido_codes)
        metrics[key]["available"] = sorted(existing)
    manifest["last_updated"] = datetime.now().isoformat(timespec="seconds")
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info(f"manifest 갱신: 소득·월세 ← 시·도 {sido_codes}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sido")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    setup_keys()

    if args.sido:
        codes = [args.sido]
    else:
        codes = sorted({p.name.split("_")[0] for p in DATA_DIR.glob("*_sigungu.parquet")})
    if not codes:
        sys.exit("❌ data/national/ 에 SGIS parquet 없음")

    success = []
    for c in codes:
        log.info(f"=== 시·도 {c} ===")
        try:
            if collect_one_sido(c, args.resume):
                success.append(c)
        except KeyboardInterrupt:
            log.warning("⚠ 중단")
            break
        except Exception as e:
            log.error(f"❌ {c}: {e}")

    if success:
        update_manifest(success)
    log.info(f"=== 완료 === {len(success)} 시·도")


if __name__ == "__main__":
    main()
