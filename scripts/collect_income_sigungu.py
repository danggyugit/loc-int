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


_DONG_NUM_RE = __import__("re").compile(r"\d+(?=동$|가$|로$)")

def _normalize_dong(adm_nm: str) -> str:
    """SGIS 행정동 이름 → 법정동 비교용 정규화.
    '서울특별시 강남구 역삼1동' → '역삼동' (마지막 토큰에서 숫자 제거).
    """
    last = adm_nm.split()[-1] if " " in adm_nm else adm_nm
    return _DONG_NUM_RE.sub("", last)


def collect_one_sido(sido_code: str, resume: bool) -> bool:
    sgg_path  = DATA_DIR / f"{sido_code}_sigungu.parquet"
    dong_path = DATA_DIR / f"{sido_code}_eupmyeondong.parquet"
    if not sgg_path.exists():
        log.warning(f"  ⏭ {sido_code} — SGIS 데이터 없음")
        return False

    sgg_gdf  = gpd.read_parquet(sgg_path)
    dong_gdf = gpd.read_parquet(dong_path) if dong_path.exists() else None

    if resume and INCOME_COL in sgg_gdf.columns and RENT_COL in sgg_gdf.columns \
       and dong_gdf is not None and INCOME_COL in dong_gdf.columns:
        log.info(f"  ⏭ {sido_code} — 이미 수집됨 (시·군·구 + 읍·면·동)")
        return True

    from src.rent_income_client import get_income_data, get_rent_data

    sgg_incomes, sgg_rents = [], []
    # 동 단위 누적: {sgg_cd: {dong_normalized: avg_value}}
    dong_income_map: dict[str, dict[str, float]] = {}
    dong_rent_map:   dict[str, dict[str, float]] = {}

    n = len(sgg_gdf)
    for i, (_, row) in enumerate(sgg_gdf.iterrows(), 1):
        sgg_cd = row["adm_cd"]
        single = gpd.GeoDataFrame([row], geometry="geometry", crs=sgg_gdf.crs)

        # 소득 (아파트 평균 매매가) — 법정동별 GDF 받음
        try:
            inc_gdf = get_income_data(single, region=row["adm_nm"])
            if inc_gdf is not None and len(inc_gdf) > 0:
                sgg_avg = float(inc_gdf["avg_price"].mean())
                # 동별 평균 누적 (data.go.kr이 같은 동에 여러 행 줄 수 있음)
                dong_income_map[sgg_cd] = dict(
                    inc_gdf.groupby("dong")["avg_price"].mean()
                )
            else:
                sgg_avg = 0.0
        except Exception as e:
            log.debug(f"income 실패 {row['adm_nm']}: {e}")
            sgg_avg = 0.0
        sgg_incomes.append(sgg_avg)

        # 월세
        try:
            rent_gdf = get_rent_data(single, region=row["adm_nm"])
            if rent_gdf is not None and len(rent_gdf) > 0:
                sgg_rent = float(rent_gdf["monthly_rent"].mean())
                dong_rent_map[sgg_cd] = dict(
                    rent_gdf.groupby("dong")["monthly_rent"].mean()
                )
            else:
                sgg_rent = 0.0
        except Exception as e:
            log.debug(f"rent 실패 {row['adm_nm']}: {e}")
            sgg_rent = 0.0
        sgg_rents.append(sgg_rent)

        n_dong_inc = len(dong_income_map.get(sgg_cd, {}))
        log.info(f"     [{i}/{n}] {row['adm_nm']} — 소득 {sgg_avg:.0f}/{n_dong_inc}동 / 월세 {sgg_rent:.1f}")

    # 시·군·구 parquet 갱신
    sgg_gdf[INCOME_COL] = sgg_incomes
    sgg_gdf[RENT_COL]   = sgg_rents
    sgg_gdf.to_parquet(sgg_path, index=False)

    # 읍·면·동 parquet 갱신 (법정동 → 행정동 broadcast)
    # Why: SGIS는 행정동(역삼1동/역삼2동), data.go.kr은 법정동(역삼동) 단위.
    #      같은 법정동의 모든 행정동에 같은 값 부여(broadcast).
    if dong_gdf is not None:
        if INCOME_COL not in dong_gdf.columns:
            dong_gdf[INCOME_COL] = 0.0
        if RENT_COL not in dong_gdf.columns:
            dong_gdf[RENT_COL] = 0.0

        n_dong_inc_total = 0
        n_dong_rent_total = 0
        for idx, row in dong_gdf.iterrows():
            sgg_cd = row.get("sgg_cd")
            if not sgg_cd:
                continue
            norm = _normalize_dong(row["adm_nm"])
            if sgg_cd in dong_income_map and norm in dong_income_map[sgg_cd]:
                dong_gdf.at[idx, INCOME_COL] = float(dong_income_map[sgg_cd][norm])
                n_dong_inc_total += 1
            if sgg_cd in dong_rent_map and norm in dong_rent_map[sgg_cd]:
                dong_gdf.at[idx, RENT_COL] = float(dong_rent_map[sgg_cd][norm])
                n_dong_rent_total += 1

        dong_gdf.to_parquet(dong_path, index=False)
        log.info(f"  ✅ {sido_code} 읍·면·동 매핑: 소득 {n_dong_inc_total}/{len(dong_gdf)}, 월세 {n_dong_rent_total}/{len(dong_gdf)}")

    n_ok_inc = sum(1 for v in sgg_incomes if v > 0)
    n_ok_rent = sum(1 for v in sgg_rents if v > 0)
    log.info(f"  ✅ {sido_code} 시·군·구: 소득 {n_ok_inc}/{n}, 월세 {n_ok_rent}/{n}")
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
