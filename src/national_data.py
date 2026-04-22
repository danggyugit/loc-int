# src/national_data.py — 전국 사전수집 데이터 로딩 + manifest 유틸
#
# Why: data/national/ 의 parquet들과 manifest.json 을 캡슐화. UI는 이 모듈만
#      import 하면 됨. 신규 지표·시·도가 추가되어도 UI 코드는 변경 불필요.

import json
import logging
from functools import lru_cache
from pathlib import Path

import geopandas as gpd
import pandas as pd

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "national"
MANIFEST_PATH = DATA_DIR / "manifest.json"


# ─────────────────────────────────────────────────────────
# manifest 조회
# ─────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def load_manifest() -> dict:
    """manifest.json 캐시 로드. 파일이 없거나 빈 경우 빈 스켈레톤 반환."""
    if not MANIFEST_PATH.exists():
        return {
            "schema_version": 1,
            "metrics": {},
            "sido_codes": {},
            "last_updated": None,
        }
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        return json.load(f)


def list_metrics(only_available: bool = True) -> list[dict]:
    """
    사용 가능한 지표 목록.

    Returns:
        [{"key", "label", "unit", "source", "cmap", "format", "available_sido"}, ...]
    """
    manifest = load_manifest()
    out = []
    for key, meta in manifest.get("metrics", {}).items():
        avail = meta.get("available", []) or []
        if only_available and not avail:
            continue
        out.append({
            "key":             key,
            "label":           meta.get("label", key),
            "unit":            meta.get("unit", ""),
            "source":          meta.get("source", ""),
            "cmap":            meta.get("cmap", "Blues"),
            "format":          meta.get("format", "{:,.0f}"),
            "available_sido":  avail,
        })
    return out


def list_available_sido() -> list[tuple[str, str]]:
    """수집된 적 있는 시·도 (적어도 한 지표라도 채워진) 목록."""
    manifest = load_manifest()
    sido_codes = manifest.get("sido_codes", {})
    seen = set()
    for meta in manifest.get("metrics", {}).values():
        seen.update(meta.get("available", []) or [])
    return [(code, sido_codes.get(code, code)) for code in sorted(seen)]


def metric_meta(metric_key: str) -> dict | None:
    """단일 지표 메타. 없으면 None."""
    manifest = load_manifest()
    return manifest.get("metrics", {}).get(metric_key)


# ─────────────────────────────────────────────────────────
# 지오데이터 로드 (시·도별, 레벨별)
# ─────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def load_sido_level(sido_code: str, level: str) -> gpd.GeoDataFrame | None:
    """
    한 시·도의 한 레벨(sigungu / eupmyeondong) parquet 로드.
    캐시되어 반복 호출 시 즉시 반환.
    """
    suffix = {"sigungu": "sigungu", "eupmyeondong": "eupmyeondong"}.get(level)
    if suffix is None:
        return None
    path = DATA_DIR / f"{sido_code}_{suffix}.parquet"
    if not path.exists():
        return None
    try:
        gdf = gpd.read_parquet(path)
        return gdf
    except Exception as e:
        log.warning(f"parquet 로드 실패 {path.name}: {e}")
        return None


def load_level(sido_codes: list[str], level: str) -> gpd.GeoDataFrame | None:
    """
    여러 시·도를 한 레벨로 합쳐 로드. 누락된 시·도는 자동 스킵.
    sido_codes 가 비면 manifest에 등록된 모든 시·도 로드.
    """
    if not sido_codes:
        sido_codes = [c for c, _ in list_available_sido()]

    parts = []
    for code in sido_codes:
        gdf = load_sido_level(code, level)
        if gdf is not None and len(gdf) > 0:
            parts.append(gdf)
    if not parts:
        return None
    merged = pd.concat(parts, ignore_index=True)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=parts[0].crs)


# ─────────────────────────────────────────────────────────
# 줌 레벨 → 자동 그래뉼래러티 결정
# ─────────────────────────────────────────────────────────

def level_for_zoom(zoom: int) -> str:
    """folium 지도 줌 레벨에 따라 적합한 행정 단위 결정.

    줌 6~10  : 시·도(전국 한눈에) — 우리는 시·도 데이터 별도 안 만들고 시·군·구로 통합 표시
    줌 10~12 : 시·군·구
    줌 13+   : 읍·면·동
    """
    if zoom >= 13:
        return "eupmyeondong"
    return "sigungu"


# ─────────────────────────────────────────────────────────
# 데이터 가용 여부 진단
# ─────────────────────────────────────────────────────────

def is_ready() -> bool:
    """최소 한 개 지표라도 어느 시·도에든 채워져 있으면 True."""
    return bool(list_available_sido())


def coverage_summary() -> dict:
    """현재 수집 상태 요약 (UI 안내용)."""
    metrics = list_metrics(only_available=False)
    total_sido = len(load_manifest().get("sido_codes", {}))
    summary = {
        "total_sido":       total_sido,
        "ready":            is_ready(),
        "metrics_total":    len(metrics),
        "metrics_active":   sum(1 for m in metrics if m["available_sido"]),
        "last_updated":     load_manifest().get("last_updated"),
        "by_metric":        {m["key"]: len(m["available_sido"]) for m in metrics},
    }
    return summary
