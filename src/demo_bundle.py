"""데모 번들 — 분석 결과를 저장/복원하여 API 키 없이 결과 화면을 체험하게 한다.

analysis_cache dict(결과 렌더 계약)를 통째로 직렬화한다:
- GeoDataFrame/DataFrame 등은 pickle (자기 저장소 내 파일이므로 안전)
- 차트 PNG·CSV는 번들 폴더로 복사하고 경로를 번들 기준으로 재작성

용도: 포트폴리오/시연 — 방문자가 카카오 API 키 없이도 '분석 시작'의
전체 결과 화면(지도·후보지·차트·상세 테이블)을 그대로 볼 수 있다.
"""
from __future__ import annotations

import json
import logging
import pickle
import shutil
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

DEMO_DIR = Path(__file__).resolve().parents[1] / "data" / "demo"
_PKL = "demo_analysis.pkl"
_META = "demo_meta.json"

# cache 안에서 '파일 경로'인 키 — 번들로 복사 후 경로 재작성 대상
_PATH_KEYS = ["chart_path", "radar_path", "dist_path", "csv_path"]


def demo_available() -> bool:
    """저장된 데모 번들이 있는지."""
    return (DEMO_DIR / _PKL).exists()


def demo_meta() -> dict:
    """데모 번들 메타 정보 (region, label, saved_at). 없으면 {}."""
    meta_path = DEMO_DIR / _META
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_demo_bundle(cache: dict) -> Path:
    """analysis_cache를 데모 번들로 저장한다.

    Returns:
        번들 디렉토리 경로.
    """
    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    bundle = dict(cache)  # 원본 세션 캐시를 건드리지 않도록 얕은 복사

    # 차트/CSV 파일을 번들 폴더로 복사하고 파일명만 저장
    for key in _PATH_KEYS:
        src = bundle.get(key)
        if not src:
            continue
        src_path = Path(src)
        if src_path.exists():
            dst = DEMO_DIR / src_path.name
            if src_path.resolve() != dst.resolve():
                shutil.copy2(src_path, dst)
            bundle[key] = src_path.name  # 번들 기준 상대 경로
        else:
            bundle[key] = None

    bundle["_demo"] = True
    with open(DEMO_DIR / _PKL, "wb") as f:
        pickle.dump(bundle, f, protocol=pickle.HIGHEST_PROTOCOL)

    meta = {
        "region": cache.get("region", ""),
        "label": cache.get("label", ""),
        "top_n": cache.get("top_n"),
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    (DEMO_DIR / _META).write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("데모 번들 저장: %s (%s / %s)", DEMO_DIR, meta["region"], meta["label"])
    return DEMO_DIR


def load_demo_bundle() -> dict | None:
    """데모 번들을 analysis_cache 형식으로 복원한다. 없으면 None."""
    pkl_path = DEMO_DIR / _PKL
    if not pkl_path.exists():
        return None
    with open(pkl_path, "rb") as f:
        bundle = pickle.load(f)

    # 상대 파일명 → 절대 경로 복원 (파일이 실제 존재할 때만)
    for key in _PATH_KEYS:
        name = bundle.get(key)
        if name:
            p = DEMO_DIR / Path(name).name
            bundle[key] = str(p) if p.exists() else None
    return bundle
