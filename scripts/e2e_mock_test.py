"""E2E 모의 테스트 — 수집만 가짜로 대체하고 분석 파이프라인 전체를 완주시킨다.

목적: 외부 API 없이 Step 2~8 + 결과 화면(지도·차트·탭·테이블) 렌더까지
      실제 앱 코드로 검증. 수집 이후의 크래시를 배포 전에 전부 색출한다.

실행: .venv/bin/python scripts/e2e_mock_test.py
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np


def _make_fake_collect_all():
    """일산동구 경계 안에 합성 포인트 데이터를 채운 collect_all 대체 함수."""
    import geopandas as gpd
    from shapely.geometry import Point

    from src.collector import get_boundary

    boundary = get_boundary("일산동구")  # 사전수집 parquet — 네트워크 불필요
    minx, miny, maxx, maxy = boundary.total_bounds
    poly = boundary.geometry.union_all()
    rng = np.random.default_rng(42)

    def _points(n: int) -> list:
        pts = []
        while len(pts) < n:
            cand = Point(rng.uniform(minx, maxx), rng.uniform(miny, maxy))
            if poly.contains(cand):
                pts.append(cand)
        return pts

    def _gdf(n: int, **cols) -> gpd.GeoDataFrame:
        geom = _points(n)
        data = {k: (v(n) if callable(v) else [v] * n) for k, v in cols.items()}
        return gpd.GeoDataFrame(data, geometry=geom, crs="EPSG:4326")

    fake = {
        "boundary":   boundary,
        "competitor": _gdf(60, place_name=lambda n: [f"카페{i}" for i in range(n)]),
        "transport":  _gdf(120, weight=lambda n: rng.choice([1, 3], n)),
        "parking":    _gdf(40),
        "diversity":  _gdf(300, cat_code=lambda n: rng.choice(
            ["CE7", "FD6", "CS2", "HP8", "MT1", "PM9"], n)),
        "population": _gdf(
            200,
            population=lambda n: rng.integers(200, 3000, n),
            avg_age=lambda n: rng.uniform(30, 50, n),
            juv_suprt_per=lambda n: rng.uniform(10, 25, n),
            oldage_suprt_per=lambda n: rng.uniform(10, 30, n),
        ),
        "workplace":  _gdf(150, workplace=lambda n: rng.integers(10, 500, n)),
        "pop_source": "sgis",
        "land_use":   None,   # Vworld 실패 시나리오 (하드 필터 없이 진행)
        "buildings":  _gdf(80),
        "roads":      None,   # Overpass 차단 시나리오 (도로 팩터 제외)
    }

    def fake_collect_all(region, category=None, keyword=None, cell_size_m=500,
                         vworld_key=None, building_key=None, progress_cb=None):
        if progress_cb:
            progress_cb("모의 수집 (E2E 테스트)")
        return fake

    return fake_collect_all


def run() -> None:
    from streamlit.testing.v1 import AppTest

    import src.collector as collector
    collector.collect_all = _make_fake_collect_all()  # 수집만 대체

    at = AppTest.from_file(str(ROOT / "app_Ver5.1.py"), default_timeout=900)
    at.run()
    at.text_input(key="key_KAKAO_API_KEY").set_value("dummy_e2e_key")
    at.selectbox[0].set_value("경기도")
    at.run()
    at.multiselect[0].set_value(["고양시 일산동구"])
    at.run()
    # 프리셋 모드 + 카페
    for r in at.radio:
        if "프리셋" in (r.options or []):
            r.set_value("프리셋")
    at.run()
    for sb in at.selectbox:
        if "카페" in (sb.options or []):
            sb.set_value("카페")
            break
    for btn in at.button:
        if "분석 시작" in (btn.label or ""):
            btn.set_value(True)
            break
    at.run()

    # ── 검증 ──
    assert not at.exception, f"E2E 크래시: {at.exception[0].message}\n{at.exception[0].stack_trace}"
    parts: list[str] = []
    for el in at.main:
        for attr in ("value", "body", "label"):
            v = getattr(el, attr, None)
            if isinstance(v, str):
                parts.append(v)
    body = " ".join(parts)
    for marker in ("1위 후보지", "격자 상세 데이터", "통합 분석 지도"):
        assert marker in body, f"결과 화면 요소 누락: {marker}"
    assert at.error == [] or all("주소" in e.value for e in at.error), \
        f"오류 박스 발생: {[e.value[:120] for e in at.error]}"
    print("PASS: E2E 완주 — 수집(모의) → Step 2~8 → 결과 화면 렌더 (예외 0)")

    # 데모 zip 다운로드 데이터 준비 확인 (다운로드 버튼 렌더 = zip 직렬화 성공)
    labels = [b.label for b in at.button]
    assert any("데모" in (l or "") for l in labels), "데모 번들 버튼 미렌더"
    print("PASS: 데모 번들 UI 렌더")


if __name__ == "__main__":
    run()
