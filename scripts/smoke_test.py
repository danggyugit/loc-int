"""로컬 스모크 테스트 — 앱 랜딩 렌더 + 데모 번들 round-trip 검증.

실행: .venv/bin/python scripts/smoke_test.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_landing_renders() -> None:
    """AppTest로 랜딩 화면(분석 전)이 예외 없이 렌더되는지 확인."""
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file(str(ROOT / "app_Ver5.1.py"), default_timeout=60)
    at.run()
    assert not at.exception, f"랜딩 렌더 예외: {at.exception}"
    print("PASS: 랜딩 렌더 (입지 분석 모드, 분석 전)")


def test_demo_bundle_roundtrip() -> None:
    """demo_bundle 저장→복원이 데이터와 경로를 보존하는지 확인."""
    import pandas as pd

    from src import demo_bundle

    # 임시 디렉토리를 번들 위치로 몽키패치
    tmp = Path(tempfile.mkdtemp())
    demo_bundle.DEMO_DIR = tmp / "demo"

    chart = tmp / "fake_chart.png"
    chart.write_bytes(b"\x89PNG fake")
    csv = tmp / "fake_top.csv"
    csv.write_text("rank,score\n1,0.9\n")

    cache = {
        "region": "서울 강남구",
        "label": "카페",
        "scored": pd.DataFrame({"grid_id": ["g1", "g2"], "score": [0.9, 0.4]}),
        "top": pd.DataFrame({"grid_id": ["g1"], "rank": [1]}),
        "chart_path": str(chart),
        "radar_path": None,
        "dist_path": str(tmp / "does_not_exist.png"),  # 없는 파일 → None 처리 기대
        "csv_path": str(csv),
        "top_n": 5,
        "profile": {"profile_key": "cafe", "weights": {"population": 0.2}},
    }

    demo_bundle.save_demo_bundle(cache)
    assert demo_bundle.demo_available()
    meta = demo_bundle.demo_meta()
    assert meta["region"] == "서울 강남구" and meta["label"] == "카페"

    loaded = demo_bundle.load_demo_bundle()
    assert loaded is not None and loaded["_demo"] is True
    assert loaded["scored"].equals(cache["scored"])
    assert Path(loaded["chart_path"]).exists()
    assert loaded["dist_path"] is None
    assert loaded["radar_path"] is None
    assert Path(loaded["csv_path"]).read_text().startswith("rank")
    # 원본 세션 캐시는 오염되지 않아야 함 (경로 재작성은 번들 사본에만)
    assert cache["chart_path"] == str(chart)
    print("PASS: demo_bundle round-trip (경로 재작성·누락 파일·원본 보존)")


def test_operator_key_never_rendered() -> None:
    """Secrets에 구성된 운영자 키가 화면(입력창 value 등)에 노출되지 않는지 확인."""
    import os

    from streamlit.testing.v1 import AppTest

    os.environ["KAKAO_API_KEY"] = "dummy_secret_for_test"
    try:
        at = AppTest.from_file(str(ROOT / "app_Ver5.1.py"), default_timeout=60)
        at.run()
        assert not at.exception
        rendered = " ".join(str(getattr(el, "value", "")) for el in at.sidebar.text_input)
        assert "dummy_secret_for_test" not in rendered, "운영자 키가 입력창에 노출됨!"
    finally:
        del os.environ["KAKAO_API_KEY"]
    print("PASS: 운영자 키 화면 비노출 (secrets 구성 시)")


if __name__ == "__main__":
    test_demo_bundle_roundtrip()
    test_landing_renders()
    test_operator_key_never_rendered()
    print("\nALL SMOKE TESTS PASSED")
