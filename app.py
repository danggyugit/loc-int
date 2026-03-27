# app.py — 입지 선정 분석 툴 (Streamlit 앱)
# 실행: streamlit run app.py

import os
import sys
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st
import folium
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from streamlit_folium import st_folium

# ─────────────────────────────────────────────────────────
# 페이지 설정
# ─────────────────────────────────────────────────────────

st.set_page_config(
    page_title="입지 선정 분석 툴",
    page_icon="📍",
    layout="wide",
    initial_sidebar_state="expanded",
)

PRESETS = {
    "카페":     "cafe",
    "음식점":   "restaurant",
    "병원":     "hospital",
    "편의점":   "convenience",
    "대형마트": "mart",
    "약국":     "pharmacy",
}

# ─────────────────────────────────────────────────────────
# 사이드바 입력
# ─────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📍 입지 선정 분석")
    st.markdown("---")

    # API 키
    kakao_key = st.text_input(
        "카카오 REST API 키",
        value=os.environ.get("KAKAO_API_KEY", ""),
        type="password",
        help="카카오 개발자 콘솔에서 발급한 REST API 키",
    )

    st.markdown("---")
    st.subheader("분석 설정")

    region = st.text_input(
        "분석 지역",
        placeholder="예: 강남구, 일산동구, 해운대구",
    )

    mode = st.radio("업종 선택 방식", ["프리셋", "키워드 직접 입력"], horizontal=True)

    if mode == "프리셋":
        preset_label = st.selectbox("업종 프리셋", list(PRESETS.keys()))
        preset  = PRESETS[preset_label]
        keyword = None
        label   = preset_label
    else:
        keyword = st.text_input(
            "업종 키워드",
            placeholder="예: 도자기 공방, 네일샵, 필라테스",
        )
        preset  = None
        label   = keyword or ""

    cell_size = st.select_slider(
        "격자 셀 크기",
        options=[250, 500, 1000, 2000],
        value=500,
        format_func=lambda x: f"{x}m",
    )

    top_n = st.slider("상위 후보지 수", min_value=5, max_value=20, value=10)

    st.markdown("---")
    run_btn = st.button("🔍 분석 시작", type="primary", use_container_width=True)

# ─────────────────────────────────────────────────────────
# 메인 화면
# ─────────────────────────────────────────────────────────

st.title("📍 입지 선정 분석 툴")
st.caption("지역과 업종을 입력하면 카카오 API로 데이터를 수집하고 최적 입지를 분석합니다.")

if not run_btn:
    st.info("왼쪽 사이드바에서 지역·업종·셀 크기를 설정하고 **분석 시작** 버튼을 누르세요.")
    st.markdown("""
    **사용 방법**
    1. 카카오 REST API 키 입력
    2. 분석할 지역 입력 (예: 강남구, 일산동구)
    3. 업종 프리셋 선택 또는 키워드 직접 입력 (예: 도자기 공방)
    4. 격자 셀 크기 선택 (작을수록 정밀, 클수록 빠름)
    5. 분석 시작 클릭

    **결과 화면**
    - 통합 지도: 입지 점수 히트맵 + 경쟁업체 + 교통 + 경쟁 공백 지역
    - 상위 후보지 순위표
    - 점수 비교 차트
    """)
    st.stop()

# ─────────────────────────────────────────────────────────
# 입력 검증
# ─────────────────────────────────────────────────────────

if not kakao_key:
    st.error("카카오 REST API 키를 입력하세요.")
    st.stop()
if not region:
    st.error("분석 지역을 입력하세요.")
    st.stop()
if mode == "키워드 직접 입력" and not keyword:
    st.error("업종 키워드를 입력하세요.")
    st.stop()

os.environ["KAKAO_API_KEY"] = kakao_key

# ─────────────────────────────────────────────────────────
# 분석 실행
# ─────────────────────────────────────────────────────────

st.markdown(f"### 📊 `{region}` / `{label}` 분석 중...")

progress = st.progress(0, text="데이터 수집 중...")

try:
    # Step 1. 데이터 수집
    from src.collector import collect_all
    data = collect_all(region=region, category=preset, keyword=keyword)
    boundary   = data["boundary"]
    competitor = data["competitor"]
    transport  = data["transport"]
    population = data["population"]
    progress.progress(30, text="격자 분석 중...")

    # Step 2. 격자 생성
    from src.grid import build_grid_features
    grid = build_grid_features(
        boundary, cell_size_m=cell_size,
        population_gdf=population,
        competitor_gdf=competitor,
        transport_gdf=transport,
    )
    progress.progress(55, text="점수화 중...")

    # Step 3. 점수화
    from src.scoring import score_and_rank
    scored, top = score_and_rank(
        grid,
        preset=preset if preset else "default",
        top_n=top_n,
    )
    progress.progress(75, text="클러스터링 중...")

    # Step 4. 클러스터링
    from src.cluster import run_cluster_analysis
    cluster_gdf, cluster_summary, gap_gdf = run_cluster_analysis(
        scored,
        score_threshold=0.6,
        eps_m=cell_size * 3,
        min_samples=3,
        comp_threshold=2,
        pop_threshold=0.3,
    )
    progress.progress(90, text="지도 생성 중...")

    # Step 5. 지도 생성
    from src.visualizer import plot_combined_map, plot_score_bar

    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)
    safe_label = label.replace(" ", "_")
    prefix     = f"{region}_{safe_label}"

    combined_path = plot_combined_map(
        scored_gdf=scored,
        gap_gdf=gap_gdf,
        competitor_gdf=competitor,
        transport_gdf=transport,
        population_gdf=population,
        label=f"{region} {label}",
        out_path=str(out_dir / f"{prefix}_combined.html"),
    )
    top["name"] = top["grid_id"]
    chart_path = plot_score_bar(
        top, name_col="name",
        out_path=str(out_dir / f"{prefix}_scores.png"),
    )
    csv_path = out_dir / f"{prefix}_top{top_n}.csv"
    top.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8-sig")

    progress.progress(100, text="완료!")
    progress.empty()

except Exception as e:
    progress.empty()
    st.error(f"분석 중 오류 발생: {e}")
    st.stop()

# ─────────────────────────────────────────────────────────
# 결과 출력
# ─────────────────────────────────────────────────────────

n_hotspot = (cluster_summary["type"] == "핫스팟").sum()
n_gap     = gap_gdf["is_gap"].sum()

# 요약 지표 카드
st.markdown("---")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("분석 셀 수",    f"{len(scored):,}개")
c2.metric("경쟁업체",      f"{len(competitor)}건")
c3.metric("교통 인프라",   f"{len(transport)}건")
c4.metric("핫스팟 클러스터", f"{n_hotspot}개")
c5.metric("경쟁 공백 지역", f"{n_gap}셀")

st.markdown("---")

# 지도 + 차트 2열 배치
map_col, chart_col = st.columns([2, 1])

with map_col:
    st.subheader("🗺️ 통합 분석 지도")
    st.caption("레이어 컨트롤(우상단)로 각 레이어 ON/OFF | 셀 클릭 시 상세 정보")
    folium_map = folium.Map()
    with open(combined_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    st.components.v1.html(html_content, height=550, scrolling=False)

with chart_col:
    st.subheader(f"🏆 상위 {top_n}개 후보지")
    st.image(chart_path, use_container_width=True)

# 후보지 상세 테이블
st.markdown("---")
st.subheader("📋 후보지 상세 데이터")
display_cols = ["rank","grid_id","population","floating","competitor_cnt","transport_cnt","score"]
available    = [c for c in display_cols if c in top.columns]
st.dataframe(
    top[available].style.format({
        "score":          "{:.3f}",
        "population":     "{:.0f}",
        "floating":       "{:.0f}",
        "competitor_cnt": "{:.0f}",
        "transport_cnt":  "{:.0f}",
    }).background_gradient(subset=["score"], cmap="RdYlGn"),
    use_container_width=True,
    hide_index=True,
)

# CSV 다운로드
with open(csv_path, "rb") as f:
    st.download_button(
        label="📥 결과 CSV 다운로드",
        data=f,
        file_name=f"{prefix}_top{top_n}.csv",
        mime="text/csv",
    )
