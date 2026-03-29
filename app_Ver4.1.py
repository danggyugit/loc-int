# app_Ver4.1.py — 입지 선정 분석 툴 v4.1 (Streamlit 앱)
# 실행: streamlit run app_Ver4.1.py
#
# v4.0 대비 개선 사항:
#   1. 지하철 노선도 레이어 추가 (OpenRailwayMap TileLayer 오버레이)
#      - 레이어 컨트롤에서 ON/OFF 토글 가능 (기본 OFF)

import os
import sys
import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import streamlit as st

# ── 한국어 폰트 설정 (다른 시각화 라이브러리 import 전에 설정) ──
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

import folium
from streamlit_folium import st_folium

# ─────────────────────────────────────────────────────────
# 페이지 설정
# ─────────────────────────────────────────────────────────

st.set_page_config(
    page_title="입지 선정 분석 툴 v4.1",
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
# 세션 상태 초기화
# ─────────────────────────────────────────────────────────

for _key, _default in [
    ("analysis_cache", None),
    ("selected_rank",  None),
    ("map_center",     None),
    ("map_zoom",       13),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default

# ─────────────────────────────────────────────────────────
# 사이드바 입력
# ─────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📍 입지 선정 분석")
    st.markdown("---")

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
        preset = None
        label  = keyword or ""

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

st.title("📍 입지 선정 분석 툴 v4.1")
st.caption("지역과 업종을 입력하면 카카오 API로 데이터를 수집하고 최적 입지를 분석합니다.")

if not run_btn and st.session_state["analysis_cache"] is None:
    st.info("왼쪽 사이드바에서 지역·업종·셀 크기를 설정하고 **분석 시작** 버튼을 누르세요.")
    st.markdown("""
    **사용 방법**
    1. 카카오 REST API 키 입력
    2. 분석할 지역 입력 (예: 강남구, 일산동구)
    3. 업종 프리셋 선택 또는 키워드 직접 입력 (예: 도자기 공방)
    4. 격자 셀 크기 선택 (작을수록 정밀, 클수록 빠름)
    5. 분석 시작 클릭

    **v4.1 개선 사항**
    - 지하철 노선도 레이어 추가 (레이어 컨트롤에서 ON/OFF)
    """)
    st.stop()

# ─────────────────────────────────────────────────────────
# 분석 실행
# ─────────────────────────────────────────────────────────

if run_btn:
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

    st.markdown(f"### 📊 `{region}` / `{label}` 분석 중...")
    progress = st.progress(0, text="데이터 수집 중...")

    try:
        # Step 1. 데이터 수집
        from src.collector import collect_all
        data       = collect_all(region=region, category=preset, keyword=keyword,
                                 cell_size_m=cell_size)
        boundary   = data["boundary"]
        competitor = data["competitor"]
        transport  = data["transport"]
        population = data["population"]
        workplace  = data.get("workplace")
        parking    = data.get("parking")
        diversity  = data.get("diversity")
        pop_source = data.get("pop_source", "unknown")
        progress.progress(30, text="격자 분석 중...")

        # Step 2. 격자 생성
        from src.grid import build_grid_features
        grid = build_grid_features(
            boundary, cell_size_m=cell_size,
            population_gdf=population,
            competitor_gdf=competitor,
            transport_gdf=transport,
            workplace_gdf=workplace,
            parking_gdf=parking,
            diversity_gdf=diversity,
        )
        progress.progress(55, text="점수화 중...")

        # Step 3. 점수화
        from src.scoring import score_and_rank
        scored, top, profile = score_and_rank(
            grid,
            preset=preset,
            keyword=keyword,
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
        progress.progress(90, text="차트 생성 중...")

        # Step 5. 차트 저장
        out_dir    = Path("output")
        out_dir.mkdir(exist_ok=True)
        safe_label = label.replace(" ", "_")
        prefix     = f"{region}_{safe_label}"

        from src.visualizer_Ver4_1 import plot_score_bar
        top["name"] = top["grid_id"]
        chart_path  = plot_score_bar(
            top, name_col="name",
            out_path=str(out_dir / f"{prefix}_scores.png"),
        )
        csv_path = out_dir / f"{prefix}_top{top_n}.csv"
        top.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8-sig")

        progress.progress(100, text="완료!")
        progress.empty()

        st.session_state["analysis_cache"] = {
            "region":          region,
            "label":           label,
            "scored":          scored,
            "top":             top,
            "gap_gdf":         gap_gdf,
            "cluster_summary": cluster_summary,
            "competitor":      competitor,
            "transport":       transport,
            "population":      population,
            "chart_path":      chart_path,
            "csv_path":        csv_path,
            "prefix":          prefix,
            "top_n":           top_n,
            "profile":         profile,
            "workplace":       workplace,
            "parking":         parking,
            "diversity":       diversity,
            "pop_source":      pop_source,
        }
        st.session_state["selected_rank"] = None
        st.session_state["map_center"]    = None
        st.session_state["map_zoom"]      = 13

    except Exception as e:
        progress.empty()
        st.error(f"분석 중 오류 발생: {e}")
        st.stop()

# ─────────────────────────────────────────────────────────
# 결과 출력 (캐시에서 로드)
# ─────────────────────────────────────────────────────────

cache = st.session_state["analysis_cache"]
if cache is None:
    st.stop()

scored          = cache["scored"]
top             = cache["top"]
gap_gdf         = cache["gap_gdf"]
cluster_summary = cache["cluster_summary"]
competitor      = cache["competitor"]
transport       = cache["transport"]
population      = cache["population"]
chart_path      = cache["chart_path"]
csv_path        = cache["csv_path"]
region_label    = cache["region"]
label_text      = cache["label"]
top_n_cached    = cache["top_n"]
prefix          = cache["prefix"]
profile         = cache.get("profile", {})
workplace       = cache.get("workplace")
parking         = cache.get("parking")
diversity       = cache.get("diversity")
pop_source      = cache.get("pop_source", "unknown")

n_hotspot = (cluster_summary["type"] == "핫스팟").sum()
n_gap     = gap_gdf["is_gap"].sum()

# 요약 지표 카드
st.markdown("---")
_pop_src_label = {"sgis": "SGIS 실인구", "apartment_proxy": "아파트 proxy", "none": "없음"}
c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
c1.metric("분석 셀 수",      f"{len(scored):,}개")
c2.metric("인구 데이터",     _pop_src_label.get(pop_source, pop_source))
c3.metric("경쟁업체",        f"{len(competitor)}건")
c4.metric("교통 인프라",     f"{len(transport)}건")
c5.metric("주차장",          f"{len(parking) if parking is not None else 0}건")
c6.metric("상권 업종",       f"{len(diversity) if diversity is not None else 0}건")
c7.metric("핫스팟 클러스터", f"{n_hotspot}개")
c8.metric("경쟁 공백 지역",  f"{n_gap}셀")

# 적용된 점수화 프로파일 정보
if profile:
    _mode_label = {
        "avoid":    "🔴 회피형 (독점 상권 선호)",
        "tolerate": "🟡 선형형 (기본)",
        "cluster":  "🟢 집적형 (카페거리·먹자골목 허용)",
    }
    _src_label = {
        "preset":  "프리셋",
        "rule":    "규칙 자동 분류",
        "cache":   "캐시 (이전 분류 재사용)",
        "claude":  "Claude AI 자동 분류",
        "default": "기본값",
        "cli":     "직접 입력",
    }
    _demo_label = {
        "all":         "전 연령",
        "children":    "유소년(0~14세) 가중",
        "elderly":     "고령(65세+) 가중",
        "young_adult": "생산가능인구(15~64세) 가중",
    }
    comp_mode = profile.get("competition_mode", "tolerate")
    demo_target = profile.get("demographic_target", "all")
    with st.expander("📐 적용된 점수화 프로파일", expanded=False):
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.markdown(f"**프로파일**  \n`{profile.get('profile_key', '-')}`")
        pc2.markdown(f"**경쟁 효과**  \n{_mode_label.get(comp_mode, comp_mode)}")
        pc3.markdown(f"**타겟 연령**  \n{_demo_label.get(demo_target, demo_target)}")
        pc4.markdown(f"**분류 출처**  \n{_src_label.get(profile.get('source',''), profile.get('source',''))}")
        st.caption(profile.get("description", ""))
        w = profile.get("weights", {})
        if w:
            st.markdown(
                f"가중치: 인구 **{w.get('population',0):.0%}** | "
                f"유동 **{w.get('floating',0):.0%}** | "
                f"직장 **{w.get('workplace',0):.0%}** | "
                f"경쟁 **{w.get('competitor',0):.0%}** | "
                f"접근성 **{w.get('accessibility',0):.0%}** | "
                f"주차 **{w.get('parking',0):.0%}** | "
                f"다양성 **{w.get('diversity',0):.0%}**"
            )

st.markdown("---")

# ── 지도 빌드 ────────────────────────────────────────────
from src.visualizer_Ver4_1 import build_combined_folium_map

selected_rank = st.session_state["selected_rank"]
map_center    = st.session_state["map_center"]
map_zoom      = st.session_state["map_zoom"]

if selected_rank is not None:
    st.info(f"지도에서 **#{selected_rank}번 후보지**가 강조 표시됩니다. 다른 행을 클릭하거나 선택 해제하세요.")

folium_map = build_combined_folium_map(
    scored_gdf=scored,
    gap_gdf=gap_gdf,
    competitor_gdf=competitor,
    transport_gdf=transport,
    population_gdf=population,
    top_gdf=top,
    label=f"{region_label} {label_text}",
    center=map_center,
    zoom_start=map_zoom,
    selected_rank=selected_rank,
)

map_col, chart_col = st.columns([2, 1])

with map_col:
    st.subheader("🗺️ 통합 분석 지도")
    st.caption("좌하단 컨트롤: 레이어 ON/OFF | 번호 마커: 상위 후보지 위치")
    st_folium(folium_map, height=550, use_container_width=True, returned_objects=[])

with chart_col:
    st.subheader(f"🏆 상위 {top_n_cached}개 후보지")
    st.image(chart_path, use_container_width=True)

# 후보지 상세 테이블
st.markdown("---")
st.subheader("📋 후보지 상세 데이터")
st.caption("행을 클릭하면 지도에서 해당 후보지를 주황색으로 강조합니다.")

display_cols = ["rank", "grid_id", "population", "floating", "workplace",
                "competitor_cnt", "transport_score", "parking_cnt", "diversity", "score"]
available    = [c for c in display_cols if c in top.columns]

selection = st.dataframe(
    top[available].style.format({
        "score":           "{:.3f}",
        "population":      "{:.0f}",
        "floating":        "{:.0f}",
        "workplace":       "{:.0f}",
        "competitor_cnt":  "{:.0f}",
        "transport_score": "{:.0f}",
        "parking_cnt":     "{:.0f}",
        "diversity":       "{:.0f}",
    }).background_gradient(subset=["score"], cmap="RdYlGn"),
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="candidate_table",
)

# ── 선택 행 처리 → 지도 중심 이동 + 강조 마커 ────────────
rows = selection.selection.rows
if rows:
    sel_row  = top.iloc[rows[0]]
    new_rank = int(sel_row.get("rank", 0))
    centroid = sel_row["geometry"].centroid

    if new_rank != st.session_state["selected_rank"]:
        st.session_state["selected_rank"] = new_rank
        st.session_state["map_center"]    = [centroid.y, centroid.x]
        st.session_state["map_zoom"]      = 15
        st.rerun()

elif st.session_state["selected_rank"] is not None:
    st.session_state["selected_rank"] = None
    st.session_state["map_center"]    = None
    st.session_state["map_zoom"]      = 13
    st.rerun()

# CSV 다운로드
with open(csv_path, "rb") as f:
    st.download_button(
        label="📥 결과 CSV 다운로드",
        data=f,
        file_name=f"{prefix}_top{top_n_cached}.csv",
        mime="text/csv",
    )
