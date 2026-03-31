# app_Ver4.3.py — 입지 선정 분석 툴 v4.3 (Streamlit 앱)
# 실행: streamlit run app_Ver4.3.py
#
# v4.2 대비 개선 사항:
#   1. 용도지역 하드 필터: Vworld API → 전용주거·녹지 등 입점불가 지역 자동 제거
#   2. 상가건물 팩터: 건축물대장 API → 격자별 상가건물 밀도 반영
#   3. 도로접근성 팩터: OSM 도로 데이터 → 대로변 가시성 반영
#   4. 11팩터 점수 모델: 기존 9팩터 + 상가건물 + 도로접근성 + 용도지역 하드필터
#
# 추가 환경변수:
#   VWORLD_API_KEY    — Vworld 오픈 API 인증키 (용도지역 필터용)
#   BUILDING_API_KEY  — 공공데이터포털 건축물대장 인증키 (상가건물 분석용)
#   DATA_GO_KR_API_KEY — 공공데이터포털 인증키 (소득·월세 수집용)

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
    page_title="입지 선정 분석 툴 v4.3",
    page_icon="📍",
    layout="wide",
    initial_sidebar_state="expanded",
)

PRESETS = {
    "카페":       "cafe",
    "음식점":     "restaurant",
    "병원":       "hospital",
    "편의점":     "convenience",
    "대형마트":   "mart",
    "약국":       "pharmacy",
    "도자기 공방": "pottery",
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

    def _get_secret(key: str, default: str = "") -> str:
        """Streamlit Secrets → 환경변수 → 기본값 순으로 조회."""
        try:
            return st.secrets[key]
        except (KeyError, FileNotFoundError):
            return os.environ.get(key, default)

    kakao_key = st.text_input(
        "카카오 REST API 키",
        value=_get_secret("KAKAO_API_KEY"),
        type="password",
        help="카카오 개발자 콘솔에서 발급한 REST API 키",
    )

    data_go_kr_key = st.text_input(
        "공공데이터포털 API 키",
        value=_get_secret("DATA_GO_KR_API_KEY"),
        type="password",
        help="data.go.kr에서 발급한 인증키 (소득·월세 수집용, 선택)",
    )

    with st.expander("추가 API 키 (용도지역·건축물·인구)", expanded=False):
        vworld_key = st.text_input(
            "Vworld API 키",
            value=_get_secret("VWORLD_API_KEY"),
            type="password",
            help="용도지역 필터링 (vworld.kr에서 발급, 선택). 미입력 시 OSM 데이터로 대체.",
        )
        building_key = st.text_input(
            "건축물대장 API 키",
            value=_get_secret("BUILDING_API_KEY"),
            type="password",
            help="상가건물 분석 (data.go.kr 건축HUB, 선택)",
        )
        sgis_key = st.text_input(
            "SGIS 서비스ID (Consumer Key)",
            value=_get_secret("SGIS_CONSUMER_KEY"),
            type="password",
            help="통계청 SGIS 실인구 데이터 (sgis.kostat.go.kr에서 발급, 선택)",
        )
        sgis_secret = st.text_input(
            "SGIS 보안KEY (Consumer Secret)",
            value=_get_secret("SGIS_CONSUMER_SECRET"),
            type="password",
            help="통계청 SGIS API 보안키",
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

st.title("📍 입지 선정 분석 툴 v4.3")
st.caption("지역과 업종을 입력하면 카카오 API로 데이터를 수집하고 최적 입지를 분석합니다.")

if not run_btn and st.session_state["analysis_cache"] is None:
    st.info("왼쪽 사이드바에서 지역·업종·셀 크기를 설정하고 **분석 시작** 버튼을 누르세요.")
    st.markdown("""
    **사용 방법**
    1. 카카오 REST API 키 입력
    2. 공공데이터포털 API 키 입력 (선택 — 소득·월세 분석용)
    3. Vworld / 건축물대장 API 키 입력 (선택 — 용도지역·상가건물 분석용)
    4. 분석할 지역 입력 (예: 강남구, 일산동구)
    5. 업종 프리셋 선택 또는 키워드 직접 입력 (예: 도자기 공방)
    6. 분석 시작 클릭

    **v4.3 개선 사항**
    - 용도지역 하드 필터: 전용주거·녹지 등 입점불가 지역 자동 제거
    - 상가건물 팩터: 격자별 상가건물 밀도 반영
    - 도로접근성 팩터: OSM 도로 데이터 기반 대로변 가시성 반영
    - 11팩터 점수 모델: +상가건물 +도로접근성 +용도지역 하드필터
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
    if data_go_kr_key:
        os.environ["DATA_GO_KR_API_KEY"] = data_go_kr_key
    if sgis_key:
        os.environ["SGIS_CONSUMER_KEY"] = sgis_key
    if sgis_secret:
        os.environ["SGIS_CONSUMER_SECRET"] = sgis_secret

    st.markdown(f"### 📊 `{region}` / `{label}` 분석 중...")
    progress = st.progress(0, text="데이터 수집 중...")

    try:
        # Step 1. 기본 데이터 수집 (기존 7팩터 + 용도지역·건축물·도로)
        from src.collector import collect_all
        data       = collect_all(region=region, category=preset, keyword=keyword,
                                 cell_size_m=cell_size,
                                 vworld_key=vworld_key, building_key=building_key)
        boundary   = data["boundary"]
        competitor = data["competitor"]
        transport  = data["transport"]
        population = data["population"]
        workplace  = data.get("workplace")
        parking    = data.get("parking")
        diversity  = data.get("diversity")
        land_use   = data.get("land_use")
        buildings  = data.get("buildings")
        roads      = data.get("roads")
        pop_source = data.get("pop_source", "unknown")
        progress.progress(20, text="소득·월세 데이터 수집 중...")

        # Step 2. 소득·월세 데이터 수집 (v4.2 계승)
        from src.rent_income_client import (
            is_data_api_available, get_income_data, get_rent_data,
            assign_nearest_to_grid,
        )
        income_gdf = None
        rent_gdf   = None
        income_source = "none"

        if is_data_api_available():
            income_gdf = get_income_data(boundary, region=region)
            rent_gdf   = get_rent_data(boundary, region=region)
            if income_gdf is not None or rent_gdf is not None:
                income_source = "data.go.kr"
            if income_gdf is not None:
                st.success(f"소득 데이터 수집 완료: {len(income_gdf)}개 동")
            else:
                st.warning("소득 데이터(아파트매매) 수집 실패 — 해당 팩터 제외하고 진행합니다.")
            if rent_gdf is not None:
                st.success(f"월세 데이터 수집 완료: {len(rent_gdf)}개 동")
            else:
                st.warning("월세 데이터(전월세) 수집 실패 — 해당 팩터 제외하고 진행합니다.")
        else:
            st.info("💡 공공데이터포털 API 키를 입력하면 소득·월세 데이터도 분석에 반영됩니다.")

        # v4.3: 용도지역·건축물·도로 수집 결과 안내
        if land_use is not None and len(land_use) > 0:
            source_label = "Vworld" if vworld_key else "OSM"
            st.success(f"용도지역 데이터 수집 완료: {len(land_use)}개 폴리곤 ({source_label})")
        else:
            st.warning("용도지역 데이터 수집 실패 — 하드 필터 없이 진행합니다.")

        if buildings is not None and len(buildings) > 0:
            st.success(f"상가건물 데이터 수집 완료: {len(buildings)}건")
        elif building_key:
            st.warning("상가건물 데이터 수집 실패 — 해당 팩터 제외하고 진행합니다.")

        if roads is not None and len(roads) > 0:
            st.success(f"도로 데이터 수집 완료: {len(roads)}개 세그먼트")

        progress.progress(35, text="격자 분석 중...")

        # Step 3. 격자 생성 (11팩터)
        from src.grid import build_grid_features
        grid = build_grid_features(
            boundary, cell_size_m=cell_size,
            population_gdf=population,
            competitor_gdf=competitor,
            transport_gdf=transport,
            workplace_gdf=workplace,
            parking_gdf=parking,
            diversity_gdf=diversity,
            zone_gdf=land_use,
            building_gdf=buildings,
            road_gdf=roads,
        )

        # Step 4. 소득·월세 격자 집계 (v4.2 계승)
        if income_gdf is not None:
            grid = assign_nearest_to_grid(grid, income_gdf, "avg_price", "income")
        else:
            grid["income"] = 0.0

        if rent_gdf is not None:
            grid = assign_nearest_to_grid(grid, rent_gdf, "monthly_rent", "rent")
        else:
            grid["rent"] = 0.0

        progress.progress(50, text="점수화 중...")

        # Step 5. 11팩터 점수화 (v4.3 scoring)
        from src.scoring_Ver4_3 import score_and_rank
        scored, top, profile = score_and_rank(
            grid,
            preset=preset,
            keyword=keyword,
            top_n=top_n,
        )
        progress.progress(70, text="클러스터링 중...")

        # Step 6. 클러스터링
        from src.cluster import run_cluster_analysis
        cluster_gdf, cluster_summary, gap_gdf = run_cluster_analysis(
            scored,
            score_threshold=0.6,
            eps_m=cell_size * 3,
            min_samples=3,
            comp_threshold=2,
            pop_threshold=0.3,
        )
        progress.progress(85, text="차트 생성 중...")

        # Step 7. 차트 저장
        out_dir    = Path("output")
        out_dir.mkdir(exist_ok=True)
        safe_label = label.replace(" ", "_")
        prefix     = f"{region}_{safe_label}"

        from src.visualizer_Ver4_2 import plot_score_bar
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
            "income_source":   income_source,
            "income_gdf":      income_gdf,
            "rent_gdf":        rent_gdf,
            "land_use":        land_use,
            "buildings":       buildings,
            "roads":           roads,
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
income_source   = cache.get("income_source", "none")
income_gdf      = cache.get("income_gdf")
rent_gdf        = cache.get("rent_gdf")
land_use        = cache.get("land_use")
buildings       = cache.get("buildings")
roads           = cache.get("roads")

n_hotspot = (cluster_summary["type"] == "핫스팟").sum()
n_gap     = gap_gdf["is_gap"].sum()

# 요약 지표 카드
st.markdown("---")
_pop_src_label = {"sgis": "SGIS 실인구", "apartment_proxy": "아파트 proxy", "none": "없음"}
_inc_src_label = {"data.go.kr": "실거래가", "none": "없음"}

# v4.3: 2행 × 6열 + 1행 × 2열 (총 14개 지표)
_row1 = st.columns(6)
_row1[0].metric("분석 셀 수",      f"{len(scored):,}개")
_row1[1].metric("인구 데이터",     _pop_src_label.get(pop_source, pop_source))
_row1[2].metric("경쟁업체",        f"{len(competitor)}건")
_row1[3].metric("교통 인프라",     f"{len(transport)}건")
_row1[4].metric("주차장",          f"{len(parking) if parking is not None else 0}건")
_row1[5].metric("상권 업종",       f"{len(diversity) if diversity is not None else 0}건")

_row2 = st.columns(6)
_row2[0].metric("소득 데이터",     _inc_src_label.get(income_source, income_source))
_row2[1].metric("월세 데이터",     f"{len(rent_gdf) if rent_gdf is not None else 0}동")
_row2[2].metric("용도지역",        f"{len(land_use) if land_use is not None else 0}건")
_row2[3].metric("상가건물",        f"{len(buildings) if buildings is not None else 0}건")
_row2[4].metric("핫스팟 클러스터", f"{n_hotspot}개")
_row2[5].metric("경쟁 공백 지역",  f"{n_gap}셀")

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
        "all":                "전 연령",
        "children":           "유소년(0~14세) 가중",
        "elderly":            "고령(65세+) 가중",
        "young_adult":        "생산가능인구(15~64세) 가중",
        "children_and_parent": "유소년(5~13) + 부모(35~45) 가중",
    }
    comp_mode = profile.get("competition_mode", "tolerate")
    demo_target = profile.get("demographic_target", "all")
    with st.expander("📐 적용된 점수화 프로파일 (11팩터)", expanded=False):
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
                f"다양성 **{w.get('diversity',0):.0%}** | "
                f"소득 **{w.get('income',0):.0%}** | "
                f"임대 **{w.get('rent',0):.0%}** | "
                f"상가 **{w.get('commercial',0):.0%}** | "
                f"도로 **{w.get('road_quality',0):.0%}**"
            )
            # 용도지역 필터 상태 표시
            if land_use is not None and len(land_use) > 0:
                n_blocked = (scored["zone_score"] == 0.0).sum() if "zone_score" in scored.columns else 0
                st.markdown(f"**용도지역 하드 필터**: 활성 (입점불가 {n_blocked}셀 제거)")
            else:
                st.markdown("**용도지역 하드 필터**: 비활성 (Vworld API 키 필요)")

st.markdown("---")

# ── 지도 빌드 ────────────────────────────────────────────
from src.visualizer_Ver4_2 import build_combined_folium_map

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

# v4.3: commercial_cnt, road_score 컬럼 추가
display_cols = [
    "rank", "grid_id", "population", "floating", "workplace",
    "competitor_cnt", "transport_score", "parking_cnt", "diversity",
    "income", "rent", "commercial_cnt", "road_score", "zone_score", "score",
]
available = [c for c in display_cols if c in top.columns]

# 포맷 설정
format_dict = {
    "score":           "{:.3f}",
    "population":      "{:.0f}",
    "floating":        "{:.0f}",
    "workplace":       "{:.0f}",
    "competitor_cnt":  "{:.0f}",
    "transport_score": "{:.0f}",
    "parking_cnt":     "{:.0f}",
    "diversity":       "{:.0f}",
    "income":          "{:.0f}",
    "rent":            "{:.0f}",
    "commercial_cnt":  "{:.0f}",
    "road_score":      "{:.2f}",
    "zone_score":      "{:.2f}",
}
format_dict = {k: v for k, v in format_dict.items() if k in available}

selection = st.dataframe(
    top[available].style.format(format_dict).background_gradient(
        subset=["score"], cmap="RdYlGn"
    ),
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    key="candidate_table",
    column_config={
        "rank":            st.column_config.NumberColumn("순위", width="small"),
        "grid_id":         st.column_config.TextColumn("격자 ID"),
        "population":      st.column_config.NumberColumn("인구"),
        "floating":        st.column_config.NumberColumn("유동인구"),
        "workplace":       st.column_config.NumberColumn("직장인구"),
        "competitor_cnt":  st.column_config.NumberColumn("경쟁업체"),
        "transport_score": st.column_config.NumberColumn("교통점수"),
        "parking_cnt":     st.column_config.NumberColumn("주차장"),
        "diversity":       st.column_config.NumberColumn("다양성"),
        "income":          st.column_config.NumberColumn("소득수준(만/㎡)", help="아파트 평균 매매가 (만원/㎡)"),
        "rent":            st.column_config.NumberColumn("월세수준(만원)", help="주변 평균 월세 (만원)"),
        "commercial_cnt":  st.column_config.NumberColumn("상가건물", help="격자 내 상가건물 수"),
        "road_score":      st.column_config.NumberColumn("도로접근성", help="도로 등급 점수 (0~1)"),
        "zone_score":      st.column_config.NumberColumn("용도지역", help="용도지역 적합도 (0=불가, 1=최적)"),
        "score":           st.column_config.NumberColumn("점수"),
    },
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
