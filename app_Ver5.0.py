# app_Ver5.0.py — 입지 선정 분석 툴 v5.0 (Streamlit 앱)
# 실행: streamlit run app_Ver5.0.py
#
# v4.9 대비 개선 사항 (소프트 런칭):
#   1. 결과 요약 헤드라인 — 1위 후보지 점수·경쟁·인구를 한 문장으로 요약
#      (비전문가도 즉시 파악 가능)
#   2. 지도 HTML 다운로드 — folium 지도를 interactive HTML로 저장하여
#      외부 공유·프레젠테이션 활용 가능
#
# v4.9 블로커 해소 유지:
#   1. 세션별 API 키 격리 (session_keys)
#   2. 분석 범위 상한 (구 5개·셀 8000개)
#
# 이전 버전(v4.8) 기능 유지:
#   - 캐시 TTL 24시간, st.status 단계별 진행, 에러 컨텍스트 힌트
#   - 다중 구 분석 시 카카오 ID 중복 제거
#   - PRESETS/SIDO_GU_MAP config.py 중앙화
#   - API 키 입력 단일 expander 통합, 2단계 드롭다운
#   - 용도지역 하드 필터, 상가건물/도로접근성, 11팩터 점수 모델
#
# 환경변수 (로컬 개발용 폴백):
#   KAKAO_API_KEY, DATA_GO_KR_API_KEY, VWORLD_API_KEY, BUILDING_API_KEY,
#   SGIS_CONSUMER_KEY, SGIS_CONSUMER_SECRET

import os
import sys
import logging
import warnings
warnings.filterwarnings("ignore")

log = logging.getLogger(__name__)

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import numpy as np
import streamlit as st

# ── 한국어 폰트 설정 (다른 시각화 라이브러리 import 전에 설정) ──
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

import folium
from streamlit_folium import st_folium

# ── OSM/Overpass 타임아웃 단축 (기본 180초 → 15초) ──
# Why: Overpass 서버 장애 시 180초 × 여러 호출 = 10분+ 대기 방지
try:
    import osmnx as ox
    ox.settings.timeout = 15
except ImportError:
    pass


# ─────────────────────────────────────────────────────────
# 캐싱 함수 — 같은 파라미터면 API 재호출 없이 즉시 반환
# Why: 클라우드 환경에서 위젯 상호작용 시 전체 스크립트가 재실행되므로
#      데이터 수집(2~3분)을 매번 반복하면 타임아웃 발생
# ─────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=86400)
def _cached_collect_all(
    region: str, category: str, keyword: str,
    cell_size_m: int, vworld_key: str, building_key: str,
    _kakao_key: str,
) -> dict:
    """collect_all 결과를 캐싱. GeoDataFrame은 dict로 변환하여 직렬화."""
    from src.collector import collect_all
    data = collect_all(
        region=region, category=category, keyword=keyword,
        cell_size_m=cell_size_m,
        vworld_key=vworld_key, building_key=building_key,
    )
    # GeoDataFrame → dict 변환 (st.cache_data 직렬화 호환)
    import geopandas as gpd
    result = {}
    for k, v in data.items():
        if isinstance(v, gpd.GeoDataFrame):
            result[k] = v.to_json() if len(v) > 0 else None
        else:
            result[k] = v
    return result


@st.cache_data(show_spinner=False, ttl=86400)
def _cached_income_rent(region: str, _data_go_key: str, boundary_json: str):
    """소득·월세 데이터 수집 캐싱."""
    import geopandas as gpd
    from io import StringIO
    boundary = gpd.read_file(StringIO(boundary_json))

    from src.rent_income_client import (
        is_data_api_available, get_income_data, get_rent_data,
    )
    income_gdf = None
    rent_gdf = None
    if is_data_api_available():
        income_gdf = get_income_data(boundary, region=region)
        rent_gdf = get_rent_data(boundary, region=region)
    return (
        income_gdf.to_json() if income_gdf is not None and len(income_gdf) > 0 else None,
        rent_gdf.to_json() if rent_gdf is not None and len(rent_gdf) > 0 else None,
    )


def _json_to_gdf(geojson_str):
    """캐시에서 복원한 GeoJSON 문자열 → GeoDataFrame."""
    if geojson_str is None:
        return None
    import geopandas as gpd
    from io import StringIO
    return gpd.read_file(StringIO(geojson_str))


def _merge_gdfs(gdfs, dedup_col: str | None = None):
    """여러 GeoDataFrame을 하나로 병합. None/빈 항목은 제외.

    Args:
        gdfs: 병합할 GeoDataFrame 리스트.
        dedup_col: 지정 시 해당 컬럼 기준으로 중복 제거.
            인접 구(예: 강남↔서초) 카카오 반경 검색이 겹쳐
            같은 biz_id/stop_id가 두 번 들어오는 케이스 방어용.
    """
    import geopandas as gpd
    import pandas as pd
    valid = [g for g in gdfs if g is not None and len(g) > 0]
    if not valid:
        return None
    merged = gpd.GeoDataFrame(pd.concat(valid, ignore_index=True))
    merged = merged.set_crs(valid[0].crs, allow_override=True)
    if dedup_col and dedup_col in merged.columns:
        before = len(merged)
        merged = merged.drop_duplicates(subset=[dedup_col], keep="first").reset_index(drop=True)
        removed = before - len(merged)
        if removed > 0:
            log.info(f"_merge_gdfs dedup({dedup_col}): {before} → {len(merged)} ({removed}건 제거)")
    return merged

# ─────────────────────────────────────────────────────────
# 페이지 설정
# ─────────────────────────────────────────────────────────

st.set_page_config(
    page_title="입지 선정 분석 툴 v5.0",
    page_icon="📍",
    layout="wide",
    initial_sidebar_state="expanded",
)

from config import PRESETS, SIDO_GU_MAP

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

    with st.expander("⚙️ API 키 설정", expanded=False):
        kakao_key = st.text_input(
            "카카오 REST API 키",
            value=_get_secret("KAKAO_API_KEY"),
            type="password",
            help="카카오 개발자 콘솔에서 발급한 REST API 키 (필수)",
        )
        data_go_kr_key = st.text_input(
            "공공데이터포털 API 키",
            value=_get_secret("DATA_GO_KR_API_KEY"),
            type="password",
            help="data.go.kr에서 발급한 인증키 (소득·월세 수집용, 선택)",
        )
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

    sido = st.selectbox("시/도", list(SIDO_GU_MAP.keys()))
    gu_options = SIDO_GU_MAP[sido]
    selected_gus = st.multiselect(
        "구/군/시 (다중 선택 가능)",
        options=gu_options,
        help="여러 구를 선택하면 통합 분석을 수행합니다.",
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

st.title("📍 입지 선정 분석 툴")
st.caption("한국 전 지역을 대상으로 업종 프리셋·키워드 기반 입지 점수화를 11개 팩터로 수행합니다.")

if not run_btn and st.session_state["analysis_cache"] is None:
    st.info("왼쪽 사이드바에서 **API 키 · 지역 · 업종 · 셀 크기**를 설정하고 **🔍 분석 시작**을 눌러보세요.")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
        **사용 흐름**
        1. `⚙️ API 키 설정` expander에서 카카오 REST API 키 입력 (필수)
        2. 시/도 → 구/군/시 다중 선택 (여러 구 통합 분석 가능)
        3. 업종 프리셋 또는 자유 키워드 (예: `도자기 공방`, `네일샵`) 선택
        4. 셀 크기 선택 (250 / 500 / 1000 / 2000m — 작을수록 정밀)
        5. 🔍 분석 시작 — 구당 약 1~3분 소요
        """)
    with c2:
        st.markdown("""
        **선택 API 키** (있으면 분석 정확도 ↑)
        - **공공데이터포털**: 아파트 실거래가 → 소득·월세 팩터
        - **Vworld**: 용도지역 데이터 → 입점 가능 지역 하드 필터
        - **건축물대장**: 상가건물 밀집도 팩터
        - **SGIS**: 통계청 실인구·사업체 통계 (아파트 proxy 대체)

        API 키 발급: 각 사이트 회원가입 → 서비스 신청. 이 앱은 입력받은 키를
        **현재 세션에서만 사용**하며 서버나 로그에 저장하지 않습니다.
        """)
    st.stop()

# ─────────────────────────────────────────────────────────
# 분석 실행
# ─────────────────────────────────────────────────────────

if run_btn:
    if not kakao_key:
        st.error("카카오 REST API 키를 입력하세요.")
        st.stop()
    if not selected_gus:
        st.error("분석할 구/군/시를 하나 이상 선택하세요.")
        st.stop()
    if mode == "키워드 직접 입력" and not keyword:
        st.error("업종 키워드를 입력하세요.")
        st.stop()

    # 분석 범위 상한: API 쿼터 소진·OOM 방지
    # Why: 구 × 셀 크기로 추정한 API 호출량과 격자 규모가 어느 임계치를 넘으면
    #      사용자도 모르게 수천 건의 호출이 발생함. Streamlit Cloud 메모리 한계도 고려.
    _GU_LIMIT = 5
    _CELLS_PER_GU_AT = {250: 1600, 500: 400, 1000: 100, 2000: 25}  # 평균적 구 면적 가정
    est_cells = len(selected_gus) * _CELLS_PER_GU_AT.get(cell_size, 400)
    est_api_calls = len(selected_gus) * 800  # 구당 카카오 약 800회 (6종 × 밀도)

    if len(selected_gus) > _GU_LIMIT:
        st.error(
            f"⚠️ 한 번에 분석 가능한 구는 최대 **{_GU_LIMIT}개**입니다. "
            f"현재 {len(selected_gus)}개 선택 — 줄여서 다시 시도하세요."
        )
        st.info(f"예상 API 호출량이 **{est_api_calls:,}회**로 너무 많습니다. "
                "카카오 일일 쿼터(10만회)를 빠르게 소진할 수 있어 제한합니다.")
        st.stop()

    if est_cells > 8000:
        st.warning(
            f"⚠️ 예상 격자 수 **{est_cells:,}개** — 메모리·렌더링 부담이 큽니다. "
            f"셀 크기를 키우거나(예: {cell_size}m → {cell_size*2}m) 구를 줄이는 것을 권장합니다."
        )

    # v4.9: session_keys로 전환 (os.environ 프로세스 전역 오염 제거)
    from src import session_keys
    session_keys.set_keys(
        KAKAO_API_KEY=kakao_key,
        DATA_GO_KR_API_KEY=data_go_kr_key,
        VWORLD_API_KEY=vworld_key,
        BUILDING_API_KEY=building_key,
        SGIS_CONSUMER_KEY=sgis_key,
        SGIS_CONSUMER_SECRET=sgis_secret,
    )

    # v4.4: 다중 구 표시 레이블
    region_display = ", ".join(selected_gus)
    region_full    = f"{sido}: {region_display}"

    # v4.4: OSM Nominatim용 region 문자열 생성 함수
    # Why: "경기도 고양시 일산동구" 3단계 형식은 Nominatim이 인식 못함
    #       도(道) 지역은 gu 값 자체가 "고양시 일산동구"로 충분히 특정됨
    #       광역시/특별시는 "서울특별시 강남구"로 중구 등 동명 구 구분 필요
    _PROVINCES = {
        "경기도", "강원특별자치도", "충청북도", "충청남도",
        "전북특별자치도", "전라남도", "경상북도", "경상남도", "제주특별자치도",
    }
    def _make_region_str(sido_name: str, gu_name: str) -> str:
        """OSM Nominatim 호환 region 문자열 생성.
        Why: v4.3에서는 사용자가 '일산동구'처럼 짧게 입력 → OSM 폴리곤 정상 반환.
             '경기도 고양시 일산동구' 또는 '고양시 일산동구' 형식은 OSM이 인식 실패."""
        if sido_name == "세종특별자치시":
            return "세종특별자치시"
        if sido_name in _PROVINCES:
            # "고양시 일산동구" → "일산동구" / "부천시" → "부천시"
            parts = gu_name.split()
            return parts[-1] if len(parts) > 1 else gu_name
        # 광역시/특별시: "서울특별시 강남구" (중구 등 동명 구 구분용)
        return f"{sido_name} {gu_name}"

    current_step = "초기화"
    try:
        with st.status(
            f"📊 `{region_full}` / `{label}` 분석 진행 중...",
            expanded=True,
        ) as status:
            # Step 1. 구별 데이터 수집 → 병합 (v4.4: 다중 구 지원)
            # Why: 각 구별로 캐싱이 적용되므로 이전에 수집한 구는 즉시 반환
            current_step = "Step 1/7 · 행정경계·경쟁업체·교통·인구 수집"
            st.write(f"📥 {current_step}")

            boundaries  = []
            competitors = []
            transports  = []
            populations = []
            workplaces  = []
            parkings    = []
            diversities = []
            land_uses   = []
            buildings_l = []
            roads_l     = []
            pop_source  = "unknown"

            n_gus = len(selected_gus)
            for idx, gu in enumerate(selected_gus):
                region_str = _make_region_str(sido, gu)
                st.write(f"  · [{idx+1}/{n_gus}] {gu}")

                cached = _cached_collect_all(
                    region=region_str, category=preset, keyword=keyword,
                    cell_size_m=cell_size,
                    vworld_key=vworld_key, building_key=building_key,
                    _kakao_key=kakao_key,
                )
                boundaries.append(_json_to_gdf(cached["boundary"]))
                competitors.append(_json_to_gdf(cached["competitor"]))
                transports.append(_json_to_gdf(cached["transport"]))
                populations.append(_json_to_gdf(cached["population"]))
                workplaces.append(_json_to_gdf(cached.get("workplace")))
                parkings.append(_json_to_gdf(cached.get("parking")))
                diversities.append(_json_to_gdf(cached.get("diversity")))
                land_uses.append(_json_to_gdf(cached.get("land_use")))
                buildings_l.append(_json_to_gdf(cached.get("buildings")))
                roads_l.append(_json_to_gdf(cached.get("roads")))
                pop_source = cached.get("pop_source", pop_source)

            # 병합 — 카카오 ID 보유 데이터셋은 인접 구 경계 중첩 시 중복 들어오므로 dedup
            current_step = "Step 2/7 · 데이터 병합·중복 제거"
            st.write(f"🔗 {current_step}")
            boundary   = _merge_gdfs(boundaries)
            competitor = _merge_gdfs(competitors,  dedup_col="biz_id")
            transport  = _merge_gdfs(transports,   dedup_col="stop_id")
            population = _merge_gdfs(populations,  dedup_col="biz_id")
            workplace  = _merge_gdfs(workplaces)
            parking    = _merge_gdfs(parkings,     dedup_col="biz_id")
            diversity  = _merge_gdfs(diversities,  dedup_col="biz_id")
            land_use   = _merge_gdfs(land_uses)
            buildings  = _merge_gdfs(buildings_l,  dedup_col="address")
            roads      = _merge_gdfs(roads_l)

            if n_gus > 1:
                st.write(f"  · {n_gus}개 구 병합 완료")

            # Step 3. 소득·월세 데이터 수집 — 구별 캐싱 후 병합
            current_step = "Step 3/7 · 소득·월세 수집"
            st.write(f"💰 {current_step}")
            from src.rent_income_client import assign_nearest_to_grid
            income_gdfs = []
            rent_gdfs   = []
            income_source = "none"

            if data_go_kr_key:
                for idx, gu in enumerate(selected_gus):
                    region_str = _make_region_str(sido, gu)
                    # 개별 boundary JSON 필요 (구별 API 호출)
                    cached_gu = _cached_collect_all(
                        region=region_str, category=preset, keyword=keyword,
                        cell_size_m=cell_size,
                        vworld_key=vworld_key, building_key=building_key,
                        _kakao_key=kakao_key,
                    )
                    boundary_json = cached_gu["boundary"]
                    income_json, rent_json = _cached_income_rent(
                        region=region_str, _data_go_key=data_go_kr_key,
                        boundary_json=boundary_json,
                    )
                    income_gdfs.append(_json_to_gdf(income_json))
                    rent_gdfs.append(_json_to_gdf(rent_json))

                income_gdf = _merge_gdfs(income_gdfs)
                rent_gdf   = _merge_gdfs(rent_gdfs)
                if income_gdf is not None or rent_gdf is not None:
                    income_source = "data.go.kr"
                if income_gdf is not None:
                    st.write(f"  ✅ 소득: {len(income_gdf)}개 동")
                else:
                    st.write("  ⚠️ 소득 데이터 수집 실패 — 해당 팩터 제외")
                if rent_gdf is not None:
                    st.write(f"  ✅ 월세: {len(rent_gdf)}개 동")
                else:
                    st.write("  ⚠️ 월세 데이터 수집 실패 — 해당 팩터 제외")
            else:
                income_gdf = None
                rent_gdf   = None
                st.write("  💡 공공데이터포털 키 없음 — 소득·월세 팩터 생략")

            # 용도지역·건축물·도로 수집 결과 안내
            if land_use is not None and len(land_use) > 0:
                source_label = "Vworld" if vworld_key else "OSM"
                st.write(f"  ✅ 용도지역: {len(land_use)}개 폴리곤 ({source_label})")
            else:
                st.write("  ⚠️ 용도지역 수집 실패 — 하드 필터 없이 진행")

            if buildings is not None and len(buildings) > 0:
                st.write(f"  ✅ 상가건물: {len(buildings)}건")
            elif building_key:
                st.write("  ⚠️ 상가건물 수집 실패 — 해당 팩터 제외")

            if roads is not None and len(roads) > 0:
                st.write(f"  ✅ 도로: {len(roads)}개 세그먼트")

            # Step 4. 격자 생성 (11팩터) — 병합된 boundary 사용
            current_step = "Step 4/7 · 격자 생성 및 지표 집계"
            st.write(f"🗺️ {current_step}")
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
            st.write(f"  · 격자 {len(grid):,}개 생성")

            # 소득·월세 격자 집계
            if income_gdf is not None:
                grid = assign_nearest_to_grid(grid, income_gdf, "avg_price", "income")
            else:
                grid["income"] = 0.0

            if rent_gdf is not None:
                grid = assign_nearest_to_grid(grid, rent_gdf, "monthly_rent", "rent")
            else:
                grid["rent"] = 0.0

            # Step 5. 11팩터 점수화
            current_step = "Step 5/7 · 11팩터 점수화"
            st.write(f"📈 {current_step}")
            from src.scoring_Ver4_3 import score_and_rank
            scored, top, profile = score_and_rank(
                grid,
                preset=preset,
                keyword=keyword,
                top_n=top_n,
            )

            # Step 6. 클러스터링
            current_step = "Step 6/7 · 클러스터링"
            st.write(f"🔍 {current_step}")
            from src.cluster import run_cluster_analysis
            cluster_gdf, cluster_summary, gap_gdf = run_cluster_analysis(
                scored,
                score_threshold=0.6,
                eps_m=cell_size * 3,
                min_samples=3,
                comp_threshold=2,
                pop_threshold=0.3,
            )

            # Step 7. 차트 저장
            current_step = "Step 7/7 · 차트·CSV 생성"
            st.write(f"📊 {current_step}")
            out_dir    = Path("output")
            out_dir.mkdir(exist_ok=True)
            safe_label = label.replace(" ", "_")
            safe_gus   = "+".join(g.replace(" ", "") for g in selected_gus)
            prefix     = f"{sido}_{safe_gus}_{safe_label}"

            from src.visualizer_Ver4_2 import plot_score_bar
            top["name"] = top["grid_id"]
            chart_path  = plot_score_bar(
                top, name_col="name",
                out_path=str(out_dir / f"{prefix}_scores.png"),
            )
            csv_path = out_dir / f"{prefix}_top{top_n}.csv"
            top.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8-sig")

            status.update(
                label=f"✅ 분석 완료 · {region_full} / {label}",
                state="complete",
                expanded=False,
            )

        st.session_state["analysis_cache"] = {
            "region":          region_full,
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
        # 단계 컨텍스트 + 흔한 원인에 대한 힌트 제공
        err_str = str(e)
        err_low = err_str.lower()
        hints = []
        if "401" in err_str or "unauthorized" in err_low or "invalid api key" in err_low:
            hints.append("API 키가 잘못되었거나 만료되었을 수 있습니다 — 사이드바 ⚙️ API 키 설정에서 확인하세요.")
        if "timeout" in err_low or "timed out" in err_low or "read timeout" in err_low:
            hints.append("외부 API 응답이 지연되었습니다 — 잠시 후 다시 시도하거나 분석 구 수를 줄여보세요.")
        if "not found" in err_low or "찾을 수 없" in err_str or "geocod" in err_low:
            hints.append("지역명을 인식하지 못했습니다 — 카카오 키가 유효한지, 다른 구를 선택하면 되는지 확인하세요.")
        if "429" in err_str or "rate limit" in err_low or "too many" in err_low:
            hints.append("API 요청 한도 초과 — 약 1분 후 다시 시도하세요.")

        st.error(f"❌ **{current_step}**에서 오류 발생\n\n```\n{err_str}\n```")
        if hints:
            st.warning("💡 **가능한 원인**\n\n" + "\n".join(f"- {h}" for h in hints))
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

# ─── v5.0: 결과 요약 헤드라인 ─────────────────────────────
st.markdown("---")
if len(top) > 0:
    _top1 = top.iloc[0]
    _top1_score = _top1.get("score", 0)
    _top1_comp  = int(_top1.get("competitor_cnt", 0))
    _top1_pop   = int(_top1.get("population", 0))
    _top1_trans = int(_top1.get("transport_score", 0))
    _top1_grid  = _top1.get("grid_id", "-")

    # 경쟁 수준 자연어
    if _top1_comp <= 2:
        _comp_phrase = f"경쟁 업체 **{_top1_comp}건**으로 희소"
    elif _top1_comp <= 8:
        _comp_phrase = f"경쟁 업체 **{_top1_comp}건** (적정)"
    else:
        _comp_phrase = f"경쟁 업체 **{_top1_comp}건** (포화)"

    # 인구 수준 자연어 (SGIS 사용 시 명확, proxy면 제한적)
    if pop_source == "sgis":
        _pop_phrase = f"실인구 약 **{_top1_pop:,}명**"
    elif pop_source == "apartment_proxy":
        _pop_phrase = f"아파트 proxy **{_top1_pop}건**"
    else:
        _pop_phrase = ""

    _hints = [
        f"점수 **{_top1_score:.2f}** · {_comp_phrase}",
    ]
    if _pop_phrase:
        _hints.append(_pop_phrase)
    if _top1_trans > 0:
        _hints.append(f"교통 점수 **{_top1_trans}**")
    if n_gap > 0:
        _hints.append(f"경쟁 공백 지역 **{n_gap}셀** 발견")

    st.markdown(
        f"### 🏆 1위 후보지 · 격자 `{_top1_grid}`\n"
        + " · ".join(_hints) + "  \n"
        f"_지도의 **1번 마커**에서 위치 확인 — 아래 테이블에서 하위 순위 비교 가능_"
    )

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

# 전체 격자 상세 테이블 (v4.4: 후보지 + 비후보지 모두 표시)
st.markdown("---")
st.subheader("📋 격자 상세 데이터")
st.caption("점수 순 정렬. 상위 후보지 행을 클릭하면 지도에서 주황색으로 강조합니다.")

# scored에 rank 컬럼 병합 (후보지만 순위 있음, 나머지 빈칸)
all_cells = scored.copy()
if "rank" not in all_cells.columns:
    all_cells["rank"] = None
if "rank" in top.columns:
    rank_map = top.set_index("grid_id")["rank"]
    all_cells["rank"] = all_cells["grid_id"].map(rank_map)
all_cells = all_cells.sort_values("score", ascending=False).reset_index(drop=True)

display_cols = [
    "rank", "grid_id", "population", "floating", "workplace",
    "competitor_cnt", "transport_score", "parking_cnt", "diversity",
    "income", "rent", "commercial_cnt", "road_score", "zone_score", "score",
]
available = [c for c in display_cols if c in all_cells.columns]

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
    all_cells[available].style.format(format_dict, na_rep="").background_gradient(
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
    sel_row  = all_cells.iloc[rows[0]]
    sel_rank = sel_row.get("rank")
    # 순위가 있는 행(상위 후보지)만 지도 강조
    if sel_rank is not None and not (isinstance(sel_rank, float) and np.isnan(sel_rank)):
        new_rank = int(sel_rank)
        centroid = sel_row["geometry"].centroid
        if new_rank != st.session_state["selected_rank"]:
            st.session_state["selected_rank"] = new_rank
            st.session_state["map_center"]    = [centroid.y, centroid.x]
            st.session_state["map_zoom"]      = 15
            st.rerun()
    else:
        # 비후보지 클릭 → 해당 위치로 지도 이동만
        centroid = sel_row["geometry"].centroid
        st.session_state["selected_rank"] = None
        st.session_state["map_center"]    = [centroid.y, centroid.x]
        st.session_state["map_zoom"]      = 15
        st.rerun()

elif st.session_state["selected_rank"] is not None:
    st.session_state["selected_rank"] = None
    st.session_state["map_center"]    = None
    st.session_state["map_zoom"]      = 13
    st.rerun()

# 다운로드
_dl1, _dl2 = st.columns(2)
with _dl1:
    with open(csv_path, "rb") as f:
        st.download_button(
            label="📥 결과 CSV 다운로드",
            data=f,
            file_name=f"{prefix}_top{top_n_cached}.csv",
            mime="text/csv",
            use_container_width=True,
        )

with _dl2:
    # v5.0: folium 지도를 interactive HTML로 저장 → 외부 공유/프레젠테이션
    # Why: PNG는 정적이고 CSV는 숫자뿐이라 insight 전달이 어려움. HTML 지도는
    #      수신자가 별도 도구 없이 브라우저에서 확대·레이어 토글까지 가능.
    _map_html = folium_map.get_root().render().encode("utf-8")
    st.download_button(
        label="🗺️ 지도 HTML 다운로드",
        data=_map_html,
        file_name=f"{prefix}_map.html",
        mime="text/html",
        use_container_width=True,
        help="브라우저에서 바로 열리는 interactive 지도. Slack/이메일 공유용.",
    )
