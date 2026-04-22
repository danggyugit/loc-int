# app_Ver5.1.py — 입지 선정 분석 툴 (단일 메인 파일)
# 실행: streamlit run app_Ver5.1.py
#
# 버전 관리 종료 (2026-04-22~): 파일명은 유지하되 이 파일에 누적 수정.
# 변경 이력은 git commit으로 추적.
#
# 누적 기능 요약:
#   [주소·시각화]
#     - 상위 후보지에 법정동+도로명 주소 자동 부여 (격자 ID 대체)
#     - 11팩터 레이더 차트 (1위 프로필, 2위 오버레이)
#     - 점수 분포 히스토그램 + 1위 percentile 표시
#   [UX]
#     - 결과 요약 헤드라인, 지도 HTML 다운로드, 지도 색상 범례,
#       격자 테이블 필터(점수·경쟁·용도지역)
#     - st.status 단계별 진행, 에러 컨텍스트 힌트
#   [안전성]
#     - 세션별 API 키 격리 (session_keys), 분석 범위 상한 (구 5개·셀 8000개)
#     - 다중 구 분석 시 카카오 ID 중복 제거
#   [분석 엔진]
#     - 시/도 → 구/군/시 2단계 드롭다운, 다중 구 통합, 용도지역 하드 필터
#     - 상가건물·도로접근성 포함 11팩터 점수 모델, 캐시 TTL 24시간
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
import pandas as pd
import streamlit as st

# ── 한국어 폰트 설정 (다른 시각화 라이브러리 import 전에 설정) ──
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 한글 폰트: Windows는 Malgun Gothic, Linux(Streamlit Cloud)는 Nanum 계열 탐색
# Why: 기존 하드코딩된 Malgun Gothic은 Cloud에서 한글이 □로 깨짐. 자동 폴백.
import platform as _platform
import matplotlib.font_manager as _fm
_font_candidates = (
    ["Malgun Gothic", "NanumGothic", "NanumBarunGothic"]
    if _platform.system() == "Windows"
    else ["NanumGothic", "NanumBarunGothic", "Nanum Gothic", "UnDotum", "Malgun Gothic"]
)
_installed = {f.name for f in _fm.fontManager.ttflist}
for _fn in _font_candidates:
    if _fn in _installed:
        plt.rcParams["font.family"] = _fn
        break
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
    page_title="입지 선정 분석 툴 v5.1",
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

    # 모드 선택: 입지 분석(특정 구) vs 전국 탐색(사전수집 데이터)
    app_mode = st.radio(
        "모드",
        ["🔍 입지 분석", "🗺️ 전국 탐색"],
        horizontal=True,
        label_visibility="collapsed",
        key="app_mode",
    )
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

    # 입지 분석 모드 입력
    if app_mode == "🔍 입지 분석":
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
    else:
        # 전국 탐색 모드 입력
        st.markdown("---")
        st.subheader("전국 탐색 설정")
        from src import national_data as _nd
        _metrics = _nd.list_metrics(only_available=True)
        if not _metrics:
            st.warning("⚠️ 사전수집 데이터가 없습니다.\n\n"
                       "터미널에서 아래 명령으로 데이터 수집:\n"
                       "```\npython scripts/collect_sgis_national.py\n```")
            nat_metric_key = None
            nat_selected_sido = []
            nat_level_choice = "auto"
        else:
            _metric_options = {m["key"]: f"{m['label']} ({m['source']})" for m in _metrics}
            nat_metric_key = st.selectbox(
                "지표",
                options=list(_metric_options.keys()),
                format_func=lambda k: _metric_options[k],
            )
            _avail_sido = _nd.list_available_sido()
            _sido_labels = {code: name for code, name in _avail_sido}
            nat_selected_sido = st.multiselect(
                "시·도 (비우면 전국)",
                options=list(_sido_labels.keys()),
                format_func=lambda c: _sido_labels[c],
                help=f"수집된 {len(_avail_sido)}개 시·도 중 선택",
            )
            nat_level_choice = st.radio(
                "표시 단위",
                ["auto", "sigungu", "eupmyeondong"],
                format_func=lambda v: {"auto": "자동(줌별)", "sigungu": "시·군·구", "eupmyeondong": "읍·면·동"}[v],
                horizontal=True,
                help="자동: 줌 13 미만은 시·군·구, 13 이상은 읍·면·동",
            )

        # 입지 분석 모드 변수 기본값 (코드 호환)
        sido = list(SIDO_GU_MAP.keys())[0]
        selected_gus = []
        mode = "프리셋"
        preset = None
        keyword = None
        label = ""
        cell_size = 500
        top_n = 10
        run_btn = False

# ─────────────────────────────────────────────────────────
# 메인 화면
# ─────────────────────────────────────────────────────────

st.title("📍 입지 선정 분석 툴")

# ─── 전국 탐색 모드 페이지 ────────────────────────────────
if app_mode == "🗺️ 전국 탐색":
    from src import national_data as _nd
    st.caption("사전수집된 전국 인구·사업체·소득·교통 등을 시·군·구/읍·면·동 단위로 탐색합니다.")

    summary = _nd.coverage_summary()
    if not summary["ready"]:
        st.warning(
            "사전수집 데이터가 없습니다. 터미널에서 아래 명령으로 수집을 시작하세요:\n\n"
            "```bash\n"
            "# Windows PowerShell\n"
            "$env:SGIS_CONSUMER_KEY=\"...\"; $env:SGIS_CONSUMER_SECRET=\"...\"\n"
            "python scripts/collect_sgis_national.py\n"
            "```\n\n"
            "수집 완료 후 git commit·push 하면 Streamlit Cloud에 자동 반영됩니다."
        )
        st.stop()

    # 상태 카드
    _c1, _c2, _c3, _c4 = st.columns(4)
    _c1.metric("수집 시·도", f"{len(_nd.list_available_sido())} / {summary['total_sido']}")
    _c2.metric("활성 지표", f"{summary['metrics_active']} / {summary['metrics_total']}")
    _c3.metric("최근 갱신", (summary["last_updated"] or "-")[:10])
    _c4.metric("표시 단위",
               {"auto": "자동", "sigungu": "시·군·구", "eupmyeondong": "읍·면·동"}[nat_level_choice])

    if not nat_metric_key:
        st.info("사이드바에서 지표를 선택하세요.")
        st.stop()

    meta = _nd.metric_meta(nat_metric_key)
    st.markdown(f"### {meta['label']} ({meta['unit']}) · 출처 {meta['source']}")

    # 데이터 로드 — 자동 모드는 시·군·구로 시작 (줌 변경은 v2에서 자동 전환)
    _level = "sigungu" if nat_level_choice == "auto" else nat_level_choice
    _gdf = _nd.load_level(nat_selected_sido, _level)
    if _gdf is None or len(_gdf) == 0:
        st.warning("선택한 시·도에 해당 단위 데이터가 없습니다.")
        st.stop()

    # folium choropleth
    import folium
    from streamlit_folium import st_folium
    import branca.colormap as cm_branca

    centroid = _gdf.unary_union.centroid
    _zoom = 7 if nat_level_choice != "eupmyeondong" else 11

    fmap = folium.Map(
        location=[centroid.y, centroid.x],
        zoom_start=_zoom,
        tiles="cartodbpositron",
    )

    vals = _gdf[nat_metric_key].astype(float)
    vmin, vmax = float(vals.min()), float(vals.max())
    if vmin == vmax:
        vmax = vmin + 1.0
    cmap = cm_branca.linear.YlOrRd_09.scale(vmin, vmax)
    cmap.caption = f"{meta['label']} ({meta['unit']})"

    # 일반 GeoJson + style_function (Choropleth보다 유연)
    def _style_fn(feature):
        v = feature["properties"].get(nat_metric_key)
        return {
            "fillColor":   cmap(float(v)) if v is not None else "#cccccc",
            "color":       "#888",
            "weight":      0.4,
            "fillOpacity": 0.72,
        }

    folium.GeoJson(
        _gdf.to_json(),
        style_function=_style_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["adm_nm", nat_metric_key],
            aliases=["행정구역", meta["label"]],
            localize=True,
            sticky=False,
            labels=True,
        ),
    ).add_to(fmap)
    cmap.add_to(fmap)

    st_folium(fmap, height=620, use_container_width=True, returned_objects=[])

    # 상위 10개 지역 카드
    st.markdown("---")
    st.subheader(f"🏅 {meta['label']} 상위 10개 지역")
    _top = _gdf.nlargest(10, nat_metric_key)[["adm_nm", nat_metric_key]].reset_index(drop=True)
    _top.index = _top.index + 1
    _top.columns = ["행정구역", meta["label"]]
    st.dataframe(_top, use_container_width=True)

    st.caption(
        f"총 {len(_gdf):,}개 {('읍·면·동' if _level == 'eupmyeondong' else '시·군·구')} | "
        f"평균 {vals.mean():,.1f} | 최대 {vmax:,.1f} | 최소 {vmin:,.1f}"
    )
    st.stop()
# ─── 전국 탐색 모드 페이지 끝 ─────────────────────────────

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
            current_step = "Step 1/8 · 행정경계·경쟁업체·교통·인구 수집"
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
            current_step = "Step 2/8 · 데이터 병합·중복 제거"
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
            current_step = "Step 3/8 · 소득·월세 수집"
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
            current_step = "Step 4/8 · 격자 생성 및 지표 집계"
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
            current_step = "Step 5/8 · 11팩터 점수화"
            st.write(f"📈 {current_step}")
            from src.scoring_Ver4_3 import score_and_rank
            scored, top, profile = score_and_rank(
                grid,
                preset=preset,
                keyword=keyword,
                top_n=top_n,
            )

            # Step 6. 클러스터링
            current_step = "Step 6/8 · 클러스터링"
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

            # Step 7. 주소 역지오코딩 (상위 100개만 병렬 호출)
            # Why: 전체 격자 역지오코딩은 비용 과다 (셀당 1회 × 수백개 = 수 분).
            #      상위 100개만 법정동 + 상위 10개 도로명이면 2~5초로 단축.
            current_step = "Step 7/8 · 주소 부여 (상위 100개)"
            st.write(f"📮 {current_step}")
            try:
                from src.geocoding import annotate_addresses
                scored, top = annotate_addresses(
                    scored, top,
                    top_with_region=100,
                    top_with_road=10,
                )
                _n_addr = (scored["address"].str.len() > 0).sum()
                st.write(f"  · 법정동 {_n_addr:,}개 / 도로명 상위 {min(10, len(top))}개")
            except Exception as e:
                log.warning(f"역지오코딩 건너뜀: {e}")
                st.write("  ⚠️ 주소 조회 실패 — 격자 ID로 표시")
                if "address" not in scored.columns:
                    scored["address"] = ""
                if "display_address" not in top.columns:
                    top["display_address"] = top["grid_id"]

            # Step 8. 차트·CSV 저장
            current_step = "Step 8/8 · 차트·CSV 생성"
            st.write(f"📊 {current_step}")
            out_dir    = Path("output")
            out_dir.mkdir(exist_ok=True)
            safe_label = label.replace(" ", "_")
            safe_gus   = "+".join(g.replace(" ", "") for g in selected_gus)
            prefix     = f"{sido}_{safe_gus}_{safe_label}"

            from src.visualizer_Ver4_2 import (
                plot_score_bar, plot_radar_top1, plot_score_distribution,
            )
            # 바 차트 라벨: 도로명 주소 있으면 그걸, 없으면 grid_id
            top["name"] = top.get("display_address", top["grid_id"]).where(
                top.get("display_address", pd.Series([""] * len(top))).astype(str).str.len() > 0,
                top["grid_id"],
            )
            chart_path = plot_score_bar(
                top, name_col="name",
                out_path=str(out_dir / f"{prefix}_scores.png"),
            )
            radar_path = plot_radar_top1(
                scored, top,
                out_path=str(out_dir / f"{prefix}_radar.png"),
            )
            dist_path = plot_score_distribution(
                scored, top,
                out_path=str(out_dir / f"{prefix}_dist.png"),
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
            "radar_path":      radar_path,
            "dist_path":       dist_path,
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
radar_path      = cache.get("radar_path")
dist_path       = cache.get("dist_path")
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
    _top1_addr  = _top1.get("display_address", "") or _top1.get("address", "")

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

    # 헤더: 주소 있으면 주소 사용, 없으면 격자 ID
    _top1_title = _top1_addr if _top1_addr else f"격자 `{_top1_grid}`"
    st.markdown(
        f"### 🏆 1위 후보지 · {_top1_title}\n"
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
    # v5.1: 색상·마커 의미 안내 — 사용자가 바로 해석할 수 있도록
    st.caption(
        "**격자 색상(점수)**: 🟩 녹색 0.7+ (유망) · 🟨 노랑 0.3~0.7 (평균) · 🟥 빨강 <0.3 (비권장) | "
        "**마커**: 🔵 경쟁업체 · 1~N번 상위 후보지 | "
        "**좌하단 컨트롤**: 레이어 ON/OFF"
    )
    st_folium(folium_map, height=550, use_container_width=True, returned_objects=[])

with chart_col:
    st.subheader("📊 후보지 분석")
    # 3개 차트를 탭으로 구성: 바·레이더·분포
    _tab_bar, _tab_radar, _tab_dist = st.tabs(
        ["🏆 상위 점수", "🧭 1위 프로필", "📈 점수 분포"]
    )
    with _tab_bar:
        st.image(chart_path, use_container_width=True)
        st.caption(f"상위 {top_n_cached}개 후보지 점수 비교")
    with _tab_radar:
        if radar_path and Path(radar_path).exists():
            st.image(radar_path, use_container_width=True)
            st.caption("전체 격자 대비 각 팩터의 상위 %. 바깥쪽일수록 해당 팩터 우위.")
        else:
            st.info("레이더 차트 없음")
    with _tab_dist:
        if dist_path and Path(dist_path).exists():
            st.image(dist_path, use_container_width=True)
            st.caption("1위 점수의 희소성을 히스토그램으로 확인.")
        else:
            st.info("분포 차트 없음")

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

# v5.1: 결과 필터 — 수천 셀 중 의미있는 것만 빠르게 보기
with st.expander("🔎 필터", expanded=False):
    _f1, _f2, _f3 = st.columns(3)
    _min_score = _f1.slider(
        "최소 점수",
        min_value=0.0, max_value=1.0, value=0.0, step=0.05,
        help="이 값 이상인 셀만 표시",
    )
    _max_comp = 999
    if "competitor_cnt" in all_cells.columns:
        _max_observed = int(all_cells["competitor_cnt"].max()) if len(all_cells) > 0 else 50
        _max_comp = _f2.number_input(
            "경쟁업체 최대",
            min_value=0, max_value=max(_max_observed, 1), value=_max_observed, step=1,
            help="이 값 이하인 셀만 표시 (낮출수록 경쟁 희소 지역만)",
        )
    _zone_only = False
    if "zone_score" in all_cells.columns:
        _zone_only = _f3.checkbox(
            "입점 가능 지역만 (zone_score > 0)",
            value=False,
            help="전용주거·녹지 등 입점 불가 셀(zone_score=0) 제외",
        )

_filtered = all_cells[all_cells["score"] >= _min_score].copy()
if "competitor_cnt" in _filtered.columns:
    _filtered = _filtered[_filtered["competitor_cnt"] <= _max_comp]
if _zone_only and "zone_score" in _filtered.columns:
    _filtered = _filtered[_filtered["zone_score"] > 0]
_filtered = _filtered.reset_index(drop=True)

if len(_filtered) != len(all_cells):
    st.caption(f"🔎 필터 적용: 전체 **{len(all_cells):,}**개 중 **{len(_filtered):,}**개 표시")

all_cells = _filtered

display_cols = [
    "rank", "address", "grid_id", "population", "floating", "workplace",
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
        "address":         st.column_config.TextColumn("위치 (법정동)", width="medium"),
        "grid_id":         st.column_config.TextColumn("격자 ID", width="small"),
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
