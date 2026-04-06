# app_Ver4.4.py — 입지 선정 분석 툴 v4.4 (Streamlit 앱)
# 실행: streamlit run app_Ver4.4.py
#
# v4.3 대비 개선 사항:
#   1. 시/도 → 구/군/시 2단계 드롭다운 선택 (text_input → selectbox + multiselect)
#   2. 다중 구 선택 → 통합 분석 지원 (각 구별 데이터 수집 후 병합)
#
# 이전 버전(v4.3) 기능 유지:
#   - 용도지역 하드 필터, 상가건물/도로접근성 팩터, 11팩터 점수 모델
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

@st.cache_data(show_spinner=False, ttl=3600)
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


@st.cache_data(show_spinner=False, ttl=3600)
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


def _merge_gdfs(gdfs):
    """여러 GeoDataFrame을 하나로 병합. None/빈 항목은 제외."""
    import geopandas as gpd
    import pandas as pd
    valid = [g for g in gdfs if g is not None and len(g) > 0]
    if not valid:
        return None
    merged = gpd.GeoDataFrame(pd.concat(valid, ignore_index=True))
    merged = merged.set_crs(valid[0].crs, allow_override=True)
    return merged

# ─────────────────────────────────────────────────────────
# 페이지 설정
# ─────────────────────────────────────────────────────────

st.set_page_config(
    page_title="입지 선정 분석 툴 v4.4",
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
# 시/도 → 구/군/시 매핑 (v4.4: 2단계 드롭다운용)
# ─────────────────────────────────────────────────────────

SIDO_GU_MAP = {
    "서울특별시": [
        "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구",
        "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구",
        "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구",
    ],
    "부산광역시": [
        "강서구", "금정구", "기장군", "남구", "동구", "동래구", "부산진구", "북구",
        "사상구", "사하구", "서구", "수영구", "연제구", "영도구", "중구", "해운대구",
    ],
    "대구광역시": [
        "남구", "달서구", "달성군", "동구", "북구", "서구", "수성구", "중구",
    ],
    "인천광역시": [
        "강화군", "계양구", "남동구", "동구", "미추홀구", "부평구", "서구", "연수구", "옹진군", "중구",
    ],
    "광주광역시": ["광산구", "남구", "동구", "북구", "서구"],
    "대전광역시": ["대덕구", "동구", "서구", "유성구", "중구"],
    "울산광역시": ["남구", "동구", "북구", "울주군", "중구"],
    "세종특별자치시": ["세종시"],
    "경기도": [
        "수원시 장안구", "수원시 권선구", "수원시 팔달구", "수원시 영통구",
        "성남시 수정구", "성남시 중원구", "성남시 분당구",
        "안양시 만안구", "안양시 동안구",
        "안산시 상록구", "안산시 단원구",
        "고양시 덕양구", "고양시 일산동구", "고양시 일산서구",
        "용인시 처인구", "용인시 기흥구", "용인시 수지구",
        "부천시", "화성시", "남양주시", "평택시", "의정부시", "시흥시", "파주시",
        "김포시", "광명시", "광주시", "군포시", "하남시", "오산시", "이천시",
        "안성시", "의왕시", "양주시", "포천시", "여주시", "동두천시", "과천시",
        "가평군", "양평군", "연천군",
    ],
    "강원특별자치도": [
        "춘천시", "원주시", "강릉시", "동해시", "태백시", "속초시", "삼척시",
        "홍천군", "횡성군", "영월군", "평창군", "정선군", "철원군", "화천군",
        "양구군", "인제군", "고성군", "양양군",
    ],
    "충청북도": [
        "청주시 상당구", "청주시 서원구", "청주시 흥덕구", "청주시 청원구",
        "충주시", "제천시", "보은군", "옥천군", "영동군", "증평군", "진천군",
        "괴산군", "음성군", "단양군",
    ],
    "충청남도": [
        "천안시 동남구", "천안시 서북구",
        "공주시", "보령시", "아산시", "서산시", "논산시", "계룡시", "당진시",
        "금산군", "부여군", "서천군", "청양군", "홍성군", "예산군", "태안군",
    ],
    "전북특별자치도": [
        "전주시 완산구", "전주시 덕진구",
        "군산시", "익산시", "정읍시", "남원시", "김제시",
        "완주군", "진안군", "무주군", "장수군", "임실군", "순창군", "고창군", "부안군",
    ],
    "전라남도": [
        "목포시", "여수시", "순천시", "나주시", "광양시",
        "담양군", "곡성군", "구례군", "고흥군", "보성군", "화순군", "장흥군",
        "강진군", "해남군", "영암군", "무안군", "함평군", "영광군", "장성군",
        "완도군", "진도군", "신안군",
    ],
    "경상북도": [
        "포항시 남구", "포항시 북구",
        "경주시", "김천시", "안동시", "구미시", "영주시", "영천시", "상주시",
        "문경시", "경산시", "의성군", "청송군", "영양군", "영덕군", "청도군",
        "고령군", "성주군", "칠곡군", "예천군", "봉화군", "울진군", "울릉군",
    ],
    "경상남도": [
        "창원시 의창구", "창원시 성산구", "창원시 마산합포구", "창원시 마산회원구", "창원시 진해구",
        "진주시", "통영시", "사천시", "김해시", "밀양시", "거제시", "양산시",
        "의령군", "함안군", "창녕군", "고성군", "남해군", "하동군",
        "산청군", "함양군", "거창군", "합천군",
    ],
    "제주특별자치도": ["제주시", "서귀포시"],
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

st.title("📍 입지 선정 분석 툴 v4.4")
st.caption("시/도와 구를 선택하면 카카오 API로 데이터를 수집하고 최적 입지를 분석합니다. 다중 구 통합 분석을 지원합니다.")

if not run_btn and st.session_state["analysis_cache"] is None:
    st.info("왼쪽 사이드바에서 지역·업종·셀 크기를 설정하고 **분석 시작** 버튼을 누르세요.")
    st.markdown("""
    **사용 방법**
    1. 카카오 REST API 키 입력
    2. 공공데이터포털 API 키 입력 (선택 — 소득·월세 분석용)
    3. Vworld / 건축물대장 API 키 입력 (선택 — 용도지역·상가건물 분석용)
    4. 시/도 선택 → 구/군/시 다중 선택 (여러 구를 동시에 분석 가능)
    5. 업종 프리셋 선택 또는 키워드 직접 입력 (예: 도자기 공방)
    6. 분석 시작 클릭

    **v4.4 개선 사항**
    - 시/도 → 구/군/시 2단계 드롭다운 선택 (텍스트 입력 → 드롭다운)
    - 다중 구 선택 → 통합 분석 지원 (경계 영역 자동 병합)
    - v4.3 기능 유지: 용도지역 하드 필터, 상가건물, 도로접근성, 11팩터 모델
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

    os.environ["KAKAO_API_KEY"] = kakao_key
    if data_go_kr_key:
        os.environ["DATA_GO_KR_API_KEY"] = data_go_kr_key
    if sgis_key:
        os.environ["SGIS_CONSUMER_KEY"] = sgis_key
    if sgis_secret:
        os.environ["SGIS_CONSUMER_SECRET"] = sgis_secret

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

    st.markdown(f"### 📊 `{region_full}` / `{label}` 분석 중...")
    progress = st.progress(0, text="데이터 수집 중...")

    try:
        # Step 1. 구별 데이터 수집 → 병합 (v4.4: 다중 구 지원)
        # Why: 각 구별로 캐싱이 적용되므로 이전에 수집한 구는 즉시 반환
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
            pct = int((idx / n_gus) * 15)
            progress.progress(pct, text=f"[{idx+1}/{n_gus}] {gu} 데이터 수집 중...")

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

        # 병합
        boundary   = _merge_gdfs(boundaries)
        competitor = _merge_gdfs(competitors)
        transport  = _merge_gdfs(transports)
        population = _merge_gdfs(populations)
        workplace  = _merge_gdfs(workplaces)
        parking    = _merge_gdfs(parkings)
        diversity  = _merge_gdfs(diversities)
        land_use   = _merge_gdfs(land_uses)
        buildings  = _merge_gdfs(buildings_l)
        roads      = _merge_gdfs(roads_l)

        if n_gus > 1:
            st.success(f"{n_gus}개 구 데이터 수집·병합 완료")

        progress.progress(20, text="소득·월세 데이터 수집 중...")

        # Step 2. 소득·월세 데이터 수집 — 구별 캐싱 후 병합
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
                st.success(f"소득 데이터 수집 완료: {len(income_gdf)}개 동")
            else:
                st.warning("소득 데이터(아파트매매) 수집 실패 — 해당 팩터 제외하고 진행합니다.")
            if rent_gdf is not None:
                st.success(f"월세 데이터 수집 완료: {len(rent_gdf)}개 동")
            else:
                st.warning("월세 데이터(전월세) 수집 실패 — 해당 팩터 제외하고 진행합니다.")
        else:
            income_gdf = None
            rent_gdf   = None
            st.info("💡 공공데이터포털 API 키를 입력하면 소득·월세 데이터도 분석에 반영됩니다.")

        # 용도지역·건축물·도로 수집 결과 안내
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

        # Step 3. 격자 생성 (11팩터) — 병합된 boundary 사용
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

        # Step 4. 소득·월세 격자 집계
        if income_gdf is not None:
            grid = assign_nearest_to_grid(grid, income_gdf, "avg_price", "income")
        else:
            grid["income"] = 0.0

        if rent_gdf is not None:
            grid = assign_nearest_to_grid(grid, rent_gdf, "monthly_rent", "rent")
        else:
            grid["rent"] = 0.0

        progress.progress(50, text="점수화 중...")

        # Step 5. 11팩터 점수화
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

        progress.progress(100, text="완료!")
        progress.empty()

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

# CSV 다운로드
with open(csv_path, "rb") as f:
    st.download_button(
        label="📥 결과 CSV 다운로드",
        data=f,
        file_name=f"{prefix}_top{top_n_cached}.csv",
        mime="text/csv",
    )
