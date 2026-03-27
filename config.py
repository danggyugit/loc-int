# config.py — 전역 상수 정의
# 하드코딩 금지: 모든 모듈은 이 파일의 상수를 참조할 것

import os
from pathlib import Path

# ─── 경로 ────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
DATA_RAW_DIR   = BASE_DIR / "data" / "raw"
DATA_PROC_DIR  = BASE_DIR / "data" / "processed"
OUTPUT_DIR     = BASE_DIR / "output"
PRESETS_DIR    = BASE_DIR / "presets"

# ─── 좌표계 ──────────────────────────────────────────────
CRS_WGS84 = "EPSG:4326"   # 저장·시각화 기준
CRS_KOREA = "EPSG:5179"   # 분석(거리 계산) 기준 — Korea 2000 TM (미터 단위)

# ─── 격자 셀 크기 ─────────────────────────────────────────
GRID_SIZE_OPTIONS = [250, 500, 1000, 2000]   # 허용값 (미터)
GRID_SIZE_DEFAULT = 500

# ─── 반경 분석 ────────────────────────────────────────────
BUFFER_RADIUS_DEFAULT = 500   # 미터

# ─── 분석 지역 ────────────────────────────────────────────
# None = 전국, 시·도명 문자열 = 해당 지역만 필터
REGION_DEFAULT = None

# 시·도 코드 매핑 (행정구역 코드 앞 2자리)
SIDO_CODE = {
    "서울특별시":  "11",
    "부산광역시":  "21",
    "대구광역시":  "22",
    "인천광역시":  "23",
    "광주광역시":  "24",
    "대전광역시":  "25",
    "울산광역시":  "26",
    "세종특별자치시": "29",
    "경기도":     "31",
    "강원특별자치도": "32",
    "충청북도":   "33",
    "충청남도":   "34",
    "전라북도":   "35",
    "전라남도":   "36",
    "경상북도":   "37",
    "경상남도":   "38",
    "제주특별자치도": "39",
}

# ─── 카카오 로컬 API ──────────────────────────────────────
KAKAO_API_KEY         = os.environ.get("KAKAO_API_KEY")
KAKAO_LOCAL_URL       = "https://dapi.kakao.com/v2/local/search/keyword.json"
KAKAO_CATEGORY_URL    = "https://dapi.kakao.com/v2/local/search/category.json"  # 카테고리 반경 검색
KAKAO_MAX_PAGE        = 45      # 최대 페이지 수
KAKAO_PAGE_SIZE       = 15      # 페이지당 결과 수 (최대 15)
KAKAO_RADIUS_MAX      = 20000   # 반경 최대값 (미터)

# 카카오 업종 카테고리 코드
KAKAO_CATEGORY = {
    "cafe":        "CE7",
    "restaurant":  "FD6",
    "hospital":    "HP8",
    "convenience": "CS2",
    "mart":        "MT1",
    "pharmacy":    "PM9",
}

# ─── 통계청 SGIS API ────────────────────────────────────
SGIS_CONSUMER_KEY    = os.environ.get("SGIS_CONSUMER_KEY")
SGIS_CONSUMER_SECRET = os.environ.get("SGIS_CONSUMER_SECRET")
SGIS_BASE_URL        = "https://sgisapi.kostat.go.kr/OpenAPI3"
SGIS_AUTH_URL         = f"{SGIS_BASE_URL}/auth/authentication.json"
SGIS_GRID_STAT_URL    = f"{SGIS_BASE_URL}/stats/grid.json"

# SGIS 격자 레벨 → 셀 크기 매핑 (미터)
SGIS_GRID_LEVELS = {
    100:  "1",
    500:  "2",
    1000: "3",
}

# ─── 점수화 기본 가중치 ───────────────────────────────────
DEFAULT_WEIGHTS = {
    "population":    0.20,
    "floating":      0.20,
    "workplace":     0.15,
    "competitor":    0.25,
    "accessibility": 0.20,
}
