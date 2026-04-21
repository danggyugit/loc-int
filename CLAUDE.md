# CLAUDE.md — 입지 선정 분석 툴 에이전트 팀

## 프로젝트 개요

- **목표:** 데이터 기반 입지 선정 분석 툴 (Python CLI + Jupyter Notebook)
- **참조 문서:** `PRD.md`, `TRD.md`
- **Python 인터프리터:** `C:\Python\python.exe`
- **작업 디렉터리:** `c:/Users/sk15y/claude/py/loc int/`

---

## 현재 활성 파일 (v5.1 기준)

| 영역 | 활성 파일 | 비고 |
|---|---|---|
| 메인 앱 | `app_Ver5.1.py` | v4.0~v5.0은 이력 보존용 |
| 점수화 | `src/scoring_Ver4_3.py` | 11팩터 |
| 시각화 | `src/visualizer_Ver4_2.py` | folium + matplotlib |
| 데이터 수집 | `src/collector.py`, `src/rent_income_client.py`, `src/building_client.py`, `src/vworld_client.py`, `src/sgis_client.py`, `src/keyword_classifier.py` | 단일 버전 |
| 분석 | `src/grid.py`, `src/cluster.py` | 단일 버전 |
| 세션 격리 | `src/session_keys.py` | API 키를 threading.local에 저장하여 다중 사용자 세션 격리 (v4.9 신규) |
| 구버전 | `src/_deprecated/` | scoring.py / scoring_Ver4_2.py / visualizer.py / visualizer_Ver4_1.py |
| 전역 상수 | `config.py` | `PRESETS`, `SIDO_GU_MAP`, 좌표계, API URL 등 |

**중요 — API 키 취급:** Streamlit Cloud는 하나의 Python 프로세스에서 여러 세션을 실행함. `os.environ` 쓰기는 프로세스 전역이라 사용자 간 키 유출 위험. **반드시 `session_keys.set_keys(...)`로 설정**하고 `session_keys.get("...")`로 읽을 것. ThreadPoolExecutor 사용 시 `snapshot()`/`apply()`로 worker에 전파.

새 버전 작업 시: `app_Ver<N+1>.py` 신규 생성, `src/` 내 활성 파일은 덮어쓰기 가능(버전 접미사 없는 모듈) 또는 `_Ver<N>.py` 신규 생성(점수화/시각화처럼 큰 변경). 구버전으로 대체되면 해당 파일을 `src/_deprecated/`로 이동하고 이 표를 갱신할 것.

---

## 에이전트 팀 구성

```
PM (Project Manager)
├── DA  (Data Agent)
│   ├── DA-K  (Kakao API Sub-agent)
│   └── DA-G  (GeoData Sub-agent)
├── AA  (Analysis Agent)
│   ├── AA-S  (Scoring Sub-agent)
│   └── AA-C  (Cluster Sub-agent)
├── VA  (Visualization Agent)
└── QA  (Quality Assurance Agent)
```

---

## 에이전트 역할 정의

### PM — Project Manager
**역할:** 전체 마일스톤 관리, 에이전트 간 작업 조율, PRD/TRD 기준 충족 여부 판단

**책임:**
- 마일스톤(M1~M5) 진행 상태 추적
- 각 에이전트 작업 완료 기준(Definition of Done) 판단
- 에이전트 간 인터페이스 충돌 감지 및 조정
- PRD/TRD 변경 발생 시 영향 범위 파악 및 공유

**산출물:** 마일스톤 체크리스트, 이슈 로그

**작업 시작 전 체크:**
- [ ] PRD.md, TRD.md 최신 버전 확인
- [ ] 현재 마일스톤 단계 확인
- [ ] 미결 사항(Open Questions) 확인

---

### DA — Data Agent
**역할:** 모든 데이터 로드·전처리·저장 담당 (`loader.py`)

**책임:**
- 데이터 소스별 로드 함수 구현
- 좌표계 표준화 (모든 출력 EPSG:4326)
- 결측·이상값 처리 및 경고 로그 출력
- `data/raw/` → `data/processed/` 파이프라인 구성

**산출물:** `src/loader.py`, `data/processed/` 내 정제 데이터

**규칙:**
- 모든 함수는 GeoDataFrame 반환
- API / CSV 폴백 분기는 반드시 환경변수 기준으로 처리
- 좌표 결측 행 제거 후 반드시 로그 출력

**하위 에이전트:**

#### DA-K — Kakao API Sub-agent
- **담당:** 카카오 로컬 API 호출 로직 (`_load_from_kakao()`)
- **환경변수:** `KAKAO_API_KEY`
- **주요 엔드포인트:** `https://dapi.kakao.com/v2/local/search/keyword.json`
- **카테고리 코드표:**

  | 코드 | 업종 |
  |---|---|
  | CE7 | 카페 |
  | FD6 | 음식점 |
  | HP8 | 병원 |
  | CS2 | 편의점 |
  | MT1 | 대형마트 |
  | PM9 | 약국 |

- **페이지네이션:** `page` 파라미터로 최대 45페이지 (총 최대 1,350건/키워드)
- **반경 검색:** `x`(경도), `y`(위도), `radius`(미터) 파라미터 활용

#### DA-G — GeoData Sub-agent
- **담당:** GIS 데이터 처리 (`load_boundary`, SHP/GeoJSON 파싱)
- **주요 라이브러리:** `geopandas`, `pyproj`
- **좌표계 변환 규칙:**
  - 입력 시: 어떤 좌표계든 → EPSG:4326으로 통일
  - 분석 시: EPSG:4326 → EPSG:5179 (거리 계산용)
  - 출력 시: EPSG:5179 → EPSG:4326 (folium용)
- **행정경계 데이터 출처:** 국토지리정보원 NGII

---

### AA — Analysis Agent
**역할:** 핵심 분석 알고리즘 구현 (`grid.py`, `buffer.py`)

**책임:**
- 격자 생성 및 지표 집계
- 반경 분석 (가변 반경 지원)
- 복수 후보지 일괄 비교

**산출물:** `src/grid.py`, `src/buffer.py`

**규칙:**
- 셀 크기·반경은 반드시 파라미터로 받을 것 (하드코딩 금지)
- 허용값 외 입력 시 `ValueError` + 허용값 안내 메시지
- 분석 연산은 항상 EPSG:5179 기준으로 수행

**하위 에이전트:**

#### AA-S — Scoring Sub-agent
- **담당:** 점수화 모델 구현 (`scoring.py`)
- **핵심 로직:**
  ```python
  # 각 지표 Min-Max 정규화 후 가중합
  score = pop_norm * w1 + float_norm * w2 - comp_norm * w3 + acc_norm * w4
  ```
- **가중치 우선순위:** CLI 직접 입력 > YAML 프리셋 > 기본값(균등 0.25)
- **검증:** 가중치 합계 != 1.0 이면 `ValueError`
- **산출물:** `src/scoring.py`, `presets/weights.yaml`

#### AA-C — Cluster Sub-agent
- **담당:** 핫스팟 클러스터링 (`cluster.py`)
- **알고리즘:** DBSCAN (`scikit-learn`)
- **출력:** 수요 밀집 지역 / 경쟁 공백 지역 레이블
- **산출물:** `src/cluster.py`

---

### VA — Visualization Agent
**역할:** 분석 결과 시각화 (`visualizer.py`)

**책임:**
- 격자 히트맵 지도 (folium)
- 반경 버퍼 오버레이 지도 (folium)
- 후보지 점수 바 차트 (matplotlib)
- 클러스터 지도 (folium)

**산출물:** `src/visualizer.py`, `output/` 내 HTML·PNG 파일

**규칙:**
- folium 지도는 HTML 파일로 저장 (`output/map_*.html`)
- 차트는 PNG로 저장 (`output/chart_*.png`)
- 모든 지도 출력 좌표계는 EPSG:4326

---

### QA — Quality Assurance Agent
**역할:** 각 모듈 완성 후 검증

**책임:**
- 함수 입출력 타입 검증
- 좌표계 변환 정합성 확인
- 가중치 합계 검증
- 셀 크기·반경 파라미터 경계값 테스트
- API 폴백(CSV) 전환 시나리오 테스트

**체크리스트 (모듈별 완료 기준):**
```
loader.py
  - [ ] GeoDataFrame 반환 확인
  - [ ] EPSG:4326 좌표계 확인
  - [ ] region 필터 전국/단일 시·도 동작 확인
  - [ ] API 없을 때 CSV 폴백 동작 확인

grid.py
  - [ ] 셀 크기 250 / 500 / 1000 / 2000 모두 동작
  - [ ] 허용값 외 입력 시 ValueError 발생
  - [ ] 격자가 행정경계 내로 한정됨

scoring.py
  - [ ] 점수 범위 0~1 확인
  - [ ] 가중치 합계 != 1.0 시 ValueError 발생
  - [ ] 프리셋 로드 정상 동작

buffer.py
  - [ ] 복수 반경 동시 처리 확인
  - [ ] 반경 내 포인트 집계 정확성 확인
```

---

## 공통 코딩 규칙

- 파일명 버전 관리: `파일명_Ver1.0.py` (기존 파일 수정 금지, 신규 버전 생성)
- 하드코딩 금지 → `config.py` 상수 사용
- 함수명: 동사로 시작 (`load_`, `make_`, `calc_`, `plot_`)
- 복잡한 로직은 **Why** 주석 작성
- 외부 입력(API, 파일, CLI) 반드시 검증

---

## 환경변수

| 변수 | 용도 | 필수 여부 |
|---|---|---|
| `KAKAO_API_KEY` | 카카오 로컬 API 인증 | 선택 (없으면 CSV 폴백) |
| `SGIS_CONSUMER_KEY` | 통계청 SGIS API 인증 | 선택 (없으면 아파트 proxy 폴백) |
| `SGIS_CONSUMER_SECRET` | 통계청 SGIS API 시크릿 | 선택 (없으면 아파트 proxy 폴백) |

PowerShell 설정:
```powershell
$env:KAKAO_API_KEY = "your_key"
$env:SGIS_CONSUMER_KEY = "your_key"
$env:SGIS_CONSUMER_SECRET = "your_secret"
```

SGIS API 키 발급: https://sgis.kostat.go.kr/developer/ 에서 회원가입 후 서비스 키 발급

---

## 마일스톤 진행 순서

| 단계 | 담당 에이전트 | 산출물 |
|---|---|---|
| M1 | DA (+ DA-K, DA-G) | `config.py`, `loader.py`, 샘플 데이터 |
| M2 | AA (+ AA-S) | `grid.py`, `scoring.py`, `weights.yaml` |
| M3 | AA + VA | `buffer.py`, `visualizer.py` |
| M4 | AA-C + VA | `cluster.py` |
| M5 | PM + QA | `main.py`, `demo.ipynb` |

각 마일스톤 완료 후 QA 검증 통과해야 다음 단계 진행.
