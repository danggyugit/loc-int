# TRD: 입지 선정 분석 툴 (Location Intelligence Tool)

**작성일:** 2026-03-27
**버전:** v0.1
**참조 문서:** PRD v0.2

---

## 1. 기술 스택

| 구분 | 선택 | 비고 |
|---|---|---|
| 언어 | Python 3.10+ | |
| 공간 분석 | `geopandas 0.14+`, `shapely 2.0+` | 격자·반경·클러스터링 |
| 좌표 변환 | `pyproj` | EPSG:4326 ↔ EPSG:5179 |
| 데이터 처리 | `pandas`, `numpy` | |
| 클러스터링 | `scikit-learn` | DBSCAN |
| 시각화 | `folium`, `matplotlib` | 지도 + 차트 |
| 설정 | `PyYAML` | 가중치 프리셋 |
| CLI | `argparse` (표준 라이브러리) | |
| 지도 API (조건부) | 카카오맵 REST API / 네이버 Places API | API 키 확보 시 활성화 |

---

## 2. 좌표계 전략

```
입력 데이터         →   저장 좌표계          →   분석 좌표계
EPSG:4326 (WGS84)      EPSG:4326 (WGS84)        EPSG:5179 (Korea TM)
(lat/lng)               (GeoDataFrame 기본)       (미터 단위 거리 계산)
```

- 모든 원본 데이터는 EPSG:4326으로 표준화 후 저장
- 격자 생성·반경 계산·면적 집계 시 EPSG:5179로 즉시 변환
- 시각화(folium) 출력 전 EPSG:4326으로 재변환

---

## 3. 프로젝트 디렉터리 구조

```
loc int/
├── PRD.md
├── TRD.md                      ← 이 문서
├── main.py                     ← CLI 진입점
├── config.py                   ← 전역 상수 (기본 셀 크기, 기본 반경 등)
├── presets/
│   └── weights.yaml            ← 업종별 가중치 프리셋
├── src/
│   ├── loader.py               ← 데이터 로드·검증·전처리
│   ├── grid.py                 ← 격자 생성 및 지표 집계
│   ├── scoring.py              ← 점수화 모델
│   ├── buffer.py               ← 반경 분석
│   ├── cluster.py              ← 핫스팟 클러스터링
│   └── visualizer.py           ← 지도·차트 출력
├── data/
│   ├── raw/                    ← 원본 데이터 (수동 다운로드)
│   │   ├── population/         ← 통계청 인구 CSV
│   │   ├── boundary/           ← NGII 행정경계 SHP/GeoJSON
│   │   ├── floating/           ← 유동인구 CSV
│   │   ├── competitor/         ← 사업자 등록 CSV
│   │   └── transport/          ← 지하철·버스 CSV
│   └── processed/              ← 전처리 완료 GeoParquet / CSV
└── notebooks/
    └── demo.ipynb
```

---

## 4. 모듈 설계

### 4.1 `config.py` — 전역 상수

```python
# 격자 셀 크기 옵션 (미터)
GRID_SIZE_OPTIONS = [250, 500, 1000, 2000]
GRID_SIZE_DEFAULT = 500

# 반경 분석 기본값 (미터)
BUFFER_RADIUS_DEFAULT = 500

# 좌표계
CRS_WGS84  = "EPSG:4326"
CRS_KOREA  = "EPSG:5179"   # Korea 2000 TM

# 분석 대상 시·도 코드 (None = 전국)
REGION_DEFAULT = None
```

---

### 4.2 `loader.py` — 데이터 로드 및 전처리

**책임:** 각 데이터 소스를 읽어 정규화된 GeoDataFrame으로 반환.

| 함수 | 입력 | 출력 |
|---|---|---|
| `load_population(path, region)` | CSV 경로, 시·도 코드 (선택) | GeoDataFrame (행정동 폴리곤 + 인구수) |
| `load_floating(path, region)` | CSV 경로, 시·도 코드 (선택) | GeoDataFrame (격자/행정동 + 유동인구) |
| `load_competitor(path, category, region)` | CSV 경로, 업종 코드, 시·도 코드 | GeoDataFrame (Point) |
| `load_transport(path, region)` | CSV 경로, 시·도 코드 (선택) | GeoDataFrame (Point) |
| `load_boundary(path, region)` | SHP/GeoJSON 경로, 시·도 코드 | GeoDataFrame (행정경계 폴리곤) |

**공통 전처리 규칙:**
- 결측 좌표 행 제거 후 로그 출력
- 전체 좌표계를 EPSG:4326으로 통일
- `region` 파라미터가 None이면 전국 전체 반환, 값이 있으면 해당 시·도 필터링

**데이터 로드 분기 (경쟁업체):**
```
API 키 존재 여부 확인
├── 있음 → 카카오맵 / 네이버 Places API 호출
└── 없음 → 공공데이터포털 CSV 로드 (폴백)
```

---

### 4.3 `grid.py` — 격자 생성 및 지표 집계

**책임:** 분석 영역을 정사각형 격자로 분할하고 셀별 지표를 집계.

| 함수 | 입력 | 출력 |
|---|---|---|
| `make_grid(boundary_gdf, cell_size_m)` | 행정경계 GDF, 셀 크기(m) | 격자 GeoDataFrame (EPSG:5179) |
| `aggregate_to_grid(grid_gdf, point_gdf, col_name)` | 격자 GDF, 포인트 GDF, 집계 컬럼명 | 격자 GDF + 집계 컬럼 추가 |

**격자 생성 알고리즘:**
```
1. boundary_gdf → EPSG:5179 변환
2. 전체 bbox(minx, miny, maxx, maxy) 산출
3. x, y 축 각각 cell_size_m 간격으로 격자 생성
4. boundary polygon과 교차(intersect)하는 셀만 보존
5. EPSG:4326으로 재변환하여 반환
```

**셀 크기 파라미터:**
- CLI: `--cell-size 500` (단위: 미터)
- 허용값: 250, 500, 1000, 2000
- 허용값 외 입력 시 ValueError 발생

---

### 4.4 `scoring.py` — 점수화 모델

**책임:** 격자 또는 후보지 단위로 가중치 기반 종합 점수 산출.

| 함수 | 입력 | 출력 |
|---|---|---|
| `normalize(series)` | pandas Series | Min-Max 정규화된 Series (0~1) |
| `calc_score(gdf, weights)` | GDF (지표 컬럼 포함), weights dict | GDF + `score` 컬럼 추가 |
| `rank_candidates(gdf, top_n)` | GDF + score, 순위 수 | 상위 N개 행 DataFrame |

**점수 계산 수식:**
```python
score = (
    pop_norm   * weights["population"]   +
    float_norm * weights["floating"]     -
    comp_norm  * weights["competitor"]   +
    acc_norm   * weights["accessibility"]
)
# 각 지표는 calc_score 내부에서 normalize() 후 적용
```

**가중치 입력 우선순위:**
```
CLI --weights '{"population":0.4,...}'
    > presets/weights.yaml 업종 프리셋
        > config.py 기본값 (균등 0.25)
```

---

### 4.5 `buffer.py` — 반경 분석

**책임:** 특정 좌표 기준 반경 내 데이터 집계.

| 함수 | 입력 | 출력 |
|---|---|---|
| `make_buffer(lat, lng, radius_m)` | 위도, 경도, 반경(m) | Shapely Polygon (EPSG:5179) |
| `query_within_buffer(buffer_poly, gdf)` | 버퍼 폴리곤, 대상 GDF | 반경 내 필터된 GDF |
| `summarize_buffer(lat, lng, radius_m, datasets)` | 좌표, 반경, 데이터셋 dict | 지표별 집계 결과 dict |

**반경 파라미터:**
- CLI: `--radius 500` (단위: 미터)
- 기본값: `BUFFER_RADIUS_DEFAULT = 500`
- 복수 반경 동시 분석: `--radius 300 500 1000`

---

### 4.6 `cluster.py` — 핫스팟 클러스터링

**책임:** 수요 밀집 지역 및 경쟁 공백 지역 탐지.

| 함수 | 입력 | 출력 |
|---|---|---|
| `find_demand_hotspot(gdf, score_col, eps_m, min_samples)` | 격자 GDF, 점수 컬럼, DBSCAN 파라미터 | 클러스터 레이블 GDF |
| `find_competition_gap(gdf, competitor_col, threshold)` | 격자 GDF, 경쟁 컬럼, 임계값 | 경쟁 공백 격자 GDF |

**알고리즘:** DBSCAN (`scikit-learn`)
- `eps`: 클러스터 반경 (미터, EPSG:5179 기준)
- `min_samples`: 최소 셀 수 (기본 3)

---

### 4.7 `visualizer.py` — 시각화

**책임:** 분석 결과를 지도(folium) 및 차트(matplotlib)로 출력.

| 함수 | 입력 | 출력 |
|---|---|---|
| `plot_grid_heatmap(grid_gdf, score_col, out_path)` | 격자 GDF, 점수 컬럼, 저장 경로 | HTML 지도 파일 |
| `plot_buffer_map(lat, lng, radius_m, gdf, out_path)` | 좌표, 반경, GDF, 저장 경로 | HTML 지도 파일 |
| `plot_score_bar(candidates_df, out_path)` | 후보지 DataFrame, 저장 경로 | PNG 파일 |
| `plot_cluster_map(cluster_gdf, out_path)` | 클러스터 GDF, 저장 경로 | HTML 지도 파일 |

---

### 4.8 `main.py` — CLI 진입점

**서브커맨드 구조:**

```
python main.py <subcommand> [options]

서브커맨드:
  grid      격자 분석 + 점수화
  buffer    반경 분석
  cluster   핫스팟 클러스터링
  compare   복수 후보지 비교
```

**공통 옵션:**
```
--region    시·도명 (예: 서울특별시) — 기본값: 전국
--preset    업종 프리셋 (예: cafe, hospital, convenience)
--weights   JSON 문자열로 가중치 직접 지정
--output    결과 저장 경로 (기본: ./output/)
```

**`grid` 서브커맨드 전용:**
```
--cell-size   격자 셀 크기(m) — 기본값: 500 / 허용값: 250, 500, 1000, 2000
```

**`buffer` 서브커맨드 전용:**
```
--lat         위도
--lng         경도
--radius      반경(m) — 기본값: 500 / 복수 입력 가능 (예: 300 500 1000)
```

**사용 예시:**
```bash
# 전국 격자 분석 (1km 셀, 카페 프리셋)
python main.py grid --cell-size 1000 --preset cafe

# 서울만 격자 분석 (500m 셀)
python main.py grid --region 서울특별시 --cell-size 500

# 특정 좌표 반경 분석 (300m, 500m, 1km 동시)
python main.py buffer --lat 37.5665 --lng 126.9780 --radius 300 500 1000

# 후보지 3곳 비교
python main.py compare --candidates candidates.csv --preset hospital
```

---

## 5. 데이터 스키마

### 5.1 인구 데이터 (정규화 후)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `adm_code` | str | 행정동 코드 (10자리) |
| `adm_name` | str | 행정동명 |
| `population` | int | 총 인구수 |
| `geometry` | Polygon | 행정동 경계 (EPSG:4326) |

### 5.2 경쟁업체 데이터 (정규화 후)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `biz_id` | str | 사업자 등록번호 |
| `category` | str | 업종 코드 |
| `name` | str | 상호명 |
| `lat` | float | 위도 |
| `lng` | float | 경도 |
| `geometry` | Point | (EPSG:4326) |

### 5.3 격자 결과 데이터

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `grid_id` | str | 격자 고유 ID |
| `population` | float | 셀 내 인구 집계 |
| `floating` | float | 셀 내 유동인구 집계 |
| `competitor_cnt` | int | 셀 내 경쟁업체 수 |
| `transport_cnt` | int | 셀 내 교통 인프라 수 |
| `score` | float | 종합 점수 (0~1) |
| `geometry` | Polygon | 격자 폴리곤 (EPSG:4326) |

---

## 6. 가중치 프리셋 스키마 (`weights.yaml`)

```yaml
cafe:
  population: 0.2
  floating: 0.4
  competitor: 0.2
  accessibility: 0.2

hospital:
  population: 0.4
  floating: 0.2
  competitor: 0.2
  accessibility: 0.2

convenience:
  population: 0.3
  floating: 0.3
  competitor: 0.2
  accessibility: 0.2

default:
  population: 0.25
  floating: 0.25
  competitor: 0.25
  accessibility: 0.25
```

> 가중치 합계는 반드시 1.0. `scoring.py` 로드 시 자동 검증.

---

## 7. API 키 분기 처리

카카오맵 / 네이버 API 키 확보 여부에 따라 `loader.py`가 자동 분기.

```python
# loader.py 내부
KAKAO_API_KEY = os.environ.get("KAKAO_API_KEY")
NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID")

def load_competitor(path, category, region):
    if KAKAO_API_KEY:
        return _load_from_kakao(category, region)
    elif NAVER_CLIENT_ID:
        return _load_from_naver(category, region)
    else:
        return _load_from_csv(path, category, region)  # 폴백
```

환경변수 설정:
```bash
export KAKAO_API_KEY=your_key
export NAVER_CLIENT_ID=your_id
export NAVER_CLIENT_SECRET=your_secret
```

---

## 8. 에러 처리 원칙

| 상황 | 처리 방법 |
|---|---|
| 셀 크기 허용값 외 입력 | `ValueError` + 허용값 안내 메시지 출력 |
| 데이터 파일 없음 | `FileNotFoundError` + 경로 안내 |
| 결측 좌표 행 | 해당 행 제거 후 경고 로그 (`WARNING: N rows dropped`) |
| 가중치 합계 ≠ 1.0 | `ValueError` + 합계 출력 |
| API 호출 실패 | 경고 로그 + CSV 폴백으로 자동 전환 |

---

## 9. 미결 기술 사항

| 항목 | 결정 기준 |
|---|---|
| 카카오맵 / 네이버 API 키 | 확보 시 `_load_from_kakao()` / `_load_from_naver()` 구현 |
| 유동인구 전국 커버리지 | SKT 빅데이터 허브 접근 가능 시 API 연동, 불가 시 공공데이터포털 CSV |
| 전국 격자 성능 | 1km 셀 기준 전국 약 10만 셀 예상 — 메모리 한계 시 시·도 단위 청크 처리 검토 |
