# scripts/ — 전국 사전수집 스크립트

전국 탐색 모드에서 사용할 데이터를 사전 수집하여 `data/national/`에 저장.

## 공통 사항

- **실행 위치**: 프로젝트 루트 (`c:/Users/sk15y/claude/py/loc int/`)
- **인터프리터**: `C:\Python\python.exe`
- **결과 파일**: `data/national/{시도코드}_*.parquet` + `manifest.json` 자동 갱신
- **idempotent**: 모든 스크립트는 `--resume` 옵션으로 누락된 시·도만 채움

수집 후 git에 commit → Streamlit Cloud는 자동 반영.

---

## 1) collect_sgis_national.py — SGIS 인구·사업체

**필요 키**: SGIS_CONSUMER_KEY, SGIS_CONSUMER_SECRET (https://sgis.kostat.go.kr/developer/)

**수집 지표**: population, workplace, corp_cnt, avg_age, juv_suprt_per, oldage_suprt_per

**시간**: 약 1~2시간 (시·도 17개 × 시·군·구 250개 × 읍면동 3,500개 순회)

```bash
# 환경변수 설정 (Windows PowerShell)
$env:SGIS_CONSUMER_KEY = "발급받은_서비스ID"
$env:SGIS_CONSUMER_SECRET = "발급받은_보안KEY"

# 전체 수집
python scripts/collect_sgis_national.py

# 특정 시·도만
python scripts/collect_sgis_national.py --sido 11    # 서울
python scripts/collect_sgis_national.py --sido 31    # 경기

# 누락된 시·도만 (재시도)
python scripts/collect_sgis_national.py --resume
```

**시·도 코드** (manifest.json 의 sido_codes 참고):
| 코드 | 지역 | 코드 | 지역 |
|---|---|---|---|
| 11 | 서울 | 32 | 강원 |
| 21 | 부산 | 33 | 충북 |
| 22 | 대구 | 34 | 충남 |
| 23 | 인천 | 35 | 전북 |
| 24 | 광주 | 36 | 전남 |
| 25 | 대전 | 37 | 경북 |
| 26 | 울산 | 38 | 경남 |
| 29 | 세종 | 39 | 제주 |
| 31 | 경기 | | |

---

## 2) collect_kakao_general.py — 카카오 일반 인프라

**필요 키**: KAKAO_API_KEY

**수집 지표**: subway_cnt, bus_cnt, parking_cnt, diversity_cnt

**시간/호출량**: 시·도당 약 1,000~6,000회 × 4지표 = 전국 약 30,000~50,000회 (일일 쿼터 10만 중 절반)

```bash
$env:KAKAO_API_KEY = "..."
python scripts/collect_kakao_general.py                       # 전체
python scripts/collect_kakao_general.py --metric subway       # 지하철만
python scripts/collect_kakao_general.py --sido 11 --metric all
python scripts/collect_kakao_general.py --resume              # 누락만
```

⚠️ SGIS 수집(`collect_sgis_national.py`)이 먼저 완료되어 있어야 함 (시·군·구 polygon 재사용).

---

## 3) collect_income_sigungu.py — 시·군·구 평균 소득·월세

**필요 키**: DATA_GO_KR_API_KEY, KAKAO_API_KEY

**수집 지표**: income_avg(아파트 평균 매매가, 만원/㎡), rent_avg(평균 월세, 만원)

**시간**: 약 30분 (시·군·구당 2~3 호출 × 250개)

**대상 단위**: 시·군·구만 (읍·면·동 단위 데이터 없음)

```bash
$env:DATA_GO_KR_API_KEY = "..."; $env:KAKAO_API_KEY = "..."
python scripts/collect_income_sigungu.py
python scripts/collect_income_sigungu.py --sido 11 --resume
```

---

## 4) collect_competitors_by_preset.py — 업종별 전국 경쟁업체 수

**필요 키**: KAKAO_API_KEY

**수집 지표**: cafe_cnt, restaurant_cnt, hospital_cnt, convenience_cnt, mart_cnt, pharmacy_cnt, pottery_cnt

**시간/호출량**: 시·도당 약 5,000~10,000회 × 7프리셋 = **전국 약 100,000~150,000회** ⚠️ 일일 쿼터 초과 가능

→ **프리셋 단위로 며칠에 분할 실행 권장**.

```bash
$env:KAKAO_API_KEY = "..."
# 1일차: 카페만
python scripts/collect_competitors_by_preset.py --preset cafe
# 2일차: 음식점
python scripts/collect_competitors_by_preset.py --preset restaurant
# ...
python scripts/collect_competitors_by_preset.py --preset all  # 한 번에 (쿼터 충분 시)
python scripts/collect_competitors_by_preset.py --preset cafe --sido 11  # 일부만
```

---

## 권장 수집 순서

1. **SGIS** (`collect_sgis_national.py`) — 인구·사업체 (~1시간, SGIS 키만)
2. **카카오 일반** (`collect_kakao_general.py`) — 지표 4종 (~수 시간)
3. **소득** (`collect_income_sigungu.py`) — 평균 소득·월세 (~30분)
4. **업종별 경쟁업체** (`collect_competitors_by_preset.py`) — 7개 프리셋, 며칠에 분산

각 단계 후 git commit·push → Streamlit Cloud 자동 반영. UI는 manifest.json을 읽어 가용 지표를 자동 노출하므로 코드 변경 불필요.
