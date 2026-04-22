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

## 추가 예정

- `collect_kakao_general.py` — 지하철·버스·주차·전체업종 다양성
- `collect_income_sigungu.py` — data.go.kr 시·군·구 평균 소득·월세
- `collect_competitors_by_preset.py` — 7개 프리셋 업종별 전국 경쟁업체 수
