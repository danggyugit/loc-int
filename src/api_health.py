"""API 연결 진단 — 각 외부 API를 1회씩 가볍게 호출해 키·서버 상태를 확인한다.

용도: "분석이 안 되는데 API 문제인가?"를 추측이 아니라 10초 만에 확인.
각 검사는 (성공 여부, 사람이 읽을 설명)을 반환하며 절대 예외를 던지지 않는다.
"""
from __future__ import annotations

import requests

_TIMEOUT = 8


def _fail_reason(e: Exception) -> str:
    s = str(e)
    if "401" in s:
        return "키가 유효하지 않습니다 (401) — 키 재확인 필요"
    if "429" in s:
        return "요청 한도 초과 (429) — 잠시 후 재시도"
    if "502" in s or "503" in s:
        return "API 서버 장애 (502/503) — 우리 문제 아님, 나중에 재시도"
    if "timed out" in s.lower() or "timeout" in s.lower():
        return "응답 시간 초과 — 서버 지연 또는 네트워크 차단"
    return f"{type(e).__name__}: {s[:120]}"


def check_kakao(key: str) -> tuple[bool, str]:
    """카카오 로컬 API — 주소 검색 1회."""
    if not key:
        return False, "키 미입력 (필수)"
    try:
        r = requests.get(
            "https://dapi.kakao.com/v2/local/search/address.json",
            headers={"Authorization": f"KakaoAK {key}"},
            params={"query": "서울특별시 강남구", "size": 1}, timeout=_TIMEOUT,
        )
        r.raise_for_status()
        n = len(r.json().get("documents", []))
        return True, f"정상 (검색 결과 {n}건 수신)"
    except Exception as e:  # noqa: BLE001
        return False, _fail_reason(e)


def check_data_go_kr(key: str) -> tuple[bool, str]:
    """공공데이터포털 — 아파트 실거래가 1건 조회."""
    if not key:
        return False, "키 미입력 (선택 — 소득·월세 팩터 제외됨)"
    try:
        r = requests.get(
            "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
            params={"serviceKey": key, "LAWD_CD": "11680", "DEAL_YMD": "202601",
                    "numOfRows": 1, "pageNo": 1}, timeout=_TIMEOUT,
        )
        r.raise_for_status()
        body = r.text[:500]
        if "SERVICE_KEY_IS_NOT_REGISTERED" in body or "SERVICE KEY" in body.upper():
            return False, "등록되지 않은 키 — data.go.kr에서 해당 API '활용신청' 필요"
        if "<resultCode>00" in body.replace(" ", "") or '"resultCode":"00"' in body.replace(" ", ""):
            return True, "정상"
        return True, f"응답 수신 (내용 확인 필요: {body[:80]}...)"
    except Exception as e:  # noqa: BLE001
        return False, _fail_reason(e)


def check_vworld(key: str) -> tuple[bool, str]:
    """Vworld — 용도지역 1건 조회."""
    if not key:
        return False, "키 미입력 (선택 — 용도지역 하드 필터 제외됨)"
    try:
        r = requests.get(
            "https://api.vworld.kr/req/data",
            params={"service": "data", "request": "GetFeature", "data": "LT_C_UQ111",
                    "key": key, "domain": "localhost",
                    "geomFilter": "BOX(127.02,37.49,127.04,37.51)",
                    "crs": "EPSG:4326", "size": 1, "format": "json"},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        status = r.json().get("response", {}).get("status", "")
        if status == "OK":
            return True, "정상"
        if "INVALID_KEY" in str(r.text).upper():
            return False, "키가 유효하지 않습니다"
        return False, f"응답 상태: {status or '알 수 없음'}"
    except Exception as e:  # noqa: BLE001
        return False, _fail_reason(e)


def check_building(key: str) -> tuple[bool, str]:
    """건축물대장 HUB — 표제부 1건 조회."""
    if not key:
        return False, "키 미입력 (선택 — 상가건물 팩터 제외됨)"
    try:
        r = requests.get(
            "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo",
            params={"serviceKey": key, "sigunguCd": "11680", "bjdongCd": "10300",
                    "numOfRows": 1, "pageNo": 1, "_type": "json"},
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        body = r.text[:300]
        if "SERVICE_KEY" in body.upper() and "NOT" in body.upper():
            return False, "등록되지 않은 키 — 건축HUB API '활용신청' 필요"
        try:
            r.json()
            return True, "정상"
        except ValueError:
            return False, f"JSON 아닌 응답 — 키/신청 상태 확인 ({body[:60]}...)"
    except Exception as e:  # noqa: BLE001
        return False, _fail_reason(e)


def check_sgis(key: str, secret: str) -> tuple[bool, str]:
    """SGIS — 인증 토큰 발급."""
    if not key or not secret:
        return False, "키 미입력 (선택 — 실인구 대신 아파트 proxy 사용)"
    try:
        r = requests.get(
            "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json",
            params={"consumer_key": key, "consumer_secret": secret},
            timeout=_TIMEOUT, verify=False,
        )
        r.raise_for_status()
        j = r.json()
        if str(j.get("errCd")) == "0":
            return True, "정상 (인증 토큰 발급 성공)"
        return False, f"인증 실패: {j.get('errMsg', j)}"
    except Exception as e:  # noqa: BLE001
        return False, _fail_reason(e)


def run_all(kakao: str, data_go: str, vworld: str,
            building: str, sgis_key: str, sgis_secret: str) -> list[tuple[str, bool, str, bool]]:
    """전체 진단. 반환: [(서비스명, 성공, 설명, 필수여부), ...]"""
    return [
        ("카카오 로컬",      *check_kakao(kakao),               True),
        ("공공데이터포털",   *check_data_go_kr(data_go),        False),
        ("Vworld 용도지역",  *check_vworld(vworld),             False),
        ("건축물대장 HUB",   *check_building(building),         False),
        ("SGIS 통계청",      *check_sgis(sgis_key, sgis_secret), False),
    ]
