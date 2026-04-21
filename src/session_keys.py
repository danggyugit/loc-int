# src/session_keys.py — 세션별 API 키 저장소
#
# Why: Streamlit Cloud는 하나의 Python 프로세스에서 여러 사용자 세션을 실행함.
#      os.environ은 프로세스 전역이라 사용자 A가 입력한 키가 B에게 노출되거나
#      B의 분석이 A의 쿼터로 돌아가는 문제가 발생.
#      threading.local을 사용해 스크립트 실행 스레드별로 키를 격리.
#
# ThreadPoolExecutor 주의:
#      collect_all 내부의 병렬 수집은 worker 스레드를 새로 생성하므로
#      상위 스레드의 threading.local 값을 볼 수 없음.
#      → snapshot()/apply(dict) 헬퍼로 명시적 전파 필요.
#
# 로컬 개발 편의: 세션 저장값이 없으면 os.environ을 폴백으로 사용.

import os
import threading

_local = threading.local()

_KEY_NAMES = (
    "KAKAO_API_KEY",
    "DATA_GO_KR_API_KEY",
    "VWORLD_API_KEY",
    "BUILDING_API_KEY",
    "SGIS_CONSUMER_KEY",
    "SGIS_CONSUMER_SECRET",
)


def set_keys(**kwargs) -> None:
    """현재 스레드(세션)용 API 키 설정. 빈 값은 무시."""
    for k, v in kwargs.items():
        if k in _KEY_NAMES and v:
            setattr(_local, k, v)


def get(name: str, default: str = "") -> str:
    """세션 키 조회. 미설정 시 os.environ 폴백 (로컬 개발용)."""
    val = getattr(_local, name, None)
    if val:
        return val
    return os.environ.get(name, default)


def snapshot() -> dict:
    """ThreadPoolExecutor worker에 전달할 키 스냅샷."""
    return {k: getattr(_local, k, "") for k in _KEY_NAMES}


def apply(snap: dict) -> None:
    """다른 스레드에서 스냅샷의 키들을 threading.local에 복원."""
    for k, v in snap.items():
        if k in _KEY_NAMES and v:
            setattr(_local, k, v)


def clear() -> None:
    """현재 스레드의 모든 세션 키 제거 (테스트용)."""
    for k in _KEY_NAMES:
        if hasattr(_local, k):
            delattr(_local, k)
