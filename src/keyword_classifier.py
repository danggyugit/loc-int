# src/keyword_classifier.py — 업종 키워드 자동 분류기
#
# 분류 우선순위:
#   1. 규칙 기반 (regex RULES) — 즉시, 캐시 불필요
#   2. 캐시 (keyword_cache.json) — 이전 CLI 분류 결과 재사용
#   3. Claude CLI subprocess — claude -p 비대화형 실행 (Max 플랜 활용)
#   4. default 폴백 — 분류 실패 시

import re
import json
import logging
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PRESETS_DIR

log = logging.getLogger(__name__)

CACHE_FILE       = PRESETS_DIR / "keyword_cache.json"
PROFILES_FILE    = PRESETS_DIR / "scoring_profiles.yaml"
CLAUDE_TIMEOUT   = 30   # seconds

# ─────────────────────────────────────────────────────────
# 유효한 프로파일 타입 (scoring_profiles.yaml 키워드 타입 목록)
# ─────────────────────────────────────────────────────────

VALID_TYPES = {
    "fnb_floating", "fnb_cluster",
    "destination_residential", "destination_hobby",
    "education", "pet_residential", "fitness",
    "office_service", "beauty_floating", "accommodation",
    "cafe", "restaurant", "hospital", "convenience", "mart", "pharmacy",
    "default",
}

# ─────────────────────────────────────────────────────────
# 규칙 기반 분류 (regex)
# 순서 중요: 더 구체적인 패턴을 앞에 배치
# ─────────────────────────────────────────────────────────

RULES: list[tuple[str, str]] = [
    # 숙박
    (r"게스트하우스|펜션|에어비앤비|호스텔|모텔|호텔|글램핑|캠핑장",          "accommodation"),

    # 애완·반려동물
    (r"애견|반려견|반려묘|펫|동물 병원|수의|고양이 카페|애묘",               "pet_residential"),

    # 교육
    (r"학원|교습|과외|독서실|스터디카페|코인 스터디|유치원|어린이집|학교",      "education"),

    # 피트니스·스포츠
    (r"헬스|피트니스|필라테스|요가|크로스핏|수영|복싱|무술|태권도|검도|격투",   "fitness"),

    # 미용 (유동)
    (r"네일|속눈썹|왁싱|미용실|헤어|바버|피부 관리|에스테틱|마사지",          "beauty_floating"),

    # 사무 서비스
    (r"인쇄|복사|제본|현수막|명함|문구|서류|사무",                          "office_service"),

    # 목적지형 취미·공방
    (r"공방|원데이클래스|스튜디오|도예|도자기|목공|가죽|향수|캔들|플라워|서점|중고 서적|독립 서점",
                                                                          "destination_hobby"),

    # 주거 밀집 목적지 (F&B 외)
    (r"정육점|반찬|세탁소|코인 세탁|마트|슈퍼|편의점",                       "destination_residential"),

    # 유동 F&B (디저트·제과류)
    (r"베이커리|빵집|디저트|케이크|마카롱|아이스크림|와플|도넛|꽃집|플라워샵",   "fnb_floating"),

    # 클러스터 F&B (음식·분식류)
    (r"분식|떡볶이|순대|타코야끼|국밥|해장|족발|치킨|피자|버거|라멘|초밥|덮밥|식당|음식점|한식|일식|중식|양식|밥집",
                                                                          "fnb_cluster"),

    # 카페 (독립 패턴)
    (r"카페|커피|티룸|브런치 카페",                                          "cafe"),
]

_COMPILED = [(re.compile(pattern, re.IGNORECASE), profile) for pattern, profile in RULES]


# ─────────────────────────────────────────────────────────
# 캐시 I/O
# ─────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    try:
        CACHE_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        log.warning(f"캐시 저장 실패: {e}")


# ─────────────────────────────────────────────────────────
# Claude CLI 분류
# ─────────────────────────────────────────────────────────

_PROFILE_LIST = "\n".join(f"  - {t}" for t in sorted(VALID_TYPES - {"default"}))

_PROMPT_TMPL = """업종 키워드 "{keyword}"를 아래 프로파일 타입 중 하나로 분류해줘.
반드시 타입명만 한 단어로 답해. 설명 금지.

프로파일 타입 목록:
{profiles}

답변 예시: destination_hobby"""


def _classify_via_claude(keyword: str) -> str | None:
    """
    Claude CLI (claude -p)로 키워드 분류.
    Max 플랜 사용 → API 키 불필요.
    실패 또는 타임아웃 시 None 반환.
    """
    prompt = _PROMPT_TMPL.format(keyword=keyword, profiles=_PROFILE_LIST)
    try:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True, text=True,
            timeout=CLAUDE_TIMEOUT, encoding="utf-8",
        )
        if result.returncode != 0:
            log.warning(f"Claude CLI 오류 (returncode={result.returncode}): {result.stderr[:200]}")
            return None

        raw = result.stdout.strip().splitlines()[-1].strip()  # 마지막 줄만 사용
        # 답변에서 유효한 타입명 추출 (앞뒤 잡음 제거)
        for token in raw.split():
            if token in VALID_TYPES:
                log.info(f"Claude CLI 분류: '{keyword}' → {token}")
                return token

        log.warning(f"Claude CLI 반환값 파싱 실패: '{raw}'")
        return None

    except subprocess.TimeoutExpired:
        log.warning(f"Claude CLI 타임아웃 ({CLAUDE_TIMEOUT}s): '{keyword}'")
        return None
    except FileNotFoundError:
        log.warning("Claude CLI를 찾을 수 없습니다. (claude 명령어 미설치)")
        return None
    except Exception as e:
        log.warning(f"Claude CLI 예외: {e}")
        return None


# ─────────────────────────────────────────────────────────
# 메인 분류 함수
# ─────────────────────────────────────────────────────────

def classify_keyword(keyword: str, use_claude: bool = True) -> dict:
    """
    업종 키워드를 scoring_profiles.yaml 타입으로 분류.

    분류 우선순위:
      1. regex RULES   — 즉시
      2. keyword_cache.json — 이전 Claude 결과 재사용
      3. claude -p     — Max 플랜 Claude CLI (use_claude=True일 때)
      4. "default"     — 폴백

    Returns:
        {
            "profile": "destination_hobby",   # 프로파일 타입명
            "source":  "rule" | "cache" | "claude" | "default",
        }
    """
    kw = keyword.strip()

    # 1. 규칙 기반
    for pattern, profile in _COMPILED:
        if pattern.search(kw):
            log.info(f"규칙 분류: '{kw}' → {profile}")
            return {"profile": profile, "source": "rule"}

    # 2. 캐시
    cache = _load_cache()
    if kw in cache and cache[kw] in VALID_TYPES:
        log.info(f"캐시 분류: '{kw}' → {cache[kw]}")
        return {"profile": cache[kw], "source": "cache"}

    # 3. Claude CLI
    if use_claude:
        claude_result = _classify_via_claude(kw)
        if claude_result:
            cache[kw] = claude_result
            _save_cache(cache)
            return {"profile": claude_result, "source": "claude"}

    # 4. 기본값 폴백
    log.warning(f"분류 실패, 기본값 사용: '{kw}' → default")
    return {"profile": "default", "source": "default"}
