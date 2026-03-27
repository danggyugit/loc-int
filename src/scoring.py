# src/scoring.py — 점수화 모델 (AA-S Sub-agent)
# 가중치 우선순위: CLI 직접 입력 > YAML 프로파일 > 기본값(균등 0.25)
#
# v2 변경 사항:
#   - weights.yaml 대신 scoring_profiles.yaml 사용
#   - competition_mode (avoid / tolerate / cluster) 별 비선형 경쟁 효과
#   - keyword 파라미터 지원 → keyword_classifier 자동 분류
#   - score_and_rank() 반환 타입: (scored, top, profile_dict) 3-tuple

import json
import logging
import yaml
import pandas as pd
import geopandas as gpd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PRESETS_DIR, DEFAULT_WEIGHTS

log = logging.getLogger(__name__)

PROFILES_FILE = PRESETS_DIR / "scoring_profiles.yaml"

# ─────────────────────────────────────────────────────────
# 프로파일 로드
# ─────────────────────────────────────────────────────────

def load_profile(preset: str | None = None, keyword: str | None = None) -> dict:
    """
    scoring_profiles.yaml에서 프로파일 로드.

    우선순위:
      1. preset 직접 지정 (카페/음식점/병원/편의점/대형마트/약국)
      2. keyword → keyword_classifier 자동 분류
      3. 'default' 폴백

    Returns:
        {
            "profile_key": "cafe",
            "weights": {...},
            "competition_mode": "cluster",
            "competition_threshold": 0.30,
            "traffic_type": "floating",
            "description": "...",
            "source": "preset" | "rule" | "cache" | "claude" | "default",
        }
    """
    with open(PROFILES_FILE, encoding="utf-8") as f:
        all_profiles = yaml.safe_load(f)

    classify_source = "preset"

    if preset and preset in all_profiles:
        profile_key = preset
        log.info(f"프로파일: '{preset}' 프리셋 사용")
    elif keyword:
        from src.keyword_classifier import classify_keyword
        result       = classify_keyword(keyword)
        profile_key  = result["profile"]
        classify_source = result["source"]
        if profile_key not in all_profiles:
            profile_key = "default"
        log.info(f"프로파일: '{keyword}' → {profile_key} (출처: {classify_source})")
    else:
        profile_key = "default"
        classify_source = "default"
        log.info("프로파일: 기본값(균등 0.25) 사용")

    p = all_profiles[profile_key]
    _validate_weights(p["weights"])

    return {
        "profile_key":          profile_key,
        "weights":              p["weights"],
        "competition_mode":     p.get("competition_mode", "tolerate"),
        "competition_threshold": p.get("competition_threshold", 0.0),
        "traffic_type":         p.get("traffic_type", "mixed"),
        "demographic_target":   p.get("demographic_target", "all"),
        "description":          p.get("description", ""),
        "source":               classify_source,
    }


def _validate_weights(weights: dict) -> None:
    required = {"population", "floating", "workplace", "competitor", "accessibility", "parking", "diversity"}
    missing = required - set(weights.keys())
    if missing:
        raise ValueError(f"가중치 항목 누락: {missing}")
    total = sum(weights.values())
    if not abs(total - 1.0) < 1e-6:
        raise ValueError(
            f"가중치 합계가 1.0이어야 합니다. 현재 합계: {total:.4f}\n"
            f"입력된 가중치: {weights}"
        )


# ─────────────────────────────────────────────────────────
# 정규화
# ─────────────────────────────────────────────────────────

def normalize(series: pd.Series) -> pd.Series:
    """Min-Max 정규화 (0~1). 분모가 0이면 0 반환."""
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.0, index=series.index)
    return (series - mn) / (mx - mn)


# ─────────────────────────────────────────────────────────
# 경쟁 효과 적용
# ─────────────────────────────────────────────────────────

def _apply_competition_effect(
    comp_norm: pd.Series,
    mode: str,
    threshold: float,
) -> pd.Series:
    """
    정규화된 경쟁 밀도(0~1)를 competition_mode에 따라 페널티 강도로 변환.

    avoid:   지수 곡선 (0.65승) → 낮은 경쟁 밀도에서도 빠르게 페널티 상승
    tolerate: 선형 (기존 동작) → comp_norm 그대로 반환
    cluster: threshold 이하 구간은 패널티 없음, 초과 시 선형 증가
             → 먹자골목·카페거리 집적 효과 모델링
    """
    if mode == "avoid":
        # Why: 편의점·마트처럼 독점 상권을 선호하는 업종은
        #      경쟁업체 1개만 있어도 큰 불이익을 주기 위해 지수 곡선 사용
        return comp_norm.pow(0.65)

    elif mode == "cluster":
        # Why: 카페거리·음식점 골목처럼 경쟁업체가 몰려 있을수록
        #      집적 효과로 오히려 유리한 경우, threshold 이하는 페널티 0
        result = pd.Series(0.0, index=comp_norm.index)
        above  = comp_norm > threshold
        if above.any() and threshold < 1.0:
            result[above] = (comp_norm[above] - threshold) / (1.0 - threshold)
        return result.clip(0.0, 1.0)

    else:  # tolerate (기본)
        return comp_norm


# ─────────────────────────────────────────────────────────
# 점수 계산
# ─────────────────────────────────────────────────────────

def _calc_demographic_modifier(
    gdf: gpd.GeoDataFrame,
    demographic_target: str,
) -> pd.Series:
    """
    인구통계 타겟에 따라 인구 점수 보정 계수(0~1) 계산.

    demographic_target 값:
        all       — 보정 없음 (계수 1.0)
        children  — 유소년 비율 높을수록 유리 (juv_suprt_per 기반)
        elderly   — 고령 비율 높을수록 유리 (oldage_suprt_per 기반)
        young_adult — 생산가능인구(15~64세) 비율 높을수록 유리
                     = (100 - juv - oldage) 근사

    Returns:
        0~1 범위 보정 계수 Series
    """
    if demographic_target == "all" or demographic_target is None:
        return pd.Series(1.0, index=gdf.index)

    juv = gdf["juv_suprt_per"] if "juv_suprt_per" in gdf.columns else pd.Series(0.0, index=gdf.index)
    old = gdf["oldage_suprt_per"] if "oldage_suprt_per" in gdf.columns else pd.Series(0.0, index=gdf.index)

    if demographic_target == "children":
        raw = juv
    elif demographic_target == "elderly":
        raw = old
    elif demographic_target == "young_adult":
        # Why: juv_suprt_per + oldage_suprt_per 가 작을수록 생산가능인구 비중이 높음
        raw = (100.0 - juv - old).clip(lower=0)
    else:
        return pd.Series(1.0, index=gdf.index)

    # 정규화 후 0.5~1.0 범위로 스케일 (완전히 0이 되지 않도록)
    norm = normalize(raw)
    return 0.5 + 0.5 * norm


def calc_score(
    gdf: gpd.GeoDataFrame,
    profile: dict,
    pop_col:   str = "population",
    float_col: str = "floating",
    work_col:  str = "workplace",
    comp_col:  str = "competitor_cnt",
    acc_col:   str = "transport_score",
    park_col:  str = "parking_cnt",
    div_col:   str = "diversity",
) -> gpd.GeoDataFrame:
    """
    격자 GeoDataFrame에 종합 점수(score) 컬럼 추가.

    수식:
        demographic_mod = demographic modifier (0.5~1.0)
        pop_adjusted = pop_norm * demographic_mod
        competition_effect = _apply_competition_effect(comp_norm, mode, threshold)
        score = pop_adjusted * w_population
              + float_norm   * w_floating
              + work_norm    * w_workplace
              - comp_effect  * w_competitor
              + acc_norm     * w_accessibility
              + park_norm    * w_parking
              + div_norm     * w_diversity

    Returns:
        score 컬럼이 추가된 GeoDataFrame (점수 범위 0~1)
    """
    out = gdf.copy()

    weights   = profile["weights"]
    comp_mode = profile["competition_mode"]
    comp_thr  = profile["competition_threshold"]
    demo_target = profile.get("demographic_target", "all")

    # 각 지표 Min-Max 정규화
    pop_norm   = normalize(out[pop_col])   if pop_col   in out.columns else pd.Series(0.0, index=out.index)
    float_norm = normalize(out[float_col]) if float_col in out.columns else pd.Series(0.0, index=out.index)
    work_norm  = normalize(out[work_col])  if work_col  in out.columns else pd.Series(0.0, index=out.index)
    comp_norm  = normalize(out[comp_col])  if comp_col  in out.columns else pd.Series(0.0, index=out.index)
    acc_norm   = normalize(out[acc_col])   if acc_col   in out.columns else pd.Series(0.0, index=out.index)
    park_norm  = normalize(out[park_col])  if park_col  in out.columns else pd.Series(0.0, index=out.index)
    div_norm   = normalize(out[div_col])   if div_col   in out.columns else pd.Series(0.0, index=out.index)

    # 인구통계 타겟 보정 (인구 점수에 곱하기)
    demo_mod = _calc_demographic_modifier(out, demo_target)
    pop_adjusted = pop_norm * demo_mod

    # 경쟁 페널티 강도 계산
    comp_effect = _apply_competition_effect(comp_norm, comp_mode, comp_thr)

    out["score"] = (
        pop_adjusted  * weights["population"]
        + float_norm  * weights["floating"]
        + work_norm   * weights["workplace"]
        - comp_effect * weights["competitor"]
        + acc_norm    * weights["accessibility"]
        + park_norm   * weights["parking"]
        + div_norm    * weights["diversity"]
    )

    # 음수 방지 후 0~1 재정규화
    out["score"] = out["score"].clip(lower=0)
    out["score"] = normalize(out["score"])

    demo_label = f"|demo={demo_target}" if demo_target != "all" else ""
    log.info(
        f"[{profile['profile_key']}|{comp_mode}{demo_label}] "
        f"점수 계산: mean={out['score'].mean():.3f}, "
        f"max={out['score'].max():.3f}, min={out['score'].min():.3f}"
    )
    return out


# ─────────────────────────────────────────────────────────
# 후보지 순위 산출
# ─────────────────────────────────────────────────────────

def rank_candidates(gdf: gpd.GeoDataFrame, top_n: int = 10) -> gpd.GeoDataFrame:
    """score 기준 상위 N개 격자 반환 (rank 컬럼 추가)."""
    if "score" not in gdf.columns:
        raise ValueError("score 컬럼이 없습니다. calc_score() 먼저 실행하세요.")
    ranked = gdf.nlargest(top_n, "score").copy()
    ranked["rank"] = range(1, len(ranked) + 1)
    log.info(f"상위 {top_n}개 후보지 선정 완료")
    return ranked.reset_index(drop=True)


# ─────────────────────────────────────────────────────────
# 편의 함수: 격자 피처 → 점수 + 순위 한 번에
# ─────────────────────────────────────────────────────────

def score_and_rank(
    grid_gdf: gpd.GeoDataFrame,
    preset: str | None = None,
    keyword: str | None = None,
    weights_json: str | None = None,
    top_n: int = 10,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, dict]:
    """
    격자 GDF에 점수화 + 순위 산출을 한 번에 수행.

    Args:
        preset:       프리셋 이름 (카페·음식점 등 6종)
        keyword:      직접 입력 키워드 → classifier로 자동 분류
        weights_json: JSON 문자열 직접 가중치 입력 (CLI용, 프로파일 override)
        top_n:        상위 후보지 수

    Returns:
        (전체 격자 GDF with score, 상위 N개 GDF with rank, profile dict)
    """
    if weights_json:
        # CLI 직접 가중치 입력 — 프로파일 없이 동작 (하위 호환)
        weights = json.loads(weights_json)
        _validate_weights(weights)
        # 누락 키 보완 (하위 호환)
        for _compat_key in ["workplace", "parking", "diversity"]:
            if _compat_key not in weights:
                weights[_compat_key] = 0.0
        profile = {
            "profile_key":          "custom",
            "weights":              weights,
            "competition_mode":     "tolerate",
            "competition_threshold": 0.0,
            "traffic_type":         "mixed",
            "demographic_target":   "all",
            "description":          "CLI 직접 가중치 입력",
            "source":               "cli",
        }
        log.info("가중치: CLI 직접 입력 사용")
    else:
        profile = load_profile(preset=preset, keyword=keyword)

    scored = calc_score(grid_gdf, profile)
    top    = rank_candidates(scored, top_n=top_n)
    return scored, top, profile
