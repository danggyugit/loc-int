# src/scoring_Ver4_3.py — 11팩터 점수화 모델 (AA-S Sub-agent)
#
# v4.2 대비 변경 사항:
#   1. 상가건물(commercial) 팩터 추가 — 양의 기여 (상가건물 밀집 지역 선호)
#   2. 도로접근성(road_quality) 팩터 추가 — 양의 기여 (대로변 가시성 중시)
#   3. 용도지역(zone_score) 하드 필터 — 전용주거·녹지 등 입점불가 지역 점수=0
#   4. 11팩터 가중치 모델: 기존 9 + commercial + road_quality
#
# v4.2 계승:
#   - 소득(income) / 임대(rent) 팩터
#   - children_and_parent 인구통계 타겟
#   - 데이터 미제공 팩터 가중치 자동 재분배

import json
import logging
import yaml
import pandas as pd
import geopandas as gpd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PRESETS_DIR

log = logging.getLogger(__name__)

PROFILES_FILE = PRESETS_DIR / "scoring_profiles_v43.yaml"

# 11팩터 키 목록
_ALL_FACTOR_KEYS = [
    "population", "floating", "workplace", "competitor",
    "accessibility", "parking", "diversity", "income", "rent",
    "commercial", "road_quality",
]


# ─────────────────────────────────────────────────────────
# 프로파일 로드
# ─────────────────────────────────────────────────────────

def load_profile(preset: str | None = None, keyword: str | None = None) -> dict:
    """
    scoring_profiles_v43.yaml에서 프로파일 로드.

    우선순위:
      1. preset 직접 지정
      2. keyword → keyword_classifier 자동 분류
      3. 'default' 폴백
    """
    with open(PROFILES_FILE, encoding="utf-8") as f:
        all_profiles = yaml.safe_load(f)

    classify_source = "preset"

    if preset and preset in all_profiles:
        profile_key = preset
        log.info(f"프로파일: '{preset}' 프리셋 사용")
    elif keyword:
        if _match_pottery_keyword(keyword):
            profile_key = "pottery"
            classify_source = "rule"
            log.info(f"프로파일: '{keyword}' → pottery (규칙 매칭)")
        else:
            from src.keyword_classifier import classify_keyword
            result = classify_keyword(keyword)
            profile_key = result["profile"]
            classify_source = result["source"]
            if profile_key not in all_profiles:
                profile_key = "default"
            log.info(f"프로파일: '{keyword}' → {profile_key} (출처: {classify_source})")
    else:
        profile_key = "default"
        classify_source = "default"
        log.info("프로파일: 기본값 사용")

    p = all_profiles[profile_key]
    weights = p["weights"].copy()

    # 하위 호환: 9팩터 프로파일에 신규 키가 없으면 0.0 추가
    for k in _ALL_FACTOR_KEYS:
        if k not in weights:
            weights[k] = 0.0

    _validate_weights(weights)

    return {
        "profile_key":           profile_key,
        "weights":               weights,
        "competition_mode":      p.get("competition_mode", "tolerate"),
        "competition_threshold": p.get("competition_threshold", 0.0),
        "traffic_type":          p.get("traffic_type", "mixed"),
        "demographic_target":    p.get("demographic_target", "all"),
        "description":           p.get("description", ""),
        "source":                classify_source,
    }


def _match_pottery_keyword(keyword: str) -> bool:
    """도자기/공방/세라믹 관련 키워드 매칭."""
    patterns = ["도자기", "공방", "세라믹", "도예", "pottery", "ceramic"]
    kw_lower = keyword.lower()
    return any(p in kw_lower for p in patterns)


def _validate_weights(weights: dict) -> None:
    """11팩터 가중치 검증."""
    required = set(_ALL_FACTOR_KEYS)
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
    """경쟁 밀도 → 페널티 강도 변환 (avoid/tolerate/cluster)."""
    if mode == "avoid":
        return comp_norm.pow(0.65)
    elif mode == "cluster":
        result = pd.Series(0.0, index=comp_norm.index)
        above = comp_norm > threshold
        if above.any() and threshold < 1.0:
            result[above] = (comp_norm[above] - threshold) / (1.0 - threshold)
        return result.clip(0.0, 1.0)
    else:
        return comp_norm


# ─────────────────────────────────────────────────────────
# 인구통계 타겟 보정
# ─────────────────────────────────────────────────────────

def _calc_demographic_modifier(
    gdf: gpd.GeoDataFrame,
    demographic_target: str,
) -> pd.Series:
    """
    인구통계 타겟에 따라 인구 점수 보정 계수(0.5~1.0) 계산.

    children_and_parent — 유소년(5~13세 proxy) + 부모세대(35~45세 proxy) 혼합
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
        raw = (100.0 - juv - old).clip(lower=0)
    elif demographic_target == "children_and_parent":
        # Why: 도자기 공방 등의 타겟은 5~13세 유소년 + 35~45세 부모
        #      juv_suprt_per는 0~14세 비율 → 5~13세 proxy로 활용
        #      유소년 비율이 높은 지역은 부모세대도 밀집하는 경향이 있으므로
        #      juv에 높은 가중(0.6) + young_adult에 낮은 가중(0.4) 적용
        children_score = juv
        parent_score = (100.0 - juv - old).clip(lower=0)
        raw = children_score * 0.6 + parent_score * 0.4
    else:
        return pd.Series(1.0, index=gdf.index)

    norm = normalize(raw)
    return 0.5 + 0.5 * norm


# ─────────────────────────────────────────────────────────
# 가중치 재분배 (데이터 미제공 팩터 처리)
# ─────────────────────────────────────────────────────────

def _redistribute_weights(
    weights: dict,
    gdf: gpd.GeoDataFrame,
) -> dict:
    """
    데이터가 없는 팩터의 가중치를 나머지 팩터에 비례 배분.

    Why: income/rent/commercial/road 데이터가 없는 환경에서도
         7팩터 모델처럼 정상 동작하려면 빈 팩터의 가중치를 다른 팩터로 이전해야 함.
    """
    col_map = {
        "population":    "population",
        "floating":      "floating",
        "workplace":     "workplace",
        "competitor":    "competitor_cnt",
        "accessibility": "transport_score",
        "parking":       "parking_cnt",
        "diversity":     "diversity",
        "income":        "income",
        "rent":          "rent",
        "commercial":    "commercial_cnt",
        "road_quality":  "road_score",
    }

    w = weights.copy()
    dead_weight = 0.0

    for factor, col in col_map.items():
        if col not in gdf.columns or gdf[col].sum() == 0:
            if w[factor] > 0:
                dead_weight += w[factor]
                w[factor] = 0.0

    if dead_weight > 0:
        active_total = sum(v for v in w.values() if v > 0)
        if active_total > 0:
            for k in w:
                if w[k] > 0:
                    w[k] += dead_weight * (w[k] / active_total)

        redistributed = [f for f, c in col_map.items()
                         if c not in gdf.columns or gdf[c].sum() == 0]
        log.info(f"가중치 재분배: {redistributed} → 나머지 팩터로 이전 ({dead_weight:.2f})")

    return w


# ─────────────────────────────────────────────────────────
# 점수 계산 (11팩터 + zone 하드필터)
# ─────────────────────────────────────────────────────────

def calc_score(
    gdf: gpd.GeoDataFrame,
    profile: dict,
) -> gpd.GeoDataFrame:
    """
    격자 GeoDataFrame에 종합 점수(score) 컬럼 추가 (11팩터).

    수식:
        score = pop_adjusted   * w_population
              + float_norm     * w_floating
              + work_norm      * w_workplace
              - comp_effect    * w_competitor
              + acc_norm       * w_accessibility
              + park_norm      * w_parking
              + div_norm       * w_diversity
              + income_norm    * w_income
              - rent_norm      * w_rent
              + comm_norm      * w_commercial
              + road_norm      * w_road_quality

        # 용도지역 하드 필터: score × zone_score (0이면 입점 불가)
    """
    out = gdf.copy()

    weights_raw = profile["weights"]
    comp_mode = profile["competition_mode"]
    comp_thr = profile["competition_threshold"]
    demo_target = profile.get("demographic_target", "all")

    # 데이터 누락 팩터 가중치 재분배
    weights = _redistribute_weights(weights_raw, out)

    # 각 지표 Min-Max 정규화
    def _norm(col):
        return normalize(out[col]) if col in out.columns else pd.Series(0.0, index=out.index)

    pop_norm    = _norm("population")
    float_norm  = _norm("floating")
    work_norm   = _norm("workplace")
    comp_norm   = _norm("competitor_cnt")
    acc_norm    = _norm("transport_score")
    park_norm   = _norm("parking_cnt")
    div_norm    = _norm("diversity")
    income_norm = _norm("income")
    rent_norm   = _norm("rent")
    comm_norm   = _norm("commercial_cnt")
    road_norm   = _norm("road_score")

    # 인구통계 타겟 보정
    demo_mod = _calc_demographic_modifier(out, demo_target)
    pop_adjusted = pop_norm * demo_mod

    # 경쟁 페널티
    comp_effect = _apply_competition_effect(comp_norm, comp_mode, comp_thr)

    out["score"] = (
        pop_adjusted   * weights["population"]
        + float_norm   * weights["floating"]
        + work_norm    * weights["workplace"]
        - comp_effect  * weights["competitor"]
        + acc_norm     * weights["accessibility"]
        + park_norm    * weights["parking"]
        + div_norm     * weights["diversity"]
        + income_norm  * weights["income"]
        - rent_norm    * weights["rent"]
        + comm_norm    * weights["commercial"]
        + road_norm    * weights["road_quality"]
    )

    out["score"] = out["score"].clip(lower=0)
    out["score"] = normalize(out["score"])

    # Why: 용도지역 하드 필터 — zone_score=0인 셀(전용주거·녹지 등)은
    #      다른 팩터가 좋아도 입점 불가이므로 점수를 0으로 만듦
    if "zone_score" in out.columns:
        out["score"] = out["score"] * out["zone_score"]
        out["score"] = normalize(out["score"])

    demo_label = f"|demo={demo_target}" if demo_target != "all" else ""
    log.info(
        f"[{profile['profile_key']}|{comp_mode}{demo_label}] "
        f"점수 계산(11F): mean={out['score'].mean():.3f}, "
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
# 편의 함수: 점수 + 순위 한 번에
# ─────────────────────────────────────────────────────────

def score_and_rank(
    grid_gdf: gpd.GeoDataFrame,
    preset: str | None = None,
    keyword: str | None = None,
    weights_json: str | None = None,
    top_n: int = 10,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, dict]:
    """
    격자 GDF에 11팩터 점수화 + 순위 산출을 한 번에 수행.

    Returns:
        (전체 격자 GDF with score, 상위 N개 GDF with rank, profile dict)
    """
    if weights_json:
        weights = json.loads(weights_json)
        for k in _ALL_FACTOR_KEYS:
            if k not in weights:
                weights[k] = 0.0
        _validate_weights(weights)
        profile = {
            "profile_key":           "custom",
            "weights":               weights,
            "competition_mode":      "tolerate",
            "competition_threshold": 0.0,
            "traffic_type":          "mixed",
            "demographic_target":    "all",
            "description":           "CLI 직접 가중치 입력",
            "source":                "cli",
        }
    else:
        profile = load_profile(preset=preset, keyword=keyword)

    scored = calc_score(grid_gdf, profile)
    top = rank_candidates(scored, top_n=top_n)
    return scored, top, profile
