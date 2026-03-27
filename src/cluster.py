# src/cluster.py — 핫스팟 클러스터링 (AA-C Sub-agent)
# 수요 밀집 지역 / 경쟁 공백 지역 탐지 (DBSCAN)

import logging
import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_KOREA

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────────────────

def _get_centroids_tm(gdf: gpd.GeoDataFrame) -> np.ndarray:
    """격자 중심 좌표를 EPSG:5179(미터) 기준 numpy 배열로 반환."""
    gdf_tm = gdf.to_crs(CRS_KOREA)
    coords = np.column_stack([
        gdf_tm.geometry.centroid.x,
        gdf_tm.geometry.centroid.y,
    ])
    return coords


# ─────────────────────────────────────────────────────────
# 수요 밀집 핫스팟
# ─────────────────────────────────────────────────────────

def find_demand_hotspot(
    gdf: gpd.GeoDataFrame,
    score_col:   str = "score",
    score_threshold: float = 0.6,
    eps_m:       float = 1500.0,
    min_samples: int   = 3,
) -> gpd.GeoDataFrame:
    """
    점수 상위 격자를 대상으로 DBSCAN 클러스터링하여 수요 밀집 지역 탐지.

    Args:
        gdf:             점수화된 격자 GeoDataFrame
        score_col:       클러스터링 기준 점수 컬럼
        score_threshold: 이 점수 이상인 셀만 클러스터링 대상
        eps_m:           DBSCAN 클러스터 반경 (미터)
        min_samples:     클러스터 최소 셀 수

    Returns:
        cluster_label 컬럼이 추가된 GeoDataFrame
        (-1 = 노이즈, 0~ = 클러스터 번호)
    """
    if score_col not in gdf.columns:
        raise ValueError(f"'{score_col}' 컬럼이 없습니다. calc_score() 먼저 실행하세요.")

    out = gdf.copy()
    out["cluster_label"] = -1  # 기본값: 노이즈

    # 점수 임계값 이상인 셀만 클러스터링
    mask = out[score_col] >= score_threshold
    target = out[mask]

    if len(target) < min_samples:
        log.warning(
            f"임계값({score_threshold}) 이상 셀이 {len(target)}개 — "
            f"min_samples({min_samples})보다 적어 클러스터링 생략"
        )
        return out

    coords = _get_centroids_tm(target)

    # DBSCAN: eps는 미터 단위 (EPSG:5179 기준)
    db = DBSCAN(eps=eps_m, min_samples=min_samples, metric="euclidean")
    labels = db.fit_predict(coords)

    out.loc[mask, "cluster_label"] = labels

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise    = (labels == -1).sum()
    log.info(
        f"수요 핫스팟 클러스터링 완료: "
        f"클러스터 {n_clusters}개, 노이즈 {n_noise}셀 "
        f"(eps={eps_m}m, min_samples={min_samples})"
    )
    return out


# ─────────────────────────────────────────────────────────
# 경쟁 공백 지역 탐지
# ─────────────────────────────────────────────────────────

def find_competition_gap(
    gdf: gpd.GeoDataFrame,
    competitor_col:    str   = "competitor_cnt",
    population_col:    str   = "population",
    comp_threshold:    float = 1.0,
    pop_threshold:     float = 0.3,
) -> gpd.GeoDataFrame:
    """
    경쟁이 적고 수요(인구)가 일정 수준 이상인 '기회 공백' 격자 탐지.

    Args:
        comp_threshold: 경쟁업체 수 이하인 셀 선택 (기본: 1개 이하)
        pop_threshold:  정규화 인구 점수 이상인 셀 선택 (기본: 상위 30%)

    Returns:
        is_gap 컬럼(bool)이 추가된 GeoDataFrame
    """
    out = gdf.copy()

    # 인구 정규화 (0~1)
    if population_col in out.columns:
        mn, mx = out[population_col].min(), out[population_col].max()
        pop_norm = (out[population_col] - mn) / (mx - mn + 1e-9)
    else:
        log.warning(f"'{population_col}' 컬럼 없음 — 인구 조건 생략")
        pop_norm = pd.Series(1.0, index=out.index)

    comp_cond = out[competitor_col] <= comp_threshold if competitor_col in out.columns else True
    pop_cond  = pop_norm >= pop_threshold

    out["is_gap"] = comp_cond & pop_cond

    n_gap = out["is_gap"].sum()
    log.info(
        f"경쟁 공백 탐지 완료: {n_gap}셀 "
        f"(경쟁<={comp_threshold}, 인구정규화>={pop_threshold})"
    )
    return out


# ─────────────────────────────────────────────────────────
# 클러스터 요약 통계
# ─────────────────────────────────────────────────────────

def summarize_clusters(
    gdf: gpd.GeoDataFrame,
    label_col: str = "cluster_label",
    score_col: str = "score",
) -> pd.DataFrame:
    """
    클러스터별 요약 통계 (셀 수, 평균 점수, 중심 좌표).

    Returns:
        클러스터 요약 DataFrame
    """
    if label_col not in gdf.columns:
        raise ValueError(f"'{label_col}' 컬럼 없음. find_demand_hotspot() 먼저 실행하세요.")

    gdf_tm = gdf.to_crs(CRS_KOREA)
    rows = []

    for label, group in gdf.groupby(label_col):
        group_tm = gdf_tm.loc[group.index]
        centroid  = group_tm.union_all().centroid

        # 중심 좌표를 WGS84로 재변환
        c_gdf = gpd.GeoDataFrame(geometry=[centroid], crs=CRS_KOREA).to_crs("EPSG:4326")
        c_wgs = c_gdf.geometry.iloc[0]

        rows.append({
            "cluster_label": label,
            "type":          "노이즈" if label == -1 else "핫스팟",
            "cell_count":    len(group),
            "avg_score":     group[score_col].mean() if score_col in group.columns else None,
            "max_score":     group[score_col].max()  if score_col in group.columns else None,
            "center_lat":    round(c_wgs.y, 6),
            "center_lng":    round(c_wgs.x, 6),
        })

    df = pd.DataFrame(rows).sort_values("avg_score", ascending=False).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────
# 편의 함수: 전체 클러스터 분석 파이프라인
# ─────────────────────────────────────────────────────────

def run_cluster_analysis(
    scored_gdf: gpd.GeoDataFrame,
    score_threshold: float = 0.6,
    eps_m:       float = 1500.0,
    min_samples: int   = 3,
    comp_threshold: float = 1.0,
    pop_threshold:  float = 0.3,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, gpd.GeoDataFrame]:
    """
    핫스팟 클러스터링 + 경쟁 공백 탐지를 한 번에 실행.

    Returns:
        (cluster_gdf, cluster_summary_df, gap_gdf)
    """
    cluster_gdf = find_demand_hotspot(
        scored_gdf,
        score_threshold=score_threshold,
        eps_m=eps_m,
        min_samples=min_samples,
    )
    summary_df = summarize_clusters(cluster_gdf)

    gap_gdf = find_competition_gap(
        scored_gdf,
        comp_threshold=comp_threshold,
        pop_threshold=pop_threshold,
    )

    return cluster_gdf, summary_df, gap_gdf
