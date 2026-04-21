# src/visualizer_Ver4_2.py — 지도 및 차트 시각화 (VA Agent)
# 지도: folium (HTML), 차트: matplotlib (PNG)
#
# v4.1 대비 개선 사항:
#   1. 히트맵 tooltip에 소득·월세 정보 추가
#   2. 지하철 노선도 TileLayer 오버레이 (v4.1에서 계승)

import logging
import pandas as pd
import geopandas as gpd
import folium
import matplotlib
matplotlib.use("Agg")   # GUI 없는 환경에서 PNG 저장용
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import numpy as np

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import OUTPUT_DIR

log = logging.getLogger(__name__)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────────────────

def _grid_center(gdf: gpd.GeoDataFrame) -> tuple[float, float]:
    """GeoDataFrame 중심 좌표 반환 (folium 초기 위치용)."""
    centroid = gdf.unary_union.centroid
    return centroid.y, centroid.x


def _score_to_color(score: float, cmap_name: str = "RdYlGn") -> str:
    """점수(0~1)를 hex 색상으로 변환."""
    cmap = cm.get_cmap(cmap_name)
    rgba = cmap(float(score))
    return mcolors.to_hex(rgba)


# ─────────────────────────────────────────────────────────
# V-01: 격자 히트맵 지도
# ─────────────────────────────────────────────────────────

def plot_grid_heatmap(
    grid_gdf: gpd.GeoDataFrame,
    score_col: str = "score",
    out_path: str | None = None,
    title: str = "입지 점수 히트맵",
) -> str:
    """
    격자별 점수를 색상으로 표현한 folium 지도 생성.

    Args:
        grid_gdf:  score 컬럼이 있는 격자 GeoDataFrame
        score_col: 색상 기준 컬럼명
        out_path:  저장 경로 (None이면 output/map_heatmap.html)

    Returns:
        저장된 파일 경로
    """
    if score_col not in grid_gdf.columns:
        raise ValueError(f"'{score_col}' 컬럼이 없습니다.")

    out_path = out_path or str(OUTPUT_DIR / "map_heatmap.html")
    center   = _grid_center(grid_gdf)

    m = folium.Map(location=center, zoom_start=12, tiles="CartoDB positron")

    # 범례용 colormap
    colormap = folium.LinearColormap(
        colors=["#d73027", "#fee08b", "#1a9850"],
        vmin=grid_gdf[score_col].min(),
        vmax=grid_gdf[score_col].max(),
        caption="입지 점수 (0=낮음, 1=높음)",
    )

    for _, row in grid_gdf.iterrows():
        score = row[score_col]
        color = colormap(score)

        folium.GeoJson(
            row["geometry"].__geo_interface__,
            style_function=lambda _, c=color: {
                "fillColor":   c,
                "color":       "gray",
                "weight":      0.3,
                "fillOpacity": 0.6,
            },
            tooltip=folium.Tooltip(
                f"점수: {score:.3f}<br>"
                f"인구: {row.get('population', 'N/A')}<br>"
                f"유동인구: {row.get('floating', 'N/A')}<br>"
                f"경쟁업체: {row.get('competitor_cnt', 'N/A')}<br>"
                f"교통: {row.get('transport_cnt', 'N/A')}"
            ),
        ).add_to(m)

    colormap.add_to(m)
    folium.LayerControl().add_to(m)

    m.save(out_path)
    log.info(f"히트맵 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-02: 반경 버퍼 오버레이 지도
# ─────────────────────────────────────────────────────────

def plot_buffer_map(
    lat: float,
    lng: float,
    radii: list[int],
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
    summary_df:     pd.DataFrame | None = None,
    out_path: str | None = None,
) -> str:
    """
    특정 좌표 기준 복수 반경 버퍼 + 주변 데이터를 지도에 표시.

    Returns:
        저장된 파일 경로
    """
    out_path = out_path or str(OUTPUT_DIR / "map_buffer.html")
    m = folium.Map(location=[lat, lng], zoom_start=15, tiles="CartoDB positron")

    # 중심점 마커
    folium.Marker(
        location=[lat, lng],
        icon=folium.Icon(color="red", icon="star"),
        tooltip="분석 지점",
    ).add_to(m)

    # 반경별 버퍼 원 (색상 순차 적용)
    radius_colors = ["#e41a1c", "#377eb8", "#4daf4a", "#984ea3"]
    for i, r in enumerate(sorted(radii)):
        color = radius_colors[i % len(radius_colors)]

        # 요약 정보 팝업
        popup_text = f"반경 {r}m"
        if summary_df is not None and not summary_df.empty:
            row = summary_df[summary_df["radius_m"] == r]
            if not row.empty:
                popup_text += (
                    f"<br>인구: {row['population'].values[0]:.0f}"
                    f"<br>유동: {row['floating'].values[0]:.0f}"
                    f"<br>경쟁: {row['competitor_cnt'].values[0]:.0f}"
                    f"<br>교통: {row['transport_cnt'].values[0]:.0f}"
                )

        folium.Circle(
            location=[lat, lng],
            radius=r,
            color=color,
            fill=True,
            fill_opacity=0.08,
            weight=2,
            tooltip=popup_text,
        ).add_to(m)

    # 경쟁업체 마커
    if competitor_gdf is not None and len(competitor_gdf) > 0:
        comp_group = folium.FeatureGroup(name="경쟁업체")
        for _, row in competitor_gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=5,
                color="#e41a1c",
                fill=True,
                fill_opacity=0.7,
                tooltip=row.get("name", "경쟁업체"),
            ).add_to(comp_group)
        comp_group.add_to(m)

    # 교통 마커
    if transport_gdf is not None and len(transport_gdf) > 0:
        trans_group = folium.FeatureGroup(name="교통 인프라")
        for _, row in transport_gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=5,
                color="#377eb8",
                fill=True,
                fill_opacity=0.7,
                tooltip=row.get("stop_name", "교통"),
            ).add_to(trans_group)
        trans_group.add_to(m)

    folium.LayerControl().add_to(m)
    m.save(out_path)
    log.info(f"버퍼 지도 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-03: 후보지 점수 바 차트
# ─────────────────────────────────────────────────────────

def plot_score_bar(
    candidates_df: pd.DataFrame,
    name_col:  str = "name",
    score_col: str = "score",
    out_path:  str | None = None,
) -> str:
    """
    후보지별 점수 가로 바 차트 생성.

    Returns:
        저장된 PNG 파일 경로
    """
    out_path = out_path or str(OUTPUT_DIR / "chart_scores.png")

    df = candidates_df.sort_values(score_col, ascending=True).copy()

    fig, ax = plt.subplots(figsize=(8, max(4, len(df) * 0.6)))

    colors = [_score_to_color(s) for s in df[score_col]]
    bars   = ax.barh(df[name_col], df[score_col], color=colors, edgecolor="white", height=0.6)

    # 바 끝에 점수 레이블
    for bar, val in zip(bars, df[score_col]):
        ax.text(
            bar.get_width() + 0.01, bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}", va="center", ha="left", fontsize=9,
        )

    ax.set_xlim(0, 1.15)
    ax.set_xlabel("입지 점수", fontsize=11)
    ax.set_title("후보지 입지 점수 비교", fontsize=13, fontweight="bold", pad=12)
    ax.spines[["top", "right"]].set_visible(False)
    ax.axvline(x=0.5, color="gray", linestyle="--", linewidth=0.8, alpha=0.5)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    log.info(f"점수 바 차트 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-03b: 11팩터 레이더 차트 (Top 1 프로필)
# ─────────────────────────────────────────────────────────

# 축별 라벨과 positive/negative 방향
# Why: competitor/rent는 낮을수록 좋으므로 percentile 계산 시 반전
_RADAR_FACTORS = [
    ("population",      "인구",         False),
    ("floating",        "유동인구",     False),
    ("workplace",       "직장인구",     False),
    ("competitor_cnt",  "경쟁 희소성",  True),   # 반전
    ("transport_score", "교통 접근성",  False),
    ("parking_cnt",     "주차",         False),
    ("diversity",       "상권 다양성",  False),
    ("income",          "소득 수준",    False),
    ("rent",            "월세 저렴",    True),   # 반전
    ("commercial_cnt",  "상가 밀집",    False),
    ("road_score",      "도로 접근성",  False),
]


def plot_radar_top1(
    scored_gdf: pd.DataFrame,
    top_gdf: pd.DataFrame,
    out_path: str | None = None,
    include_top2: bool = True,
) -> str:
    """1위 후보지의 11팩터 상대 위치(전체 대비 percentile)를 레이더로 표시.

    Args:
        scored_gdf: 전체 격자 (percentile 계산용)
        top_gdf:    상위 후보지 (1위는 iloc[0])
        include_top2: True면 2위 점선 오버레이로 비교

    Why: 점수 하나로는 '왜' 1위인지 알 수 없음. 11개 팩터의 전체 대비 순위를
         방사형으로 그리면 이 입지의 강점·약점이 한눈에 드러남.
    """
    out_path = out_path or str(OUTPUT_DIR / "chart_radar.png")
    if len(top_gdf) == 0:
        return out_path

    # 사용 가능한 팩터만 필터 (데이터 누락 팩터 제외)
    avail = [(c, lbl, inv) for c, lbl, inv in _RADAR_FACTORS if c in scored_gdf.columns]
    n = len(avail)
    if n == 0:
        return out_path

    def _percentile(row, factors):
        """각 팩터에 대한 전체 대비 percentile (0~1)."""
        out = []
        for col, _, inv in factors:
            series = scored_gdf[col]
            val = row[col]
            pct = float((series <= val).mean())
            out.append(1.0 - pct if inv else pct)
        return out

    top1_pct = _percentile(top_gdf.iloc[0], avail)
    labels = [lbl for _, lbl, _ in avail]

    # 레이더를 닫기 위해 첫 값 끝에 반복
    angles = np.linspace(0, 2 * np.pi, n, endpoint=False).tolist()
    top1_pct_closed = top1_pct + [top1_pct[0]]
    angles_closed = angles + [angles[0]]

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw={"projection": "polar"})
    ax.plot(angles_closed, top1_pct_closed, color="#2E7D32", linewidth=2.2, label="1위")
    ax.fill(angles_closed, top1_pct_closed, color="#2E7D32", alpha=0.22)

    if include_top2 and len(top_gdf) >= 2:
        top2_pct = _percentile(top_gdf.iloc[1], avail)
        top2_closed = top2_pct + [top2_pct[0]]
        ax.plot(angles_closed, top2_closed, color="#0277BD",
                linewidth=1.3, linestyle="--", label="2위")

    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_xticks(angles)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylim(0, 1)
    ax.set_yticks([0.25, 0.5, 0.75])
    ax.set_yticklabels(["25%", "50%", "75%"], fontsize=8, color="gray")
    ax.set_title("1위 후보지 11팩터 프로필 (전체 대비 상위 %)",
                 fontsize=12, fontweight="bold", pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.20, 1.05), fontsize=9)
    ax.grid(alpha=0.4)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    log.info(f"레이더 차트 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-03c: 점수 분포 히스토그램 (1위 위치 표시)
# ─────────────────────────────────────────────────────────

def plot_score_distribution(
    scored_gdf: pd.DataFrame,
    top_gdf: pd.DataFrame,
    score_col: str = "score",
    out_path: str | None = None,
) -> str:
    """전체 격자의 점수 분포를 히스토그램으로 그리고 1위 점수를 표시.

    Why: 1위 후보지의 점수가 전체에서 얼마나 드문 값인지 파악하면
         이 입지가 '압도적 1위'인지 '간신히 1위'인지 의사결정에 결정적.
    """
    out_path = out_path or str(OUTPUT_DIR / "chart_dist.png")
    if len(scored_gdf) == 0 or len(top_gdf) == 0:
        return out_path

    scores = scored_gdf[score_col].values
    top1_score = float(top_gdf.iloc[0][score_col])
    top1_pct = float((scores <= top1_score).mean()) * 100

    fig, ax = plt.subplots(figsize=(8, 4))
    n, bins, patches = ax.hist(scores, bins=30, color="#B0BEC5",
                                edgecolor="white", alpha=0.8)

    # 1위 셀이 속한 bin만 강조색
    top1_bin_idx = np.searchsorted(bins, top1_score) - 1
    top1_bin_idx = max(0, min(top1_bin_idx, len(patches) - 1))
    patches[top1_bin_idx].set_facecolor("#2E7D32")
    patches[top1_bin_idx].set_alpha(1.0)

    ax.axvline(top1_score, color="#E53935", linestyle="--", linewidth=1.5)
    ax.annotate(
        f"1위: {top1_score:.3f}\n(상위 {100 - top1_pct:.1f}%)",
        xy=(top1_score, max(n) * 0.88),
        xytext=(12, 0), textcoords="offset points",
        fontsize=10, color="#E53935", fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#E53935", lw=1),
    )

    ax.set_xlabel("입지 점수", fontsize=11)
    ax.set_ylabel("격자 수", fontsize=11)
    ax.set_title(f"전체 {len(scored_gdf):,}개 격자의 점수 분포",
                 fontsize=12, fontweight="bold", pad=10)
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    log.info(f"점수 분포 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-04: 클러스터 지도
# ─────────────────────────────────────────────────────────

def plot_cluster_map(
    cluster_gdf: gpd.GeoDataFrame,
    label_col:   str = "cluster_label",
    out_path:    str | None = None,
) -> str:
    """
    클러스터 레이블별로 색상을 달리한 격자 지도 생성.

    Returns:
        저장된 파일 경로
    """
    out_path = out_path or str(OUTPUT_DIR / "map_cluster.html")
    center   = _grid_center(cluster_gdf)

    m = folium.Map(location=center, zoom_start=12, tiles="CartoDB positron")

    # 레이블별 색상 맵 (-1 = 노이즈 → 회색)
    labels  = cluster_gdf[label_col].unique()
    palette = plt.cm.get_cmap("tab20", max(len(labels), 1))
    color_map = {
        label: ("gray" if label == -1 else mcolors.to_hex(palette(i)))
        for i, label in enumerate(sorted(labels))
    }

    for _, row in cluster_gdf.iterrows():
        label = row[label_col]
        color = color_map.get(label, "gray")
        label_text = "노이즈" if label == -1 else f"클러스터 {label}"

        folium.GeoJson(
            row["geometry"].__geo_interface__,
            style_function=lambda _, c=color: {
                "fillColor":   c,
                "color":       "white",
                "weight":      0.3,
                "fillOpacity": 0.65,
            },
            tooltip=folium.Tooltip(
                f"{label_text}<br>점수: {row.get('score', 'N/A')}"
            ),
        ).add_to(m)

    folium.LayerControl().add_to(m)
    m.save(out_path)
    log.info(f"클러스터 지도 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-05: 통합 지도 (모든 레이어 한 장에)
# ─────────────────────────────────────────────────────────

def plot_combined_map(
    scored_gdf:     gpd.GeoDataFrame,
    gap_gdf:        gpd.GeoDataFrame | None = None,
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
    population_gdf: gpd.GeoDataFrame | None = None,
    label:          str = "분석",
    out_path:       str | None = None,
) -> str:
    """
    입지 점수 히트맵 + 경쟁업체 + 교통 + 인구proxy + 경쟁공백을
    레이어 토글로 한 지도에 표시.

    우측 상단 레이어 컨트롤로 각 레이어 ON/OFF 가능.

    Returns:
        저장된 HTML 파일 경로
    """
    out_path = out_path or str(OUTPUT_DIR / "map_combined.html")
    center   = _grid_center(scored_gdf)

    m = folium.Map(location=center, zoom_start=13, tiles="CartoDB positron")

    # ── 레이어 1: 입지 점수 히트맵 ──────────────────────
    score_col  = "score"
    colormap   = folium.LinearColormap(
        colors=["#d73027", "#fee08b", "#1a9850"],
        vmin=scored_gdf[score_col].min(),
        vmax=scored_gdf[score_col].max(),
        caption="입지 점수",
    )
    heatmap_layer = folium.FeatureGroup(name="입지 점수 히트맵", show=True)
    for _, row in scored_gdf.iterrows():
        score = row[score_col]
        folium.GeoJson(
            row["geometry"].__geo_interface__,
            style_function=lambda _, c=colormap(score): {
                "fillColor": c, "color": "gray",
                "weight": 0.3, "fillOpacity": 0.55,
            },
            tooltip=folium.Tooltip(
                f"<b>입지 점수: {score:.3f}</b><br>"
                f"인구: {row.get('population', 0):.0f}<br>"
                f"경쟁업체: {row.get('competitor_cnt', 0):.0f}<br>"
                f"교통: {row.get('transport_cnt', 0):.0f}"
            ),
        ).add_to(heatmap_layer)
    heatmap_layer.add_to(m)
    colormap.add_to(m)

    # ── 레이어 2: 경쟁 공백 지역 ────────────────────────
    if gap_gdf is not None:
        gap_layer = folium.FeatureGroup(name="경쟁 공백 지역 (기회 구역)", show=True)
        for _, row in gap_gdf[gap_gdf["is_gap"]].iterrows():
            folium.GeoJson(
                row["geometry"].__geo_interface__,
                style_function=lambda _: {
                    "fillColor": "#0066ff", "color": "#0033cc",
                    "weight": 2.5, "fillOpacity": 0,
                },
                tooltip=folium.Tooltip(
                    f"<b>경쟁 공백 구역</b><br>"
                    f"경쟁업체: {row.get('competitor_cnt', 0):.0f}<br>"
                    f"점수: {row.get('score', 0):.3f}"
                ),
            ).add_to(gap_layer)
        gap_layer.add_to(m)

    # ── 레이어 3: 기존 경쟁업체 ─────────────────────────
    if competitor_gdf is not None and len(competitor_gdf) > 0:
        comp_layer = folium.FeatureGroup(name=f"기존 경쟁업체 ({len(competitor_gdf)}건)", show=True)
        for _, row in competitor_gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=6,
                color="#e41a1c",
                fill=True,
                fill_color="#e41a1c",
                fill_opacity=0.85,
                tooltip=folium.Tooltip(
                    f"<b>{row.get('name', '경쟁업체')}</b><br>"
                    f"{row.get('address', '')}"
                ),
            ).add_to(comp_layer)
        comp_layer.add_to(m)

    # ── 레이어 4: 교통 인프라 ────────────────────────────
    if transport_gdf is not None and len(transport_gdf) > 0:
        icon_map = {"subway": ("train", "#377eb8"), "bus": ("bus", "#984ea3")}
        trans_layer = folium.FeatureGroup(name=f"교통 인프라 ({len(transport_gdf)}건)", show=True)
        for _, row in transport_gdf.iterrows():
            t = row.get("type", "subway")
            icon_name, color = icon_map.get(t, ("info-sign", "#888888"))
            folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=folium.Icon(color="blue" if t == "subway" else "purple",
                                 icon=icon_name, prefix="fa"),
                tooltip=folium.Tooltip(
                    f"<b>{row.get('stop_name', '교통')}</b><br>{t}"
                ),
            ).add_to(trans_layer)
        trans_layer.add_to(m)

    # ── 레이어 5: 인구 proxy (아파트) ───────────────────
    if population_gdf is not None and len(population_gdf) > 0:
        pop_layer = folium.FeatureGroup(name=f"인구 proxy - 아파트 ({len(population_gdf)}건)", show=False)
        for _, row in population_gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=3,
                color="#ff7f00",
                fill=True,
                fill_color="#ff7f00",
                fill_opacity=0.6,
                tooltip=folium.Tooltip(row.get("name", "아파트")),
            ).add_to(pop_layer)
        pop_layer.add_to(m)

    # ── 범례 HTML ────────────────────────────────────────
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                background:white;padding:14px 18px;border-radius:10px;
                box-shadow:0 2px 10px rgba(0,0,0,0.25);font-size:13px;
                line-height:1.5;min-width:210px;">
        <b style="font-size:14px;display:block;margin-bottom:8px;">
            {label}
        </b>

        <!-- 입지 점수 히트맵 -->
        <div style="margin-bottom:6px;">
            <div style="display:inline-block;width:80px;height:12px;border-radius:3px;
                        background:linear-gradient(to right,#d73027,#fee08b,#1a9850);
                        vertical-align:middle;margin-right:6px;border:1px solid #ccc;">
            </div>
            <span style="vertical-align:middle;">입지 점수 (낮음→높음)</span>
        </div>

        <!-- 경쟁 공백 -->
        <div style="margin-bottom:6px;">
            <div style="display:inline-block;width:20px;height:12px;
                        border:2.5px solid #0033cc;border-radius:2px;
                        vertical-align:middle;margin-right:6px;">
            </div>
            <span style="vertical-align:middle;">경쟁 공백 지역 (기회 구역)</span>
        </div>

        <!-- 경쟁업체 -->
        <div style="margin-bottom:6px;">
            <span style="display:inline-block;width:14px;height:14px;
                         background:#e41a1c;border-radius:50%;
                         vertical-align:middle;margin-right:6px;"></span>
            <span style="vertical-align:middle;">기존 경쟁업체</span>
        </div>

        <!-- 지하철 -->
        <div style="margin-bottom:6px;">
            <span style="display:inline-block;font-size:15px;
                         vertical-align:middle;margin-right:6px;">🚇</span>
            <span style="vertical-align:middle;">지하철역</span>
        </div>

        <!-- 버스 -->
        <div style="margin-bottom:6px;">
            <span style="display:inline-block;font-size:15px;
                         vertical-align:middle;margin-right:6px;">🚌</span>
            <span style="vertical-align:middle;">버스정류장</span>
        </div>

        <!-- 아파트 -->
        <div>
            <span style="display:inline-block;width:10px;height:10px;
                         background:#ff7f00;border-radius:50%;
                         vertical-align:middle;margin-right:6px;"></span>
            <span style="vertical-align:middle;">아파트 (인구 proxy)</span>
        </div>
    </div>
    """.format(label=label)
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl(collapsed=False).add_to(m)
    m.save(out_path)
    log.info(f"통합 지도 저장: {out_path}")
    return out_path


# ─────────────────────────────────────────────────────────
# V-06: 통합 지도 (folium.Map 객체 반환 — Streamlit v2용)
# ─────────────────────────────────────────────────────────

def build_combined_folium_map(
    scored_gdf:     gpd.GeoDataFrame,
    gap_gdf:        gpd.GeoDataFrame | None = None,
    competitor_gdf: gpd.GeoDataFrame | None = None,
    transport_gdf:  gpd.GeoDataFrame | None = None,
    population_gdf: gpd.GeoDataFrame | None = None,
    top_gdf:        gpd.GeoDataFrame | None = None,
    label:          str = "분석",
    center:         tuple[float, float] | None = None,
    zoom_start:     int = 13,
    selected_rank:  int | None = None,
) -> folium.Map:
    """
    통합 분석 지도를 folium.Map 객체로 반환 (파일 저장 없음).

    범례와 레이어 토글을 좌하단 단일 커스텀 컨트롤로 통합.
    상위 후보지에 번호 마커를 표시하며, selected_rank 지정 시 주황색 강조.

    v4.1: 지하철 노선도 TileLayer 오버레이 추가 (OpenRailwayMap)

    Args:
        top_gdf:       rank, geometry 컬럼이 있는 상위 후보지 GeoDataFrame
        center:        지도 초기 중심 (lat, lng); None이면 scored_gdf 중심
        zoom_start:    초기 줌 레벨
        selected_rank: 강조 표시할 후보지 순위 (주황색 대형 마커)

    Returns:
        folium.Map 객체 (st_folium에 직접 전달 가능)
    """
    if center is None:
        center = _grid_center(scored_gdf)

    m = folium.Map(location=center, zoom_start=zoom_start, tiles="CartoDB positron")

    # ── 범례 아이콘 HTML (Leaflet이 name을 innerHTML로 렌더링) ──
    _I_HEAT = (
        '<div style="display:inline-block;width:52px;height:9px;vertical-align:middle;'
        'border-radius:2px;background:linear-gradient(to right,#d73027,#fee08b,#1a9850);'
        'border:1px solid #ccc;margin-right:5px;"></div>'
    )
    _I_GAP  = (
        '<div style="display:inline-block;width:14px;height:9px;vertical-align:middle;'
        'border:2.5px solid #0033cc;border-radius:1px;margin-right:5px;"></div>'
    )
    _I_COMP = (
        '<span style="display:inline-block;width:11px;height:11px;vertical-align:middle;'
        'background:#e41a1c;border-radius:50%;margin-right:5px;"></span>'
    )
    _I_TRANS = (
        '<span style="display:inline-block;vertical-align:middle;'
        'font-size:13px;margin-right:3px;">&#x1F687;&#x1F68C;</span>'
    )
    _I_POP  = (
        '<span style="display:inline-block;width:9px;height:9px;vertical-align:middle;'
        'background:#ff7f00;border-radius:50%;margin-right:5px;"></span>'
    )
    _I_CAND = (
        '<span style="display:inline-block;width:16px;height:16px;vertical-align:middle;'
        'background:#2c7bb6;border-radius:50%;color:white;font-size:9px;font-weight:bold;'
        'text-align:center;line-height:16px;margin-right:5px;">N</span>'
    )
    _I_SUBWAY = (
        '<span style="display:inline-block;vertical-align:middle;'
        'font-size:13px;margin-right:3px;">&#x1F6E4;&#xFE0F;</span>'
    )

    # ── 레이어 1: 입지 점수 히트맵 ──────────────────────
    score_col = "score"
    colormap  = folium.LinearColormap(
        colors=["#d73027", "#fee08b", "#1a9850"],
        vmin=scored_gdf[score_col].min(),
        vmax=scored_gdf[score_col].max(),
    )
    heatmap_layer = folium.FeatureGroup(name=f"{_I_HEAT} 입지 점수 히트맵", show=True)
    for _, row in scored_gdf.iterrows():
        score = row[score_col]
        # v4.2: tooltip에 소득·월세 정보 추가
        _tip_extra = ""
        if "income" in scored_gdf.columns and row.get("income", 0) > 0:
            _tip_extra += f"<br>소득수준: {row['income']:.0f}만/㎡"
        if "rent" in scored_gdf.columns and row.get("rent", 0) > 0:
            _tip_extra += f"<br>월세수준: {row['rent']:.0f}만원"
        _grid_id = row.get("grid_id", "")
        folium.GeoJson(
            row["geometry"].__geo_interface__,
            style_function=lambda _, c=colormap(score): {
                "fillColor": c, "color": "gray",
                "weight": 0.3, "fillOpacity": 0.55,
            },
            tooltip=folium.Tooltip(
                f"<b>[{_grid_id}] 점수: {score:.3f}</b><br>"
                f"인구: {row.get('population', 0):.0f}<br>"
                f"경쟁업체: {row.get('competitor_cnt', 0):.0f}<br>"
                f"교통: {row.get('transport_cnt', 0):.0f}"
                f"{_tip_extra}"
            ),
        ).add_to(heatmap_layer)
    heatmap_layer.add_to(m)

    # ── 레이어 2: 경쟁 공백 지역 ────────────────────────
    gap_layer = None
    if gap_gdf is not None and "is_gap" in gap_gdf.columns:
        gap_rows = gap_gdf[gap_gdf["is_gap"]]
        if len(gap_rows) > 0:
            gap_layer = folium.FeatureGroup(name=f"{_I_GAP} 경쟁 공백 지역", show=True)
            for _, row in gap_rows.iterrows():
                folium.GeoJson(
                    row["geometry"].__geo_interface__,
                    style_function=lambda _: {
                        "fillColor": "#0066ff", "color": "#0033cc",
                        "weight": 2.5, "fillOpacity": 0,
                    },
                    tooltip=folium.Tooltip(
                        f"<b>경쟁 공백 구역</b><br>"
                        f"경쟁업체: {row.get('competitor_cnt', 0):.0f}<br>"
                        f"점수: {row.get('score', 0):.3f}"
                    ),
                ).add_to(gap_layer)
            gap_layer.add_to(m)

    # ── 레이어 3: 기존 경쟁업체 ─────────────────────────
    comp_layer = None
    if competitor_gdf is not None and len(competitor_gdf) > 0:
        comp_layer = folium.FeatureGroup(name=f"{_I_COMP} 기존 경쟁업체", show=True)
        for _, row in competitor_gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=6,
                color="#e41a1c",
                fill=True,
                fill_color="#e41a1c",
                fill_opacity=0.85,
                tooltip=folium.Tooltip(
                    f"<b>{row.get('name', '경쟁업체')}</b><br>"
                    f"{row.get('address', '')}"
                ),
            ).add_to(comp_layer)
        comp_layer.add_to(m)

    # ── 레이어 4: 교통 인프라 ────────────────────────────
    trans_layer = None
    if transport_gdf is not None and len(transport_gdf) > 0:
        trans_layer = folium.FeatureGroup(name=f"{_I_TRANS} 교통 인프라", show=True)
        for _, row in transport_gdf.iterrows():
            t = row.get("type", "subway")
            folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=folium.Icon(
                    color="blue" if t == "subway" else "purple",
                    icon="train" if t == "subway" else "bus",
                    prefix="fa",
                ),
                tooltip=folium.Tooltip(
                    f"<b>{row.get('stop_name', '교통')}</b><br>{t}"
                ),
            ).add_to(trans_layer)
        trans_layer.add_to(m)

    # ── 레이어 5: 인구 proxy (아파트) ───────────────────
    pop_layer = None
    if population_gdf is not None and len(population_gdf) > 0:
        pop_layer = folium.FeatureGroup(name=f"{_I_POP} 인구 proxy (아파트)", show=False)
        for _, row in population_gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=3,
                color="#ff7f00",
                fill=True,
                fill_color="#ff7f00",
                fill_opacity=0.6,
                tooltip=folium.Tooltip(row.get("name", "아파트")),
            ).add_to(pop_layer)
        pop_layer.add_to(m)

    # ── 레이어 6: 상위 후보지 번호 마커 ─────────────────
    cand_layer = None
    if top_gdf is not None and len(top_gdf) > 0:
        cand_layer = folium.FeatureGroup(name=f"{_I_CAND} 상위 후보지", show=True)
        for _, row in top_gdf.iterrows():
            rank   = int(row.get("rank", 0))
            centroid = row["geometry"].centroid
            is_sel = (selected_rank is not None and rank == selected_rank)
            bg     = "#FF6600" if is_sel else "#2c7bb6"
            border = "3px solid #FF3300" if is_sel else "2px solid white"
            sz     = 30 if is_sel else 22

            folium.Marker(
                location=[centroid.y, centroid.x],
                icon=folium.DivIcon(
                    html=(
                        f'<div style="background:{bg};color:white;border-radius:50%;'
                        f'width:{sz}px;height:{sz}px;display:flex;align-items:center;'
                        f'justify-content:center;font-weight:bold;font-size:11px;'
                        f'border:{border};box-shadow:0 2px 6px rgba(0,0,0,0.45);">'
                        f'{rank}</div>'
                    ),
                    icon_size=(sz + 4, sz + 4),
                    icon_anchor=((sz + 4) // 2, (sz + 4) // 2),
                ),
                tooltip=folium.Tooltip(
                    f"<b>#{rank}</b> | 점수: {row.get('score', 0):.3f}"
                ),
            ).add_to(cand_layer)
        cand_layer.add_to(m)

    # ── 레이어 7: 지하철 노선도 (OpenRailwayMap TileLayer) ──
    # 기본 OFF — 사용자가 레이어 컨트롤에서 토글
    folium.TileLayer(
        tiles="https://{s}.tiles.openrailwaymap.org/standard/{z}/{x}/{y}.png",
        attr='<a href="https://www.openrailwaymap.org/">OpenRailwayMap</a>',
        name=f"{_I_SUBWAY} 지하철 노선도",
        overlay=True,
        show=False,
        max_zoom=19,
        opacity=0.7,
    ).add_to(m)

    # ── 네이티브 LayerControl (bottomleft) ──────────────
    folium.LayerControl(position='bottomleft', collapsed=False).add_to(m)

    safe_label = label.replace("'", "\\'")
    enhance_html = f"""
<style>
/* 기본지도(베이스레이어) + 구분선 숨김 */
.leaflet-control-layers-base,
.leaflet-control-layers-separator {{ display: none !important; }}
/* attribution 숨김 */
.leaflet-control-attribution {{ display: none !important; }}
/* 패널 외관 */
.leaflet-bottom.leaflet-left .leaflet-control-layers {{
    border-radius: 10px !important;
    padding: 12px 16px 10px !important;
    box-shadow: 0 2px 10px rgba(0,0,0,.28) !important;
    font-size: 12px !important;
    line-height: 1.8 !important;
    min-width: 220px !important;
    border: none !important;
}}
.leaflet-bottom.leaflet-left .leaflet-control-layers label {{
    display: flex !important;
    align-items: center !important;
    margin-bottom: 2px !important;
    cursor: pointer !important;
}}
.leaflet-bottom.leaflet-left .leaflet-control-layers input[type=checkbox] {{
    margin: 0 8px 0 0 !important;
    flex-shrink: 0 !important;
    cursor: pointer !important;
}}
</style>
<script>
(function addTitle() {{
    var overlays = document.querySelector('.leaflet-control-layers-overlays');
    if (!overlays) {{ setTimeout(addTitle, 100); return; }}
    var ctrl = overlays.closest('.leaflet-control-layers');
    if (!ctrl || document.getElementById('__lc_title')) return;
    var t = document.createElement('b');
    t.id = '__lc_title';
    t.style.cssText = 'display:block;margin-bottom:10px;font-size:13px;';
    t.textContent = '{safe_label}';
    ctrl.insertBefore(t, ctrl.firstChild);
}})();
</script>
"""
    m.get_root().html.add_child(folium.Element(enhance_html))

    log.info(f"통합 지도(folium.Map) 생성 완료: {label}")
    return m
