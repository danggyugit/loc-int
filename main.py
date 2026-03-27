# main.py - CLI 진입점
# 사용법: python main.py <subcommand> [options]

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# 공통 파서
# ─────────────────────────────────────────────────────────

def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--region",   default=None,  help="시·도명 (예: 서울특별시). 기본값: 전국")
    parser.add_argument("--preset",   default=None,  help="업종 프리셋 (cafe / hospital / convenience 등)")
    parser.add_argument("--weights",  default=None,  help='가중치 JSON (예: \'{"population":0.4,"floating":0.3,"competitor":0.2,"accessibility":0.1}\')')
    parser.add_argument("--output",   default=None,  help="결과 저장 디렉터리 (기본: ./output/)")
    parser.add_argument("--top-n",    default=10, type=int, help="상위 N개 후보지 (기본: 10)")


# ─────────────────────────────────────────────────────────
# grid 서브커맨드
# ─────────────────────────────────────────────────────────

def cmd_grid(args: argparse.Namespace) -> None:
    """
    격자 분석 + 점수화.

    필수 데이터:
      --boundary: 행정경계 GIS 파일 (SHP / GeoJSON)
    선택 데이터:
      --population, --floating, --competitor-csv, --transport
    """
    from src.loader import load_boundary, load_population, load_floating, load_competitor, load_transport
    from src.grid import build_grid_features
    from src.scoring import score_and_rank
    from src.visualizer import plot_grid_heatmap, plot_score_bar

    if not args.boundary:
        log.error("--boundary 경로를 지정하세요.")
        sys.exit(1)

    log.info(f"=== grid 분석 시작 | cell={args.cell_size}m | region={args.region or '전국'} ===")

    boundary = load_boundary(args.boundary, region=args.region)

    population_gdf = load_population(args.population, region=args.region) if args.population else None
    floating_gdf   = load_floating(args.floating, region=args.region)     if args.floating   else None
    transport_gdf  = load_transport(args.transport, region=args.region)   if args.transport  else None

    # 경쟁업체: API 또는 CSV
    competitor_gdf = load_competitor(
        category=args.preset or "default",
        region=args.region,
        csv_path=args.competitor_csv,
    ) if (args.preset or args.competitor_csv) else None

    grid = build_grid_features(
        boundary,
        cell_size_m=args.cell_size,
        population_gdf=population_gdf,
        floating_gdf=floating_gdf,
        competitor_gdf=competitor_gdf,
        transport_gdf=transport_gdf,
    )

    scored, top = score_and_rank(grid, preset=args.preset, weights_json=args.weights, top_n=args.top_n)

    # 출력 경로
    out_dir = Path(args.output) if args.output else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    heatmap_path = plot_grid_heatmap(scored, out_path=str(out_dir / "map_heatmap.html"))
    chart_path   = plot_score_bar(top, name_col="grid_id", out_path=str(out_dir / "chart_scores.png"))

    # 상위 후보지 CSV 저장
    csv_path = out_dir / "top_candidates.csv"
    top.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"\n[결과]")
    print(f"  히트맵 지도  : {heatmap_path}")
    print(f"  점수 차트    : {chart_path}")
    print(f"  상위 후보지  : {csv_path}")
    print(f"\n상위 {args.top_n}개 후보지:")
    print(top[["rank", "grid_id", "population", "floating", "competitor_cnt", "transport_cnt", "score"]].to_string(index=False))


# ─────────────────────────────────────────────────────────
# buffer 서브커맨드
# ─────────────────────────────────────────────────────────

def cmd_buffer(args: argparse.Namespace) -> None:
    """특정 좌표 기준 반경 분석."""
    from src.loader import load_population, load_floating, load_competitor, load_transport
    from src.buffer import analyze_multi_radius
    from src.visualizer import plot_buffer_map

    log.info(f"=== buffer 분석 시작 | lat={args.lat}, lng={args.lng} | radius={args.radius} ===")

    population_gdf = load_population(args.population, region=args.region) if args.population else None
    floating_gdf   = load_floating(args.floating, region=args.region)     if args.floating   else None
    transport_gdf  = load_transport(args.transport, region=args.region)   if args.transport  else None
    competitor_gdf = load_competitor(
        category=args.preset or "default",
        region=args.region,
        csv_path=args.competitor_csv,
    ) if (args.preset or args.competitor_csv) else None

    summary = analyze_multi_radius(
        lat=args.lat,
        lng=args.lng,
        radii=args.radius,
        population_gdf=population_gdf,
        floating_gdf=floating_gdf,
        competitor_gdf=competitor_gdf,
        transport_gdf=transport_gdf,
    )

    out_dir = Path(args.output) if args.output else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    map_path = plot_buffer_map(
        lat=args.lat, lng=args.lng,
        radii=args.radius,
        competitor_gdf=competitor_gdf,
        transport_gdf=transport_gdf,
        summary_df=summary,
        out_path=str(out_dir / "map_buffer.html"),
    )
    csv_path = out_dir / "buffer_summary.csv"
    summary.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"\n[반경 분석 결과]")
    print(summary.to_string(index=False))
    print(f"\n  버퍼 지도 : {map_path}")
    print(f"  요약 CSV  : {csv_path}")


# ─────────────────────────────────────────────────────────
# cluster 서브커맨드
# ─────────────────────────────────────────────────────────

def cmd_cluster(args: argparse.Namespace) -> None:
    """격자 분석 후 핫스팟 클러스터링."""
    from src.loader import load_boundary, load_population, load_floating, load_competitor, load_transport
    from src.grid import build_grid_features
    from src.scoring import score_and_rank
    from src.cluster import run_cluster_analysis
    from src.visualizer import plot_cluster_map

    if not args.boundary:
        log.error("--boundary 경로를 지정하세요.")
        sys.exit(1)

    log.info(f"=== cluster 분석 시작 | eps={args.eps}m | min_samples={args.min_samples} ===")

    boundary       = load_boundary(args.boundary, region=args.region)
    population_gdf = load_population(args.population, region=args.region) if args.population else None
    floating_gdf   = load_floating(args.floating, region=args.region)     if args.floating   else None
    transport_gdf  = load_transport(args.transport, region=args.region)   if args.transport  else None
    competitor_gdf = load_competitor(
        category=args.preset or "default",
        region=args.region,
        csv_path=args.competitor_csv,
    ) if (args.preset or args.competitor_csv) else None

    grid = build_grid_features(
        boundary, cell_size_m=args.cell_size,
        population_gdf=population_gdf, floating_gdf=floating_gdf,
        competitor_gdf=competitor_gdf, transport_gdf=transport_gdf,
    )
    scored, _ = score_and_rank(grid, preset=args.preset, weights_json=args.weights, top_n=args.top_n)

    cluster_gdf, summary_df, gap_gdf = run_cluster_analysis(
        scored,
        score_threshold=args.score_threshold,
        eps_m=args.eps,
        min_samples=args.min_samples,
        comp_threshold=args.comp_threshold,
        pop_threshold=args.pop_threshold,
    )

    out_dir = Path(args.output) if args.output else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    map_path = plot_cluster_map(cluster_gdf, out_path=str(out_dir / "map_cluster.html"))
    csv_path = out_dir / "cluster_summary.csv"
    summary_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    gap_path = out_dir / "gap_cells.csv"
    gap_gdf[gap_gdf["is_gap"]].drop(columns="geometry").to_csv(gap_path, index=False, encoding="utf-8-sig")

    print(f"\n[클러스터 요약]")
    print(summary_df[["cluster_label","type","cell_count","avg_score","center_lat","center_lng"]].to_string(index=False))
    print(f"\n경쟁 공백 셀: {gap_gdf['is_gap'].sum()}개")
    print(f"\n  클러스터 지도  : {map_path}")
    print(f"  클러스터 요약  : {csv_path}")
    print(f"  공백 지역 CSV  : {gap_path}")


# ─────────────────────────────────────────────────────────
# compare 서브커맨드
# ─────────────────────────────────────────────────────────

def cmd_compare(args: argparse.Namespace) -> None:
    """복수 후보지 CSV를 읽어 반경 기반 비교."""
    import pandas as pd
    from src.loader import load_population, load_floating, load_competitor, load_transport
    from src.buffer import compare_candidates
    from src.scoring import load_weights, normalize
    from src.visualizer import plot_score_bar

    if not args.candidates:
        log.error("--candidates CSV 경로를 지정하세요. (컬럼: name, lat, lng)")
        sys.exit(1)

    candidates_df = pd.read_csv(args.candidates)
    candidates = candidates_df.to_dict("records")
    log.info(f"=== compare 분석 시작 | {len(candidates)}개 후보지 | radius={args.radius[0]}m ===")

    population_gdf = load_population(args.population, region=args.region) if args.population else None
    floating_gdf   = load_floating(args.floating, region=args.region)     if args.floating   else None
    transport_gdf  = load_transport(args.transport, region=args.region)   if args.transport  else None
    competitor_gdf = load_competitor(
        category=args.preset or "default",
        region=args.region,
        csv_path=args.competitor_csv,
    ) if (args.preset or args.competitor_csv) else None

    result = compare_candidates(
        candidates, radius_m=args.radius[0],
        population_gdf=population_gdf, floating_gdf=floating_gdf,
        competitor_gdf=competitor_gdf, transport_gdf=transport_gdf,
    )

    # 가중치 기반 종합 점수 계산
    weights = load_weights(preset=args.preset, weights_json=args.weights)
    result["score"] = (
        normalize(result["population"])    * weights["population"]
        + normalize(result["floating"])    * weights["floating"]
        - normalize(result["competitor_cnt"]) * weights["competitor"]
        + normalize(result["transport_cnt"])  * weights["accessibility"]
    ).clip(lower=0)
    result["score"]  = normalize(result["score"])
    result["rank"]   = result["score"].rank(ascending=False).astype(int)
    result = result.sort_values("rank")

    out_dir = Path(args.output) if args.output else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    chart_path = plot_score_bar(result, out_path=str(out_dir / "chart_compare.png"))
    csv_path   = out_dir / "compare_result.csv"
    result.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"\n[후보지 비교 결과] (반경 {args.radius[0]}m)")
    print(result[["rank","name","population","floating","competitor_cnt","transport_cnt","score"]].to_string(index=False))
    print(f"\n  비교 차트 : {chart_path}")
    print(f"  결과 CSV  : {csv_path}")


# ─────────────────────────────────────────────────────────
# 메인 파서 조립
# ─────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="locint",
        description="입지 선정 분석 툴 - 데이터 기반 최적 입지 탐색",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── grid ──────────────────────────────────────────────
    p_grid = sub.add_parser("grid", help="격자 분석 + 점수화")
    _add_common_args(p_grid)
    p_grid.add_argument("--boundary",       required=True, help="행정경계 파일 경로 (SHP / GeoJSON)")
    p_grid.add_argument("--cell-size",      default=500, type=int, choices=[250,500,1000,2000],
                        dest="cell_size",   help="격자 셀 크기(m). 기본: 500")
    p_grid.add_argument("--population",     default=None, help="인구 CSV 경로")
    p_grid.add_argument("--floating",       default=None, help="유동인구 CSV 경로")
    p_grid.add_argument("--competitor-csv", default=None, dest="competitor_csv", help="경쟁업체 CSV (API 없을 때)")
    p_grid.add_argument("--transport",      default=None, help="교통 CSV 경로")
    p_grid.set_defaults(func=cmd_grid)

    # ── buffer ────────────────────────────────────────────
    p_buf = sub.add_parser("buffer", help="특정 좌표 반경 분석")
    _add_common_args(p_buf)
    p_buf.add_argument("--lat",  required=True, type=float, help="위도")
    p_buf.add_argument("--lng",  required=True, type=float, help="경도")
    p_buf.add_argument("--radius", nargs="+", type=int, default=[500], help="반경(m) 복수 입력 가능. 기본: 500")
    p_buf.add_argument("--population",     default=None)
    p_buf.add_argument("--floating",       default=None)
    p_buf.add_argument("--competitor-csv", default=None, dest="competitor_csv")
    p_buf.add_argument("--transport",      default=None)
    p_buf.set_defaults(func=cmd_buffer)

    # ── cluster ───────────────────────────────────────────
    p_cls = sub.add_parser("cluster", help="핫스팟 클러스터링 + 경쟁 공백 탐지")
    _add_common_args(p_cls)
    p_cls.add_argument("--boundary",         required=True)
    p_cls.add_argument("--cell-size",        default=500, type=int, choices=[250,500,1000,2000], dest="cell_size")
    p_cls.add_argument("--eps",              default=1500.0, type=float, help="DBSCAN 반경(m). 기본: 1500")
    p_cls.add_argument("--min-samples",      default=3, type=int, dest="min_samples", help="클러스터 최소 셀 수. 기본: 3")
    p_cls.add_argument("--score-threshold",  default=0.6, type=float, dest="score_threshold", help="핫스팟 점수 임계값. 기본: 0.6")
    p_cls.add_argument("--comp-threshold",   default=1.0, type=float, dest="comp_threshold", help="공백 경쟁 임계값. 기본: 1.0")
    p_cls.add_argument("--pop-threshold",    default=0.3, type=float, dest="pop_threshold", help="공백 인구 정규화 임계값. 기본: 0.3")
    p_cls.add_argument("--population",       default=None)
    p_cls.add_argument("--floating",         default=None)
    p_cls.add_argument("--competitor-csv",   default=None, dest="competitor_csv")
    p_cls.add_argument("--transport",        default=None)
    p_cls.set_defaults(func=cmd_cluster)

    # ── compare ───────────────────────────────────────────
    p_cmp = sub.add_parser("compare", help="복수 후보지 비교")
    _add_common_args(p_cmp)
    p_cmp.add_argument("--candidates", required=True, help="후보지 CSV (컬럼: name, lat, lng)")
    p_cmp.add_argument("--radius",     nargs="+", type=int, default=[500])
    p_cmp.add_argument("--population",     default=None)
    p_cmp.add_argument("--floating",       default=None)
    p_cmp.add_argument("--competitor-csv", default=None, dest="competitor_csv")
    p_cmp.add_argument("--transport",      default=None)
    p_cmp.set_defaults(func=cmd_compare)

    return parser


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
