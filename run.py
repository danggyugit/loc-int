# run.py — 원커맨드 입지 분석 실행기
# 지역 + 업종(또는 키워드)만 입력하면 데이터 수집 -> 분석 -> 결과 자동 출력
#
# 사용 예 (프리셋):
#   python run.py --region "강남구" --preset cafe
#   python run.py --region "마포구" --preset hospital --cell-size 500
#
# 사용 예 (자유 키워드):
#   python run.py --region "일산동구" --keyword "도자기 공방"
#   python run.py --region "홍대" --keyword "네일샵" --cell-size 250

import os
import sys
import logging
import argparse
import webbrowser
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PRESETS = ["cafe", "restaurant", "hospital", "convenience", "mart", "pharmacy"]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="run",
        description="입지 선정 분석 - 데이터 수집부터 결과 출력까지 자동 실행",
    )
    p.add_argument("--region",    required=True, help="분석 지역 (예: 강남구, 일산동구)")

    # preset / keyword 중 하나만 필수
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--preset",  help=f"업종 프리셋: {', '.join(PRESETS)}")
    group.add_argument("--keyword", help="자유 검색 키워드 (예: '도자기 공방', '네일샵', '필라테스')")

    p.add_argument("--cell-size", default=500, type=int, choices=[250, 500, 1000, 2000],
                   dest="cell_size", help="격자 셀 크기(m). 기본: 500")
    p.add_argument("--top-n",     default=10, type=int, dest="top_n",
                   help="상위 후보지 수. 기본: 10")
    p.add_argument("--weights",   default=None,
                   help='가중치 직접 지정 JSON (예: \'{"population":0.4,...}\')')
    p.add_argument("--no-browser", action="store_true", dest="no_browser",
                   help="브라우저 자동 열기 비활성화")
    p.add_argument("--output",    default="output", help="결과 저장 폴더. 기본: output/")
    return p


def run(args: argparse.Namespace) -> None:
    import warnings
    warnings.filterwarnings("ignore")

    # 분석 레이블 (파일명 + 출력용)
    label        = args.keyword if args.keyword else args.preset
    safe_label   = label.replace(" ", "_")   # 파일명 공백 제거
    out_dir      = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1. 데이터 자동 수집 ──────────────────────────
    mode = f"키워드: '{args.keyword}'" if args.keyword else f"프리셋: {args.preset}"
    print(f"\n[1/5] 데이터 수집: {args.region} / {mode}")

    from src.collector import collect_all
    data = collect_all(
        region=args.region,
        category=args.preset,
        keyword=args.keyword,
    )

    boundary   = data["boundary"]
    competitor = data["competitor"]
    transport  = data["transport"]
    population = data["population"]

    print(f"      경쟁업체   : {len(competitor)}건")
    print(f"      교통 인프라: {len(transport)}건")
    print(f"      인구 proxy : {len(population) if population is not None else 0}건")

    # ── Step 2. 격자 생성 + 지표 집계 ────────────────────
    print(f"\n[2/5] 격자 분석 (셀 크기: {args.cell_size}m)")
    from src.grid import build_grid_features
    grid = build_grid_features(
        boundary,
        cell_size_m=args.cell_size,
        population_gdf=population,
        competitor_gdf=competitor,
        transport_gdf=transport,
    )
    print(f"      생성 셀 수: {len(grid)}")

    # ── Step 3. 점수화 ────────────────────────────────────
    print(f"\n[3/5] 점수화 ({mode})")
    from src.scoring import score_and_rank

    # 키워드 모드는 preset 없으므로 "default" 가중치 사용 (또는 --weights 직접 지정)
    scored, top = score_and_rank(
        grid,
        preset=args.preset if args.preset else "default",
        weights_json=args.weights,
        top_n=args.top_n,
    )
    print(f"      점수 범위: {scored['score'].min():.3f} ~ {scored['score'].max():.3f}")

    # ── Step 4. 클러스터링 ────────────────────────────────
    print(f"\n[4/5] 핫스팟 클러스터링")
    from src.cluster import run_cluster_analysis
    cluster_gdf, cluster_summary, gap_gdf = run_cluster_analysis(
        scored,
        score_threshold=0.6,
        eps_m=args.cell_size * 3,
        min_samples=3,
        comp_threshold=2,
        pop_threshold=0.3,
    )
    n_hotspot = (cluster_summary["type"] == "핫스팟").sum()
    n_gap     = gap_gdf["is_gap"].sum()
    print(f"      핫스팟 클러스터: {n_hotspot}개 | 경쟁 공백: {n_gap}셀")

    # ── Step 5. 결과 출력 ─────────────────────────────────
    print(f"\n[5/5] 결과 생성")
    from src.visualizer import plot_combined_map, plot_score_bar

    prefix = f"{args.region}_{safe_label}"

    # 통합 지도 (모든 레이어 한 장)
    combined_path = plot_combined_map(
        scored_gdf=scored,
        gap_gdf=gap_gdf,
        competitor_gdf=competitor,
        transport_gdf=transport,
        population_gdf=population,
        label=f"{args.region} {label}",
        out_path=str(out_dir / f"{prefix}_combined.html"),
    )
    top["name"] = top["grid_id"]
    chart_path = plot_score_bar(
        top, name_col="name",
        out_path=str(out_dir / f"{prefix}_scores.png"),
    )
    csv_path = out_dir / f"{prefix}_top{args.top_n}.csv"
    top.drop(columns="geometry").to_csv(csv_path, index=False, encoding="utf-8-sig")

    # ── 결과 요약 ─────────────────────────────────────────
    print("\n" + "="*55)
    print(f" 분석 완료: {args.region} / {label}")
    print("="*55)
    print(f" 분석 셀 수     : {len(scored)}")
    print(f" 경쟁업체       : {len(competitor)}건")
    print(f" 교통 인프라    : {len(transport)}건")
    print(f" 핫스팟 클러스터: {n_hotspot}개")
    print(f" 경쟁 공백 지역 : {n_gap}셀")
    print("="*55)
    print(f"\n 상위 {args.top_n}개 후보지:")
    cols      = ["rank","grid_id","population","floating","competitor_cnt","transport_cnt","score"]
    available = [c for c in cols if c in top.columns]
    print(top[available].to_string(index=False))
    print(f"\n 저장된 파일:")
    print(f"   통합 지도    : {combined_path}")
    print(f"   점수 차트    : {chart_path}")
    print(f"   상위 후보 CSV: {csv_path}")

    # ── 브라우저 자동 열기 ────────────────────────────────
    if not args.no_browser:
        print("\n 브라우저에서 결과를 열고 있습니다...")
        webbrowser.open(f"file:///{Path(combined_path).absolute()}")


def main() -> None:
    if not os.environ.get("KAKAO_API_KEY"):
        print("[오류] KAKAO_API_KEY 환경변수를 설정하세요.")
        print("  PowerShell: $env:KAKAO_API_KEY = 'your_key'")
        sys.exit(1)

    parser = build_parser()
    args   = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
