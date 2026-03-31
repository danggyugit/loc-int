# src/vworld_client.py — 용도지역 분석 클라이언트
# 용도지역 조회 → 업종 입점 가능 여부 판단 (하드 필터)
#
# 데이터 소스 우선순위:
#   1. Vworld 2D데이터 API (LT_C_UQ111) — 정밀 한국 용도지역
#   2. OSM landuse 폴백 — Vworld 키 미등록/실패 시 사용
#
# OSM landuse → 한국 용도지역 매핑 근거:
#   OSM commercial/retail → 상업지역 (score 1.0)
#   OSM residential       → 일반주거지역 (score 0.4~0.6)
#   OSM industrial        → 공업지역 (score 0.3)
#   OSM forest/farmland   → 녹지/농림 (score 0.0~0.1)

import logging
import requests
import geopandas as gpd
from shapely.geometry import shape

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import CRS_WGS84

log = logging.getLogger(__name__)

# ── 용도지역 상업 적합도 점수 (0.0 ~ 1.0) ──────────────
# 0.0 = 상업 활동 불가 (하드 필터로 제거)
# 1.0 = 모든 상업 업종 허용
ZONE_COMMERCIAL_SCORE = {
    "중심상업지역":     1.0,
    "일반상업지역":     1.0,
    "근린상업지역":     1.0,
    "유통상업지역":     1.0,
    "준주거지역":       0.9,
    "준공업지역":       0.8,
    "제3종일반주거지역": 0.6,
    "제2종일반주거지역": 0.4,
    "제1종일반주거지역": 0.2,
    "제1종전용주거지역": 0.0,
    "제2종전용주거지역": 0.0,
    "전용공업지역":     0.0,
    "일반공업지역":     0.3,
    "보전녹지지역":     0.0,
    "생산녹지지역":     0.0,
    "자연녹지지역":     0.1,
    "보전관리지역":     0.0,
    "생산관리지역":     0.1,
    "계획관리지역":     0.3,
    "농림지역":         0.0,
    "자연환경보전지역": 0.0,
}

# OSM landuse → 한국 용도지역 근사 매핑
# Why: Vworld 2D데이터 API 키가 별도 등록이 필요하여 실패할 경우,
#      OSM 데이터로 대략적인 용도지역 판단을 제공
# landuse 태그 매핑
_OSM_LANDUSE_MAP = {
    "commercial":   ("일반상업지역",   1.0),
    "retail":       ("근린상업지역",   1.0),
    "residential":  ("제2종일반주거지역", 0.4),
    "industrial":   ("일반공업지역",   0.3),
    "construction": ("제3종일반주거지역", 0.6),
    "brownfield":   ("준공업지역",     0.8),
    "forest":       ("자연녹지지역",   0.1),
    "farmland":     ("농림지역",       0.0),
    "grass":        ("자연녹지지역",   0.1),
    "meadow":       ("자연녹지지역",   0.1),
    "cemetery":     ("보전녹지지역",   0.0),
    "military":     ("보전녹지지역",   0.0),
    "railway":      ("준공업지역",     0.8),
    "religious":    ("제2종일반주거지역", 0.4),
    "education":    ("제2종일반주거지역", 0.4),
}

# Why: 공원·호수 등은 landuse가 아닌 leisure/natural 태그를 사용하므로
#      별도 매핑 필요. 이 영역은 상업 입점이 불가하므로 zone_score=0.0
_OSM_LEISURE_MAP = {
    "park":           ("공원",       0.0),
    "garden":         ("정원",       0.0),
    "playground":     ("놀이터",     0.0),
    "pitch":          ("운동장",     0.0),
    "track":          ("운동장",     0.0),
    "sports_centre":  ("체육시설",   0.0),
    "golf_course":    ("골프장",     0.0),
    "dog_park":       ("공원",       0.0),
    "nature_reserve": ("자연보전",   0.0),
}

_OSM_NATURAL_MAP = {
    "water":     ("수역",       0.0),
    "wood":      ("산림",       0.0),
    "wetland":   ("습지",       0.0),
    "sand":      ("사지",       0.0),
    "bare_rock": ("암석지",     0.0),
    "grassland": ("초지",       0.0),
}


def _estimate_area_sq_km(gdf: gpd.GeoDataFrame) -> float:
    """GeoDataFrame 영역의 대략적인 면적(km²) 추정."""
    from config import CRS_KOREA
    try:
        gdf_tm = gdf.to_crs(CRS_KOREA)
        return gdf_tm.unary_union.area / 1e6
    except Exception:
        return 999  # 추정 불가 시 큰 값 → OSM 폴백 건너뜀


def _match_zone_score(zone_name: str) -> float:
    """용도지역명에서 상업 적합도 점수 추출 (부분 매칭)."""
    if not zone_name:
        return 0.5
    for key, score in ZONE_COMMERCIAL_SCORE.items():
        if key in zone_name:
            return score
    # Why: 매칭 실패는 데이터 이상이므로 보수적 0.5
    log.warning(f"알 수 없는 용도지역: '{zone_name}' → 0.5 기본값")
    return 0.5


def get_land_use_zones(
    boundary_gdf: gpd.GeoDataFrame,
    api_key: str,
) -> gpd.GeoDataFrame | None:
    """
    용도지역 폴리곤 조회.

    1차: Vworld 2D데이터 API 시도
    2차: 실패 시 OSM landuse 폴백

    Args:
        boundary_gdf: 분석 영역 경계 (EPSG:4326)
        api_key:      Vworld API 키 (빈 문자열이면 바로 OSM 폴백)

    Returns:
        용도지역 GeoDataFrame (zone_name, zone_score, geometry) 또는 None
    """
    # 1차: Vworld 2D데이터 API 시도
    if api_key:
        result = _try_vworld_data_api(boundary_gdf, api_key)
        if result is not None and len(result) > 0:
            return result
        log.info("Vworld 2D데이터 API 실패 → OSM landuse 폴백")

    # 2차: OSM landuse 폴백 (Overpass API 직접 호출 — 구 단위도 2~5초)
    return _get_osm_landuse(boundary_gdf)


def _try_vworld_data_api(
    boundary_gdf: gpd.GeoDataFrame,
    api_key: str,
) -> gpd.GeoDataFrame | None:
    """Vworld 2D데이터 API로 용도지역 조회 (기존 로직)."""
    bounds = boundary_gdf.total_bounds  # [minx, miny, maxx, maxy]
    bbox_str = f"BOX({bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]})"

    url = "https://api.vworld.kr/req/data"
    all_features = []
    page = 1
    max_pages = 50

    log.info("Vworld 2D데이터 API로 용도지역 조회 시도")

    while page <= max_pages:
        params = {
            "service": "data",
            "request": "GetFeature",
            "data": "LT_C_UQ111",
            "key": api_key,
            "domain": "localhost",
            "geomFilter": bbox_str,
            "crs": "EPSG:4326",
            "size": 1000,
            "page": page,
            "format": "json",
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.warning(f"Vworld API 요청 오류: {e}")
            return None

        response_obj = data.get("response", {})
        status = response_obj.get("status")

        if status != "OK":
            error_text = response_obj.get("error", {}).get("text", "알 수 없는 오류")
            log.warning(f"Vworld API 오류 응답: {status} — {error_text}")
            if not all_features:
                return None
            break

        result = response_obj.get("result", {})
        fc = result.get("featureCollection", {})
        features = fc.get("features", [])

        if not features:
            break

        all_features.extend(features)

        total = int(result.get("totalcount", 0))
        if page * 1000 >= total:
            break
        page += 1

    if not all_features:
        return None

    records = []
    for feat in all_features:
        props = feat.get("properties", {})
        geom_dict = feat.get("geometry")
        if not geom_dict:
            continue
        try:
            geom = shape(geom_dict)
            zone_name = props.get("UNAME", "")
            records.append({
                "zone_name": zone_name,
                "zone_score": _match_zone_score(zone_name),
                "geometry": geom,
            })
        except Exception:
            continue

    if not records:
        return None

    gdf = gpd.GeoDataFrame(records, crs=CRS_WGS84)
    boundary_union = boundary_gdf.unary_union
    gdf = gdf[gdf.geometry.intersects(boundary_union)].copy()

    log.info(f"Vworld 용도지역 조회 완료: {len(gdf)}개 폴리곤")
    for zn, cnt in gdf["zone_name"].value_counts().head(5).items():
        log.info(f"  {zn}: {cnt}개")
    return gdf


def _get_osm_landuse(
    boundary_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame | None:
    """
    Overpass API 직접 호출로 OSM landuse 폴리곤 조회.

    Why: osmnx.features_from_polygon은 구 단위에서 60초+ 소요되지만,
         Overpass API 직접 호출은 2~5초로 충분히 빠름.
    """
    from shapely.geometry import Polygon

    boundary_wgs = boundary_gdf.to_crs(CRS_WGS84)
    bounds = boundary_wgs.total_bounds  # minx, miny, maxx, maxy
    bbox = f"{bounds[1]},{bounds[0]},{bounds[3]},{bounds[2]}"  # S,W,N,E

    query = f"""
    [out:json][timeout:25];
    (way["landuse"]({bbox});
     relation["landuse"]({bbox});
     way["leisure"~"park|garden|playground|pitch|track|sports_centre|golf_course|dog_park|nature_reserve"]({bbox});
     way["natural"~"water|wood|wetland|sand|bare_rock|grassland"]({bbox}););
    out body;>;out skel qt;
    """

    # Why: Overpass API 메인 서버가 간헐적으로 504 반환 → 미러 서버 폴백
    overpass_endpoints = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
    ]

    log.info("Overpass API로 OSM landuse 조회 시작")

    data = None
    for endpoint in overpass_endpoints:
        try:
            resp = requests.post(
                endpoint,
                data={"data": query},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            log.info(f"Overpass 응답 수신: {endpoint}")
            break
        except Exception as e:
            log.warning(f"Overpass 실패 ({endpoint}): {e}")
            continue

    if data is None:
        log.warning("Overpass 직접 호출 실패 → osmnx 폴백 시도")
        return _get_osm_landuse_via_osmnx(boundary_gdf)

    elements = data.get("elements", [])
    if not elements:
        log.warning("OSM landuse 데이터 없음")
        return None

    # 노드 좌표 딕셔너리 구축
    nodes = {}
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = (el["lon"], el["lat"])

    # Way → Polygon 변환 (landuse + leisure + natural)
    records = []
    for el in elements:
        if el["type"] != "way" or "tags" not in el:
            continue

        tags = el["tags"]
        # landuse → leisure → natural 순으로 태그 확인
        osm_type = tags.get("landuse", "")
        tag_source = "landuse"
        if not osm_type:
            osm_type = tags.get("leisure", "")
            tag_source = "leisure"
        if not osm_type:
            osm_type = tags.get("natural", "")
            tag_source = "natural"
        if not osm_type:
            continue

        node_refs = el.get("nodes", [])
        coords = [nodes[nid] for nid in node_refs if nid in nodes]
        if len(coords) < 4:
            continue

        try:
            poly = Polygon(coords)
            if not poly.is_valid or poly.area == 0:
                continue
        except Exception:
            continue

        # 태그 종류에 따라 적절한 매핑 테이블 선택
        if tag_source == "landuse":
            mapped = _OSM_LANDUSE_MAP.get(osm_type)
        elif tag_source == "leisure":
            mapped = _OSM_LEISURE_MAP.get(osm_type)
        else:
            mapped = _OSM_NATURAL_MAP.get(osm_type)

        if mapped is None:
            zone_name = f"기타({osm_type})"
            zone_score = 0.5
        else:
            zone_name, zone_score = mapped

        records.append({
            "zone_name": zone_name,
            "zone_score": zone_score,
            "geometry": poly,
        })

    if not records:
        log.warning("OSM landuse 폴리곤 변환 실패")
        return None

    gdf = gpd.GeoDataFrame(records, crs=CRS_WGS84)

    # 경계 내 클리핑
    boundary_union = boundary_wgs.unary_union
    gdf = gdf[gdf.geometry.intersects(boundary_union)].copy()

    log.info(f"OSM 용도지역 조회 완료: {len(gdf)}개 폴리곤")
    for zn, cnt in gdf["zone_name"].value_counts().head(5).items():
        log.info(f"  {zn}: {cnt}개")
    return gdf


def _get_osm_landuse_via_osmnx(
    boundary_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame | None:
    """
    osmnx features_from_bbox로 OSM landuse 조회 (Overpass 직접 호출 실패 시 폴백).

    Why: osmnx는 자체 캐시·재시도 로직이 있어서 직접 호출보다 안정적.
         첫 호출은 느릴 수 있으나(30~60초), 이후엔 캐시에서 즉시 반환.
    """
    try:
        import osmnx as ox
    except ImportError:
        log.warning("osmnx 미설치 → 용도지역 분석 건너뜀")
        return None

    boundary_wgs = boundary_gdf.to_crs(CRS_WGS84)
    bounds = boundary_wgs.total_bounds  # minx, miny, maxx, maxy

    log.info("osmnx로 OSM landuse+leisure+natural 조회 시도 (캐시 활용)")

    bbox = (bounds[0], bounds[1], bounds[2], bounds[3])
    all_gdfs = []
    for tags in [
        {"landuse": True},
        {"leisure": ["park", "garden", "playground", "pitch", "track",
                     "sports_centre", "golf_course", "dog_park", "nature_reserve"]},
        {"natural": ["water", "wood", "wetland", "sand", "bare_rock", "grassland"]},
    ]:
        try:
            gdf_part = ox.features_from_bbox(bbox=bbox, tags=tags)
            if not gdf_part.empty:
                all_gdfs.append(gdf_part)
        except Exception as e:
            log.warning(f"osmnx {list(tags.keys())[0]} 조회 실패: {e}")

    if not all_gdfs:
        log.warning("osmnx 데이터 없음")
        return None

    import pandas as pd
    gdf_raw = pd.concat(all_gdfs, ignore_index=True)

    records = []
    for _, row in gdf_raw.iterrows():
        if row.geometry is None:
            continue
        if row.geometry.geom_type not in ("Polygon", "MultiPolygon"):
            continue

        # landuse → leisure → natural 순으로 태그 확인
        osm_type = row.get("landuse", "")
        if osm_type and not (isinstance(osm_type, float)):
            mapped = _OSM_LANDUSE_MAP.get(osm_type)
        else:
            osm_type = row.get("leisure", "")
            if osm_type and not (isinstance(osm_type, float)):
                mapped = _OSM_LEISURE_MAP.get(osm_type)
            else:
                osm_type = row.get("natural", "")
                if osm_type and not (isinstance(osm_type, float)):
                    mapped = _OSM_NATURAL_MAP.get(osm_type)
                else:
                    continue

        if mapped is None:
            zone_name = f"기타({osm_type})"
            zone_score = 0.5
        else:
            zone_name, zone_score = mapped

        records.append({
            "zone_name": zone_name,
            "zone_score": zone_score,
            "geometry": row.geometry,
        })

    if not records:
        log.warning("osmnx landuse 폴리곤 변환 실패")
        return None

    gdf = gpd.GeoDataFrame(records, crs=CRS_WGS84)

    boundary_union = boundary_wgs.unary_union
    gdf = gdf[gdf.geometry.intersects(boundary_union)].copy()

    log.info(f"osmnx 용도지역 조회 완료: {len(gdf)}개 폴리곤")
    for zn, cnt in gdf["zone_name"].value_counts().head(5).items():
        log.info(f"  {zn}: {cnt}개")
    return gdf
