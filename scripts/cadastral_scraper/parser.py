# -*- coding: utf-8 -*-
"""
Парсинг Mapbox Vector Tiles (MVT/PBF) у структуровані дані ділянок.

Координати в MVT — у просторі тайлу (0–4096). Конвертуємо у WGS84.
"""

import math
import os
from typing import Any, Dict, List, Optional

_DEBUG = os.environ.get("CADASTRAL_DEBUG", "").lower() in ("1", "true", "yes")

try:
    import mapbox_vector_tile
except ImportError:
    mapbox_vector_tile = None

try:
    from shapely.geometry import Polygon as ShapelyPolygon
    from shapely import make_valid
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False


def _tile_coord_to_wgs84(px: float, py: float, z: int, tx: int, ty: int) -> tuple:
    """
    Конвертує координати з простору тайлу (0–4096) у WGS84 (lon, lat).
    """
    n = 2 ** z
    lon = (tx + px / 4096) / n * 360 - 180
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1 - py / 4096) / n)))
    lat = math.degrees(lat_rad)
    return (lon, lat)


def _decode_geometry(geom: dict, z: int, tx: int, ty: int) -> Optional[List[List[float]]]:
    """
    Декодує geometry з MVT (Polygon або MultiPolygon) у список координат [[lon, lat], ...].
    MVT coordinates — у просторі тайлу (0–4096).
    """
    if not geom or "coordinates" not in geom:
        return None
    coords = geom["coordinates"]
    if not coords:
        return None

    def get_xy(pt):
        if isinstance(pt, (list, tuple)) and len(pt) >= 2:
            a, b = pt[0], pt[1]
            if isinstance(a, (list, tuple)):
                return (a[0], a[1]) if len(a) >= 2 else (None, None)
            return (float(a), float(b))
        return (None, None)

    def extract_ring(ring: list) -> List[List[float]]:
        result = []
        for pt in ring:
            x_val, y_val = get_xy(pt)
            if x_val is not None and y_val is not None:
                lon, lat = _tile_coord_to_wgs84(x_val, y_val, z, tx, ty)
                result.append([lon, lat])
        return result

    geom_type = (geom.get("type") or "").lower()
    rings_to_try = []

    if geom_type == "polygon":
        # Polygon: coordinates = [[ring1], [ring2], ...], ring = [[x,y], [x,y], ...]
        rings_to_try = coords
    elif geom_type == "multipolygon":
        # MultiPolygon: coordinates = [[[ring1], [ring2], ...], ...] — масив полігонів
        for polygon in coords:
            if polygon:
                rings_to_try.extend(polygon)
            if rings_to_try:
                break
    else:
        rings_to_try = coords

    for ring in rings_to_try:
        result = extract_ring(ring)
        if len(result) >= 3:
            return result
    return None


def _to_geojson_polygon(coords: List[List[float]]) -> Optional[Dict[str, Any]]:
    """Формує GeoJSON Polygon для MongoDB 2dsphere. Виправляє самоперетин через Shapely."""
    if not coords or len(coords) < 3:
        return None
    ring = list(coords)
    if ring[0] != ring[-1]:
        ring.append(ring[0])

    if SHAPELY_AVAILABLE:
        try:
            poly = ShapelyPolygon(ring)
            if not poly.is_valid:
                fixed = make_valid(poly)
                if fixed.is_empty:
                    return None
                # make_valid може повернути Polygon, MultiPolygon або GeometryCollection
                if hasattr(fixed, "geoms"):
                    polygons = [g for g in fixed.geoms if hasattr(g, "exterior") and not g.is_empty]
                    if not polygons:
                        return None
                    fixed = max(polygons, key=lambda g: g.area)
                if not hasattr(fixed, "exterior") or fixed.is_empty:
                    return None
                coords_fixed = list(fixed.exterior.coords)
                if len(coords_fixed) < 4:
                    return None
                return {"type": "Polygon", "coordinates": [coords_fixed]}
            return {"type": "Polygon", "coordinates": [ring]}
        except Exception:
            return None  # Пропускаємо невалідні полігони
    return {"type": "Polygon", "coordinates": [ring]}


def _get_prop(props: dict, *keys: str) -> Optional[Any]:
    """Безпечно отримує властивість."""
    if not props:
        return None
    for k in keys:
        if k in props and props[k] is not None:
            return props[k]
    return None


def _parse_float(val: Any) -> Optional[float]:
    """Безпечно парсить число."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def parse_mvt_tile(
    pbf_bytes: Optional[bytes],
    zoom: int,
    tile_x: int,
    tile_y: int,
    source_cell_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Парсить PBF (MVT) тайл у список ділянок для cadastral_parcels.

    Args:
        pbf_bytes: бінарні дані тайлу
        zoom, tile_x, tile_y: координати тайлу для конвертації
        source_cell_id: ID комірки

    Returns:
        Список словників для upsert
    """
    if not pbf_bytes:
        if _DEBUG:
            print("[Cadastral DEBUG] parse_mvt_tile: pbf_bytes is None/empty")
        return []
    if not pbf_bytes.strip():
        if _DEBUG:
            print("[Cadastral DEBUG] parse_mvt_tile: pbf_bytes.strip() is empty")
        return []
    if mapbox_vector_tile is None:
        if _DEBUG:
            print("[Cadastral DEBUG] parse_mvt_tile: mapbox_vector_tile not installed")
        return []

    try:
        data = mapbox_vector_tile.decode(pbf_bytes)
    except Exception as e:
        if _DEBUG:
            print(f"[Cadastral DEBUG] parse_mvt_tile: decode failed: {e}")
        return []

    if not isinstance(data, dict):
        if _DEBUG:
            print(f"[Cadastral DEBUG] parse_mvt_tile: data is not dict: {type(data)}")
        return []

    result: List[Dict[str, Any]] = []
    skip_no_coords = skip_no_bounds = skip_no_cadnum = 0

    # Шукаємо шар land_polygons або polygons
    for layer_name, layer_data in data.items():
        if "land_polygons" not in layer_name.lower() and "polygons" not in layer_name.lower():
            continue
        features = layer_data.get("features") or []
        for f in features:
            if not isinstance(f, dict):
                continue
            geom = f.get("geometry")
            props = f.get("properties") or {}
            coords = _decode_geometry(geom, zoom, tile_x, tile_y)
            if not coords:
                skip_no_coords += 1
                continue

            bounds = _to_geojson_polygon(coords)
            if not bounds:
                skip_no_bounds += 1
                continue

            cadastral_number = (
                _get_prop(props, "cadnum", "cadastral_number", "kadastr", "number", "id", "code", "cadastralNumber")
                or _get_prop(props, "kadastr_number")
            )
            if not cadastral_number:
                skip_no_cadnum += 1
                continue
            cadastral_number = str(cadastral_number).strip()
            if not cadastral_number:
                skip_no_cadnum += 1
                continue

            # area: у MVT часто в гектарах (0.0222 ha = 222 m²)
            area_val = _parse_float(_get_prop(props, "area_sqm", "area", "площа", "area_m2"))
            if area_val is not None and area_val < 1 and area_val > 0:
                area_val = area_val * 10000  # га → м²
            parcel: Dict[str, Any] = {
                "cadastral_number": cadastral_number,
                "bounds": bounds,
                "address": _get_prop(props, "address", "full_address", "addr", "адреса"),
                "purpose": _get_prop(props, "purpose_code", "purpose", "призначення"),
                "purpose_label": _get_prop(props, "purpose", "purpose_label", "purpose_name"),
                "category": _get_prop(props, "category", "category_name", "категорія"),
                "area_sqm": area_val,
                "ownership_form": _get_prop(props, "ownership", "ownership_form", "форма_владності"),
            }
            if source_cell_id:
                parcel["source_cell_id"] = source_cell_id
            result.append(parcel)

    if _DEBUG and not result:
        total_f = sum(len(layer_data.get("features") or []) for _, layer_data in data.items())
        layer_names = list(data.keys())
        matching = [n for n in layer_names if "land_polygons" in n.lower() or "polygons" in n.lower()]
        print(f"[Cadastral DEBUG] parsed=0: layers={layer_names}, matching={matching}, features={total_f}, "
              f"skip_coords={skip_no_coords}, skip_bounds={skip_no_bounds}, skip_cadnum={skip_no_cadnum}")

    return result


def parse_geojson_response(
    raw_text: Optional[str],
    source_cell_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Парсить GeoJSON (legacy, якщо API поверне JSON).
    """
    if not raw_text or not raw_text.strip():
        return []
    try:
        import json
        data = json.loads(raw_text)
    except Exception:
        return []
    features = data.get("features") or []
    result = []
    for f in features:
        geom = f.get("geometry")
        props = f.get("properties") or {}
        if geom and geom.get("type") == "Polygon":
            coords = geom.get("coordinates", [[]])[0]
            if len(coords) >= 3:
                bounds = _to_geojson_polygon(coords)
                cn = _get_prop(props, "cadastral_number", "kadastr", "number")
                if cn and bounds:
                    result.append({
                        "cadastral_number": str(cn).strip(),
                        "bounds": bounds,
                        "address": _get_prop(props, "address", "full_address", "addr"),
                        "purpose": _get_prop(props, "purpose"),
                        "purpose_label": _get_prop(props, "purpose_label"),
                        "category": _get_prop(props, "category"),
                        "area_sqm": _parse_float(_get_prop(props, "area_sqm", "area")),
                        "ownership_form": _get_prop(props, "ownership_form"),
                        "source_cell_id": source_cell_id,
                    })
    return result
