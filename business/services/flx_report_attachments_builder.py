# -*- coding: utf-8 -*-
"""
Додатки до HTML-звіту Flx (детерміновано з evidence, без LLM).

Формує:
- CSV з кадастровими ділянками та оголошеннями (UTF-8 з BOM для Excel);
- PNG «теплова» карта центроїдів (Google Static Maps, емуляція через heatmap_points);
- інтерактивна HTML-карта (Leaflet + OSM) для перегляду тих самих точок.
"""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_MAX_PARCEL_ROWS = 500
_MAX_LISTING_ROWS = 350
_MAX_MAP_MARKERS = 58
_LEAFLET_MAX_POINTS = 400


def _centroid_lat_lng(centroid: Any) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(centroid, dict):
        return None, None
    lat = centroid.get("latitude") if "latitude" in centroid else centroid.get("lat")
    lng = centroid.get("longitude") if "longitude" in centroid else centroid.get("lng")
    try:
        return float(lat), float(lng)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None, None


def _flatten_cadastral_rows(evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for ev in evidence:
        if ev.get("kind") != "cadastral_finding":
            continue
        pl = ev.get("payload") or {}
        if not isinstance(pl, dict):
            continue
        for bucket in ("clusters", "single_parcels"):
            for g in pl.get(bucket) or []:
                if not isinstance(g, dict):
                    continue
                lat, lng = _centroid_lat_lng(g.get("centroid"))
                cns = [str(x).strip() for x in (g.get("cadastral_numbers") or []) if str(x).strip()]
                pc = int(g.get("parcel_count") or len(cns) or 0)
                total_a = g.get("total_area_sqm")
                try:
                    total_f = float(total_a) if total_a is not None else 0.0
                except (TypeError, ValueError):
                    total_f = 0.0
                per_parcel = total_f / max(pc, 1) if total_f > 0 else 0.0
                plab = str(g.get("purpose_label") or g.get("purpose") or "")
                own = str(g.get("ownership_form") or "")
                cid = str(g.get("cluster_id") or "")
                for cn in cns:
                    if len(rows) >= _MAX_PARCEL_ROWS:
                        return rows
                    rows.append(
                        {
                            "cadastral_number": cn,
                            "cluster_id": cid,
                            "is_singleton": bool(g.get("is_singleton")),
                            "cluster_parcel_count": pc,
                            "cluster_total_area_sqm": total_f,
                            "area_sqm_hint": round(per_parcel, 2) if per_parcel else "",
                            "purpose_label": plab,
                            "ownership_form": own,
                            "lat": lat,
                            "lng": lng,
                        }
                    )
    return rows


def _flatten_listing_rows(evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for ev in evidence:
        if ev.get("kind") != "listings_finding":
            continue
        pl = ev.get("payload") or {}
        if not isinstance(pl, dict):
            continue
        for it in pl.get("items") or []:
            if not isinstance(it, dict):
                continue
            if len(rows) >= _MAX_LISTING_ROWS:
                return rows
            ch = it.get("contact_hint")
            contact = str(ch).strip()[:200] if ch else ""
            rows.append(
                {
                    "source": str(it.get("source") or ""),
                    "source_id": str(it.get("source_id") or ""),
                    "title": str(it.get("title") or "")[:500],
                    "address": str(it.get("address") or it.get("address_text") or "")[:500],
                    "city": str(it.get("city") or ""),
                    "region": str(it.get("region") or ""),
                    "property_type": str(it.get("property_type") or ""),
                    "price_uah": it.get("price_uah"),
                    "price_usd": it.get("price_usd"),
                    "building_area_sqm": it.get("building_area_sqm"),
                    "land_area_sqm": it.get("land_area_sqm"),
                    "url": str(it.get("url") or ""),
                    "contact_hint": contact,
                }
            )
    return rows


def _utf8_csv_bytes(fieldnames: List[str], dict_rows: List[Dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    buf.write("\ufeff")
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
    w.writeheader()
    for r in dict_rows:
        w.writerow({k: ("" if v is None else v) for k, v in r.items() if k in fieldnames})
    return buf.getvalue().encode("utf-8")


def _register_bytes_artifact(
    *,
    user_id: Optional[str],
    content_bytes: bytes,
    artifact_type: str,
    filename: str,
    content_type: str,
    ttl_seconds: int,
) -> Optional[Dict[str, Any]]:
    try:
        from business.services.artifact_service import ArtifactService

        b64 = base64.b64encode(content_bytes).decode("ascii")
        return ArtifactService().register_with_token(
            user_id=str(user_id) if user_id else None,
            artifact_type=artifact_type,
            content_base64=b64,
            metadata={
                "filename": filename,
                "content_type": content_type,
            },
            ttl_seconds=ttl_seconds,
        )
    except Exception as e:
        logger.warning("flx_report_attachments: artifact register failed: %s", e)
        return None


def _sample_evenly(items: List[Any], max_n: int) -> List[Any]:
    if len(items) <= max_n:
        return items
    step = (len(items) - 1) / float(max_n - 1)
    out: List[Any] = []
    for i in range(max_n):
        idx = int(round(i * step))
        idx = min(max(0, idx), len(items) - 1)
        out.append(items[idx])
    return out


def _build_leaflet_html(points: List[Dict[str, Any]], title: str) -> str:
    """Проста повноекранна карта OSM; точки — GeoJSON FeatureCollection."""
    feats: List[Dict[str, Any]] = []
    for p in points[:_LEAFLET_MAX_POINTS]:
        lat, lng = p.get("lat"), p.get("lng")
        if lat is None or lng is None:
            continue
        try:
            la, ln = float(lat), float(lng)
        except (TypeError, ValueError):
            continue
        feats.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [ln, la]},
                "properties": {
                    "cn": str(p.get("cadastral_number") or ""),
                    "purpose": str(p.get("purpose_label") or "")[:120],
                },
            }
        )
    gj = {"type": "FeatureCollection", "features": feats}
    gj_json = json.dumps(gj, ensure_ascii=False)
    title_esc = html.escape(title, quote=True)
    return f"""<!doctype html>
<html lang="uk"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title_esc}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<style>html,body,#m{{height:100%;margin:0}} .lbl{{font-size:11px;max-width:220px}}</style>
</head><body>
<div id="m"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const data = {gj_json};
const map = L.map('m');
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap'
}}).addTo(map);
const layer = L.geoJSON(data, {{
  pointToLayer: function(feature, ll) {{
    return L.circleMarker(ll, {{ radius: 6, fillOpacity: 0.75, color: '#1a5fb4', weight: 1 }});
  }},
  onEachFeature: function(feature, lyr) {{
    const p = feature.properties || {{}};
    lyr.bindPopup('<div class="lbl"><b>' + (p.cn || '') + '</b><br/>' + (p.purpose || '') + '</div>');
  }}
}}).addTo(map);
try {{ map.fitBounds(layer.getBounds(), {{ padding: [28, 28], maxZoom: 16 }}); }} catch (e) {{ map.setView([49.0, 31.0], 6); }}
</script>
</body></html>"""


def enrich_flx_report_with_attachments(
    *,
    structured: Dict[str, Any],
    evidence: List[Dict[str, Any]],
    user_id: Optional[str],
    settings: Any,
) -> Dict[str, Any]:
    """Повертає копію structured з полем `attachments` (список словників для Jinja)."""
    out = dict(structured or {})
    attachments: List[Dict[str, Any]] = list(out.get("attachments") or [])
    ttl = int(getattr(settings, "artifact_ttl_seconds", 0) or 7 * 24 * 3600)

    parcel_rows = _flatten_cadastral_rows(evidence)
    listing_rows = _flatten_listing_rows(evidence)

    if parcel_rows:
        pfields = [
            "cadastral_number",
            "cluster_id",
            "is_singleton",
            "cluster_parcel_count",
            "cluster_total_area_sqm",
            "area_sqm_hint",
            "purpose_label",
            "ownership_form",
            "lat",
            "lng",
        ]
        csv_b = _utf8_csv_bytes(pfields, parcel_rows)
        reg = _register_bytes_artifact(
            user_id=user_id,
            content_bytes=csv_b,
            artifact_type="report_attachment_csv",
            filename="flx_cadastral_parcels.csv",
            content_type="text/csv; charset=utf-8",
            ttl_seconds=ttl,
        )
        if reg:
            attachments.append(
                {
                    "kind": "csv",
                    "role": "cadastral_parcels",
                    "title": "Перелік кадастрових ділянок (CSV)",
                    "description": f"Експорт {len(parcel_rows)} рядків із evidence кадастру (номер, кластер, площа, призначення, координати центроїда за наявності).",
                    "artifact_id": reg["artifact_id"],
                    "download_token": reg["download_token"],
                }
            )

        geo_rows = [r for r in parcel_rows if r.get("lat") is not None and r.get("lng") is not None]
        if geo_rows:
            sampled = _sample_evenly(geo_rows, _MAX_MAP_MARKERS)
            markers: List[Dict[str, Any]] = []
            heat_points: List[Dict[str, Any]] = []
            for r in sampled:
                la, ln = float(r["lat"]), float(r["lng"])  # type: ignore[arg-type]
                color = "orange" if int(r.get("cluster_parcel_count") or 0) > 1 else "green"
                markers.append({"lat": la, "lng": ln, "color": color})
                val = r.get("area_sqm_hint")
                try:
                    vf = float(val) if val not in ("", None) else None
                except (TypeError, ValueError):
                    vf = None
                hp: Dict[str, Any] = {"lat": la, "lng": ln}
                if vf and vf > 0:
                    hp["value"] = vf
                heat_points.append(hp)

            try:
                from business.services.static_map_service import StaticMapService

                sm_svc = StaticMapService(settings)
                if len(geo_rows) == 1:
                    la0 = float(geo_rows[0]["lat"])  # type: ignore[index]
                    ln0 = float(geo_rows[0]["lng"])  # type: ignore[index]
                    sm = sm_svc.render_to_artifact(
                        user_id=user_id,
                        center=(la0, ln0),
                        markers=markers,
                        heatmap_points=heat_points,
                        ttl_seconds=ttl,
                    )
                else:
                    lats = [float(x["lat"]) for x in geo_rows]  # type: ignore[index]
                    lngs = [float(x["lng"]) for x in geo_rows]  # type: ignore[index]
                    min_la, max_la = min(lats), max(lats)
                    min_ln, max_ln = min(lngs), max(lngs)
                    if abs(max_la - min_la) < 1e-5 and abs(max_ln - min_ln) < 1e-5:
                        min_la -= 0.002
                        max_la += 0.002
                        min_ln -= 0.002
                        max_ln += 0.002
                    bbox = (min_la, min_ln, max_la, max_ln)
                    sm = sm_svc.render_to_artifact(
                        user_id=user_id,
                        bbox=bbox,
                        markers=markers,
                        heatmap_points=heat_points,
                        ttl_seconds=ttl,
                    )
                if sm.get("ok") and sm.get("artifact_id"):
                    attachments.append(
                        {
                            "kind": "static_map",
                            "role": "cadastral_heatmap",
                            "title": "Карта центроїдів ділянок (тепловий відтінок за площею)",
                            "description": "Google Static Map: маркери кластерів та одиночок; кольорові «теплові» точки за відносною площею в кластері.",
                            "artifact_id": sm["artifact_id"],
                            "download_token": sm.get("download_token"),
                        }
                    )
            except Exception as e:
                logger.warning("flx_report_attachments: static map failed: %s", e)

            try:
                html_doc = _build_leaflet_html(geo_rows, "Кадастрові ділянки — інтерактивна карта")
                regm = _register_bytes_artifact(
                    user_id=user_id,
                    content_bytes=html_doc.encode("utf-8"),
                    artifact_type="report_attachment_html",
                    filename="flx_cadastral_map.html",
                    content_type="text/html; charset=utf-8",
                    ttl_seconds=ttl,
                )
                if regm:
                    attachments.append(
                        {
                            "kind": "html_map",
                            "role": "cadastral_interactive",
                            "title": "Інтерактивна карта (Leaflet)",
                            "description": "Перегляд центроїдів на OpenStreetMap; клік по точці — номер і призначення.",
                            "artifact_id": regm["artifact_id"],
                            "download_token": regm.get("download_token"),
                        }
                    )
            except Exception as e:
                logger.warning("flx_report_attachments: leaflet html failed: %s", e)

    if listing_rows:
        lfields = [
            "source",
            "source_id",
            "title",
            "address",
            "city",
            "region",
            "property_type",
            "price_uah",
            "price_usd",
            "building_area_sqm",
            "land_area_sqm",
            "contact_hint",
            "url",
        ]
        csv_l = _utf8_csv_bytes(lfields, listing_rows)
        reg2 = _register_bytes_artifact(
            user_id=user_id,
            content_bytes=csv_l,
            artifact_type="report_attachment_csv",
            filename="flx_listings.csv",
            content_type="text/csv; charset=utf-8",
            ttl_seconds=ttl,
        )
        if reg2:
            attachments.append(
                {
                    "kind": "csv",
                    "role": "listings",
                    "title": "Перелік оголошень (CSV)",
                    "description": f"Експорт {len(listing_rows)} оголошень з evidence (контакт — якщо є в даних БД).",
                    "artifact_id": reg2["artifact_id"],
                    "download_token": reg2["download_token"],
                }
            )

    out["attachments"] = attachments
    return out
