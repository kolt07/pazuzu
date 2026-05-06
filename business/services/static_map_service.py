# -*- coding: utf-8 -*-
"""
Сервіс рендеру Google Static Maps PNG для звітів агента Flx.

Підтримує:
- центр (center) або bbox (visible) як рамку охоплення
- markers різних кольорів і label'ів
- path (polyline) для контурів районів
- "емуляцію" heatmap через групи кольорових маркерів за бакетами цін

Згенерований PNG зберігається через ArtifactService як artifact_type='static_map' з
download token для безпечного відкриття у Mini App.

Документація: https://developers.google.com/maps/documentation/maps-static/start
"""

import base64
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from config.settings import Settings

logger = logging.getLogger(__name__)

STATIC_MAPS_URL = "https://maps.googleapis.com/maps/api/staticmap"
DEFAULT_TIMEOUT = 20

# Палітра для емуляції теплової карти цін (від низької до високої).
# Використовуємо стандартні Google-кольори для маркерів — щоб усе залишалося читаним
# і без додаткових запитів на icon-URL.
HEATMAP_COLORS_BY_BUCKET = [
    "0x2ECC71",  # green — low
    "0xF1C40F",  # yellow — mid-low
    "0xE67E22",  # orange — mid-high
    "0xE74C3C",  # red — high
    "0x8E44AD",  # purple — very high
]

# Кольори для звичайних маркерів (POI / конкуренти / оголошення)
ALLOWED_MARKER_COLORS = {
    "red", "blue", "green", "yellow", "orange", "purple", "white", "black", "gray", "brown"
}


class StaticMapService:
    """Будує URL Google Static Maps та віддає PNG-байти або реєструє артефакт."""

    def __init__(self, settings: Optional[Settings] = None):
        self._settings = settings or Settings()

    def render_to_artifact(
        self,
        *,
        user_id: Optional[str],
        center: Optional[Tuple[float, float]] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        markers: Optional[List[Dict[str, Any]]] = None,
        heatmap_points: Optional[List[Dict[str, Any]]] = None,
        paths: Optional[List[Dict[str, Any]]] = None,
        size: Optional[str] = None,
        zoom: Optional[int] = None,
        scale: int = 2,
        maptype: str = "roadmap",
        ttl_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Генерує PNG карти та реєструє його як артефакт.

        Returns:
          {
            "ok": bool,
            "artifact_id": str | None,
            "download_token": str | None,
            "url_preview": str,  # лише для діагностики (без ключа в логах не світимо повний)
            "error": str | None,
          }
        """
        api_key = (self._settings.google_maps_api_key or "").strip()
        if not api_key:
            return {"ok": False, "error": "GOOGLE_MAPS_API_KEY не налаштовано"}

        params = self._build_params(
            api_key=api_key,
            center=center,
            bbox=bbox,
            markers=markers or [],
            heatmap_points=heatmap_points or [],
            paths=paths or [],
            size=size or self._settings.flx_static_maps_default_size,
            zoom=zoom,
            scale=int(scale),
            maptype=maptype,
        )

        try:
            resp = requests.get(STATIC_MAPS_URL, params=params, timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            logger.warning("Static Maps request failed: %s", e)
            return {"ok": False, "error": f"static_maps_request_failed: {e}"}

        if resp.status_code != 200 or not resp.content:
            logger.warning(
                "Static Maps non-200: status=%s body=%s",
                resp.status_code,
                resp.text[:200] if resp.text else "",
            )
            return {
                "ok": False,
                "error": f"static_maps_status_{resp.status_code}",
            }

        png_b64 = base64.b64encode(resp.content).decode("ascii")
        try:
            from business.services.artifact_service import ArtifactService

            svc = ArtifactService()
            ttl = int(ttl_seconds or getattr(self._settings, "artifact_ttl_seconds", 0) or 7 * 24 * 3600)
            reg = svc.register_with_token(
                user_id=str(user_id) if user_id else None,
                artifact_type="static_map",
                content_base64=png_b64,
                metadata={
                    "filename": "flx_map.png",
                    "content_type": "image/png",
                    "size_bytes": len(resp.content),
                },
                ttl_seconds=ttl,
            )
        except Exception as e:
            logger.warning("Failed to register static map artifact: %s", e)
            return {"ok": False, "error": f"artifact_register_failed: {e}"}

        return {
            "ok": True,
            "artifact_id": reg["artifact_id"],
            "download_token": reg["download_token"],
            "size_bytes": len(resp.content),
        }

    def _build_params(
        self,
        *,
        api_key: str,
        center: Optional[Tuple[float, float]],
        bbox: Optional[Tuple[float, float, float, float]],
        markers: List[Dict[str, Any]],
        heatmap_points: List[Dict[str, Any]],
        paths: List[Dict[str, Any]],
        size: str,
        zoom: Optional[int],
        scale: int,
        maptype: str,
    ) -> List[Tuple[str, str]]:
        """Будує список (key, value) для requests.get з можливістю повторення ключів (markers, path)."""
        params: List[Tuple[str, str]] = [
            ("key", api_key),
            ("size", size),
            ("scale", str(max(1, min(int(scale), 4)))),
            ("maptype", maptype if maptype in ("roadmap", "satellite", "terrain", "hybrid") else "roadmap"),
            ("language", "uk"),
            ("region", "ua"),
        ]

        if center is not None:
            lat, lng = center
            params.append(("center", f"{float(lat):.6f},{float(lng):.6f}"))
            if zoom is not None:
                params.append(("zoom", str(int(zoom))))
            else:
                params.append(("zoom", str(int(self._settings.flx_static_maps_default_zoom))))

        if bbox is not None and not center:
            # Емуляція bbox: ставимо visible-маркер у двох протилежних кутах,
            # щоб Google sam підібрав zoom без штучного `visible=` (його іноді ігнорує).
            min_lat, min_lng, max_lat, max_lng = bbox
            params.append(("visible", f"{min_lat:.6f},{min_lng:.6f}|{max_lat:.6f},{max_lng:.6f}"))

        # POI / маркери
        for m in markers[:60]:
            color = str(m.get("color") or "red").lower()
            if color not in ALLOWED_MARKER_COLORS:
                color = "red"
            label = str(m.get("label") or "")[:1].upper()
            lat = m.get("lat")
            lng = m.get("lng")
            if lat is None or lng is None:
                continue
            label_part = f"|label:{label}" if label and label.isalnum() else ""
            params.append(("markers", f"color:{color}{label_part}|{float(lat):.6f},{float(lng):.6f}"))

        # Heatmap-emulation: точки з price бакетуємо в 5 кошиків, малюємо різнокольоровими маркерами.
        if heatmap_points:
            buckets = self._bucketize_heatmap(heatmap_points)
            for bucket_idx, points in buckets.items():
                color = HEATMAP_COLORS_BY_BUCKET[min(bucket_idx, len(HEATMAP_COLORS_BY_BUCKET) - 1)]
                # Google Static Maps дозволяє кільком маркерам ділити одну ключ-секцію
                # (через групу `color:0xRRGGBB|lat,lng|lat,lng|...`)
                coords = "|".join(
                    f"{float(p['lat']):.6f},{float(p['lng']):.6f}"
                    for p in points
                    if p.get("lat") is not None and p.get("lng") is not None
                )
                if coords:
                    params.append(("markers", f"size:tiny|color:{color}|{coords}"))

        # Polyline-контури
        for p in paths[:5]:
            color = str(p.get("color") or "0x3367D6")
            weight = int(p.get("weight") or 3)
            points = p.get("points") or []
            if not isinstance(points, list) or not points:
                continue
            coords = "|".join(
                f"{float(pt[0]):.6f},{float(pt[1]):.6f}"
                for pt in points
                if isinstance(pt, (list, tuple)) and len(pt) >= 2
            )
            if coords:
                params.append(
                    ("path", f"color:{color}|weight:{max(1, min(int(weight), 8))}|{coords}")
                )

        return params

    def _bucketize_heatmap(
        self, points: List[Dict[str, Any]]
    ) -> Dict[int, List[Dict[str, Any]]]:
        """Розбиває heatmap_points на 5 кошиків за полем `value` (наприклад, ціна за м²).

        Якщо value відсутній — всі точки потрапляють у середній кошик.
        """
        clean = [p for p in points if p.get("lat") is not None and p.get("lng") is not None]
        if not clean:
            return {}
        values = [float(p["value"]) for p in clean if p.get("value") is not None]
        if not values:
            return {2: clean}
        vmin = min(values)
        vmax = max(values)
        if vmax <= vmin:
            return {2: clean}
        n_buckets = len(HEATMAP_COLORS_BY_BUCKET)
        out: Dict[int, List[Dict[str, Any]]] = {i: [] for i in range(n_buckets)}
        for p in clean:
            v = p.get("value")
            if v is None:
                idx = n_buckets // 2
            else:
                ratio = (float(v) - vmin) / (vmax - vmin) if vmax > vmin else 0.5
                idx = min(n_buckets - 1, max(0, int(ratio * n_buckets)))
            out[idx].append(p)
        # Прибираємо порожні бакети для компактнішого URL
        return {k: v for k, v in out.items() if v}
