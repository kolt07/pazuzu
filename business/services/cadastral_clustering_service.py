# -*- coding: utf-8 -*-
"""
Сервіс кластеризації кадастрових ділянок.

Кластер — група ділянок, що:
- мають спільні кордони (торкаються) або кордони в межах N метрів;
- мають однакове призначення (purpose) та форму власності (ownership_form).
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from shapely.geometry import shape
from shapely.strtree import STRtree

from data.repositories.cadastral_parcel_clusters_repository import (
    CadastralParcelClustersRepository,
)
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository

# Буфер у градусах для "кордони в кількох метрах" (~5 м: 1° ≈ 111 км, 5 м ≈ 0.000045°)
BUFFER_DEGREES = 0.00005


def _normalize_for_cluster(val: Optional[str]) -> str:
    """Нормалізує значення для порівняння (пустий рядок = однакова група)."""
    if val is None:
        return ""
    return str(val).strip()


def _union_find_merge(parent: Dict[str, str], a: str, b: str) -> None:
    """Об'єднує дві групи в union-find."""
    ra = _find(parent, a)
    rb = _find(parent, b)
    if ra != rb:
        parent[ra] = rb


def _find(parent: Dict[str, str], x: str) -> str:
    """Знаходить корінь з path compression."""
    if parent[x] != x:
        parent[x] = _find(parent, parent[x])
    return parent[x]


class CadastralClusteringService:
    """
    Кластеризація ділянок за географічною близькістю та однаковими атрибутами.
    """

    def __init__(
        self,
        parcels_repo: Optional[CadastralParcelsRepository] = None,
        clusters_repo: Optional[CadastralParcelClustersRepository] = None,
        buffer_degrees: float = BUFFER_DEGREES,
    ):
        self._parcels_repo = parcels_repo or CadastralParcelsRepository()
        self._clusters_repo = clusters_repo or CadastralParcelClustersRepository()
        self._buffer_degrees = buffer_degrees

    def build_clusters(
        self,
        max_parcels: Optional[int] = None,
        min_cluster_size: int = 2,
        progress_callback: Optional[Any] = None,
    ) -> Dict[str, int]:
        """
        Побудовує кластери з усіх ділянок у cadastral_parcels.

        Args:
            max_parcels: максимум ділянок для обробки (None — усі)
            min_cluster_size: мінімальна кількість ділянок у кластері (2 = пара і більше)
            progress_callback: викликається з (processed, clusters_found)

        Returns:
            {"parcels_processed": N, "clusters_created": K, "parcels_in_clusters": M}
        """
        parcels = list(
            self._parcels_repo.collection.find(
                {"bounds": {"$exists": True, "$ne": None}},
                {"cadastral_number": 1, "bounds": 1, "purpose": 1, "purpose_label": 1, "ownership_form": 1, "area_sqm": 1},
            ).limit(max_parcels or 0)
        )
        if not max_parcels:
            parcels = list(parcels)

        items: List[Tuple[str, Any, str, Optional[str], str, Optional[float]]] = []
        for doc in parcels:
            cn = doc.get("cadastral_number")
            bounds = doc.get("bounds")
            if not cn or not bounds:
                continue
            try:
                geom = shape(bounds)
                if geom.is_empty or not geom.is_valid:
                    geom = geom.buffer(0) if hasattr(geom, "buffer") else None
                if geom is None or geom.is_empty:
                    continue
            except Exception:
                continue
            purpose = _normalize_for_cluster(doc.get("purpose"))
            purpose_label = doc.get("purpose_label")
            ownership = _normalize_for_cluster(doc.get("ownership_form"))
            area = doc.get("area_sqm")
            items.append((str(cn).strip(), geom, purpose, purpose_label, ownership, area))

        if len(items) < 2:
            return {"parcels_processed": len(items), "clusters_created": 0, "parcels_in_clusters": 0}

        # STRtree для швидкого пошуку сусідів
        geoms = [x[1] for x in items]
        tree = STRtree(geoms)

        parent: Dict[str, str] = {x[0]: x[0] for x in items}
        cadnum_by_idx = [x[0] for x in items]

        for i, (cadnum, geom, purpose, _, ownership, _) in enumerate(items):
            buffered = geom.buffer(self._buffer_degrees)
            candidates = tree.query(buffered)
            for j in candidates:
                if i == j:
                    continue
                other_cadnum, other_geom, other_purpose, _, other_ownership, _ = items[j]
                if purpose != other_purpose or ownership != other_ownership:
                    continue
                if geom.touches(other_geom) or geom.intersects(other_geom) or buffered.intersects(other_geom):
                    _union_find_merge(parent, cadnum, other_cadnum)

        # Збираємо кластери
        clusters: Dict[str, Set[str]] = {}
        for cadnum in parent:
            root = _find(parent, cadnum)
            if root not in clusters:
                clusters[root] = set()
            clusters[root].add(cadnum)

        clusters_filtered = {k: v for k, v in clusters.items() if len(v) >= min_cluster_size}
        parcels_in_clusters = sum(len(v) for v in clusters_filtered.values())

        # Зберігаємо кластери
        for root, cadnums in clusters_filtered.items():
            cadnum_list = sorted(cadnums)
            first = cadnum_list[0]
            cluster_id = f"cluster_{first.replace(':', '_')}"
            purpose_val = next((x[2] for x in items if x[0] == first), "")
            purpose_label_val = next((x[3] for x in items if x[0] == first), None)
            ownership_val = next((x[4] for x in items if x[0] == first), "")
            total_area = sum(
                (x[5] or 0) for x in items if x[0] in cadnums
            )

            # Центр і bounds — з першої ділянки (спрощено)
            centroid = None
            bounds_union = None
            first_doc = self._parcels_repo.find_by_cadastral_number(first)
            if first_doc and first_doc.get("bounds"):
                try:
                    g = shape(first_doc["bounds"])
                    centroid = {"type": "Point", "coordinates": list(g.centroid.coords)[0]}
                    bounds_union = first_doc["bounds"]
                except Exception:
                    pass

            self._clusters_repo.upsert_cluster(
                cluster_id=cluster_id,
                cadastral_numbers=cadnum_list,
                purpose=purpose_val or None,
                purpose_label=purpose_label_val,
                ownership_form=ownership_val or None,
                parcel_count=len(cadnum_list),
                centroid=centroid,
                bounds=bounds_union,
                total_area_sqm=total_area if total_area > 0 else None,
            )

        if progress_callback:
            progress_callback(len(items), len(clusters_filtered))

        return {
            "parcels_processed": len(items),
            "clusters_created": len(clusters_filtered),
            "parcels_in_clusters": parcels_in_clusters,
        }
