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


def _purpose_rule_group_key(
    purpose_normalized: str,
    purpose_group_rules: Optional[List[List[str]]],
) -> str:
    """Ключ групи призначення: або правило rule_i, або нормалізований purpose."""
    if not purpose_group_rules:
        return purpose_normalized
    for gi, grp in enumerate(purpose_group_rules):
        grp_set = {_normalize_for_cluster(x) for x in (grp or [])}
        if purpose_normalized in grp_set:
            return f"__rule_{gi}"
    return purpose_normalized


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

    def build_clusters_partition(
        self,
        partition_key: str,
        parcels: List[Dict[str, Any]],
        min_cluster_size: int = 2,
        progress_callback: Optional[Any] = None,
    ) -> Dict[str, int]:
        """
        Кластеризація лише ділянок однієї партиції (напр. одна область).
        Перед upsert видаляє попередні кластери з тим самим `partition_key`.
        cluster_id містить префікс партиції, щоб уникнути колізій між областями.
        """
        pk = str(partition_key or "").strip()
        if not pk:
            return {"parcels_processed": 0, "clusters_created": 0, "parcels_in_clusters": 0}
        self._clusters_repo.delete_by_partition_key(pk)
        pk_slug = pk.replace(":", "_").replace(" ", "_").replace("/", "_")[:48]

        items: List[Tuple[str, Any, str, Optional[str], str, Optional[float]]] = []
        for doc in parcels or []:
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

        geoms = [x[1] for x in items]
        tree = STRtree(geoms)
        parent: Dict[str, str] = {x[0]: x[0] for x in items}

        for i, (cadnum, geom, purpose, _, ownership, _) in enumerate(items):
            buffered = geom.buffer(self._buffer_degrees)
            candidates = tree.query(buffered)
            for j in candidates:
                if i == j:
                    continue
                _, other_geom, other_purpose, _, other_ownership, _ = items[j]
                if purpose != other_purpose or ownership != other_ownership:
                    continue
                if geom.touches(other_geom) or geom.intersects(other_geom) or buffered.intersects(other_geom):
                    _union_find_merge(parent, cadnum, items[j][0])

        clusters: Dict[str, Set[str]] = {}
        for cadnum in parent:
            root = _find(parent, cadnum)
            clusters.setdefault(root, set()).add(cadnum)

        clusters_filtered = {k: v for k, v in clusters.items() if len(v) >= min_cluster_size}
        parcels_in_clusters = sum(len(v) for v in clusters_filtered.values())

        for root, cadnums in clusters_filtered.items():
            cadnum_list = sorted(cadnums)
            first = cadnum_list[0]
            cluster_id = f"cluster_{pk_slug}_{first.replace(':', '_')}"
            purpose_val = next((x[2] for x in items if x[0] == first), "")
            purpose_label_val = next((x[3] for x in items if x[0] == first), None)
            ownership_val = next((x[4] for x in items if x[0] == first), "")
            total_area = sum((x[5] or 0) for x in items if x[0] in cadnums)

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
                partition_key=pk,
            )

        if progress_callback:
            progress_callback(len(items), len(clusters_filtered))

        return {
            "parcels_processed": len(items),
            "clusters_created": len(clusters_filtered),
            "parcels_in_clusters": parcels_in_clusters,
        }

    def cluster_parcels_in_memory(
        self,
        parcels: List[Dict[str, Any]],
        min_cluster_size: int = 1,
        buffer_degrees: Optional[float] = None,
        purpose_group_rules: Optional[List[List[str]]] = None,
        ignore_ownership_for_grouping: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Кластеризує переданий список ділянок (без запису в БД).

        Використовується tools-ами FLX, коли потрібно «з'єднати суміжні ділянки»
        у живій вибірці (наприклад, після `find_within_radius`). На відміну від
        `build_clusters` нічого не пише у Mongo і повертає кластери в пам'яті,
        включно з одиночками (`parcel_count == 1`), якщо `min_cluster_size == 1`.

        Args:
            parcels: документи з полями `cadastral_number`, `bounds`, `purpose`,
                `purpose_label`, `ownership_form`, `area_sqm`.
            min_cluster_size: фільтр мінімального розміру (1 = повертати все,
                включно з одиночними; 2 = тільки реальні кластери).
            buffer_degrees: радіус «дотику» в градусах; за замовчуванням
                використовується self._buffer_degrees (~5 м на широті ~48°).
            purpose_group_rules: опційно список груп КВЦПЗ-кодів
                (напр. [["03.07","12.04"],["01.01"]]) — кластеризуються разом, якщо
                географія дозволяє. None — злиття лише при однаковому `purpose`.
            ignore_ownership_for_grouping: якщо True — ігнорує форму власності при
                об'єднанні (лише group_key + геометрія).

        Returns:
            Список кластерів, відсортований за `total_area_sqm` (desc). Кожен
            кластер: `cluster_id`, `parcel_count`, `cadastral_numbers` (list),
            `total_area_sqm`, `purpose`, `purpose_label`, `ownership_form`,
            `purposes_in_cluster` (унікальні коди), `centroid`, `area_stats_sqm`, `is_singleton`.
        """
        buf = buffer_degrees if buffer_degrees is not None else self._buffer_degrees
        items: List[Dict[str, Any]] = []
        for doc in parcels or []:
            cn = (doc.get("cadastral_number") or "").strip()
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
            try:
                area = float(doc.get("area_sqm") or 0.0)
            except (TypeError, ValueError):
                area = 0.0
            pur_n = _normalize_for_cluster(doc.get("purpose"))
            own_n = _normalize_for_cluster(doc.get("ownership_form"))
            grp_key = _purpose_rule_group_key(pur_n, purpose_group_rules)
            own_merge = "" if ignore_ownership_for_grouping else own_n
            items.append({
                "cadastral_number": cn,
                "geom": geom,
                "purpose": pur_n,
                "purpose_label": doc.get("purpose_label"),
                "ownership_form": own_n,
                "group_key": grp_key,
                "own_merge": own_merge,
                "area_sqm": area,
                "raw": doc,
            })
        if not items:
            return []

        parent: Dict[str, str] = {it["cadastral_number"]: it["cadastral_number"] for it in items}

        if len(items) > 1:
            geoms = [it["geom"] for it in items]
            tree = STRtree(geoms)
            for i, it in enumerate(items):
                buffered = it["geom"].buffer(buf)
                for j in tree.query(buffered):
                    if i == j:
                        continue
                    other = items[j]
                    if it["group_key"] != other["group_key"] or it["own_merge"] != other["own_merge"]:
                        continue
                    if (
                        it["geom"].touches(other["geom"])
                        or it["geom"].intersects(other["geom"])
                        or buffered.intersects(other["geom"])
                    ):
                        _union_find_merge(parent, it["cadastral_number"], other["cadastral_number"])

        groups: Dict[str, List[Dict[str, Any]]] = {}
        for it in items:
            root = _find(parent, it["cadastral_number"])
            groups.setdefault(root, []).append(it)

        def _percentile(values: List[float], pct: float) -> Optional[float]:
            if not values:
                return None
            vs = sorted(values)
            k = max(0, min(len(vs) - 1, int(round((pct / 100.0) * (len(vs) - 1)))))
            return float(vs[k])

        clusters_out: List[Dict[str, Any]] = []
        min_size = max(1, int(min_cluster_size or 1))
        for root, group in groups.items():
            if len(group) < min_size:
                continue
            cadnums = sorted(it["cadastral_number"] for it in group)
            areas = [it["area_sqm"] for it in group if it["area_sqm"] > 0]
            total_area = float(sum(areas)) if areas else 0.0
            centroid: Optional[Dict[str, Any]] = None
            try:
                xs: List[float] = []
                ys: List[float] = []
                for it in group:
                    c = it["geom"].centroid
                    xs.append(float(c.x))
                    ys.append(float(c.y))
                if xs and ys:
                    centroid = {
                        "type": "Point",
                        "coordinates": [sum(xs) / len(xs), sum(ys) / len(ys)],
                    }
            except Exception:
                centroid = None
            head = group[0]
            purposes_distinct = sorted({(it["purpose"] or "") for it in group if it.get("purpose")})
            cluster_id = f"mem_cluster_{root.replace(':', '_')}"
            clusters_out.append({
                "cluster_id": cluster_id,
                "parcel_count": len(group),
                "is_singleton": len(group) == 1,
                "cadastral_numbers": cadnums,
                "purpose": head["purpose"] or None,
                "purpose_label": head["purpose_label"],
                "ownership_form": (None if ignore_ownership_for_grouping else (head["ownership_form"] or None)),
                "purposes_in_cluster": purposes_distinct,
                "total_area_sqm": total_area,
                "area_stats_sqm": {
                    "count": len(areas),
                    "min": (float(min(areas)) if areas else None),
                    "max": (float(max(areas)) if areas else None),
                    "avg": (float(sum(areas) / len(areas)) if areas else None),
                    "p25": _percentile(areas, 25),
                    "p50": _percentile(areas, 50),
                    "p75": _percentile(areas, 75),
                },
                "centroid": centroid,
            })

        clusters_out.sort(key=lambda c: (c["total_area_sqm"], c["parcel_count"]), reverse=True)
        return clusters_out
