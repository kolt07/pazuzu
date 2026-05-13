# -*- coding: utf-8 -*-
"""
CadastralDomainService — єдина доменна обгортка для всієї роботи з кадастром.

Це фасад над:
    * CadastralParcelsRepository (MongoDB 2dsphere на bounds)
    * CadastralParcelLocationIndexRepository (індекс oblast/koatuu)
    * CadastralClusteringService (in-memory кластеризація суміжних ділянок)
    * VectorIndexService (Qdrant + bge-m3 семантичний пошук)
    * CadastralClassificationCatalog (статичний словник КВЦПЗ-кодів і бізнес-груп)
    * CadastralToponymBoundaryService + кеш полігон-запитів

Публічні методи для FLX-агента (tools):
    get_knowledge, discover_in_area, search,
    list_parcels_in_region_polygon, list_polygon_query_page,
    cluster_parcels, get_cluster_meta, get_parcel_summary, get_parcel_full.

Сирі колекції та довільні Mongo-запити для агента закриті.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from business.services.cadastral_classification_catalog import (
    CadastralClassificationCatalog,
)
from business.services.cadastral_clustering_service import CadastralClusteringService
from config.settings import Settings
from data.repositories.cadastral_parcel_location_index_repository import (
    CadastralParcelLocationIndexRepository,
)
from data.repositories.cadastral_parcel_clusters_repository import (
    CadastralParcelClustersRepository,
)
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
from data.repositories.cadastral_polygon_query_cache_repository import (
    CadastralPolygonQueryCacheRepository,
)

logger = logging.getLogger(__name__)

# Fallback-дефолти, якщо Settings недоступні. Реальні значення підтягуються
# з `Settings().cadastral_*` (див. config.yaml → cadastral.*).
DEFAULT_SEARCH_MAX_PARCELS = 5000
DEFAULT_DISCOVER_MAX_PARCELS = 2000
DEFAULT_SEARCH_HARD_CAP = 20000
DEFAULT_MAX_RADIUS_METERS = 50000.0
DEFAULT_AREA_BUCKETS_SQM: Tuple[Tuple[float, float, str], ...] = (
    (0.0, 1000.0, "до 0.1 га"),
    (1000.0, 5000.0, "0.1-0.5 га"),
    (5000.0, 10000.0, "0.5-1 га"),
    (10000.0, 50000.0, "1-5 га"),
    (50000.0, 200000.0, "5-20 га"),
    (200000.0, float("inf"), ">20 га"),
)


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    vs = sorted(values)
    k = max(0, min(len(vs) - 1, int(round((pct / 100.0) * (len(vs) - 1)))))
    return float(vs[k])


def _slim_parcel(doc: Mapping[str, Any]) -> Dict[str, Any]:
    """Компактна форма ділянки для агента (без bounds, з координатами центру)."""
    if not isinstance(doc, Mapping):
        return {}
    centroid = None
    bounds = doc.get("bounds") or {}
    coords = (bounds or {}).get("coordinates") if isinstance(bounds, Mapping) else None
    if isinstance(coords, list) and coords and isinstance(coords[0], list) and coords[0]:
        try:
            ring = coords[0]
            xs = [float(p[0]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
            ys = [float(p[1]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
            if xs and ys:
                centroid = {"longitude": sum(xs) / len(xs), "latitude": sum(ys) / len(ys)}
        except (TypeError, ValueError):
            centroid = None
    return {
        "cadastral_number": doc.get("cadastral_number"),
        "purpose": doc.get("purpose"),
        "purpose_label": doc.get("purpose_label"),
        "category": doc.get("category"),
        "ownership_form": doc.get("ownership_form"),
        "area_sqm": doc.get("area_sqm"),
        "address": doc.get("address"),
        "centroid": centroid,
    }


def _area_bucket_name(area_sqm: Optional[float]) -> str:
    if area_sqm is None:
        return "unknown"
    try:
        a = float(area_sqm)
    except (TypeError, ValueError):
        return "unknown"
    for lo, hi, name in DEFAULT_AREA_BUCKETS_SQM:
        if lo <= a < hi:
            return name
    return "unknown"


class CadastralDomainService:
    """Доменний фасад для кадастру (без сирого доступу до колекцій ззовні)."""

    def __init__(
        self,
        *,
        parcels_repo: Optional[CadastralParcelsRepository] = None,
        location_index_repo: Optional[CadastralParcelLocationIndexRepository] = None,
        clustering_service: Optional[CadastralClusteringService] = None,
        clusters_repo: Optional[CadastralParcelClustersRepository] = None,
        catalog: Optional[CadastralClassificationCatalog] = None,
        vector_index_service: Any = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self._parcels = parcels_repo or CadastralParcelsRepository()
        self._loc_index = location_index_repo or CadastralParcelLocationIndexRepository()
        self._clustering = clustering_service or CadastralClusteringService(
            parcels_repo=self._parcels,
        )
        self._clusters_repo = clusters_repo or CadastralParcelClustersRepository()
        self._catalog = catalog or CadastralClassificationCatalog.get_instance()
        self._vector = vector_index_service  # ліниво підвантажується у search()
        self._boundary_svc = None  # CadastralToponymBoundaryService
        self._polygon_cache_repo: Optional[CadastralPolygonQueryCacheRepository] = None

        try:
            s = settings or Settings()
            self._search_default_limit = max(
                100, int(getattr(s, "cadastral_search_max_parcels", DEFAULT_SEARCH_MAX_PARCELS))
            )
            self._discover_default_limit = max(
                100, int(getattr(s, "cadastral_discover_max_parcels", DEFAULT_DISCOVER_MAX_PARCELS))
            )
            self._search_hard_cap = max(
                self._search_default_limit,
                int(getattr(s, "cadastral_search_hard_cap", DEFAULT_SEARCH_HARD_CAP)),
            )
            self._max_radius_m = max(
                100.0,
                float(getattr(s, "cadastral_max_radius_meters", DEFAULT_MAX_RADIUS_METERS)),
            )
            self._polygon_max = max(
                1000,
                int(getattr(s, "cadastral_polygon_max_parcels", 80000)),
            )
            self._polygon_page_default = max(
                10,
                int(getattr(s, "cadastral_polygon_page_size_default", 200)),
            )
        except Exception as e:
            logger.warning(
                "Settings() unavailable for CadastralDomainService, fallback to defaults: %s", e
            )
            self._search_default_limit = DEFAULT_SEARCH_MAX_PARCELS
            self._discover_default_limit = DEFAULT_DISCOVER_MAX_PARCELS
            self._search_hard_cap = DEFAULT_SEARCH_HARD_CAP
            self._max_radius_m = DEFAULT_MAX_RADIUS_METERS
            self._polygon_max = 80000
            self._polygon_page_default = 200

    # ============================================================
    #                  1) get_knowledge
    # ============================================================

    def get_knowledge(
        self,
        *,
        scope: Optional[Mapping[str, Any]] = None,
        top_n_codes: int = 50,
    ) -> Dict[str, Any]:
        """Каталог КВЦПЗ + статистика реальних значень у БД.

        Args:
            scope: опційний обмежувач статистики у вигляді
                `{"oblast_code": "..."}` або `{"oblast_name": "..."}`.
                Якщо None — статистика по всій колекції.
            top_n_codes: скільки найчастіших purpose-кодів повертати у
                `db_stats.by_purpose`.

        Повертає:
            {
              "classification": {"codes": [...], "business_groups": [...]},
              "db_stats": {
                  "total_parcels": int,
                  "by_purpose": [{"code", "label", "count", "business_groups": [...]}, ...],
                  "by_purpose_label_top": [{"purpose_label", "count"}, ...],
                  "by_ownership_form": [{"ownership_form", "count"}, ...],
                  "by_category": [{"category", "count"}, ...],
                  "scope_applied": {...} | None
              },
              "usage": "коротка інструкція як використовувати каталог"
            }
        """
        classification = {
            "codes": self._catalog.all_codes(),
            "business_groups": self._catalog.all_business_groups(),
            "note": (
                "У БД зберігаються 2 ключових поля: `purpose` — короткий КВЦПЗ-код "
                "(напр. '03.07'); `purpose_label` — повна офіційна фраза. "
                "Працюй із групами або кодами, а не з вільним текстом."
            ),
        }

        db_stats = self._collect_db_stats(scope=scope, top_n_codes=top_n_codes)
        usage = (
            "Workflow: спершу обирай `business_groups` зі списку (commercial / "
            "industrial / ...), за потреби звужуй `purpose_codes`. У `db_stats.by_purpose` "
            "видно реально присутні коди в БД — нічого зайвого не задавай. Далі — "
            "`cadastral.discover_in_area` для конкретної локації, потім `cadastral.search`."
        )
        return {
            "ok": True,
            "classification": classification,
            "db_stats": db_stats,
            "usage": usage,
        }

    def _collect_db_stats(
        self,
        *,
        scope: Optional[Mapping[str, Any]],
        top_n_codes: int,
    ) -> Dict[str, Any]:
        """Реальні значення `purpose`/`purpose_label`/`ownership_form` у БД."""
        match: Dict[str, Any] = {}
        scope_applied: Optional[Dict[str, Any]] = None
        if scope:
            oblast_code = (scope.get("oblast_code") or "").strip()
            oblast_name = (scope.get("oblast_name") or "").strip()
            if oblast_code or oblast_name:
                # Маємо location index по кадастровому номеру. Для звуження
                # вибірки колекції parcels робимо subquery в location_index і
                # збираємо набір cadastral_number, потім матчимо в parcels.
                loc_filter: Dict[str, Any] = {}
                if oblast_code:
                    loc_filter["oblast_code"] = oblast_code
                elif oblast_name:
                    loc_filter["oblast_name"] = {"$regex": oblast_name, "$options": "i"}
                try:
                    cad_numbers = [
                        d.get("cadastral_number")
                        for d in self._loc_index.collection.find(
                            loc_filter, {"cadastral_number": 1}
                        )
                        if d and d.get("cadastral_number")
                    ]
                except Exception as e:
                    logger.warning("location index lookup failed: %s", e)
                    cad_numbers = []
                if cad_numbers:
                    match["cadastral_number"] = {"$in": cad_numbers[:50000]}
                scope_applied = {
                    "oblast_code": oblast_code or None,
                    "oblast_name": oblast_name or None,
                    "resolved_cadastral_numbers": len(cad_numbers),
                }

        total = 0
        by_purpose: Counter = Counter()
        by_purpose_label: Counter = Counter()
        by_ownership: Counter = Counter()
        by_category: Counter = Counter()
        try:
            cursor = self._parcels.collection.find(
                match,
                {
                    "purpose": 1,
                    "purpose_label": 1,
                    "ownership_form": 1,
                    "category": 1,
                },
            ).limit(50000)
            for doc in cursor:
                total += 1
                p = (doc.get("purpose") or "").strip()
                pl = (doc.get("purpose_label") or "").strip()
                of = (doc.get("ownership_form") or "").strip()
                cat = (doc.get("category") or "").strip()
                if p:
                    by_purpose[p] += 1
                if pl:
                    by_purpose_label[pl] += 1
                if of:
                    by_ownership[of] += 1
                if cat:
                    by_category[cat] += 1
        except Exception as e:
            logger.warning("parcels stats failed: %s", e)

        return {
            "total_parcels": total,
            "by_purpose": [
                {
                    "code": code,
                    "label": self._catalog.label_for_code(code),
                    "count": cnt,
                    "business_groups": self._catalog.groups_for_code(code),
                }
                for code, cnt in by_purpose.most_common(max(1, int(top_n_codes or 50)))
            ],
            "by_purpose_label_top": [
                {"purpose_label": k, "count": v}
                for k, v in by_purpose_label.most_common(20)
            ],
            "by_ownership_form": [
                {"ownership_form": k, "count": v}
                for k, v in by_ownership.most_common(15)
            ],
            "by_category": [
                {"category": k, "count": v}
                for k, v in by_category.most_common(15)
            ],
            "scope_applied": scope_applied,
        }

    # ============================================================
    #                  2) discover_in_area
    # ============================================================

    def discover_in_area(
        self,
        *,
        scope: Mapping[str, Any],
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Reconnaissance: що є у вказаній локації, без жодних фільтрів.

        Args:
            scope: одне з:
                * `{"latitude": ..., "longitude": ..., "radius_meters": ...}`
                * `{"oblast_code": "..."}` або `{"oblast_name": "..."}`
            limit: максимум документів у вибірці для аналізу. За замовчуванням
                береться `Settings().cadastral_discover_max_parcels` (2000).
                Hard-capped до `search_hard_cap`.

        Повертає:
            {
              "ok": True,
              "scope": {...},
              "total_parcels": N,
              "fetch_limit": int,         # який ліміт реально застосовано
              "scope_truncated": bool,    # True якщо ми вперлися у ліміт
              "by_business_group": [{"group", "label", "count"}],
              "by_purpose": [{"code", "label", "count"}],
              "by_purpose_label_top": [{"purpose_label", "count"}],
              "by_ownership_form": [...],
              "by_category": [...],
              "by_area_bucket": [{"bucket", "count"}],
              "area_stats_sqm": {min, max, avg, p25, p50, p75},
              "suggested_filters": [
                  {"business_group": "commercial", "purpose_codes": [...], "explain": "..."}
              ]
            }
        """
        effective_limit = self._resolve_limit(limit, default=self._discover_default_limit)
        try:
            parcels = self._resolve_scope_to_parcels(scope, limit=effective_limit)
        except ValueError as e:
            return {"ok": False, "error": str(e)}

        scope_truncated = len(parcels) >= effective_limit

        if not parcels:
            return {
                "ok": True,
                "scope": dict(scope or {}),
                "total_parcels": 0,
                "fetch_limit": effective_limit,
                "scope_truncated": False,
                "by_business_group": [],
                "by_purpose": [],
                "by_purpose_label_top": [],
                "by_ownership_form": [],
                "by_category": [],
                "by_area_bucket": [],
                "area_stats_sqm": None,
                "suggested_filters": [],
                "note": (
                    "У цій локації немає записів у `cadastral_parcels`. "
                    "Або кадастр ще не вивантажено для цього регіону, або scope не "
                    "перетинається з даними. Спробуй розширити радіус, перевірити "
                    "координати або викликати `cadastral.get_knowledge` із scope=oblast."
                ),
            }

        by_purpose: Counter = Counter()
        by_purpose_label: Counter = Counter()
        by_ownership: Counter = Counter()
        by_category: Counter = Counter()
        by_group: Counter = Counter()
        by_bucket: Counter = Counter()
        areas: List[float] = []

        for p in parcels:
            code = (p.get("purpose") or "").strip()
            pl = (p.get("purpose_label") or "").strip()
            of = (p.get("ownership_form") or "").strip()
            cat = (p.get("category") or "").strip()
            if code:
                by_purpose[code] += 1
                for g in self._catalog.groups_for_code(code):
                    by_group[g] += 1
            if pl:
                by_purpose_label[pl] += 1
            if of:
                by_ownership[of] += 1
            if cat:
                by_category[cat] += 1
            by_bucket[_area_bucket_name(p.get("area_sqm"))] += 1
            try:
                a = float(p.get("area_sqm") or 0)
                if a > 0:
                    areas.append(a)
            except (TypeError, ValueError):
                pass

        suggested = self._suggest_filters_from_groups(by_group)

        return {
            "ok": True,
            "scope": dict(scope or {}),
            "total_parcels": len(parcels),
            "fetch_limit": effective_limit,
            "scope_truncated": scope_truncated,
            "truncation_note": (
                (
                    f"Reconnaissance обрізано на {effective_limit} ділянках — "
                    "у scope їх більше. Це не критично для пошуку (далі викликай "
                    "`cadastral.search` з фільтрами — він уміє підняти ліміт і "
                    "вмикає server-side фільтрацію), але цифри в `by_*` — лише по "
                    "вибірці. За потреби звузь radius або викликай discover з "
                    "вищим `limit`."
                )
                if scope_truncated
                else None
            ),
            "by_business_group": [
                {
                    "group": g,
                    "label": (self._catalog.business_group(g).label
                              if self._catalog.business_group(g) else g),
                    "count": cnt,
                }
                for g, cnt in by_group.most_common(15)
            ],
            "by_purpose": [
                {
                    "code": code,
                    "label": self._catalog.label_for_code(code),
                    "count": cnt,
                }
                for code, cnt in by_purpose.most_common(20)
            ],
            "by_purpose_label_top": [
                {"purpose_label": k, "count": v}
                for k, v in by_purpose_label.most_common(15)
            ],
            "by_ownership_form": [
                {"ownership_form": k, "count": v}
                for k, v in by_ownership.most_common(10)
            ],
            "by_category": [
                {"category": k, "count": v}
                for k, v in by_category.most_common(10)
            ],
            "by_area_bucket": [
                {"bucket": k, "count": v} for k, v in by_bucket.most_common()
            ],
            "area_stats_sqm": (
                {
                    "count": len(areas),
                    "min": float(min(areas)),
                    "max": float(max(areas)),
                    "avg": float(sum(areas) / len(areas)),
                    "p25": _percentile(areas, 25),
                    "p50": _percentile(areas, 50),
                    "p75": _percentile(areas, 75),
                }
                if areas
                else None
            ),
            "suggested_filters": suggested,
            "usage": (
                "Це лише краєвид (landscape). Щоб отримати конкретні ділянки + "
                "кластеризацію — викликай `cadastral.search` з обраними "
                "`business_groups` або `purpose_codes` зі списку вище."
            ),
        }

    def _suggest_filters_from_groups(
        self, by_group: Counter
    ) -> List[Dict[str, Any]]:
        """Топ-3 бізнес-групи з cuсідніми кодами."""
        out: List[Dict[str, Any]] = []
        for g, cnt in by_group.most_common(3):
            grp = self._catalog.business_group(g)
            if not grp:
                continue
            out.append(
                {
                    "business_group": g,
                    "label": grp.label,
                    "purpose_codes": list(grp.purpose_codes),
                    "parcels_in_area": cnt,
                    "explain": (
                        f"У scope знайдено {cnt} ділянок у групі «{grp.label}». "
                        "Виклич `cadastral.search` з `filters.business_groups=['"
                        f"{g}']` для отримання кадастрових номерів і кластерів."
                    ),
                }
            )
        return out

    # ============================================================
    #                  3) search
    # ============================================================

    def search(
        self,
        *,
        scope: Mapping[str, Any],
        filters: Optional[Mapping[str, Any]] = None,
        output: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Основний пошуковий метод: вибірка → фільтрація → кластеризація → ranking.

        Args:
            scope: одне з `{latitude, longitude, radius_meters}` або
                `{oblast_code|oblast_name}`.
            filters (всі опційні):
                * `business_groups: [str, ...]`  — мапиться у purpose_codes через каталог.
                * `purpose_codes: [str, ...]`    — точні КВЦПЗ-коди (об'єднується з groups).
                * `purpose_label_contains: str`  — case-insensitive substring.
                * `ownership_form: str` / `ownership_form_contains: str`
                * `min_area_sqm: float` / `max_area_sqm: float` — фільтр площі ділянки.
                * `min_cluster_area_sqm: float`  — фільтр сумарної площі кластера.
                * `min_cluster_parcels: int`     — мінімум ділянок у кластері.
                * `semantic_query: str`          — пере-ранжує через Qdrant.
            output (опційно):
                * `top_n: int = 15`
                * `include_singles: bool = True`
                * `include_landscape: bool = True`

        Повертає:
            {
              "ok": True,
              "scope": {...},
              "applied_filters": {...},
              "fetch_limit": int,             # який ліміт реально застосовано
              "scope_total_parcels": int,
              "scope_truncated": bool,        # True якщо ми вперлися у fetch_limit
              "server_side_filtered": bool,   # True якщо purpose_codes/area було пушнуто у Mongo
              "filtered_total_parcels": int,
              "clusters": [{...cluster з cadastral_numbers...}],
              "single_parcels": [{...}],
              "landscape": {...},      # якщо include_landscape
              "semantic_used": bool,
              "note_when_empty": "..." # підказка з landscape, якщо filtered=0
            }
        """
        filters = dict(filters or {})
        output_cfg = dict(output or {})
        top_n = max(1, min(int(output_cfg.get("top_n") or 15), 100))
        include_singles = bool(output_cfg.get("include_singles", True))
        include_landscape = bool(output_cfg.get("include_landscape", True))
        fetch_limit_override = output_cfg.get("fetch_limit")

        # ---- Нормалізація доменних фільтрів через каталог ----
        norm = self._catalog.normalize_purpose_filter(
            purpose_codes=filters.get("purpose_codes"),
            business_groups=filters.get("business_groups"),
            purpose_label_contains=filters.get("purpose_label_contains"),
        )

        ownership_form = (filters.get("ownership_form") or "").strip() or None
        ownership_form_contains = (filters.get("ownership_form_contains") or "").strip() or None
        try:
            min_area = (
                float(filters["min_area_sqm"]) if filters.get("min_area_sqm") is not None else None
            )
        except (TypeError, ValueError):
            min_area = None
        try:
            max_area = (
                float(filters["max_area_sqm"]) if filters.get("max_area_sqm") is not None else None
            )
        except (TypeError, ValueError):
            max_area = None
        try:
            min_cluster_area = (
                float(filters["min_cluster_area_sqm"])
                if filters.get("min_cluster_area_sqm") is not None
                else None
            )
        except (TypeError, ValueError):
            min_cluster_area = None
        try:
            min_cluster_parcels = int(filters.get("min_cluster_parcels") or 1)
        except (TypeError, ValueError):
            min_cluster_parcels = 1
        min_cluster_parcels = max(1, min_cluster_parcels)
        semantic_query = (filters.get("semantic_query") or "").strip() or None

        purpose_codes = list(norm["purpose_codes"] or [])
        purpose_label_contains_raw = (filters.get("purpose_label_contains") or "").strip() or None

        # ---- Landscape (broad reconnaissance, без фільтрів) ----
        landscape = None
        if include_landscape:
            try:
                recon = self._resolve_scope_to_parcels(
                    scope, limit=self._discover_default_limit
                )
            except ValueError as e:
                return {"ok": False, "error": str(e)}
            landscape = self._build_landscape_summary(recon)

        # ---- Адаптивний ліміт: якщо є строгі server-side фільтри (коди, area),
        #      можемо безпечно підняти його до search_hard_cap, бо Mongo сам
        #      зменшить набір. Без фільтрів — тримаємо базове значення. ----
        has_strict_server_filter = bool(purpose_codes) or (
            min_area is not None and min_area > 0
        )
        if fetch_limit_override is not None:
            try:
                effective_limit = max(1, min(int(fetch_limit_override), self._search_hard_cap))
            except (TypeError, ValueError):
                effective_limit = self._search_default_limit
        elif has_strict_server_filter:
            effective_limit = self._search_hard_cap
        else:
            effective_limit = self._search_default_limit

        # ---- Server-side push фільтрів у Mongo (для radius-scope — через
        #      $geoWithin + $in purpose + range area; для oblast-scope —
        #      через aggregation з тими ж предикатами). ----
        try:
            base_parcels, server_side_filtered = self._fetch_filtered_parcels(
                scope=scope,
                purpose_codes=purpose_codes,
                purpose_label_contains=(
                    purpose_label_contains_raw
                    # якщо у нас вже є точні коди — label не пушимо, інакше
                    # отримаємо AND двох умов, що звужує даремно
                    if (purpose_label_contains_raw and not purpose_codes)
                    else None
                ),
                ownership_form=ownership_form,
                ownership_form_contains=ownership_form_contains,
                min_area_sqm=min_area,
                max_area_sqm=max_area,
                limit=effective_limit,
            )
        except ValueError as e:
            return {"ok": False, "error": str(e)}

        scope_truncated = len(base_parcels) >= effective_limit

        # ---- Доуточнення in-memory (label regex з каталогу, ownership_contains,
        #      area) — server-side вже відсік основну масу, тут лиш «полірування»
        #      і єдина точка для polish-фільтрів, які Mongo не покриває. ----
        filtered = self._apply_filters(
            base_parcels,
            purpose_codes=purpose_codes,
            purpose_label_regex=norm["purpose_label_regex"],
            ownership_form=ownership_form,
            ownership_form_contains=ownership_form_contains,
            min_area_sqm=min_area,
            max_area_sqm=max_area,
        )

        # ---- Семантичне пере-ранжування (опційно) ----
        semantic_used = False
        if semantic_query and filtered:
            try:
                filtered = self._rerank_by_semantic(
                    filtered, semantic_query=semantic_query, scope=scope
                )
                semantic_used = True
            except Exception as e:
                logger.warning("Semantic rerank failed: %s", e)

        # ---- Кластеризація + ranking ----
        groups = self._clustering.cluster_parcels_in_memory(
            filtered, min_cluster_size=min_cluster_parcels
        )
        if min_cluster_area is not None:
            groups = [
                g for g in groups
                if (g.get("total_area_sqm") or 0.0) >= min_cluster_area
            ]

        multi = [g for g in groups if not g.get("is_singleton")][:top_n]
        singles = (
            [g for g in groups if g.get("is_singleton")][:top_n]
            if include_singles
            else []
        )

        note_when_empty = None
        if not multi and not singles:
            if landscape and (landscape.get("total_parcels") or 0) > 0:
                tops = ", ".join(
                    f"\"{it['purpose_label']}\" ({it['count']})"
                    for it in (landscape.get("by_purpose_label_top") or [])[:5]
                )
                note_when_empty = (
                    f"З цими фільтрами знайдено 0 ділянок, але у scope є "
                    f"{landscape['total_parcels']}. Перевір `landscape.by_business_group` "
                    f"і `by_purpose_label_top` ({tops}) — або обери інші business_groups, "
                    "або задай конкретні `purpose_codes`, або зніми `min_area_sqm`."
                )
            else:
                note_when_empty = (
                    "Ані за фільтрами, ані без них у scope ділянок не знайдено. "
                    "Перевір координати/oblast або скоригуй радіус."
                )

        truncation_note = None
        if scope_truncated:
            if has_strict_server_filter:
                truncation_note = (
                    f"Вибірку обрізано на {effective_limit} ділянках (hard_cap) — "
                    "у scope більше відповідних кодам/площі парцелей. Звузь radius "
                    "або викликай search повторно з тіснішим scope."
                )
            else:
                truncation_note = (
                    f"Вибірку обрізано на {effective_limit} ділянках. Це сталося, "
                    "бо фільтри неточні (без `business_groups`/`purpose_codes`/"
                    "`min_area_sqm`) — Mongo не зміг звузити вибірку server-side. "
                    "Додай хоча б одну строгу умову — і пошук автоматично "
                    f"підніме ліміт до {self._search_hard_cap}."
                )

        return {
            "ok": True,
            "scope": dict(scope or {}),
            "applied_filters": {
                "business_groups_resolved": norm["business_groups_resolved"],
                "business_groups_unknown": norm["business_groups_unknown"],
                "purpose_codes_effective": norm["purpose_codes"],
                "purpose_label_regex": norm["purpose_label_regex"],
                "ownership_form": ownership_form,
                "ownership_form_contains": ownership_form_contains,
                "min_area_sqm": min_area,
                "max_area_sqm": max_area,
                "min_cluster_area_sqm": min_cluster_area,
                "min_cluster_parcels": min_cluster_parcels,
                "semantic_query": semantic_query,
            },
            "fetch_limit": effective_limit,
            "scope_total_parcels": len(base_parcels),
            "scope_truncated": scope_truncated,
            "server_side_filtered": server_side_filtered,
            "truncation_note": truncation_note,
            "filtered_total_parcels": len(filtered),
            "clusters_count": len(multi),
            "single_parcels_count": len(singles),
            "clusters": multi,
            "single_parcels": singles,
            "landscape": landscape,
            "semantic_used": semantic_used,
            "note_when_empty": note_when_empty,
            "hint": (
                "Кожен елемент `clusters[]`/`single_parcels[]` містить ПОВНИЙ перелік "
                "`cadastral_numbers`, `total_area_sqm`, `centroid`, `purpose_label`, "
                "`ownership_form`. У звіт обов'язково включай конкретні номери."
            ),
        }

    # ============================================================
    #                   internal helpers
    # ============================================================

    def _resolve_limit(
        self,
        explicit: Optional[int],
        *,
        default: int,
    ) -> int:
        """Безпечно нормалізує ліміт: дефолт, але не вище search_hard_cap."""
        if explicit is None:
            return max(1, int(default))
        try:
            v = int(explicit)
        except (TypeError, ValueError):
            return max(1, int(default))
        return max(1, min(v, self._search_hard_cap))

    def _resolve_scope_to_parcels(
        self,
        scope: Optional[Mapping[str, Any]],
        *,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Перетворює scope у список parcels-документів (без фільтрів по атрибутах)."""
        if not scope:
            raise ValueError("scope is required: provide latitude/longitude/radius_meters or oblast")

        lat = scope.get("latitude")
        lng = scope.get("longitude")
        radius = scope.get("radius_meters")
        if lat is not None and lng is not None and radius is not None:
            try:
                lat_f = float(lat)
                lng_f = float(lng)
                radius_f = max(1.0, min(float(radius), self._max_radius_m))
            except (TypeError, ValueError) as e:
                raise ValueError(f"invalid scope coordinates: {e}") from e
            return self._parcels.find_within_radius(
                latitude=lat_f,
                longitude=lng_f,
                radius_meters=radius_f,
                limit=int(limit),
            )

        oblast_code = (scope.get("oblast_code") or "").strip()
        oblast_name = (scope.get("oblast_name") or "").strip()
        if oblast_code or oblast_name:
            return self._fetch_parcels_by_oblast(
                oblast_code=oblast_code or None,
                oblast_name=oblast_name or None,
                limit=int(limit),
            )

        raise ValueError(
            "scope must contain either (latitude, longitude, radius_meters) "
            "or (oblast_code/oblast_name)"
        )

    def _fetch_filtered_parcels(
        self,
        *,
        scope: Mapping[str, Any],
        purpose_codes: Sequence[str],
        purpose_label_contains: Optional[str],
        ownership_form: Optional[str],
        ownership_form_contains: Optional[str],
        min_area_sqm: Optional[float],
        max_area_sqm: Optional[float],
        limit: int,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Тягне ділянки зі scope з push-фільтрами на рівні MongoDB.

        Returns:
            (parcels, server_side_filtered) — `server_side_filtered=True`, якщо
            хоча б один атрибутний фільтр було пушнуто у Mongo.
        """
        if not scope:
            raise ValueError(
                "scope is required: provide latitude/longitude/radius_meters or oblast"
            )

        codes = [str(c).strip() for c in (purpose_codes or []) if c and str(c).strip()]
        has_atomic_filter = bool(
            codes
            or purpose_label_contains
            or ownership_form
            or ownership_form_contains
            or (min_area_sqm is not None)
            or (max_area_sqm is not None)
        )

        lat = scope.get("latitude")
        lng = scope.get("longitude")
        radius = scope.get("radius_meters")
        if lat is not None and lng is not None and radius is not None:
            try:
                lat_f = float(lat)
                lng_f = float(lng)
                radius_f = max(1.0, min(float(radius), self._max_radius_m))
            except (TypeError, ValueError) as e:
                raise ValueError(f"invalid scope coordinates: {e}") from e
            parcels = self._parcels.find_within_radius(
                latitude=lat_f,
                longitude=lng_f,
                radius_meters=radius_f,
                purpose_codes_in=codes or None,
                purpose_label_contains=purpose_label_contains,
                ownership_form=ownership_form,
                ownership_form_contains=ownership_form_contains,
                area_sqm_min=min_area_sqm,
                area_sqm_max=max_area_sqm,
                limit=int(limit),
            )
            return parcels, has_atomic_filter

        oblast_code = (scope.get("oblast_code") or "").strip()
        oblast_name = (scope.get("oblast_name") or "").strip()
        if oblast_code or oblast_name:
            parcels = self._fetch_parcels_by_oblast_filtered(
                oblast_code=oblast_code or None,
                oblast_name=oblast_name or None,
                purpose_codes=codes,
                purpose_label_contains=purpose_label_contains,
                ownership_form=ownership_form,
                ownership_form_contains=ownership_form_contains,
                min_area_sqm=min_area_sqm,
                max_area_sqm=max_area_sqm,
                limit=int(limit),
            )
            return parcels, has_atomic_filter

        raise ValueError(
            "scope must contain either (latitude, longitude, radius_meters) "
            "or (oblast_code/oblast_name)"
        )

    def _fetch_parcels_by_oblast_filtered(
        self,
        *,
        oblast_code: Optional[str],
        oblast_name: Optional[str],
        purpose_codes: Sequence[str],
        purpose_label_contains: Optional[str],
        ownership_form: Optional[str],
        ownership_form_contains: Optional[str],
        min_area_sqm: Optional[float],
        max_area_sqm: Optional[float],
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Oblast-scope: набір cadastral_number з location_index + Mongo-фільтри."""
        import re as _re

        loc_docs = self._loc_index.find_by_oblast(
            oblast_code=oblast_code, oblast_name=oblast_name
        )
        if not loc_docs:
            return []
        cad_numbers = [
            d.get("cadastral_number") for d in loc_docs if d and d.get("cadastral_number")
        ]
        if not cad_numbers:
            return []
        # Для $in зрізаємо до розумного розміру (інакше query plan стане важким).
        cad_numbers = cad_numbers[: max(int(limit), 50000)]

        criteria: Dict[str, Any] = {"cadastral_number": {"$in": cad_numbers}}
        codes = [str(c).strip() for c in (purpose_codes or []) if c and str(c).strip()]
        if codes:
            criteria["purpose"] = {"$in": codes}
        if purpose_label_contains:
            criteria["purpose_label"] = {
                "$regex": _re.escape(purpose_label_contains.strip()),
                "$options": "i",
            }
        if ownership_form:
            criteria["ownership_form"] = ownership_form
        elif ownership_form_contains:
            criteria["ownership_form"] = {
                "$regex": _re.escape(ownership_form_contains.strip()),
                "$options": "i",
            }
        area_filter: Dict[str, Any] = {}
        if min_area_sqm is not None:
            try:
                area_filter["$gte"] = float(min_area_sqm)
            except (TypeError, ValueError):
                pass
        if max_area_sqm is not None:
            try:
                area_filter["$lte"] = float(max_area_sqm)
            except (TypeError, ValueError):
                pass
        if area_filter:
            criteria["area_sqm"] = area_filter

        try:
            cursor = self._parcels.collection.find(criteria).limit(max(1, int(limit)))
            out: List[Dict[str, Any]] = []
            for doc in cursor:
                if doc and "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                out.append(doc)
            return out
        except Exception as e:
            logger.warning("oblast filtered parcels fetch failed: %s", e)
            return []

    def _fetch_parcels_by_oblast(
        self,
        *,
        oblast_code: Optional[str],
        oblast_name: Optional[str],
        limit: int,
    ) -> List[Dict[str, Any]]:
        loc_docs = self._loc_index.find_by_oblast(
            oblast_code=oblast_code, oblast_name=oblast_name
        )
        if not loc_docs:
            return []
        cad_numbers = [
            d.get("cadastral_number") for d in loc_docs if d and d.get("cadastral_number")
        ]
        if not cad_numbers:
            return []
        cad_numbers = cad_numbers[: max(1, int(limit))]
        try:
            cursor = self._parcels.collection.find(
                {"cadastral_number": {"$in": cad_numbers}}
            ).limit(max(1, int(limit)))
            out: List[Dict[str, Any]] = []
            for doc in cursor:
                if doc and "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                out.append(doc)
            return out
        except Exception as e:
            logger.warning("oblast parcels fetch failed: %s", e)
            return []

    def _build_landscape_summary(
        self, parcels: Sequence[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        by_purpose: Counter = Counter()
        by_purpose_label: Counter = Counter()
        by_ownership: Counter = Counter()
        by_group: Counter = Counter()
        for p in parcels or []:
            code = (p.get("purpose") or "").strip()
            pl = (p.get("purpose_label") or "").strip()
            of = (p.get("ownership_form") or "").strip()
            if code:
                by_purpose[code] += 1
                for g in self._catalog.groups_for_code(code):
                    by_group[g] += 1
            if pl:
                by_purpose_label[pl] += 1
            if of:
                by_ownership[of] += 1
        return {
            "total_parcels": len(parcels),
            "by_business_group": [
                {
                    "group": g,
                    "label": (self._catalog.business_group(g).label
                              if self._catalog.business_group(g) else g),
                    "count": cnt,
                }
                for g, cnt in by_group.most_common(10)
            ],
            "by_purpose": [
                {
                    "code": code,
                    "label": self._catalog.label_for_code(code),
                    "count": cnt,
                }
                for code, cnt in by_purpose.most_common(10)
            ],
            "by_purpose_label_top": [
                {"purpose_label": k, "count": v}
                for k, v in by_purpose_label.most_common(10)
            ],
            "by_ownership_form": [
                {"ownership_form": k, "count": v}
                for k, v in by_ownership.most_common(5)
            ],
        }

    def _apply_filters(
        self,
        parcels: Sequence[Mapping[str, Any]],
        *,
        purpose_codes: Sequence[str],
        purpose_label_regex: Optional[str],
        ownership_form: Optional[str],
        ownership_form_contains: Optional[str],
        min_area_sqm: Optional[float],
        max_area_sqm: Optional[float],
    ) -> List[Dict[str, Any]]:
        import re as _re

        code_set = set(c for c in purpose_codes if c)
        label_pat = _re.compile(purpose_label_regex, _re.IGNORECASE) if purpose_label_regex else None
        own_pat = (
            _re.compile(_re.escape(ownership_form_contains), _re.IGNORECASE)
            if ownership_form_contains else None
        )
        own_exact = ownership_form.strip() if ownership_form else None

        out: List[Dict[str, Any]] = []
        for p in parcels or []:
            if code_set and (p.get("purpose") or "").strip() not in code_set:
                continue
            if label_pat and not label_pat.search(str(p.get("purpose_label") or "")):
                continue
            if own_exact and (p.get("ownership_form") or "").strip() != own_exact:
                continue
            if own_pat and not own_pat.search(str(p.get("ownership_form") or "")):
                continue
            try:
                area = float(p.get("area_sqm") or 0)
            except (TypeError, ValueError):
                area = 0.0
            if min_area_sqm is not None and area < min_area_sqm:
                continue
            if max_area_sqm is not None and area > max_area_sqm:
                continue
            out.append(dict(p))
        return out

    def _rerank_by_semantic(
        self,
        parcels: Sequence[Mapping[str, Any]],
        *,
        semantic_query: str,
        scope: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        """Перерангує parcels за релевантністю semantic_query через Qdrant."""
        try:
            if self._vector is None:
                from business.services.vector_index_service import VectorIndexService
                self._vector = VectorIndexService()
            hits = self._vector.search_parcels(
                query_text=semantic_query,
                top_k=min(50, len(parcels) * 2 or 20),
                filters={"doc_type": "parcel"},
            )
        except Exception as e:
            logger.warning("vector.search_parcels unavailable: %s", e)
            return list(parcels)

        ranked_cn = []
        seen = set()
        for h in hits:
            cn = (h.get("payload") or {}).get("cadastral_number") or h.get("cadastral_number")
            if cn and cn not in seen:
                seen.add(cn)
                ranked_cn.append(cn)

        by_cn = {(p.get("cadastral_number") or ""): p for p in parcels}
        out: List[Dict[str, Any]] = []
        for cn in ranked_cn:
            if cn in by_cn:
                out.append(by_cn[cn])
        for cn, p in by_cn.items():
            if cn not in seen:
                out.append(p)
        return out

    def _get_polygon_cache(self) -> CadastralPolygonQueryCacheRepository:
        if self._polygon_cache_repo is None:
            self._polygon_cache_repo = CadastralPolygonQueryCacheRepository()
        return self._polygon_cache_repo

    def _boundary_service(self) -> Any:
        if self._boundary_svc is None:
            from business.services.cadastral_toponym_boundary_service import (
                CadastralToponymBoundaryService,
            )
            self._boundary_svc = CadastralToponymBoundaryService()
        return self._boundary_svc

    @staticmethod
    def _polygon_query_fingerprint(
        *,
        toponym: str,
        polygon: Mapping[str, Any],
        purpose_codes: Sequence[str],
        business_groups: Sequence[str],
        ownership_form: Optional[str],
        min_area: Optional[float],
        max_area: Optional[float],
        purpose_label_contains: Optional[str],
    ) -> str:
        poly_blob = json.dumps(
            polygon.get("coordinates"),
            sort_keys=True,
            ensure_ascii=False,
        )
        parts = [
            (toponym or "").strip().lower(),
            poly_blob,
            ",".join(sorted(str(c).strip() for c in purpose_codes if c)),
            ",".join(sorted(str(b).strip() for b in business_groups if b)),
            ownership_form or "",
            str(min_area if min_area is not None else ""),
            str(max_area if max_area is not None else ""),
            (purpose_label_contains or "").strip().lower(),
        ]
        raw = "|".join(parts)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def list_parcels_in_region_polygon(
        self,
        *,
        toponym: str,
        filters: Optional[Mapping[str, Any]] = None,
        region: str = "ua",
    ) -> Dict[str, Any]:
        """
        Ділянки в межах viewport-полігона топоніма (Geocoding + Place Details).
        Повертає query_id для пагінації; великі списки не дублюються у відповіді.
        """
        text = (toponym or "").strip()
        if not text:
            return {"ok": False, "error": "toponym порожній"}
        filters = dict(filters or {})

        norm = self._catalog.normalize_purpose_filter(
            purpose_codes=filters.get("purpose_codes"),
            business_groups=filters.get("business_groups"),
            purpose_label_contains=filters.get("purpose_label_contains"),
        )
        purpose_codes = list(norm["purpose_codes"] or [])
        purpose_label_contains_raw = (filters.get("purpose_label_contains") or "").strip() or None
        ownership_form = (filters.get("ownership_form") or "").strip() or None
        try:
            min_area = (
                float(filters["min_area_sqm"]) if filters.get("min_area_sqm") is not None else None
            )
        except (TypeError, ValueError):
            min_area = None
        try:
            max_area = (
                float(filters["max_area_sqm"]) if filters.get("max_area_sqm") is not None else None
            )
        except (TypeError, ValueError):
            max_area = None

        boundary = self._boundary_service().resolve_boundary(
            text, region=region or "ua", caller="cadastral_domain.list_parcels_in_region_polygon"
        )
        if not boundary.get("ok"):
            return {
                "ok": False,
                "error": boundary.get("error") or "boundary_failed",
                "toponym": text,
                "boundary_detail": {k: v for k, v in boundary.items() if k != "polygon"},
            }
        polygon = boundary["polygon"]
        fp = self._polygon_query_fingerprint(
            toponym=text,
            polygon=polygon,
            purpose_codes=purpose_codes,
            business_groups=list(norm.get("business_groups_resolved") or []),
            ownership_form=ownership_form,
            min_area=min_area,
            max_area=max_area,
            purpose_label_contains=purpose_label_contains_raw,
        )

        cached = self._get_polygon_cache().find_by_fingerprint(fp)
        if cached:
            total = int(cached.get("total_count") or 0)
            qid = cached.get("query_id") or ""
            preview_n = 50
            ids = cached.get("cadastral_numbers") or []
            preview = [{"cadastral_number": cn} for cn in ids[:preview_n]]
            return {
                "ok": True,
                "from_cache": True,
                "query_id": qid,
                "total_count": total,
                "truncated": bool(cached.get("truncated")),
                "boundary_kind": cached.get("boundary_kind"),
                "formatted_address": cached.get("formatted_address"),
                "place_id": cached.get("place_id"),
                "applied_filters": {
                    "purpose_codes_effective": purpose_codes,
                    "business_groups_resolved": norm.get("business_groups_resolved"),
                    "ownership_form": ownership_form,
                    "min_area_sqm": min_area,
                    "max_area_sqm": max_area,
                    "purpose_label_contains": purpose_label_contains_raw,
                },
                "preview_parcels": preview,
                "preview_note": (
                    f"Перші {len(preview)} кадастрових номерів; решта — через cadastral.list_polygon_query_page."
                    if total > len(preview)
                    else None
                ),
                "usage": "Викликай cadastral.list_polygon_query_page(query_id, page) для сторінок.",
            }

        pl_contains = (
            purpose_label_contains_raw
            if (purpose_label_contains_raw and not purpose_codes)
            else None
        )
        total_db = self._parcels.collection.count_documents(
            self._mongo_criteria_for_polygon(
                polygon,
                purpose_codes=purpose_codes,
                purpose_label_contains=pl_contains,
                ownership_form=ownership_form,
                min_area_sqm=min_area,
                max_area_sqm=max_area,
            )
        )
        max_keep = min(self._polygon_max, total_db)
        truncated = total_db > max_keep

        criteria = self._mongo_criteria_for_polygon(
            polygon,
            purpose_codes=purpose_codes,
            purpose_label_contains=pl_contains,
            ownership_form=ownership_form,
            min_area_sqm=min_area,
            max_area_sqm=max_area,
        )
        cadnums: List[str] = []
        for doc in self._parcels.collection.find(criteria, {"cadastral_number": 1}).limit(max_keep):
            cn = (doc.get("cadastral_number") or "").strip()
            if cn:
                cadnums.append(cn)

        query_id = str(uuid.uuid4())
        bg_res = [str(x) for x in (norm.get("business_groups_resolved") or []) if x]
        self._get_polygon_cache().save_query_result(
            query_fingerprint=fp,
            query_id=query_id,
            toponym=text,
            boundary_kind=str(boundary.get("boundary_kind") or "viewport_rectangle"),
            formatted_address=boundary.get("formatted_address"),
            place_id=boundary.get("place_id"),
            polygon=dict(polygon),
            purpose_codes=purpose_codes,
            business_groups=bg_res,
            cadastral_numbers=cadnums,
            total_count=total_db,
            truncated=truncated,
        )

        preview_n = 50
        preview = [{"cadastral_number": cn} for cn in cadnums[:preview_n]]
        return {
            "ok": True,
            "from_cache": False,
            "query_id": query_id,
            "total_count": total_db,
            "truncated": truncated,
            "boundary_kind": boundary.get("boundary_kind"),
            "formatted_address": boundary.get("formatted_address"),
            "place_id": boundary.get("place_id"),
            "applied_filters": {
                "purpose_codes_effective": purpose_codes,
                "business_groups_resolved": norm.get("business_groups_resolved"),
                "ownership_form": ownership_form,
                "min_area_sqm": min_area,
                "max_area_sqm": max_area,
                "purpose_label_contains": purpose_label_contains_raw,
            },
            "preview_parcels": preview,
            "preview_note": (
                f"Перші {len(preview)} кадастрових номерів; решта — через cadastral.list_polygon_query_page."
                if total_db > len(preview)
                else None
            ),
            "usage": "Викликай cadastral.list_polygon_query_page(query_id, page) для сторінок.",
        }

    def _mongo_criteria_for_polygon(
        self,
        polygon: Mapping[str, Any],
        *,
        purpose_codes: Sequence[str],
        purpose_label_contains: Optional[str],
        ownership_form: Optional[str],
        min_area_sqm: Optional[float],
        max_area_sqm: Optional[float],
    ) -> Dict[str, Any]:
        import re as _re

        criteria: Dict[str, Any] = {
            "bounds": {"$geoIntersects": {"$geometry": dict(polygon)}},
        }
        codes = [str(c).strip() for c in purpose_codes if c and str(c).strip()]
        if codes:
            criteria["purpose"] = {"$in": codes}
        if purpose_label_contains:
            criteria["purpose_label"] = {
                "$regex": _re.escape(str(purpose_label_contains).strip()),
                "$options": "i",
            }
        if ownership_form:
            criteria["ownership_form"] = str(ownership_form).strip()
        area_filter: Dict[str, Any] = {}
        if min_area_sqm is not None:
            try:
                area_filter["$gte"] = float(min_area_sqm)
            except (TypeError, ValueError):
                pass
        if max_area_sqm is not None:
            try:
                area_filter["$lte"] = float(max_area_sqm)
            except (TypeError, ValueError):
                pass
        if area_filter:
            criteria["area_sqm"] = area_filter
        return criteria

    def list_polygon_query_page(
        self,
        *,
        query_id: str,
        page: int = 0,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Сторінка кадастрових номерів за query_id з кешу полігон-запиту."""
        qid = (query_id or "").strip()
        if not qid:
            return {"ok": False, "error": "query_id порожній"}
        ps = int(page_size or self._polygon_page_default)
        ps = max(1, min(ps, 2000))
        pg = max(0, int(page or 0))

        doc = self._get_polygon_cache().find_by_query_id(qid)
        if not doc:
            return {"ok": False, "error": "query_not_found", "query_id": qid}

        ids: List[str] = list(doc.get("cadastral_numbers") or [])
        start = pg * ps
        chunk = ids[start : start + ps]
        items = [{"cadastral_number": cn} for cn in chunk]
        return {
            "ok": True,
            "query_id": qid,
            "page": pg,
            "page_size": ps,
            "returned_count": len(items),
            "total_count": int(doc.get("total_count") or len(ids)),
            "truncated": bool(doc.get("truncated")),
            "has_more": start + len(chunk) < len(ids),
            "items": items,
        }

    def cluster_parcels(
        self,
        *,
        cadastral_numbers: Sequence[str],
        purpose_group_rules: Optional[Sequence[Sequence[str]]] = None,
        min_cluster_size: int = 1,
        ignore_ownership_for_grouping: bool = False,
    ) -> Dict[str, Any]:
        """
        Кластеризує переданий набір кадастрових номерів (завантажує bounds з БД).
        Повертає компактні описи кластерів для LLM.
        """
        raw_rules: Optional[List[List[str]]] = None
        if purpose_group_rules:
            raw_rules = []
            for grp in purpose_group_rules:
                if not grp:
                    continue
                raw_rules.append([str(x).strip() for x in grp if str(x).strip()])
            if not raw_rules:
                raw_rules = None

        cns = [str(x).strip() for x in cadastral_numbers if x and str(x).strip()]
        if not cns:
            return {"ok": False, "error": "cadastral_numbers порожній"}

        docs: List[Dict[str, Any]] = []
        missing: List[str] = []
        for cn in cns:
            d = self._parcels.find_by_cadastral_number(cn)
            if d and d.get("bounds"):
                docs.append(d)
            else:
                missing.append(cn)

        clusters = self._clustering.cluster_parcels_in_memory(
            docs,
            min_cluster_size=max(1, int(min_cluster_size or 1)),
            purpose_group_rules=raw_rules,
            ignore_ownership_for_grouping=bool(ignore_ownership_for_grouping),
        )
        preview_cap = 80
        out_clusters: List[Dict[str, Any]] = []
        for c in clusters:
            ref = f"tmp_cluster_{uuid.uuid4().hex}"
            nums: List[str] = list(c.get("cadastral_numbers") or [])
            preview = nums[:preview_cap]
            out_clusters.append({
                "cluster_ref": ref,
                "parcel_count": int(c.get("parcel_count") or 0),
                "is_singleton": bool(c.get("is_singleton")),
                "cadastral_numbers_preview": preview,
                "cadastral_numbers_omitted": max(0, len(nums) - len(preview)),
                "purpose": c.get("purpose"),
                "purpose_label": c.get("purpose_label"),
                "ownership_form": c.get("ownership_form"),
                "purposes_in_cluster": c.get("purposes_in_cluster"),
                "total_area_sqm": c.get("total_area_sqm"),
                "centroid": c.get("centroid"),
                "internal_cluster_id": c.get("cluster_id"),
            })
        return {
            "ok": True,
            "clusters": out_clusters,
            "parcels_input": len(cns),
            "parcels_loaded_with_bounds": len(docs),
            "missing_or_no_geometry": missing[:100],
            "missing_count": len(missing),
            "usage": "Повні межі ділянок — cadastral.get_parcel; кластери в БД — cadastral.get_cluster_meta.",
        }

    def get_cluster_meta(
        self,
        *,
        cluster_id: str,
        cadastral_preview_limit: int = 50,
    ) -> Dict[str, Any]:
        """Метадані кластера з cadastral_parcel_clusters (без повного списку номерів при великому розмірі)."""
        cid = (cluster_id or "").strip()
        if not cid:
            return {"ok": False, "error": "cluster_id порожній"}
        doc = self._clusters_repo.find_by_cluster_id(cid)
        if not doc:
            return {"ok": False, "error": "cluster_not_found", "cluster_id": cid}
        nums = list(doc.get("cadastral_numbers") or [])
        cap = max(1, min(int(cadastral_preview_limit or 50), 500))
        preview = nums[:cap]
        return {
            "ok": True,
            "cluster_id": doc.get("cluster_id"),
            "parcel_count": int(doc.get("parcel_count") or len(nums)),
            "purpose": doc.get("purpose"),
            "purpose_label": doc.get("purpose_label"),
            "ownership_form": doc.get("ownership_form"),
            "total_area_sqm": doc.get("total_area_sqm"),
            "centroid": doc.get("centroid"),
            "partition_key": doc.get("partition_key"),
            "cadastral_numbers_preview": preview,
            "cadastral_numbers_omitted": max(0, len(nums) - len(preview)),
            "usage": "Повний перелік номерів у документі кластера — обмежено; використовуй preview або повторний пошук.",
        }

    def list_cluster_parcels_page(
        self,
        *,
        cluster_id: str,
        page: int = 0,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """Пагінація кадастрових номерів з кластера в БД."""
        cid = (cluster_id or "").strip()
        if not cid:
            return {"ok": False, "error": "cluster_id порожній"}
        ps = max(1, min(int(page_size or 100), 2000))
        pg = max(0, int(page or 0))
        doc = self._clusters_repo.find_by_cluster_id(cid)
        if not doc:
            return {"ok": False, "error": "cluster_not_found", "cluster_id": cid}
        nums = list(doc.get("cadastral_numbers") or [])
        start = pg * ps
        chunk = nums[start : start + ps]
        return {
            "ok": True,
            "cluster_id": cid,
            "page": pg,
            "page_size": ps,
            "total_count": len(nums),
            "has_more": start + len(chunk) < len(nums),
            "items": [{"cadastral_number": x} for x in chunk],
        }

    def get_parcel_summary(self, *, cadastral_number: str) -> Dict[str, Any]:
        doc = self._parcels.find_by_cadastral_number(str(cadastral_number or "").strip())
        if not doc:
            return {"ok": False, "error": "parcel_not_found"}
        slim = _slim_parcel(doc)
        return {"ok": True, "parcel": slim}

    def get_parcel_full(self, *, cadastral_number: str) -> Dict[str, Any]:
        doc = self._parcels.find_by_cadastral_number(str(cadastral_number or "").strip())
        if not doc:
            return {"ok": False, "error": "parcel_not_found"}
        return {"ok": True, "parcel": dict(doc)}
