# -*- coding: utf-8 -*-
"""Smoke-test для нового CadastralDomainService.

Сценарій: знайти у кадастрі ділянки / кластери в межах смт Турійськ
(Волинська обл.), які підходять під супермаркет 1500+ м².

Запуск: py scripts/test_cadastral_domain_smoke.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
os.chdir(_PROJECT_ROOT)


def _dump(obj, max_chars: int = 4000) -> str:
    s = json.dumps(obj, ensure_ascii=False, indent=2, default=str)
    if len(s) > max_chars:
        return s[:max_chars] + f"\n... [truncated, total {len(s)} chars]"
    return s


def main() -> int:
    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.services.cadastral_domain_service import CadastralDomainService

    MongoDBConnection.initialize(Settings())
    svc = CadastralDomainService()

    print("=" * 80)
    print("SMOKE: cadastral.get_knowledge(scope=Волинська)")
    print("=" * 80)
    knowledge = svc.get_knowledge(scope={"oblast_name": "Волинська"}, top_n_codes=15)
    print("ok:", knowledge.get("ok"))
    classification = knowledge.get("classification") or {}
    bg = classification.get("business_groups") or []
    print(f"business_groups: {len(bg)}")
    for g in bg[:14]:
        print(f"  - {g.get('name')}: {g.get('label')} (codes={len(g.get('purpose_codes', []))})")
    stats = knowledge.get("db_stats") or {}
    print(f"db_stats.total_parcels (sample limit): {stats.get('total_parcels')}")
    print(f"scope_applied: {stats.get('scope_applied')}")
    print("Top purpose codes у Волинській (sample):")
    for row in (stats.get("by_purpose") or [])[:10]:
        groups = ",".join(row.get("business_groups") or []) or "-"
        print(f"  {row.get('code'):>6}  cnt={row.get('count'):<6}  groups=[{groups}]  {row.get('label')}")
    print("Top ownership_form у Волинській:")
    for row in (stats.get("by_ownership_form") or [])[:5]:
        print(f"  cnt={row.get('count'):<6}  {row.get('ownership_form')}")

    turiisk_lat = 51.0931
    turiisk_lng = 24.5328
    radius_m = 6000

    print()
    print("=" * 80)
    print(f"SMOKE: cadastral.discover_in_area(Турійськ, radius={radius_m} м) — default limit")
    print("=" * 80)
    discover = svc.discover_in_area(
        scope={
            "latitude": turiisk_lat,
            "longitude": turiisk_lng,
            "radius_meters": radius_m,
        },
    )
    print("ok:", discover.get("ok"))
    print(
        "total_parcels_in_area:", discover.get("total_parcels"),
        " fetch_limit:", discover.get("fetch_limit"),
        " scope_truncated:", discover.get("scope_truncated"),
    )
    if discover.get("truncation_note"):
        print("truncation_note:", discover.get("truncation_note"))
    print("by_business_group (топ-5):")
    for row in (discover.get("by_business_group") or [])[:5]:
        print(f"  - {row.get('group'):<15}  cnt={row.get('count'):<5}  {row.get('label')}")
    print("by_purpose (топ-10):")
    for row in (discover.get("by_purpose") or [])[:10]:
        print(f"  - {row.get('code'):>6}  cnt={row.get('count'):<5}  {row.get('label')}")
    print("by_area_bucket:")
    for row in discover.get("by_area_bucket") or []:
        print(f"  - {row.get('bucket'):<20}  cnt={row.get('count')}")
    area_stats = discover.get("area_stats_sqm") or {}
    if area_stats:
        print(f"area_stats: min={area_stats.get('min'):.0f}  p50={area_stats.get('p50'):.0f}  "
              f"p75={area_stats.get('p75'):.0f}  max={area_stats.get('max'):.0f}")
    print("suggested_filters:")
    for sf in discover.get("suggested_filters") or []:
        print(f"  - business_group={sf.get('business_group')}  "
              f"parcels_in_area={sf.get('parcels_in_area')}  "
              f"purpose_codes={sf.get('purpose_codes')}")

    scenarios = [
        {
            "title": "A) Тільки commercial (КВЦПЗ 03.07-12.11), парцель ≥1500 м², кластер ≥1500 м²",
            "filters": {
                "business_groups": ["commercial"],
                "min_area_sqm": 1500,
                "min_cluster_area_sqm": 1500,
            },
        },
        {
            "title": "B) Розширено: commercial + industrial + public_service, парцель ≥1500 м²",
            "filters": {
                "business_groups": ["commercial", "industrial", "public_service"],
                "min_area_sqm": 1500,
                "min_cluster_area_sqm": 1500,
            },
        },
        {
            "title": "C) Без min_area на парцелі, лише сумарна площа кластера ≥1500 м² (агрегація сусідніх)",
            "filters": {
                "business_groups": ["commercial", "industrial", "public_service"],
                "min_cluster_area_sqm": 1500,
                "min_cluster_parcels": 1,
            },
        },
        {
            "title": "D) Резерв: agricultural ≥1500 м² (потенційна зміна призначення під торгівлю)",
            "filters": {
                "business_groups": ["agricultural"],
                "min_area_sqm": 1500,
                "min_cluster_area_sqm": 1500,
            },
        },
    ]

    for sc in scenarios:
        print()
        print("=" * 80)
        print(f"SMOKE: cadastral.search — {sc['title']}")
        print("=" * 80)
        result = svc.search(
            scope={
                "latitude": turiisk_lat,
                "longitude": turiisk_lng,
                "radius_meters": radius_m,
            },
            filters=sc["filters"],
            output={"top_n": 15, "include_singles": True, "include_landscape": True},
        )
        af = result.get("applied_filters") or {}
        print(
            "ok:", result.get("ok"),
            " scope_total_parcels:", result.get("scope_total_parcels"),
            " filtered_total_parcels:", result.get("filtered_total_parcels"),
            " semantic_used:", result.get("semantic_used"),
        )
        print(
            "fetch_limit:", result.get("fetch_limit"),
            " scope_truncated:", result.get("scope_truncated"),
            " server_side_filtered:", result.get("server_side_filtered"),
        )
        if result.get("truncation_note"):
            print("truncation_note:", result.get("truncation_note"))
        print(
            "applied: groups=", af.get("business_groups_resolved"),
            " codes_count=", len(af.get("purpose_codes_effective") or []),
            " min_area_sqm=", af.get("min_area_sqm"),
            " min_cluster_area_sqm=", af.get("min_cluster_area_sqm"),
        )
        clusters = result.get("clusters") or []
        singles = result.get("single_parcels") or []
        print(f"clusters (multi-parcel) found: {len(clusters)}")
        for i, c in enumerate(clusters[:10], 1):
            cads = c.get("cadastral_numbers") or []
            cent = c.get("centroid") or {}
            coords = cent.get("coordinates") if isinstance(cent, dict) else None
            cent_str = f"{coords[1]:.5f},{coords[0]:.5f}" if coords and len(coords) >= 2 else "-"
            print(
                f"  [{i}] cluster_id={c.get('cluster_id')}  parcels={c.get('parcel_count')}  "
                f"total_area={c.get('total_area_sqm'):.0f} м²  purpose={c.get('purpose')}  "
                f"ownership={c.get('ownership_form')}  centroid={cent_str}"
            )
            print(f"        purpose_label: {c.get('purpose_label')}")
            print(f"        cadastral_numbers ({len(cads)}): {cads[:8]}{' ...' if len(cads) > 8 else ''}")

        print(f"single_parcels found: {len(singles)}")
        for i, p in enumerate(singles[:15], 1):
            cads = p.get("cadastral_numbers") or []
            cent = p.get("centroid") or {}
            coords = cent.get("coordinates") if isinstance(cent, dict) else None
            cent_str = f"{coords[1]:.5f},{coords[0]:.5f}" if coords and len(coords) >= 2 else "-"
            area = p.get("total_area_sqm") or 0
            print(
                f"  [{i}] {(cads[0] if cads else '-')}  area={area:.0f} м²  "
                f"purpose={p.get('purpose')}  ownership={p.get('ownership_form')}  centroid={cent_str}"
            )
            if p.get("purpose_label"):
                print(f"        {p.get('purpose_label')}")

        if not clusters and not singles:
            print("note_when_empty:", result.get("note_when_empty"))
            ls = result.get("landscape") or {}
            if ls:
                print(f"  landscape.total_parcels: {ls.get('total_parcels')}")
                for sf in ls.get("suggested_filters") or []:
                    print(
                        f"  landscape.suggested {sf.get('business_group')}: "
                        f"parcels_in_area={sf.get('parcels_in_area')}"
                    )

    print()
    print("=" * 80)
    print("SMOKE: cadastral.list_parcels_in_region_polygon (Турійськ + commercial)")
    print("=" * 80)
    poly = svc.list_parcels_in_region_polygon(
        toponym="смт Турійськ Волинська область Україна",
        filters={"business_groups": ["commercial"]},
        region="ua",
    )
    print("ok:", poly.get("ok"), " query_id:", poly.get("query_id"), " total:", poly.get("total_count"))
    if poly.get("ok") and poly.get("query_id"):
        page0 = svc.list_polygon_query_page(query_id=poly["query_id"], page=0, page_size=5)
        print("page0 items:", len(page0.get("items") or []), _dump(page0, 1200))

    print()
    print("=" * 80)
    print("SMOKE: cadastral.cluster_parcels (номери з короткого search)")
    print("=" * 80)
    last_search = svc.search(
        scope={"latitude": turiisk_lat, "longitude": turiisk_lng, "radius_meters": 2000},
        filters={"business_groups": ["commercial"]},
        output={"top_n": 3},
    )
    sample_cns: list[str] = []
    for c in (last_search.get("clusters") or [])[:1]:
        sample_cns.extend((c.get("cadastral_numbers") or [])[:12])
    for s in (last_search.get("single_parcels") or [])[:3]:
        sample_cns.extend((s.get("cadastral_numbers") or [])[:4])
    sample_cns = list(dict.fromkeys(sample_cns))[:20]
    if sample_cns:
        cl = svc.cluster_parcels(cadastral_numbers=sample_cns, min_cluster_size=1)
        print("cluster_parcels ok:", cl.get("ok"), " clusters:", len(cl.get("clusters") or []))
        print(_dump(cl, 2500))
    else:
        print("skip cluster_parcels: no sample cadastral_numbers")

    print()
    print("=" * 80)
    print("SMOKE complete.")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
