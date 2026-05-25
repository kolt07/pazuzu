# -*- coding: utf-8 -*-
"""
Скрапінг населених пунктів з mista.ua → raw_mista_settlements.

Список НП: послідовно по кожній області (фільтр ?obl= на сторінці пошуку),
далі пагінація в межах області.

Запуск:
  py scripts/mista_scraper/run_scrape.py
  py scripts/mista_scraper/run_scrape.py --list-only
  py scripts/mista_scraper/run_scrape.py --details --limit 10
  py scripts/mista_scraper/run_scrape.py --region "Волинська область"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.raw_mista_settlements_repository import RawMistaSettlementsRepository
from scripts.mista_scraper.fetcher import (
    fetch_page,
    fetch_settlements_list_page,
    fetch_settlements_search_page,
    get_session,
)
from scripts.mista_scraper.parser import (
    parse_detail_page,
    parse_list_max_page,
    parse_list_page,
    parse_region_filter_options,
)

ProgressCallback = Callable[[Dict[str, Any]], None]
LogCallback = Callable[[str], None]


def _emit(
    progress_callback: Optional[ProgressCallback],
    log_fn: Optional[LogCallback],
    payload: Dict[str, Any],
) -> None:
    if progress_callback:
        progress_callback(payload)
    msg = payload.get("message")
    if msg and log_fn:
        log_fn(msg)


def load_region_filters(session=None) -> List[Dict[str, Any]]:
    """Завантажує перелік областей з dropdown на сторінці пошуку."""
    html = fetch_settlements_search_page(session=session)
    regions = parse_region_filter_options(html)
    if not regions:
        raise RuntimeError("Не вдалося розпарсити фільтр областей з mista.ua")
    return regions


def scrape_list_for_region(
    obl_id: int,
    region_name: str,
    *,
    max_pages: int = 0,
    session=None,
    progress_callback: Optional[ProgressCallback] = None,
    log_fn: Optional[LogCallback] = None,
) -> int:
    """Скрапить усі сторінки списку НП для однієї області (obl_id)."""
    repo = RawMistaSettlementsRepository()
    sess = session or get_session()
    page = 1
    total = 0
    empty_streak = 0
    list_max_page: Optional[int] = None

    while True:
        if max_pages and page > max_pages:
            break
        if list_max_page is not None and page > list_max_page:
            break
        _emit(
            progress_callback,
            log_fn,
            {
                "phase": "list",
                "region_name": region_name,
                "obl_id": obl_id,
                "list_page": page,
                "list_rows_total": total,
                "message": f"{region_name}: сторінка {page}…",
            },
        )
        html = fetch_settlements_list_page(page, obl_id=obl_id, session=sess)
        if list_max_page is None:
            list_max_page = parse_list_max_page(html)
        rows = parse_list_page(html, list_page=page)
        if not rows:
            empty_streak += 1
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0
            for row in rows:
                row["region_name"] = row.get("region_name") or region_name
                row["mista_obl_id"] = obl_id
                repo.upsert_list_row(row)
                total += 1
        _emit(
            progress_callback,
            log_fn,
            {
                "phase": "list",
                "region_name": region_name,
                "obl_id": obl_id,
                "list_page": page,
                "list_rows_total": total,
                "list_rows_page": len(rows),
                "list_max_page": list_max_page,
                "message": f"{region_name}: стор. {page}/{list_max_page or '?'}, +{len(rows)}",
            },
        )
        page += 1

    return total


def scrape_list_by_regions(
    *,
    regions: Optional[List[Dict[str, Any]]] = None,
    region_filter: Optional[str] = None,
    max_pages_per_region: int = 0,
    progress_callback: Optional[ProgressCallback] = None,
    log_fn: Optional[LogCallback] = None,
) -> int:
    """
    Проходить усі області з фільтра mista.ua і збирає НП кожної.
    region_filter — підрядок назви області (наприклад «Волинська»).
    """
    sess = get_session()
    if regions is None:
        regions = load_region_filters(session=sess)

    if region_filter:
        key = region_filter.strip().lower()
        regions = [r for r in regions if key in (r.get("region_name") or "").lower()]
        if not regions:
            raise ValueError(f"Область не знайдена у фільтрі mista: {region_filter!r}")

    total_all = 0
    n_regions = len(regions)
    for idx, reg in enumerate(regions, 1):
        obl_id = int(reg["obl_id"])
        rname = reg.get("region_name") or str(obl_id)
        _emit(
            progress_callback,
            log_fn,
            {
                "phase": "list",
                "region_index": idx,
                "regions_total": n_regions,
                "region_name": rname,
                "obl_id": obl_id,
                "message": f"Область {idx}/{n_regions}: {rname}",
            },
        )
        n = scrape_list_for_region(
            obl_id,
            rname,
            max_pages=max_pages_per_region,
            session=sess,
            progress_callback=progress_callback,
            log_fn=log_fn,
        )
        total_all += n
        _emit(
            progress_callback,
            log_fn,
            {
                "phase": "list",
                "region_index": idx,
                "regions_total": n_regions,
                "region_name": rname,
                "list_rows_total": total_all,
                "message": f"{rname}: {n} НП (разом {total_all})",
            },
        )
    return total_all


def scrape_details(
    limit: int = 0,
    *,
    progress_callback: Optional[ProgressCallback] = None,
    log_fn: Optional[LogCallback] = None,
) -> Dict[str, int]:
    repo = RawMistaSettlementsRepository()
    pending = repo.find_pending_details(limit=limit if limit else 0)
    total_pending = len(pending)
    if not pending:
        _emit(
            progress_callback,
            log_fn,
            {
                "phase": "details",
                "details_done": 0,
                "details_total": 0,
                "details_errors": 0,
                "message": "Немає записів list_only для деталей",
            },
        )
        return {"done": 0, "errors": 0, "total": 0}

    session = get_session()
    done = 0
    errors = 0
    for i, doc in enumerate(pending, 1):
        url = doc.get("mista_url")
        if not url:
            continue
        name = doc.get("name") or url
        try:
            _emit(
                progress_callback,
                log_fn,
                {
                    "phase": "details",
                    "details_done": done,
                    "details_total": total_pending,
                    "details_errors": errors,
                    "current_url": url,
                    "current_name": name,
                    "message": f"Деталі: {i}/{total_pending} — {name}",
                },
            )
            html = fetch_page(url, session=session)
            parsed = parse_detail_page(html, mista_url=url)
            if not parsed.get("mista_id") and doc.get("mista_id"):
                parsed["mista_id"] = doc["mista_id"]
            repo.upsert_parsed(url, parsed)
            done += 1
            _emit(
                progress_callback,
                log_fn,
                {
                    "phase": "details",
                    "details_done": done,
                    "details_total": total_pending,
                    "details_errors": errors,
                    "message": f"Деталі: {done}/{total_pending} готово",
                },
            )
        except Exception as e:
            errors += 1
            if log_fn:
                log_fn(f"Помилка {url}: {e}")

    return {"done": done, "errors": errors, "total": total_pending}


def run_mista_scraper(
    *,
    settings: Optional[Settings] = None,
    mode: str = "full",
    max_pages: int = 0,
    detail_limit: int = 0,
    region_filter: Optional[str] = None,
    log_fn: Optional[LogCallback] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    """
    Запуск скрапера mista.ua.

    mode: list | details | full (list по областях + details)
    max_pages: макс. сторінок на одну область (0 — усі)
    region_filter: лише одна область (підрядок назви)
    """
    if settings is None:
        settings = Settings()
    MongoDBConnection.initialize(settings)
    RawMistaSettlementsRepository()._ensure_indexes()

    mode = (mode or "full").strip().lower()
    do_list = mode in ("list", "full")
    do_details = mode in ("details", "full")

    list_total = 0
    regions_count = 0
    details_result = {"done": 0, "errors": 0, "total": 0}

    if do_list:
        regions = load_region_filters()
        regions_count = len(regions)
        if region_filter:
            key = region_filter.strip().lower()
            regions = [r for r in regions if key in (r.get("region_name") or "").lower()]
            regions_count = len(regions)
        list_total = scrape_list_by_regions(
            regions=regions,
            max_pages_per_region=max_pages,
            progress_callback=progress_callback,
            log_fn=log_fn,
        )

    if do_details:
        details_result = scrape_details(
            limit=detail_limit,
            progress_callback=progress_callback,
            log_fn=log_fn,
        )

    repo = RawMistaSettlementsRepository()
    stats = repo.count_by_status()
    msg = (
        f"Готово. Областей: {regions_count}; список: {list_total} рядків; "
        f"деталі: {details_result['done']}/{details_result['total']}"
        + (f", помилок: {details_result['errors']}" if details_result["errors"] else "")
        + f". Статуси: {stats}"
    )
    _emit(
        progress_callback,
        log_fn,
        {
            "phase": "done",
            "regions_total": regions_count,
            "list_rows_total": list_total,
            "details_done": details_result["done"],
            "details_total": details_result["total"],
            "details_errors": details_result["errors"],
            "status_counts": stats,
            "message": msg,
        },
    )
    return {
        "message": msg,
        "regions_total": regions_count,
        "list_rows_total": list_total,
        "details_done": details_result["done"],
        "details_total": details_result["total"],
        "details_errors": details_result["errors"],
        "status_counts": stats,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Скрапер mista.ua (населені пункти)")
    parser.add_argument("--list-only", action="store_true", help="Лише список")
    parser.add_argument("--details", action="store_true", help="Лише деталі")
    parser.add_argument(
        "--pages",
        type=int,
        default=0,
        help="Макс. сторінок списку на область (0=всі)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Ліміт детальних сторінок")
    parser.add_argument(
        "--region",
        type=str,
        default="",
        help="Лише область (підрядок, напр. Волинська)",
    )
    args = parser.parse_args()

    mode = "full"
    if args.list_only and not args.details:
        mode = "list"
    elif args.details and not args.list_only:
        mode = "details"

    run_mista_scraper(
        mode=mode,
        max_pages=args.pages,
        detail_limit=args.limit,
        region_filter=args.region or None,
        log_fn=lambda m: print(f"[mista] {m}", flush=True),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
