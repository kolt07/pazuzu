# -*- coding: utf-8 -*-
"""
Головна логіка скрапера кадастрової карти.
Запускається з адмін-панелі або окремо.

Цикл: отримати наступну комірку → завантажити дані → розпарсити → зберегти → оновити прогрес.

Індекс місцезнаходження (cadastral_parcel_location_index) будується окремо:
  py scripts/cadastral_build_location_index.py
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
from data.repositories.cadastral_scraper_progress_repository import (
    CadastralScraperProgressRepository,
    STATUS_PENDING,
)
from scripts.cadastral_scraper import config as scraper_config
from scripts.cadastral_scraper.fetcher import fetch_cell_data, get_session
from scripts.cadastral_scraper.grid_iterator import get_all_cells_for_ukraine
from scripts.cadastral_scraper.parser import parse_mvt_tile
from tqdm import tqdm


def _diagnose_parse_failure(raw: bytes, zoom: int, tile_x: int, tile_y: int, log_fn) -> None:
    """Діагностика при parsed=0: декодує MVT і логує структуру."""
    try:
        import mapbox_vector_tile
        data = mapbox_vector_tile.decode(raw)
        layers = list(data.keys())
        total_f = sum(len(layer_data.get("features") or []) for _, layer_data in data.items())
        log_fn(f"[Cadastral] Діагностика: layers={layers}, features={total_f}, tile={zoom}/{tile_x}/{tile_y}")
        if total_f > 0:
            f0 = next((ld.get("features", [])[0] for _, ld in data.items() if ld.get("features")), None)
            if f0:
                log_fn(f"[Cadastral] Перша feature props: {list((f0.get('properties') or {}).keys())}")
    except Exception as e:
        log_fn(f"[Cadastral] Діагностика decode: {e}")


def run_cadastral_scraper(
    settings: Optional[Settings] = None,
    max_cells: Optional[int] = None,
    workers: int = 1,
    log_fn: Optional[Callable[[str], None]] = None,
    stop_flag: Optional[Callable[[], bool]] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Запускає скрапер кадастрової карти.

    Args:
        settings: налаштування (якщо None — створюються нові)
        max_cells: максимум комірок за один запуск (None — без обмеження)
        workers: кількість паралельних потоків (1 — послідовно)
        log_fn: функція для логів
        stop_flag: callable, що повертає True для зупинки
        progress_callback: викликається після кожної комірки з dict:
            cells_processed, parcels_saved, total_parcels, cells_total, cells_done, current_cell_id

    Returns:
        Словник: success, cells_processed, parcels_saved, total_parcels, message
    """
    settings = settings or Settings()
    # Гарантуємо робочу директорію = корінь проекту (для однакового контексту з інтерфейсом)
    try:
        os.chdir(_PROJECT_ROOT)
    except OSError:
        pass

    MongoDBConnection.initialize(settings)
    parcels_repo = CadastralParcelsRepository()
    progress_repo = CadastralScraperProgressRepository()
    parcels_repo.ensure_index()
    progress_repo.ensure_index()

    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    # Діагностика: БД та початкова кількість (для перевірки контексту)
    db_info = f"{settings.mongodb_database_name} @ {settings.mongodb_host}:{settings.mongodb_port}"
    initial_count = parcels_repo.count_total()
    log(f"[Cadastral] БД: {db_info} | Ділянок до старту: {initial_count}")

    # Скидаємо завислі processing (5 хв — швидше відновлення після збою)
    progress_repo.reset_stale_processing(max_age_minutes=5)

    # Якщо комірок немає — створюємо сітку
    stats = progress_repo.get_stats()
    if stats["total"] == 0:
        log("[Cadastral] Ініціалізація сітки для території України...")
        cells = get_all_cells_for_ukraine()
        created = progress_repo.ensure_cells_exist(cells)
        log(f"[Cadastral] Створено {created} комірок (всього {len(cells)}).")
        stats = progress_repo.get_stats()

    cells_total = stats["total"]
    cells_done = stats.get("done", 0)

    # Прогрес-бар з ETA (тільки для CLI, без progress_callback)
    bar_total = max_cells if max_cells is not None else max(0, cells_total - cells_done)
    pbar: Optional[tqdm] = None
    if progress_callback is None and bar_total > 0:
        pbar = tqdm(
            total=bar_total,
            unit="тайл",
            desc="Кадастр",
            ncols=100,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        )

    def _log_loop(msg: str) -> None:
        """Лог у циклі: через pbar.write щоб не ламати прогрес-бар."""
        if pbar is not None:
            pbar.write(msg)
        else:
            log(msg)

    def _report_progress(
        cells_processed: int,
        parcels_saved: int,
        current_cell_id: str = "",
    ) -> None:
        total_parcels = parcels_repo.count_total_estimated()
        done = cells_done + cells_processed
        if progress_callback:
            progress_callback({
                "cells_processed": cells_processed,
                "parcels_saved": parcels_saved,
                "total_parcels": total_parcels,
                "cells_total": cells_total,
                "cells_done": done,
                "current_cell_id": current_cell_id,
            })

    cells_processed = 0
    parcels_saved = 0
    errors = 0
    _report_progress(0, 0, "")

    state_lock = threading.Lock()

    def _process_one_cell(
        cell: Dict[str, Any],
        is_first_in_session: bool,
        session: Optional[Any] = None,
        use_subsequent_delay: bool = False,
    ) -> None:
        """Обробка однієї комірки (використовується в послідовному та паралельному режимах)."""
        nonlocal cells_processed, parcels_saved, errors
        cell_id = cell.get("cell_id", "")
        zoom = cell.get("zoom", scraper_config.DEFAULT_ZOOM)
        tile_x = cell.get("tile_x")
        tile_y = cell.get("tile_y")
        if tile_x is None or tile_y is None:
            parts = cell_id.split("_")
            if len(parts) >= 3:
                try:
                    tile_x = int(parts[1]) if tile_x is None else tile_x
                    tile_y = int(parts[2]) if tile_y is None else tile_y
                except ValueError:
                    pass

        try:
            _timing = os.environ.get("CADASTRAL_TIMING", "").lower() in ("1", "true", "yes")
            t0 = time.perf_counter() if _timing else None

            raw = fetch_cell_data(
                cell,
                delay_before=is_first_in_session or use_subsequent_delay,
                delay_subsequent=use_subsequent_delay and not is_first_in_session,
                session=session,
            )
            raw_size = len(raw) if raw else 0
            t_fetch = (time.perf_counter() - t0) * 1000 if _timing else None

            parcels = parse_mvt_tile(raw, zoom, tile_x or 0, tile_y or 0, source_cell_id=cell_id)
            t_total = (time.perf_counter() - t0) * 1000 if _timing else None

            if _timing and t_total is not None:
                _log_loop(f"[Cadastral] Тайл {cell_id}: fetch={t_fetch:.0f}ms, total={t_total:.0f}ms, parcels={len(parcels) if parcels else 0}")

            if parcels:
                n = parcels_repo.upsert_many(parcels, source_cell_id=cell_id)
                with state_lock:
                    parcels_saved += n
                    cells_processed += 1
                _log_loop(f"[Cadastral] +{n} ділянок | Всього: {parcels_repo.count_total_estimated()}")
                progress_repo.mark_cell_done(cell_id, parcels_count=n)
            else:
                n = 0
                with state_lock:
                    cells_processed += 1
                if raw_size == 0:
                    _log_loop(f"[Cadastral] 0 ділянок (тайл порожній або помилка завантаження)")
                else:
                    _log_loop(f"[Cadastral] 0 ділянок (raw={raw_size} b, parsed=0)")
                    _diagnose_parse_failure(raw, zoom, tile_x or 0, tile_y or 0, _log_loop)
                progress_repo.mark_cell_done(cell_id, parcels_count=n)

            if pbar is not None:
                pbar.update(1)
                pbar.set_postfix(ділянок=parcels_repo.count_total_estimated(), за_сесію=parcels_saved)
            _report_progress(cells_processed, parcels_saved, cell_id)

        except Exception as e:
            progress_repo.mark_cell_error(cell_id, str(e))
            with state_lock:
                errors += 1
            if pbar is not None:
                pbar.update(1)
                pbar.set_postfix(ділянок=parcels_repo.count_total_estimated(), за_сесію=parcels_saved)
            _log_loop(f"[Cadastral] Помилка комірки {cell_id}: {e}")
            _report_progress(cells_processed, parcels_saved, cell_id)

    def _worker(worker_id: int) -> int:
        """Потік-воркер: бере комірки з черги, обробляє. Повертає кількість оброблених."""
        processed = 0
        # Кожен воркер має свою HTTP-сесію (requests.Session не thread-safe)
        http_session = get_session()
        # Затримка тільки для першого запиту воркера — rate limiting без надмірного уповільнення
        first_in_session = True
        while True:
            if stop_flag and stop_flag():
                break
            if max_cells is not None:
                with state_lock:
                    if cells_processed + errors >= max_cells:
                        break
            cell = progress_repo.get_next_pending_cell()
            if not cell:
                break
            _process_one_cell(
                cell,
                is_first_in_session=first_in_session,
                session=http_session,
                use_subsequent_delay=(workers > 1),
            )
            first_in_session = False
            processed += 1
        return processed

    if workers <= 1:
        # Послідовний режим: одна HTTP-сесія для повторного використання з'єднання
        http_session = get_session()
        while True:
            if stop_flag and stop_flag():
                log("[Cadastral] Отримано сигнал зупинки.")
                break
            if max_cells is not None and cells_processed + errors >= max_cells:
                log(f"[Cadastral] Досягнуто ліміт {max_cells} комірок.")
                break
            cell = progress_repo.get_next_pending_cell()
            if not cell:
                log("[Cadastral] Немає комірок для обробки.")
                break
            total_parcels_now = parcels_repo.count_total_estimated()
            done_now = cells_done + cells_processed
            if pbar is None:
                pct = f" ({100 * done_now // max(1, cells_total)}%)" if cells_total else ""
                log(f"[Cadastral] Тайл {cells_processed + 1}/{cells_total or '?'}{pct} | Ділянок: {total_parcels_now} | {cell.get('cell_id', '')}")
            _process_one_cell(
                cell,
                is_first_in_session=(cells_processed + errors == 0),
                session=http_session,
                use_subsequent_delay=False,
            )
    else:
        # Паралельний режим
        log(f"[Cadastral] Запуск {workers} потоків.")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_worker, i) for i in range(workers)]
            for _ in as_completed(futures):
                pass
        if max_cells is not None and cells_processed + errors >= max_cells:
            log(f"[Cadastral] Досягнуто ліміт {max_cells} комірок.")

    if pbar is not None:
        pbar.close()

    total_parcels = parcels_repo.count_total_estimated()
    msg = f"Оброблено комірок: {cells_processed}, збережено ділянок: {parcels_saved}, всього в БД: {total_parcels}"
    if errors:
        msg += f", помилок: {errors}"

    return {
        "success": True,
        "cells_processed": cells_processed,
        "parcels_saved": parcels_saved,
        "total_parcels": total_parcels,
        "errors": errors,
        "message": msg,
    }


def main() -> None:
    """Точка входу для запуску з командного рядка."""
    import argparse
    parser = argparse.ArgumentParser(description="Скрапер кадастрової карти kadastrova-karta.com")
    parser.add_argument("--max-cells", type=int, default=None, help="Максимум комірок за запуск")
    parser.add_argument("--workers", type=int, default=5, help="Кількість паралельних потоків")
    args = parser.parse_args()
    os.chdir(_PROJECT_ROOT)
    result = run_cadastral_scraper(max_cells=args.max_cells, workers=args.workers)
    print(result.get("message", "Готово."))


if __name__ == "__main__":
    main()
