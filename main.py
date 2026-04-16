# -*- coding: utf-8 -*-
"""
Головний клас застосунку - вхідна точка програми.
"""

import argparse
import logging
import subprocess
import sys
import threading
import time
from pathlib import Path
from config.settings import Settings

# Фільтр: приховує інформаційні повідомлення сторонніх бібліотек (наприклад AFC / max remote calls від Gemini)
class _SuppressAfcInfoFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = (record.getMessage() or "").strip()
        if "AFC" in msg and "max remote calls" in msg.lower():
            return False
        return True


# Налаштування логування для всього проекту
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
for h in logging.root.handlers:
    h.addFilter(_SuppressAfcInfoFilter())

# Зменшуємо рівень логування для сторонніх бібліотек, щоб не засмічувати вивід
logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
# Google GenAI / LangChain: приховуємо інфо (наприклад "AFC is enabled with max remote calls: 10")
logging.getLogger('google').setLevel(logging.WARNING)
logging.getLogger('google.genai').setLevel(logging.WARNING)
logging.getLogger('langchain_google_genai').setLevel(logging.WARNING)
from business.services import ProZorroService
from business.services.telegram_bot_service import TelegramBotService
from business.services.scheduler_service import SchedulerService, TelegramSchedulerNotifier
from business.services.source_data_load_service import run_full_pipeline
from business.services.logging_service import LoggingService
from business.services.vast_runtime_supervisor_service import VastRuntimeSupervisorService
from data.database.connection import MongoDBConnection
from business.services.currency_rate_service import CurrencyRateService


class Application:
    """Головний клас застосунку."""

    def __init__(self):
        """Ініціалізація застосунку."""
        self._running = False
        # Перевірка та міграція конфігу перед завантаженням налаштувань
        try:
            from config.config_migration_runner import run_if_needed
            run_if_needed()
        except Exception as e:
            print(f"Попередження: перевірка конфігу — {e}")
        self.settings = Settings()
        self.prozorro_service = None
        self.telegram_bot_service = None
        self._bot_thread = None
        self._background_update_thread = None
        self._mcp_processes = []
        self.scheduler_service = None
        self.currency_rate_service = None
        self.runtime_supervisor_service = None
        
        # Ініціалізуємо MongoDB підключення ПЕРЕД створенням сервісів, які його використовують
        try:
            MongoDBConnection.initialize(self.settings)
        except Exception as e:
            print(f"Попередження: не вдалося ініціалізувати MongoDB: {e}")
        
        # Тепер створюємо сервіси після ініціалізації MongoDB
        self.logging_service = LoggingService()
        try:
            self.currency_rate_service = CurrencyRateService(self.settings)
        except Exception as e:
            print(f"Попередження: не вдалося ініціалізувати сервіс курсів валют: {e}")
        
        # Логуємо старт застосунку
        try:
            self.logging_service.log_app_event(
                message="Застосунок запущено",
                event_type='start'
            )
        except Exception as e:
            print(f"Попередження: не вдалося залогувати подію старту: {e}")
        
        # Фонове оновлення курсу USD, якщо на сьогодні ще немає запису
        if self.currency_rate_service is not None:
            try:
                threading.Thread(
                    target=self.currency_rate_service.ensure_today_usd_rate_if_missing,
                    daemon=True,
                    name="UsdRateInit",
                ).start()
            except Exception as e:
                print(f"Попередження: не вдалося запустити фонове оновлення курсу USD: {e}")

    def initialize(self):
        """Ініціалізація компонентів застосунку."""
        self.prozorro_service = ProZorroService(self.settings)

    def _notify_admins_sync(self, message: str) -> bool:
        """Best-effort сповіщення адміністраторів через Telegram бота."""
        if not self.telegram_bot_service:
            return False
        try:
            return bool(self.telegram_bot_service.notify_admins_sync(message))
        except Exception:
            return False

    def _start_runtime_supervisor(self) -> None:
        if self.runtime_supervisor_service is not None:
            return
        try:
            self.runtime_supervisor_service = VastRuntimeSupervisorService(
                self.settings,
                notify_admins_fn=self._notify_admins_sync,
            )
            self.runtime_supervisor_service.start()
        except Exception as e:
            print(f"Попередження: не вдалося запустити runtime supervisor: {e}")

    def run(
        self,
        days: int = None,
    ):
        """
        Запуск застосунку.
        Pipeline: Phase 1 — сирі дані в raw-колекції (без LLM), Phase 2 — promote + LLM для обраних, Phase 3 — аналітика.

        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
        """
        if self._running:
            return

        self.initialize()
        self._running = True
        self._start_runtime_supervisor()
        print("Застосунок запущено")

        # Єдиний pipeline: raw → main + LLM для обраних → аналітика (без LLM на етапі збору сирих даних)
        result = run_full_pipeline(
            settings=self.settings,
            sources=["olx", "prozorro"],
            days=days,
        )
        if result.get("phase1"):
            p1 = result["phase1"]
            if p1.get("olx"):
                print(f"✓ OLX raw: {p1['olx'].get('total_listings', 0)} оголошень, {len(p1['olx'].get('loaded_urls') or [])} завантажено/оновлено")
            if p1.get("prozorro"):
                print(f"✓ ProZorro raw: {p1['prozorro'].get('count', 0)} аукціонів")
        if result.get("phase2"):
            p2 = result["phase2"]
            print(f"✓ Phase 2: OLX LLM — {p2.get('olx_llm_processed', 0)}, ProZorro LLM — {p2.get('prozorro_llm_processed', 0)}")

        try:
            from business.services.domain_cache_service import invalidate_domain_caches
            invalidate_domain_caches(["olx", "prozorro"])
        except Exception as e:
            print(f"  Попередження: інвалідація кешів — {e}")

    def fetch_real_estate_auctions(
        self,
        days: int = None,
    ):
        """
        Отримує та зберігає список аукціонів про нерухомість за останні N днів.
        
        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
        """
        if not self.prozorro_service:
            print("Помилка: сервіс ProZorro не ініціалізовано")
            return

        # Простий прогрес у терміналі для LLM-обробки ProZorro
        start_ts = time.time()
        last_print_ts = 0.0

        def _cli_progress(p: dict) -> None:
            nonlocal last_print_ts
            now = time.time()
            # Обмежуємося оновленням раз на ~1 сек, щоб не засмічувати вивід
            if now - last_print_ts < 1.0:
                return
            last_print_ts = now
            current = p.get("current") or 0
            total = p.get("total") or 0
            msg = p.get("message") or ""
            elapsed = now - start_ts
            rate_txt = ""
            if elapsed > 1 and current > 0:
                per_min = current / (elapsed / 60.0)
                rate_txt = f" | ~{per_min:.1f} об/хв"
            if total:
                print(f"[ProZorro LLM] {current}/{total}{rate_txt} — {msg}", flush=True)
            else:
                print(f"[ProZorro LLM] {msg}", flush=True)

        result = self.prozorro_service.fetch_and_save_real_estate_auctions(
            days=days,
            progress_callback=_cli_progress,
        )
        if result['success']:
            print(f"✓ {result['message']}")
            if result['file_path']:
                print(f"  Файл: {result['file_path']}")
            try:
                from business.services.collection_knowledge_service import refresh_knowledge_after_sources
                refresh_knowledge_after_sources(["prozorro"])
            except Exception as e:
                print(f"  Попередження: оновлення знань про колекції — {e}")
            try:
                from business.services.domain_cache_service import invalidate_domain_caches
                invalidate_domain_caches(["prozorro"])
            except Exception as e:
                print(f"  Попередження: інвалідація кешів домен-шару — {e}")
        else:
            print(f"✗ Помилка: {result['message']}")

    def run_olx_data_update(self, days: int = None) -> None:
        """Оновлює оголошення OLX через pipeline raw → LLM (Phase 1 без LLM). days=1 або 7 — зупинка по даті."""
        try:
            result = run_full_pipeline(
                settings=self.settings,
                sources=["olx"],
                days=days or 1,
            )
            p1 = result.get("phase1", {}).get("olx", {})
            p2 = result.get("phase2", {})
            print(f"✓ OLX: raw {p1.get('total_listings', 0)} огол., LLM оброблено: {p2.get('olx_llm_processed', 0)}")
            try:
                from business.services.domain_cache_service import invalidate_domain_caches
                invalidate_domain_caches(["olx"])
            except Exception as e:
                print(f"  Попередження: інвалідація кешів — {e}")
        except Exception as e:
            print(f"✗ OLX: {e}")

    def _background_data_update_loop(self) -> None:
        """
        Цикл регламентного фонового оновлення: кожні N хвилин оновлює дані за минулу добу
        (ProZorro + OLX). Працює поки застосунок запущений.
        """
        logger = logging.getLogger(__name__)
        interval_seconds = self.settings.background_update_interval_minutes * 60
        if interval_seconds <= 0:
            return
        while self._running:
            time.sleep(interval_seconds)
            if not self._running:
                break
            try:
                logger.info("Фонове оновлення даних: старт (минула добу, pipeline raw → LLM)")
                run_full_pipeline(
                    settings=self.settings,
                    sources=["olx", "prozorro"],
                    days=1,
                )
                try:
                    from business.services.domain_cache_service import invalidate_domain_caches
                    invalidate_domain_caches(["prozorro", "olx"])
                except Exception as e:
                    logger.debug("Інвалідація кешів домен-шару після фонового оновлення: %s", e)
                logger.info("Фонове оновлення даних: завершено")
            except Exception:
                logger.exception("Фонове оновлення даних: помилка")

    def run_telegram_bot(self):
        """Запускає Telegram бота у фоновому потоці та регламентне оновлення даних."""
        if not self.settings.telegram_bot_token:
            print("Помилка: Telegram bot token не вказано в налаштуваннях")
            return

        self.initialize()
        self._running = True
        self._start_runtime_supervisor()

        # Тимчасово вимкнено: оновлення даних — через меню адміністратора (за добу / за тиждень)
        # if self.settings.background_update_interval_minutes > 0:
        #     self._background_update_thread = threading.Thread(
        #         target=self._background_data_update_loop,
        #         daemon=True,
        #         name="BackgroundDataUpdate",
        #     )
        #     self._background_update_thread.start()
        #     print(
        #         f"Регламентне оновлення даних: кожні {self.settings.background_update_interval_minutes} хв (минула добу)"
        #     )
        
        try:
            self.telegram_bot_service = TelegramBotService(self.settings)
            self._bot_thread = threading.Thread(target=self.telegram_bot_service.run, daemon=True)
            self._bot_thread.start()
            print("Telegram бот запущено у фоновому потоці")
        except Exception as e:
            print(f"Помилка запуску Telegram бота: {e}")

        # Планувальник подій (регламентні звіти, нагадування, оновлення даних — тільки для адмінів)
        try:
            notifier = TelegramSchedulerNotifier(self.telegram_bot_service)
            self.scheduler_service = SchedulerService(self.settings, notifier=notifier)
            self.scheduler_service.set_user_service(self.telegram_bot_service.user_service)
            self.scheduler_service.start()
            print("Планувальник подій запущено")
        except Exception as e:
            print(f"Попередження: не вдалося запустити планувальник подій: {e}")

        # Запуск сервера Telegram Mini App (якщо порт задано)
        mini_app_port = getattr(self.settings, "mini_app_port", 0)
        if mini_app_port > 0:
            try:
                from telegram_mini_app.server import run_server
                self._mini_app_thread = threading.Thread(
                    target=run_server,
                    kwargs={"settings": self.settings, "host": "0.0.0.0", "port": mini_app_port},
                    daemon=True,
                    name="MiniAppServer",
                )
                self._mini_app_thread.start()
                print(f"Telegram Mini App сервер запущено на порту {mini_app_port}")
            except ModuleNotFoundError as e:
                print(f"Попередження: не вдалося запустити Mini App сервер: {e}")
                print("  Встановіть залежності: pip install -r requirements.txt")
            except Exception as e:
                print(f"Попередження: не вдалося запустити Mini App сервер: {e}")
    
    def start_mcp_servers(self):
        """Запускає всі MCP сервери у фоновому режимі."""
        project_root = Path(__file__).parent
        mcp_servers = [
            'mcp_servers.schema_mcp_server',
            'mcp_servers.query_builder_mcp_server',
            'mcp_servers.analytics_mcp_server',
            'mcp_servers.report_mcp_server',
            'mcp_servers.export_mcp_server',
            'mcp_servers.geocoding_mcp_server',
            'mcp_servers.data_update_mcp_server',
        ]
        
        print("Запуск MCP серверів...")
        
        for server_module in mcp_servers:
            try:
                process = subprocess.Popen(
                    [sys.executable, '-m', server_module],
                    cwd=str(project_root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                self._mcp_processes.append((server_module, process))
                print(f"✓ Запущено MCP сервер: {server_module} (PID: {process.pid})")
            except Exception as e:
                print(f"✗ Помилка запуску MCP сервера {server_module}: {e}")
        
        if self._mcp_processes:
            print(f"Запущено {len(self._mcp_processes)} MCP серверів")
    
    def stop(self):
        """Зупинка застосунку."""
        if not self._running:
            return

        self._running = False

        # Зупиняємо планувальник подій
        if self.scheduler_service:
            self.scheduler_service.shutdown(wait=True)
            self.scheduler_service = None

        # Зупиняємо supervisor Vast runtime
        if self.runtime_supervisor_service:
            try:
                self.runtime_supervisor_service.stop()
            except Exception:
                pass
            self.runtime_supervisor_service = None
        
        # Зупиняємо Telegram бота
        if self.telegram_bot_service:
            self.telegram_bot_service.stop()
        
        # Зупиняємо MCP сервери
        if self._mcp_processes:
            print("Зупинка MCP серверів...")
            for server_module, process in self._mcp_processes:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                    print(f"✓ Зупинено MCP сервер: {server_module}")
                except Exception as e:
                    print(f"✗ Помилка зупинки MCP сервера {server_module}: {e}")
                    process.kill()
            self._mcp_processes = []
        
        # Логуємо зупинку
        try:
            self.logging_service.log_app_event(
                message="Застосунок зупинено",
                event_type='stop'
            )
        except:
            pass
        
        print("Застосунок зупинено")

    @property
    def is_running(self):
        """Перевірка, чи працює застосунок."""
        return self._running


def main():
    """Головна функція для запуску застосунку."""
    parser = argparse.ArgumentParser(
        prog="prozzorro-parser",
        description="Telegram бот для роботи з аукціонами ProZorro.Sale.",
    )
    parser.add_argument(
        "--generate-file",
        action="store_true",
        help="Запустити формування файлу (замість Telegram бота).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Кількість днів для виборки (використовується тільки з --generate-file).",
    )
    parser.add_argument(
        "--start-mcp",
        action="store_true",
        help="Запустити MCP сервери при старті застосунку.",
    )

    args = parser.parse_args()

    if args.days is not None and args.days <= 0:
        print("Помилка: --days повинно бути більше 0")
        return Application()

    app = Application()
    
    # Запускаємо MCP сервери, якщо вказано
    if args.start_mcp:
        app.start_mcp_servers()
    
    # Запускаємо формування файлу, якщо вказано
    if args.generate_file:
        app.run(days=args.days)
    else:
        # За замовчуванням запускаємо Telegram бота
        app.run_telegram_bot()
        # Чекаємо на завершення
        try:
            if app._bot_thread:
                app._bot_thread.join()
        except KeyboardInterrupt:
            print("\nОтримано сигнал переривання, зупиняємо застосунок...")
            app.stop()
    
    return app


if __name__ == "__main__":
    main()

