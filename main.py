# -*- coding: utf-8 -*-
"""
Головний клас застосунку - вхідна точка програми.
"""

import argparse
import logging
import threading
import subprocess
import sys
from pathlib import Path
from config.settings import Settings

# Налаштування логування для всього проекту
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Зменшуємо рівень логування для сторонніх бібліотек, щоб не засмічувати вивід
logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
from business.services import ProZorroService
from business.services.telegram_bot_service import TelegramBotService
from business.services.logging_service import LoggingService
from data.database.connection import MongoDBConnection


class Application:
    """Головний клас застосунку."""

    def __init__(self):
        """Ініціалізація застосунку."""
        self._running = False
        self.settings = Settings()
        self.prozorro_service = None
        self.telegram_bot_service = None
        self._bot_thread = None
        self._mcp_processes = []
        
        # Ініціалізуємо MongoDB підключення ПЕРЕД створенням сервісів, які його використовують
        try:
            MongoDBConnection.initialize(self.settings)
        except Exception as e:
            print(f"Попередження: не вдалося ініціалізувати MongoDB: {e}")
        
        # Тепер створюємо сервіси після ініціалізації MongoDB
        self.logging_service = LoggingService()
        
        # Логуємо старт застосунку
        try:
            self.logging_service.log_app_event(
                message="Застосунок запущено",
                event_type='start'
            )
        except Exception as e:
            print(f"Попередження: не вдалося залогувати подію старту: {e}")

    def initialize(self):
        """Ініціалізація компонентів застосунку."""
        self.prozorro_service = ProZorroService(self.settings)

    def run(
        self,
        days: int = None,
    ):
        """
        Запуск застосунку.
        
        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
        """
        if self._running:
            return

        self.initialize()
        self._running = True
        print("Застосунок запущено")
        
        # Отримання та збереження аукціонів про нерухомість
        self.fetch_real_estate_auctions(
            days=days,
        )

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
        
        result = self.prozorro_service.fetch_and_save_real_estate_auctions(
            days=days,
        )
        
        if result['success']:
            print(f"✓ {result['message']}")
            if result['file_path']:
                print(f"  Файл: {result['file_path']}")
        else:
            print(f"✗ Помилка: {result['message']}")

    def run_telegram_bot(self):
        """Запускає Telegram бота у фоновому потоці."""
        if not self.settings.telegram_bot_token:
            print("Помилка: Telegram bot token не вказано в налаштуваннях")
            return
        
        try:
            self.telegram_bot_service = TelegramBotService(self.settings)
            self._bot_thread = threading.Thread(target=self.telegram_bot_service.run, daemon=True)
            self._bot_thread.start()
            print("Telegram бот запущено у фоновому потоці")
        except Exception as e:
            print(f"Помилка запуску Telegram бота: {e}")
    
    def start_mcp_servers(self):
        """Запускає всі MCP сервери у фоновому режимі."""
        project_root = Path(__file__).parent
        mcp_servers = [
            'mcp_servers.schema_mcp_server',
            'mcp_servers.query_builder_mcp_server',
            'mcp_servers.analytics_mcp_server',
            'mcp_servers.report_mcp_server'
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

