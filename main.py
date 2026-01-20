# -*- coding: utf-8 -*-
"""
Головний клас застосунку - вхідна точка програми.
"""

import argparse
import threading
from config.settings import Settings
from business.services import ProZorroService
from business.services.telegram_bot_service import TelegramBotService


class Application:
    """Головний клас застосунку."""

    def __init__(self):
        """Ініціалізація застосунку."""
        self._running = False
        self.settings = Settings()
        self.prozorro_service = None
        self.telegram_bot_service = None
        self._bot_thread = None

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
    
    def stop(self):
        """Зупинка застосунку."""
        if not self._running:
            return

        self._running = False
        
        # Зупиняємо Telegram бота
        if self.telegram_bot_service:
            self.telegram_bot_service.stop()
        
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

    args = parser.parse_args()

    if args.days is not None and args.days <= 0:
        print("Помилка: --days повинно бути більше 0")
        return Application()

    app = Application()
    
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

