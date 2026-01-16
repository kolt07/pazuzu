# -*- coding: utf-8 -*-
"""
Головний клас застосунку - вхідна точка програми.
"""

import argparse
from config.settings import Settings
from business.services import ProZorroService


class Application:
    """Головний клас застосунку."""

    def __init__(self):
        """Ініціалізація застосунку."""
        self._running = False
        self.settings = Settings()
        self.prozorro_service = None

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

    def stop(self):
        """Зупинка застосунку."""
        if not self._running:
            return

        self._running = False
        print("Застосунок зупинено")

    @property
    def is_running(self):
        """Перевірка, чи працює застосунок."""
        return self._running


def main():
    """Головна функція для запуску застосунку."""
    parser = argparse.ArgumentParser(
        prog="prozzorro-parser",
        description="Отримання аукціонів з ProZorro.Sale та збереження у JSON (temp/).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Кількість днів для виборки (за замовчуванням береться з налаштувань).",
    )

    args = parser.parse_args()

    if args.days is not None and args.days <= 0:
        print("Помилка: --days повинно бути більше 0")
        return Application()

    app = Application()
    app.run(
        days=args.days,
    )
    return app


if __name__ == "__main__":
    main()

