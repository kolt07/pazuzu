# -*- coding: utf-8 -*-
"""
Створює індекси для колекції flx_strategy_registry (FLX Strategy Engine).
Запуск: py scripts/migrations/ensure_flx_strategy_registry_indexes.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.repositories.strategy_registry_repository import StrategyRegistryRepository  # noqa: E402


def main() -> None:
    repo = StrategyRegistryRepository()
    repo.ensure_indexes()
    print("OK: flx_strategy_registry indexes ensured.", flush=True)


if __name__ == "__main__":
    main()
