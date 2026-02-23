# -*- coding: utf-8 -*-
"""
Обгортка для запуску скрапера кадастру з гарантованим контекстом.
Встановлює робочу директорію = корінь проекту, потім викликає run_scraper.
Запускайте: py scripts/run_cadastral_from_project_root.py [--max-cells N] [--workers W]
"""
import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(_PROJECT_ROOT)
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.cadastral_scraper.run_scraper import run_cadastral_scraper

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Скрапер кадастру (з кореня проекту)")
    parser.add_argument("--max-cells", type=int, default=None)
    parser.add_argument("--workers", type=int, default=5, help="Кількість паралельних потоків")
    args = parser.parse_args()
    result = run_cadastral_scraper(max_cells=args.max_cells, workers=args.workers)
    print(result.get("message", "Готово."))
