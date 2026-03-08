# -*- coding: utf-8 -*-
"""
Тест фільтрації землі через URL OLX (без с/г призначення).
Запуск: py scripts/olx_scraper/test_land_flow.py
"""

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from scripts.olx_scraper import config as scraper_config
from scripts.olx_scraper.run_update import _get_base_categories


def test_land_urls():
    """Перевірка, що земля завантажується через окремі URL типу (без с/г)."""
    cats = _get_base_categories()
    land_cats = [c for c in cats if "Земля" in c.get("label", "")]
    assert len(land_cats) >= 7, f"Очікувалось >=7 категорій землі, отримано: {len(land_cats)}"
    land_slugs = scraper_config.get_olx_land_type_slugs()
    assert "zemlya-slskogospodarskogo-priznachennya" not in land_slugs.values(), "С/г не має бути в списку"
    for cat in land_cats[:2]:
        url = cat["get_list_url"](1)
        assert "prodazha-zemli" in url, f"URL має містити prodazha-zemli: {url}"
    print("OK: земля завантажується через фільтри OLX (7 типів без с/г)")


if __name__ == "__main__":
    test_land_urls()
    print("Тест пройдено.")
