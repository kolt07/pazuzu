# -*- coding: utf-8 -*-
"""
Тест завантаження кількох сторінок оголошень OLX через браузер і перевірка даних з detail.

Завантажує одну сторінку списку, бере перші N URL оголошень, відкриває їх через
BrowserPageFetcher.get_detail_page(), парсить parse_detail_page() і виводить зведення:
наявність опису, довжина, ціна, параметри, локація. Повний detail зберігається в JSON для перегляду.

Запуск з кореня проекту:
  py scripts/olx_scraper/test_detail_download.py [--count 3]
  py scripts/olx_scraper/test_detail_download.py --urls "https://www.olx.ua/d/uk/obyavlenie/..." "https://..."
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.olx_scraper import config as scraper_config
from scripts.olx_scraper.fetcher import fetch_page
from scripts.olx_scraper.parser import parse_listings_page, parse_detail_page, get_detail_page_inactive_reason


def _save_first_html(html: str, url: str, out_dir: Path, filename: str = "test_detail_first_page.html") -> None:
    """Зберігає raw HTML для відлагодження."""
    out_file = out_dir / filename
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  [test] Raw HTML збережено ({len(html)} байт): {out_file}", flush=True)


def _print_detail_summary(detail_data: dict, url: str, raw_html: Optional[str] = None) -> None:
    """Виводить зведення по detail: опис, ціна, параметри, локація. raw_html — для діагностики _inactive."""
    desc = detail_data.get("description") or ""
    desc_len = len(desc)
    price_text = detail_data.get("price_text")
    price_value = detail_data.get("price_value")
    params = detail_data.get("parameters") or []
    location = detail_data.get("location")
    contact = detail_data.get("contact")
    inactive = detail_data.get("_inactive")
    print(f"  _inactive: {bool(inactive)}", flush=True)
    if inactive and raw_html:
        reason = get_detail_page_inactive_reason(raw_html)
        if reason:
            print(f"  inactive_reason: {reason}", flush=True)
    print(f"  description: {desc_len} символів", flush=True)
    if desc:
        preview = desc.strip()[:400].replace("\n", " ")
        print(f"  preview: {preview}...", flush=True)
    else:
        print("  preview: (немає)", flush=True)
    print(f"  price_text: {price_text!r}", flush=True)
    print(f"  price_value: {price_value}", flush=True)
    print(f"  parameters: {len(params)} шт.", flush=True)
    if params:
        for p in params[:5]:
            lbl = (p or {}).get("label", "")
            val = (p or {}).get("value", "")
            print(f"    - {lbl}: {val[:60]}", flush=True)
    print(f"  location: {location!r}", flush=True)
    print(f"  contact (phones): {(contact or {}).get('phones', [])}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test OLX detail page download via browser")
    parser.add_argument("--count", type=int, default=3, help="Скільки detail-сторінок завантажити (1–5), якщо не задано --urls")
    parser.add_argument("--urls", nargs="+", metavar="URL", help="Конкретні URL оголошень для тесту (без завантаження списку)")
    parser.add_argument("--no-browser", action="store_true", help="Використати requests замість браузера для detail")
    parser.add_argument("--dump-html", action="store_true", help="Зберегти raw HTML першої detail-сторінки в output для перегляду")
    args = parser.parse_args()

    if args.urls:
        urls = [u.strip() for u in args.urls if u and u.strip()]
        print(f"[test] Задано {len(urls)} URL для тесту", flush=True)
    else:
        count = max(1, min(5, args.count))
        list_url = scraper_config.get_commercial_real_estate_list_url(page=1, region_slug=None)
        print(f"[test] Список: {list_url[:70]}...", flush=True)
        resp = fetch_page(list_url, delay_before=True)
        html = resp.text
        listings = parse_listings_page(html, use_llm=False)
        print(f"[test] Оголошень на сторінці: {len(listings)}", flush=True)
        if not listings:
            print("[test] Немає оголошень для тесту.", flush=True)
            return
        urls = []
        for item in listings:
            u = (item or {}).get("url")
            if u and u.strip():
                urls.append(u.strip())
                if len(urls) >= count:
                    break
        print(f"[test] Будемо завантажувати detail для {len(urls)} URL", flush=True)

    if not urls:
        print("[test] Немає URL для тесту.", flush=True)
        return

    out_dir = Path(__file__).resolve().parent / scraper_config.OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    results = []
    if args.no_browser:
        from scripts.olx_scraper.run_update import _fetch_detail_page
        for i, url in enumerate(urls, start=1):
            print(f"\n--- Detail {i}/{len(urls)}: {url[:60]}...", flush=True)
            detail_resp = _fetch_detail_page(url, session=None, log_fn=print)
            detail_html = detail_resp.text
            detail_data = parse_detail_page(detail_html)
            _print_detail_summary(detail_data, url, detail_html)
            results.append({"url": url, "detail": detail_data, "html_length": len(detail_html)})
            if args.dump_html and i == 1:
                _save_first_html(detail_html, url, out_dir)
            if detail_data.get("_inactive") and i == 1:
                _save_first_html(detail_html, url, out_dir, "test_detail_inactive_sample.html")
    else:
        from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher
        with BrowserPageFetcher(headless=True, log_fn=lambda s: print(s, flush=True)) as browser:
            for i, url in enumerate(urls, start=1):
                print(f"\n--- Detail {i}/{len(urls)}: {url[:60]}...", flush=True)
                detail_result = browser.get_detail_page(url)
                detail_html = detail_result.text
                detail_data = parse_detail_page(detail_html)
                _print_detail_summary(detail_data, url, detail_html)
                results.append({"url": url, "detail": detail_data, "html_length": len(detail_html)})
                if args.dump_html and i == 1:
                    _save_first_html(detail_html, url, out_dir)
                if detail_data.get("_inactive") and i == 1:
                    _save_first_html(detail_html, url, out_dir, "test_detail_inactive_sample.html")

    # Зберегти повний JSON у output скрапера
    out_file = out_dir / "test_detail_download_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n[test] Повний detail збережено: {out_file}", flush=True)


if __name__ == "__main__":
    main()
