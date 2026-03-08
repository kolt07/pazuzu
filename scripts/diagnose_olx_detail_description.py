# -*- coding: utf-8 -*-
"""
Діагностика: порівняння отримання сторінки оголошення OLX двома способами
(HTTP requests vs браузер) та перевірка, чи парсер витягує повний опис.

Запуск:
  py scripts/diagnose_olx_detail_description.py "https://www.olx.ua/d/uk/obyavlenie/..."
  py scripts/diagnose_olx_detail_description.py --url "https://..."

Після запуску:
- виводить довжину HTML, результат parse_detail_page (довжина description, параметри);
- порівнює, чи потрапляє повний опис у текст для LLM;
- опційно зберігає HTML у scripts/temp/olx_detail_diagnostic/ для ручного огляду.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Діагностика опису OLX: requests vs browser")
    parser.add_argument("url", nargs="?", help="URL сторінки оголошення OLX")
    parser.add_argument("--url", dest="url_opt", help="URL (альтернатива позиційному аргументу)")
    parser.add_argument("--save-html", action="store_true", help="Зберегти HTML обох варіантів у temp")
    parser.add_argument("--no-browser", action="store_true", help="Не запускати браузер, тільки requests")
    args = parser.parse_args()
    url = args.url or args.url_opt
    if not url or not url.strip().startswith("http"):
        print("Вкажіть URL сторінки оголошення OLX.", file=sys.stderr)
        sys.exit(1)
    url = url.strip()

    from scripts.olx_scraper.parser import parse_detail_page

    out_dir = None
    if args.save_html:
        out_dir = project_root / "scripts" / "temp" / "olx_detail_diagnostic"
        out_dir.mkdir(parents=True, exist_ok=True)

    def _report(name: str, html: str, detail: dict) -> None:
        desc = (detail.get("description") or "").strip()
        params = detail.get("parameters") or []
        print(f"\n--- {name} ---")
        print(f"  HTML length: {len(html)} chars")
        print(f"  description length: {len(desc)} chars")
        print(f"  parameters count: {len(params)}")
        if desc:
            preview = desc[:400] + "..." if len(desc) > 400 else desc
            print(f"  description preview:\n    {preview!r}")
        else:
            print("  description: (порожній — буде використано raw_snippet з картки)")
        if out_dir:
            fpath = out_dir / f"{name.replace(' ', '_')}.html"
            fpath.write_text(html, encoding="utf-8")
            print(f"  HTML saved: {fpath}")

    # 1) Requests
    print("Завантаження через requests...")
    try:
        from scripts.olx_scraper.fetcher import fetch_page

        resp = fetch_page(url, delay_before=True, delay_after=False, is_detail=True)
        html_requests = resp.text if hasattr(resp, "text") else ""
        detail_requests = parse_detail_page(html_requests)
        _report("requests", html_requests, detail_requests)
    except Exception as e:
        print(f"  Помилка requests: {e}")
        html_requests = ""
        detail_requests = {}

    # 2) Browser
    if not args.no_browser:
        print("\nЗавантаження через браузер (Playwright)...")
        try:
            from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher

            with BrowserPageFetcher(headless=True, log_fn=print) as fetcher:
                result = fetcher.get_detail_page(url)
                html_browser = result.text
                detail_browser = parse_detail_page(html_browser)
                _report("browser", html_browser, detail_browser)
        except Exception as e:
            print(f"  Помилка browser: {e}")
            import traceback

            traceback.print_exc()
    else:
        html_browser = ""
        detail_browser = {}

    # Текст для LLM (як у RealEstateObjectsService._build_description_from_olx): тільки detail
    def build_llm_text(detail: dict, source_label: str) -> str:
        parts = []
        params = detail.get("parameters") or []
        desc = (detail.get("description") or "").strip()
        if desc:
            parts.append("Повний опис оголошення:")
            parts.append(desc)
        else:
            parts.append("Текст з картки оголошення (опис сторінки відсутній): [raw_snippet з search_data]")
        for p in params:
            if isinstance(p, dict) and (p.get("label") or p.get("value")):
                parts.append(f"- {p.get('label', '')}: {p.get('value', '')}")
        return "\n".join(parts)

    llm_req = build_llm_text(detail_requests, "requests") if detail_requests and not detail_requests.get("_inactive") else ""
    llm_br = build_llm_text(detail_browser, "browser") if detail_browser and not detail_browser.get("_inactive") else ""
    print("\n--- Текст для LLM (фрагмент) ---")
    print("  requests:", (llm_req[:300] + "..." if len(llm_req) > 300 else llm_req or "(немає)"))
    if llm_br:
        print("  browser: ", (llm_br[:300] + "..." if len(llm_br) > 300 else llm_br))
    print(f"  Довжина тексту для LLM: requests={len(llm_req)}, browser={len(llm_br)}")

    # Порівняння
    desc_req = (detail_requests.get("description") or "").strip()
    desc_br = (detail_browser.get("description") or "").strip() if detail_browser else ""
    print("\n--- Висновок ---")
    if desc_req or desc_br:
        if len(desc_br) > len(desc_req):
            print(f"  Браузер дав довший опис (+{len(desc_br) - len(desc_req)} символів).")
        elif len(desc_req) > len(desc_br):
            print(f"  Requests дав довший опис (+{len(desc_req) - len(desc_br)} символів).")
        else:
            print("  Довжини опису збігаються.")
    else:
        print("  У обох варіантів опис порожній — у LLM потраплятиме лише текст з картки (raw_snippet).")
        print("  Можливі причини: інший селектор опису на OLX, опис підвантажується JS пізніше, або min_length у конфігу відсікає короткий текст.")


if __name__ == "__main__":
    main()
