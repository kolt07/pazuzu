# -*- coding: utf-8 -*-
"""
Допоміжний скрипт: аналіз структури сторінок OLX за допомогою Gemini.

Мета:
- Взяти кілька URL оголошень OLX з raw_olx_listings (або зі списку),
- Завантажити їх HTML (через fetch_page),
- Передати HTML у Gemini з проханням запропонувати оновлені CSS-селектори
  для опису, параметрів, локації тощо,
- Вивести пропозиції у вигляді YAML-конфігу для config/olx_detail_selectors.yaml.

Цей скрипт НІЧОГО не змінює автоматично у проекті — лише друкує пропозиції.
Адміністратор може відредагувати/вставити результат вручну.

Запуск (з кореня проекту):
  py scripts/olx_scraper/analyze_structure_with_gemini.py --limit 3
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from business.services.llm_service import LLMService
from scripts.olx_scraper.fetcher import fetch_page


PROMPT_TEMPLATE = """
You are helping to maintain an OLX scraper for real-estate listings.

We parse the OLX detail page HTML with BeautifulSoup and CSS selectors.
Your task: analyse the HTML snippets and propose robust CSS selectors for three fields, and return ONLY JSON.

Return exactly one JSON object with this structure (no markdown, no prose, no comments):
{{
  "description_selectors": ["css1", "css2"],
  "parameters_selectors": ["css_for_params"],
  "location_selectors": ["css_for_location"]
}}

Fields:
- description_selectors: CSS selectors (in priority order) for the LONG body text of the listing.
- parameters_selectors: CSS selectors for the container(s) where label/value parameters (area, floor, cadastral number, etc.) live.
- location_selectors: CSS selectors for the block that contains city/region and "Місцезнаходження".

Rules:
- Use selectors suitable for BeautifulSoup .select_one / .select (e.g. [data-testid="ad-description"], [data-cy="ad_description_content"]).
- Prefer data-testid / data-cy attributes if present.
- Do not use long structural chains like :nth-child; use stable attributes/classes/ids instead.
- The response MUST be valid JSON and MUST NOT contain markdown code fences or any free-form explanation.

HTML samples (trimmed):
{html_samples}
"""


def collect_sample_html(limit: int) -> List[str]:
    repo = RawOlxListingsRepository()
    docs = repo.get_recent_for_health_check(days=7, limit=limit)
    htmls: List[str] = []
    for d in docs:
        url = d.get("url")
        if not url:
            continue
        try:
            resp = fetch_page(url, delay_before=False)
            text = resp.text or ""
            if text:
                # Обрізаємо до ~25k символів, щоб не роздувати промпт
                htmls.append(text[:25000])
        except Exception as e:
            print(f"Помилка завантаження {url[:80]}: {e}", file=sys.stderr)
    return htmls


def main() -> None:
    parser = argparse.ArgumentParser(description="Аналіз структури сторінок OLX через Gemini")
    parser.add_argument("--limit", type=int, default=3, help="Кількість оголошень для вибірки (HTML з detail-сторінок)")
    args = parser.parse_args()

    settings = Settings()
    MongoDBConnection.initialize(settings)
    llm = LLMService(settings)

    samples = collect_sample_html(limit=args.limit)
    if not samples:
        print("Не вдалося зібрати жодного HTML для аналізу. Перевірте raw_olx_listings та доступ до OLX.")
        return

    joined = "\n\n----- HTML SAMPLE SEPARATOR -----\n\n".join(samples)
    prompt = PROMPT_TEMPLATE.format(html_samples=joined[:60000])

    print("Надсилаю запит до Gemini для аналізу структури OLX...\n", file=sys.stderr)
    raw = llm.generate_text(
        prompt=prompt,
        system_prompt=(
            "You are a senior scraping engineer. "
            "Always return ONLY one JSON object, no markdown, no prose."
        ),
        temperature=0.0,
        _caller="olx_scraper.analyze_structure_with_gemini",
    )

    if not raw:
        print("Gemini повернув порожню відповідь або сталася помилка.", file=sys.stderr)
        return

    try:
        data: Dict[str, Any] = json.loads(raw)
    except Exception as e:
        print("Не вдалося розпарсити JSON від Gemini. Сира відповідь нижче.\n", file=sys.stderr)
        print(str(e), file=sys.stderr)
        print(raw.strip())
        return

    desc_selectors = data.get("description_selectors") or []
    params_selectors = data.get("parameters_selectors") or []
    loc_selectors = data.get("location_selectors") or []

    if not isinstance(desc_selectors, list):
        desc_selectors = []
    if not isinstance(params_selectors, list):
        params_selectors = []
    if not isinstance(loc_selectors, list):
        loc_selectors = []

    # Будуємо YAML-конфіг на основі JSON від Gemini
    try:
        import yaml
    except ImportError:
        print("Модуль yaml не встановлено. Виводжу JSON як є:\n")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return

    config: Dict[str, Any] = {
        "version": 1,
        "description": {
            "selectors": desc_selectors,
            "min_length": 50,
            "max_length": 50000,
        },
        "parameters": {
            "selectors": params_selectors,
        },
        "location": {
            "selectors": loc_selectors,
        },
    }

    yaml_text = yaml.safe_dump(config, allow_unicode=True, sort_keys=False)
    print("# Пропозиція конфігу olx_detail_selectors.yaml від Gemini (через JSON → YAML):\n")
    print(yaml_text.strip())


if __name__ == "__main__":
    main()

