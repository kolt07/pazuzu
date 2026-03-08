# -*- coding: utf-8 -*-
"""
Бенчмарк: отримання структурованої інформації зі сторінки OLX без LLM.

Порівнює інструменти парсингу HTML (BeautifulSoup, lxml, Selectolax, Parsel)
та екстракцію полів (параметри + regex) з результатами LLM.

Запуск:
  py scripts/olx_html_parsing_benchmark.py --limit 50
  py scripts/olx_html_parsing_benchmark.py --limit 500 --cache-html
  py scripts/olx_html_parsing_benchmark.py --limit 100 --parsers bs4,lxml --no-fetch

Параметри:
  --limit N         Кількість оголошень (за замовчуванням 50)
  --cache-html      Зберігати HTML на диск для повторних запусків
  --no-fetch        Не завантажувати HTML (використовувати тільки кеш)
  --parsers X,Y     Список парсерів: bs4, lxml, selectolax, parsel
  --output FILE     Шлях до звіту (за замовчуванням docs/olx_parsing_benchmark_report.md)
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository
from scripts.olx_scraper.parser import parse_detail_page
from utils.listing_regex_extractor import extract_from_description

# Маппінг лейблів параметрів OLX на поля LLM
PARAM_LABEL_MAP = {
    "площа": "building_area_sqm",
    "площа будівлі": "building_area_sqm",
    "площа приміщення": "building_area_sqm",
    "площа ділянки": "land_area_ha",
    "площа земельної ділянки": "land_area_ha",
    "площа землі": "land_area_ha",
    "поверх": "floor",
    "кадастровий номер": "cadastral_number",
    "тип нерухомості": "property_type",
    "призначення": "property_type",
    "призначення ділянки": "property_type",
}

# Площа в м²: "80 м²", "120 кв.м"
_RE_BUILDING_AREA = re.compile(r"([\d\s.,]+)\s*(?:м²|м2|кв\.?\s*м)", re.I)
# Площа в га: "0.5 га", "5 гектар", "10 соток"
_RE_LAND_AREA = re.compile(
    r"([\d\s.,]+)\s*(?:га|гектар|гектарів)|([\d\s.,]+)\s*соток",
    re.I,
)


def _parse_float(value: str) -> Optional[float]:
    """Парсить число з українського формату."""
    if not value:
        return None
    cleaned = str(value).replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_from_params(parameters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Витягує структуровані дані з параметрів (label/value).
    Повертає словник, сумісний з parse_auction_description.
    """
    result: Dict[str, Any] = {
        "cadastral_number": "",
        "building_area_sqm": "",
        "land_area_ha": "",
        "addresses": [],
        "floor": "",
        "property_type": "",
        "utilities": "",
        "tags": [],
        "arrests_info": "",
    }
    params_by_label: Dict[str, str] = {}
    for p in parameters or []:
        label = (p.get("label") or "").strip()
        value = (p.get("value") or "").strip()
        if label:
            params_by_label[label.lower()] = value

    for param_label, val in params_by_label.items():
        field = None
        for known_label, f in sorted(PARAM_LABEL_MAP.items(), key=lambda x: -len(x[0])):
            if known_label in param_label:
                field = f
                break
        if not field or not val:
            continue
        if field == "building_area_sqm":
            m = _RE_BUILDING_AREA.search(val)
            if m:
                f = _parse_float(m.group(1))
                if f and 0.1 < f < 1000000:
                    result[field] = str(f)
        elif field == "land_area_ha":
            m = _RE_LAND_AREA.search(val)
            if m:
                f = _parse_float(m.group(1) or m.group(2))
                if f:
                    if "соток" in val.lower():
                        f *= 0.01
                    if 0.001 < f < 100000:
                        result[field] = str(f)
        elif field == "floor":
            result[field] = val.strip()
        elif field == "cadastral_number":
            result[field] = val.strip()
        elif field == "property_type":
            result[field] = val.strip()

    return result


def _extract_from_location(location: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Конвертує location в формат addresses."""
    if not location or not isinstance(location, dict):
        return []
    addr = {}
    if location.get("city"):
        addr["settlement"] = location["city"]
    if location.get("region"):
        addr["region"] = location["region"]
    if location.get("raw"):
        addr["formatted"] = location["raw"]
    for k in ["region", "district", "settlement_type", "settlement", "street", "building"]:
        if k not in addr:
            addr[k] = ""
    return [addr] if addr else []


def _merge_extractions(
    from_params: Dict[str, Any],
    from_regex: Dict[str, Any],
    from_location: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Об'єднує результати: параметри мають пріоритет, regex — fallback.
    """
    result = from_params.copy()
    for k, v in from_regex.items():
        if k == "addresses":
            if not result.get("addresses") and from_location:
                result["addresses"] = from_location
            if not result.get("addresses") and v:
                result["addresses"] = v
        elif not result.get(k) and v:
            result[k] = v
    if not result.get("addresses") and from_location:
        result["addresses"] = from_location
    return result


def parse_with_bs4(html: str) -> Dict[str, Any]:
    """Парсинг через BeautifulSoup (lxml) — поточний parse_detail_page."""
    detail = parse_detail_page(html)
    from_params = _extract_from_params(detail.get("parameters") or [])
    from_location = _extract_from_location(detail.get("location"))
    text = (detail.get("description") or "") + " " + " ".join(
        f"{p.get('label', '')}: {p.get('value', '')}"
        for p in (detail.get("parameters") or [])
    )
    if detail.get("location"):
        loc = detail["location"]
        text += f" {loc.get('city', '')} {loc.get('region', '')} {loc.get('raw', '')}"
    from_regex = extract_from_description(text) if text.strip() else {}
    return _merge_extractions(from_params, from_regex, from_location)


def parse_with_lxml(html: str) -> Dict[str, Any]:
    """Парсинг через lxml напряму."""
    try:
        from lxml import html as lxml_html
        from lxml.etree import tostring
    except ImportError:
        return {}

    tree = lxml_html.fromstring(html)
    result: Dict[str, Any] = {
        "description": None,
        "parameters": [],
        "location": None,
    }

    # Опис
    for sel in ['[data-cy="ad_description"]', '[data-cy="ad_description_content"]']:
        els = tree.cssselect(sel)
        if els:
            result["description"] = " ".join(els[0].itertext()).strip()
            break

    # Параметри
    params_by_label: Dict[str, str] = {}
    for sel in ['[data-cy="ad_parameters"]', '[data-testid="ad-parameters-container"]']:
        for cont in tree.cssselect(sel):
            for p in cont.cssselect("p, li, div"):
                txt = " ".join(p.itertext()).strip()
                if ":" in txt:
                    parts = re.split(r"\s*:\s*", txt, 1)
                    if len(parts) == 2:
                        label, value = parts[0].strip(), parts[1].strip()
                        if label and (not params_by_label.get(label.lower()) or len(value) > len(params_by_label.get(label.lower(), ""))):
                            params_by_label[label.lower()] = value
    result["parameters"] = [{"label": k, "value": v} for k, v in params_by_label.items()]

    # Локація (Місцезнаходження)
    for el in tree.iter():
        if el.text and "Місцезнаходження" in (el.text or ""):
            texts = [t.strip() for t in el.itertext() if t.strip()]
            if len(texts) >= 2:
                result["location"] = {
                    "city": texts[1] if len(texts) > 1 else None,
                    "region": texts[2] if len(texts) > 2 else None,
                    "raw": " | ".join(texts[1:]) if len(texts) > 1 else None,
                }
            break

    from_params = _extract_from_params(result["parameters"])
    from_location = _extract_from_location(result["location"])
    text = (result.get("description") or "") + " " + " ".join(
        f"{p.get('label', '')}: {p.get('value', '')}" for p in result["parameters"]
    )
    if result.get("location"):
        loc = result["location"]
        text += f" {loc.get('city', '')} {loc.get('region', '')} {loc.get('raw', '')}"
    from_regex = extract_from_description(text) if text.strip() else {}
    return _merge_extractions(from_params, from_regex, from_location)


def parse_with_selectolax(html: str) -> Dict[str, Any]:
    """Парсинг через Selectolax."""
    try:
        from selectolax.parser import HTMLParser
    except ImportError:
        return {}

    tree = HTMLParser(html)
    result: Dict[str, Any] = {
        "description": None,
        "parameters": [],
        "location": None,
    }

    desc_el = tree.css_first('[data-cy="ad_description"]') or tree.css_first('[data-cy="ad_description_content"]')
    if desc_el:
        result["description"] = desc_el.text(separator=" ", strip=True)

    params_by_label: Dict[str, str] = {}
    for sel in ['[data-cy="ad_parameters"]', '[data-testid="ad-parameters-container"]']:
        for node in tree.css(sel):
            for p in node.css("p, li"):
                txt = p.text(separator=" ", strip=True)
                if txt and ":" in txt:
                    parts = re.split(r"\s*:\s*", txt, 1)
                    if len(parts) == 2:
                        label, value = parts[0].strip(), parts[1].strip()
                        if label and (label.lower() not in params_by_label or len(value) > len(params_by_label.get(label.lower(), ""))):
                            params_by_label[label.lower()] = value
    result["parameters"] = [{"label": k, "value": v} for k, v in params_by_label.items()]

    body_text = tree.body.text(separator=" ", strip=True) if tree.body else ""
    if "Місцезнаходження" in body_text:
        idx = body_text.find("Місцезнаходження")
        fragment = body_text[idx : idx + 200]
        parts = fragment.split()
        if len(parts) >= 2:
            result["location"] = {
                "city": parts[1] if parts[1] != "Місцезнаходження" else (parts[2] if len(parts) > 2 else None),
                "region": None,
                "raw": fragment[:150],
            }

    from_params = _extract_from_params(result["parameters"])
    from_location = _extract_from_location(result["location"])
    text = (result.get("description") or "") + " " + " ".join(
        f"{p.get('label', '')}: {p.get('value', '')}" for p in result["parameters"]
    )
    if result.get("location"):
        loc = result["location"]
        text += f" {loc.get('city', '')} {loc.get('region', '')} {loc.get('raw', '')}"
    from_regex = extract_from_description(text) if text.strip() else {}
    return _merge_extractions(from_params, from_regex, from_location)


def parse_with_parsel(html: str) -> Dict[str, Any]:
    """Парсинг через Parsel."""
    try:
        from parsel import Selector
    except ImportError:
        return {}

    sel = Selector(html)
    result: Dict[str, Any] = {
        "description": None,
        "parameters": [],
        "location": None,
    }

    desc_el = sel.css('[data-cy="ad_description"] ::text').getall() or sel.css('[data-cy="ad_description_content"] ::text').getall()
    if desc_el:
        result["description"] = " ".join(desc_el).strip()

    params_by_label: Dict[str, str] = {}
    for cont in sel.css('[data-cy="ad_parameters"], [data-testid="ad-parameters-container"]'):
        for p in cont.css("p::text, li::text").getall():
            p = p.strip() if p else ""
            if p and ":" in p:
                parts = re.split(r"\s*:\s*", p, 1)
                if len(parts) == 2:
                    label, value = parts[0].strip(), parts[1].strip()
                    if label and (label.lower() not in params_by_label or len(value) > len(params_by_label.get(label.lower(), ""))):
                        params_by_label[label.lower()] = value
    result["parameters"] = [{"label": k, "value": v} for k, v in params_by_label.items()]

    loc_texts = sel.xpath("//*[contains(text(), 'Місцезнаходження')]/following-sibling::*//text()").getall()
    if loc_texts:
        texts = [t.strip() for t in loc_texts if t.strip()]
        if len(texts) >= 1:
            result["location"] = {
                "city": texts[0],
                "region": texts[1] if len(texts) > 1 else None,
                "raw": " | ".join(texts),
            }

    from_params = _extract_from_params(result["parameters"])
    from_location = _extract_from_location(result["location"])
    text = (result.get("description") or "") + " " + " ".join(
        f"{p.get('label', '')}: {p.get('value', '')}" for p in result["parameters"]
    )
    if result.get("location"):
        loc = result["location"]
        text += f" {loc.get('city', '')} {loc.get('region', '')} {loc.get('raw', '')}"
    from_regex = extract_from_description(text) if text.strip() else {}
    return _merge_extractions(from_params, from_regex, from_location)


PARSERS = {
    "bs4": ("BeautifulSoup (lxml)", parse_with_bs4),
    "lxml": ("lxml", parse_with_lxml),
    "selectolax": ("Selectolax", parse_with_selectolax),
    "parsel": ("Parsel", parse_with_parsel),
}


def _normalize_for_compare(val: Any) -> str:
    """Нормалізує значення для порівняння."""
    if val is None:
        return ""
    if isinstance(val, list):
        if not val:
            return ""
        if isinstance(val[0], dict):
            return json.dumps(val, sort_keys=True)[:200]
        return "|".join(str(v) for v in val)
    s = str(val).strip()
    if s and s.replace(".", "").replace(",", "").isdigit():
        try:
            return str(float(s.replace(",", ".")))
        except ValueError:
            pass
    return s.lower() if s else ""


def _compare_with_llm(parsed: Dict[str, Any], llm: Dict[str, Any]) -> Dict[str, Any]:
    """Порівнює результат парсера з LLM."""
    fields = [
        "cadastral_number",
        "building_area_sqm",
        "land_area_ha",
        "floor",
        "property_type",
        "addresses",
    ]
    matches = 0
    total = 0
    details = []

    for f in fields:
        pv = parsed.get(f)
        lv = llm.get(f)
        pn = _normalize_for_compare(pv)
        ln = _normalize_for_compare(lv)
        if ln or pn:
            total += 1
            eq = pn == ln or (not pn and not ln)
            if eq:
                matches += 1
            details.append({
                "field": f,
                "parsed": str(pv)[:40] if pv else "",
                "llm": str(lv)[:40] if lv else "",
                "match": eq,
            })

    return {
        "match_ratio": matches / total if total > 0 else 1.0,
        "matches": matches,
        "total": total,
        "details": details,
    }


def _count_filled(result: Dict[str, Any]) -> int:
    """Рахує заповнені поля."""
    count = 0
    if result.get("cadastral_number"):
        count += 1
    if result.get("building_area_sqm"):
        count += 1
    if result.get("land_area_ha"):
        count += 1
    if result.get("addresses") and len(result["addresses"]) > 0:
        count += 1
    if result.get("floor"):
        count += 1
    if result.get("property_type"):
        count += 1
    if result.get("utilities"):
        count += 1
    if result.get("tags") and len(result["tags"]) > 0:
        count += 1
    return count


def run_benchmark(
    limit: int = 50,
    parsers: Optional[List[str]] = None,
    cache_html: bool = False,
    no_fetch: bool = False,
    output_path: Optional[Path] = None,
) -> None:
    """Запускає бенчмарк."""
    parsers = parsers or ["bs4", "lxml", "selectolax", "parsel"]
    cache_dir = project_root / "scripts" / "temp" / "olx_html_cache"
    if cache_html or no_fetch:
        cache_dir.mkdir(parents=True, exist_ok=True)

    Settings()
    MongoDBConnection.initialize(Settings())
    repo = OlxListingsRepository()

    cursor = repo.collection.find(
        {
            "detail.llm": {"$exists": True},
            "url": {"$exists": True, "$ne": ""},
        },
        {"url": 1, "search_data": 1, "detail": 1},
    ).limit(limit)

    listings = list(cursor)
    if not listings:
        print("Немає оголошень з detail.llm. Запустіть оновлення OLX.")
        return

    print(f"Бенчмарк: {len(listings)} оголошень, парсери: {parsers}")
    print("=" * 60)

    times: Dict[str, List[float]] = {p: [] for p in parsers}
    results_by_listing: List[Dict[str, Any]] = []
    fetch_errors = 0

    for i, doc in enumerate(listings):
        url = doc.get("url", "")
        llm = (doc.get("detail") or {}).get("llm") or {}
        short_url = url[:60] + "..." if len(url) > 60 else url

        html = None
        if no_fetch:
            cache_file = cache_dir / f"{hash(url) % 10**8}.html"
            if cache_file.exists():
                html = cache_file.read_text(encoding="utf-8", errors="replace")
        if html is None and not no_fetch:
            try:
                from scripts.olx_scraper.fetcher import fetch_page
                from scripts.olx_scraper import config as scraper_config
                import time as _time
                _time.sleep(scraper_config.get_delay_detail_seconds())
                resp = fetch_page(url, delay_before=False)
                html = resp.text
                if cache_html:
                    cache_file = cache_dir / f"{hash(url) % 10**8}.html"
                    cache_file.write_text(html, encoding="utf-8")
            except Exception as e:
                print(f"  [{i+1}] Помилка завантаження: {e}")
                fetch_errors += 1
                continue

        if not html:
            continue

        print(f"\n[{i+1}/{len(listings)}] {short_url}")

        listing_results: Dict[str, Any] = {"url": url, "llm": llm, "parsed": {}}
        for pkey in parsers:
            if pkey not in PARSERS:
                continue
            name, parser_fn = PARSERS[pkey]
            try:
                start = time.perf_counter()
                parsed = parser_fn(html)
                elapsed = time.perf_counter() - start
                times[pkey].append(elapsed)
                listing_results["parsed"][pkey] = parsed
                cmp = _compare_with_llm(parsed, llm)
                filled = _count_filled(parsed)
                print(f"  {name}: {cmp['matches']}/{cmp['total']} збігів, {filled}/8 полів, {elapsed:.3f}s")
            except Exception as e:
                print(f"  {name}: помилка — {e}")

        results_by_listing.append(listing_results)

    # Підсумок
    print("\n" + "=" * 60)
    print("Підсумок")
    print("=" * 60)

    summary: Dict[str, Any] = {
        "total_listings": len(results_by_listing),
        "fetch_errors": fetch_errors,
        "parsers": {},
        "field_accuracy": {},
    }

    for pkey in parsers:
        if pkey not in PARSERS or not times.get(pkey):
            continue
        name = PARSERS[pkey][0]
        avg_time = sum(times[pkey]) / len(times[pkey])
        matches_total = 0
        total_total = 0
        filled_total = 0
        for r in results_by_listing:
            pr = r.get("parsed", {}).get(pkey)
            if pr:
                cmp = _compare_with_llm(pr, r.get("llm", {}))
                matches_total += cmp["matches"]
                total_total += cmp["total"]
                filled_total += _count_filled(pr)
        n = len([r for r in results_by_listing if r.get("parsed", {}).get(pkey)])
        summary["parsers"][pkey] = {
            "name": name,
            "avg_time_ms": round(avg_time * 1000, 1),
            "match_ratio": matches_total / total_total if total_total > 0 else 0,
            "filled_avg": filled_total / n if n > 0 else 0,
            "samples": n,
        }
        print(f"{name}: {avg_time*1000:.0f}ms/стор, збіг {matches_total}/{total_total} ({100*matches_total/total_total if total_total else 0:.0f}%), заповнено полів: {filled_total/n if n else 0:.1f}")

    # Точність по полях
    fields = ["cadastral_number", "building_area_sqm", "land_area_ha", "floor", "property_type", "addresses"]
    for f in fields:
        by_parser: Dict[str, float] = {}
        for pkey in parsers:
            if pkey not in PARSERS:
                continue
            match_count = 0
            total = 0
            for r in results_by_listing:
                llm_val = (r.get("llm") or {}).get(f)
                if llm_val is None and f == "addresses":
                    llm_val = []
                if _normalize_for_compare(llm_val) or True:
                    total += 1
                    pr = r.get("parsed", {}).get(pkey)
                    if pr:
                        pv = pr.get(f)
                        if _normalize_for_compare(pv) == _normalize_for_compare(llm_val):
                            match_count += 1
            by_parser[pkey] = match_count / total if total > 0 else 0
        summary["field_accuracy"][f] = by_parser

    # Збереження звіту
    out_path = output_path or project_root / "docs" / "olx_parsing_benchmark_report.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    report_lines = [
        "# Звіт: парсинг OLX без LLM",
        "",
        "## Порівняння інструментів",
        "",
        "| Парсер | Час (мс/стор) | Збіг з LLM | Заповнено полів |",
        "|--------|---------------|------------|-----------------|",
    ]
    for pkey, data in summary.get("parsers", {}).items():
        report_lines.append(
            f"| {data['name']} | {data['avg_time_ms']} | {data['match_ratio']*100:.0f}% | {data['filled_avg']:.1f}/8 |"
        )
    report_lines.extend([
        "",
        "## Точність по полях",
        "",
        "| Поле | " + " | ".join(PARSERS.get(p, (p,))[0] for p in parsers if p in PARSERS) + " |",
        "|------|" + "|".join(["---"] * len([p for p in parsers if p in PARSERS])) + "|",
    ])
    for f in fields:
        row = f"| {f} |"
        for pkey in parsers:
            if pkey in PARSERS:
                acc = summary.get("field_accuracy", {}).get(f, {}).get(pkey, 0)
                row += f" {acc*100:.0f}% |"
        report_lines.append(row)

    report_lines.extend([
        "",
        "## Висновки",
        "",
        "Детальний аналіз в консолі вище.",
        "",
    ])

    out_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nЗвіт збережено: {out_path}")

    # JSON для подальшого аналізу
    json_path = out_path.with_suffix(".json")
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Метрики (JSON): {json_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Бенчмарк парсингу OLX без LLM")
    parser.add_argument("--limit", type=int, default=50, help="Кількість оголошень")
    parser.add_argument("--cache-html", action="store_true", help="Зберігати HTML на диск")
    parser.add_argument("--no-fetch", action="store_true", help="Не завантажувати (тільки кеш)")
    parser.add_argument("--parsers", type=str, default="bs4,lxml,selectolax,parsel", help="Парсери через кому")
    parser.add_argument("--output", type=str, help="Шлях до звіту")
    args = parser.parse_args()

    parsers = [p.strip() for p in args.parsers.split(",") if p.strip()]
    output = Path(args.output) if args.output else None

    run_benchmark(
        limit=args.limit,
        parsers=parsers,
        cache_html=args.cache_html,
        no_fetch=args.no_fetch,
        output_path=output,
    )


if __name__ == "__main__":
    main()
