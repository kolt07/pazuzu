# -*- coding: utf-8 -*-
"""
Парсинг HTML сторінки списку оголошень OLX.
Використовується BeautifulSoup; опційно LLM для нормалізації полів (ціна, локація, дата).
"""

import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from bs4 import BeautifulSoup

# Корінь проекту для імпортів
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Базовий URL для абсолютних посилань
_BASE_URL = "https://www.olx.ua"

# Конфіг селекторів для сторінки деталей OLX (опис, параметри тощо).
_OLX_SELECTORS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "olx_detail_selectors.yaml"
_OLX_SELECTORS_CACHE: Optional[Dict[str, Any]] = None


def _load_olx_detail_selectors() -> Dict[str, Any]:
    """
    Завантажує YAML з конфігурацією селекторів для сторінки деталей OLX.
    Повертає словник або порожній dict при помилці/відсутності файлу.
    """
    global _OLX_SELECTORS_CACHE
    if _OLX_SELECTORS_CACHE is not None:
        return _OLX_SELECTORS_CACHE
    if not _OLX_SELECTORS_PATH.exists():
        _OLX_SELECTORS_CACHE = {}
        return _OLX_SELECTORS_CACHE
    try:
        import yaml

        with open(_OLX_SELECTORS_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if isinstance(data, dict):
            _OLX_SELECTORS_CACHE = data
        else:
            _OLX_SELECTORS_CACHE = {}
    except Exception:
        _OLX_SELECTORS_CACHE = {}
    return _OLX_SELECTORS_CACHE


def _format_phone(num: str) -> str:
    """Нормалізує номер: 0993316424 -> 099 331 62 44."""
    digits = re.sub(r"\D", "", num)
    if len(digits) == 10 and digits.startswith("0"):
        return f"{digits[:3]} {digits[3:6]} {digits[6:8]} {digits[8:]}"
    return num.strip()


def _normalize_url(href: Optional[str]) -> Optional[str]:
    if not href or not href.strip():
        return None
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return _BASE_URL.rstrip("/") + href
    return href


def _extract_price_value(text: str) -> Optional[float]:
    """
    Витягує числове значення ціни з рядка на кшталт '1 855 521.17 грн.' або 'Договірна'.
    Нормалізує роздільники тисяч (·, nbsp, narrow nbsp), щоб '2·000·000' не парсилось як 2.
    Якщо є кілька чисел (наприклад 4.68 га та 2 000 000 $) — повертає найбільше (ціна зазвичай більша за площу).
    """
    if not text:
        return None
    # Нормалізація: роздільники тисяч та кома як десяткова -> пробіл/крапка
    text = (
        text.replace("\u00a0", " ")   # non-breaking space
        .replace("\u202f", " ")       # narrow no-break space
        .replace("\u00b7", " ")       # middle dot (·) — OLX може використовувати
        .replace(",", ".")
    )
    numbers = re.findall(r"[\d\s.]+", text)
    candidates: List[float] = []
    for n in numbers:
        n_clean = n.replace(" ", "").strip()
        if n_clean and n_clean.replace(".", "").isdigit():
            try:
                v = float(n_clean)
                if v > 0:
                    candidates.append(v)
            except ValueError:
                continue
    if not candidates:
        return None
    # Якщо є число >= 100 — це ймовірно ціна (площі/га зазвичай < 100)
    prices = [c for c in candidates if c >= 100]
    if prices:
        return max(prices)
    return max(candidates)


def _extract_area_m2(text: str) -> Optional[float]:
    """Витягує площу в м² з рядка на кшталт '80 м²' або '24.34 м²'."""
    if not text:
        return None
    match = re.search(r"([\d.,]+)\s*м²", text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(",", ".").replace(" ", ""))
        except ValueError:
            pass
    return None


# Місяці українською (генітив): для парсингу "04 лютого 2026 р."
_UA_MONTHS = {
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4, "травня": 5, "червня": 6,
    "липня": 7, "серпня": 8, "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
}


def _split_location_and_date(loc_date_text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Розділяє рядок виду "Петро-Михайлівка - 04 лютого 2026 р." або "Сьогодні о 07:28"
    на локацію та текст дати. Повертає (location, date_text).
    """
    if not loc_date_text or not loc_date_text.strip():
        return None, None
    text = loc_date_text.strip()
    # Формат "Місце - Дата": шукаємо останній " - " (пробіл-тире-пробіл), щоб не розбити "Петро-Михайлівка"
    idx = text.rfind(" - ")
    if idx >= 0:
        location = text[:idx].strip() or None
        date_text = text[idx + 3 :].strip() or None
        return location, date_text
    # Один блок без " - " — вважаємо весь текст датою/локацією (наприклад "Сьогодні о 07:28")
    return None, text


# Паттерн дати в кінці блоку "Локація - Дата" (у тексті картки OLX)
_RE_DATE_UA = re.compile(
    r" - (Сьогодні о \d{1,2}:\d{2}|Вчора о \d{1,2}:\d{2}|\d{1,2}\s+(?:січня|лютого|березня|квітня|травня|червня|липня|серпня|вересня|жовтня|листопада|грудня)\s+\d{4}\s*(?:р\.)?)",
    re.I,
)


def _extract_location_and_date_from_body(body: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Витягує локацію та дату з тексту картки оголошення (body/raw_snippet), якщо в HTML
    немає окремого елемента з data-cy="ad-location". Формат у тексті: "... Location - Date ... м²".
    Повертає (location, date_text).
    """
    if not body or not body.strip():
        return None, None
    text = body.strip()
    match = _RE_DATE_UA.search(text)
    if not match:
        return None, None
    # Повний фрагмент " - Date" знайдено; дата — group(1)
    date_text = match.group(1).strip()
    start = match.start()
    # Локація — текст перед " - "; обрізаємо зліва ціну/заголовок (беремо після останнього "грн." або "Договірна")
    prefix = text[:start].strip()
    cut = -1
    for sep in ("грн.", "Договірна"):
        i = prefix.rfind(sep)
        if i > cut:
            cut = i + len(sep)
    if cut >= 0:
        prefix = prefix[cut:].strip()
    location = prefix or None
    return location, date_text


def _parse_listed_date_ua(date_text: str) -> Optional[str]:
    """
    Парсить дату оголошення з тексту українською: "04 лютого 2026 р.", "Сьогодні о 07:28", "Вчора о 12:00".
    Повертає ISO-рядок (UTC) або None.
    """
    if not date_text or not date_text.strip():
        return None
    text = date_text.strip()
    now = datetime.now(timezone.utc)

    # "Сьогодні о HH:MM" / "Сьогодні о 07:28"
    today_match = re.match(r"Сьогодні\s+о\s+(\d{1,2}):(\d{2})", text, re.I)
    if today_match:
        h, m = int(today_match.group(1)), int(today_match.group(2))
        dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        return dt.isoformat()

    # "Вчора о HH:MM"
    yesterday_match = re.match(r"Вчора\s+о\s+(\d{1,2}):(\d{2})", text, re.I)
    if yesterday_match:
        h, m = int(yesterday_match.group(1)), int(yesterday_match.group(2))
        dt = (now - timedelta(days=1)).replace(hour=h, minute=m, second=0, microsecond=0)
        return dt.isoformat()

    # "DD місяць YYYY р." або "DD місяць YYYY"
    for month_name, month_num in _UA_MONTHS.items():
        match = re.search(r"(\d{1,2})\s+" + re.escape(month_name) + r"\s+(\d{4})\s*(?:р\.)?", text, re.I)
        if match:
            day, year = int(match.group(1)), int(match.group(2))
            try:
                dt = datetime(year, month_num, day, 12, 0, 0, tzinfo=timezone.utc)
                return dt.isoformat()
            except ValueError:
                pass
            break

    return None


def parse_listing_cards(html: str, base_url: str = _BASE_URL) -> List[Dict[str, Any]]:
    """
    Парсить HTML сторінки списку оголошень і повертає список словників з полями:
    title, price_text, price_value, currency, location, date_text, listed_at_iso, area_m2, url, raw_snippet.
    date_text — дата оголошення як у списку (наприклад "04 лютого 2026 р."); listed_at_iso — та сама дата в ISO для сортування.
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Картки оголошень: спочатку data-cy="l-card", потім резервні варіанти
    cards = soup.select('[data-cy="l-card"]')
    if not cards:
        cards = soup.select('[data-aut-id="itemBox"]')
    if not cards:
        # Посилання на оголошення: /uk/d/... або /d/...
        link_containers = set()
        for a in soup.select('a[href*="/d/"]'):
            parent = a.find_parent(["div", "li", "article"])
            if parent and parent not in link_containers:
                link_containers.add(parent)
        cards = list(link_containers)

    for card in cards:
        item: Dict[str, Any] = {
            "title": None,
            "price_text": None,
            "price_value": None,
            "currency": None,
            "location": None,
            "date_text": None,
            "listed_at_iso": None,
            "area_m2": None,
            "url": None,
            "raw_snippet": None,
        }

        # Посилання та заголовок
        link = card.select_one('[data-cy="listing-ad-title"], a[href*="/d/"]') or card.find("a", href=re.compile(r"/d/"))
        if link:
            href = link.get("href")
            item["url"] = _normalize_url(href) if href else None
            title = (link.get_text(strip=True) or "").strip()
            if title:
                item["title"] = title
        if not item["title"]:
            h6 = card.select_one("h6")
            if h6:
                item["title"] = h6.get_text(strip=True)

        # Ціна
        price_el = (
            card.select_one('[data-cy="ad-price"], [data-aut-id="itemPrice"]')
            or card.find(string=re.compile(r"грн|Договірна|\$|€", re.I))
        )
        if price_el:
            if hasattr(price_el, "get_text"):
                price_text = price_el.get_text(strip=True)
            else:
                price_text = str(price_el).strip() if price_el else ""
            if price_text:
                item["price_text"] = price_text
                item["price_value"] = _extract_price_value(price_text)
                if "грн" in price_text.lower():
                    item["currency"] = "UAH"
                elif "$" in price_text:
                    item["currency"] = "USD"
                elif "€" in price_text:
                    item["currency"] = "EUR"

        # Локація та дата оголошення (формат у списку: "Петро-Михайлівка - 04 лютого 2026 р." або "Сьогодні о 07:28")
        location_el = card.select_one('[data-cy="ad-location"], [data-aut-id="itemLocation"]') or card.find(
            "span", class_=re.compile(r"location|date|breadcrumb", re.I)
        )
        if location_el:
            loc_text = location_el.get_text(strip=True)
            if loc_text:
                location_part, date_part = _split_location_and_date(loc_text)
                if location_part is not None:
                    item["location"] = location_part
                else:
                    item["location"] = loc_text
                item["date_text"] = date_part or loc_text
                item["listed_at_iso"] = _parse_listed_date_ua(item["date_text"]) if item["date_text"] else None

        # Площа та текст картки для fallback
        body = card.get_text(separator=" ", strip=True)
        # Якщо дату не знайшли з окремого елемента — витягуємо з тексту картки (OLX часто змінює розмітку)
        if not item["date_text"] and body:
            loc_from_body, date_from_body = _extract_location_and_date_from_body(body)
            if date_from_body:
                item["date_text"] = date_from_body
                item["listed_at_iso"] = _parse_listed_date_ua(date_from_body)
            if loc_from_body and not item["location"]:
                item["location"] = loc_from_body
        area_match = re.search(r"([\d.,]+)\s*м²", body)
        if area_match:
            try:
                item["area_m2"] = float(area_match.group(1).replace(",", ".").replace(" ", ""))
            except ValueError:
                pass
        if item["area_m2"] is None and body:
            item["area_m2"] = _extract_area_m2(body)

        # Невеликий raw-фрагмент для можливого LLM
        item["raw_snippet"] = body[:500] if body else None

        # Заголовок з raw-фрагмента, якщо не знайшли в розмітці (наприклад, перед "грн" або "Договірна")
        if not item["title"] and item.get("raw_snippet"):
            before_price = re.split(r"\s+\d[\d\s.,]*(?:грн|[\$€])|Договірна", item["raw_snippet"], 1)[0]
            item["title"] = before_price.strip() or None

        if item["title"] or item["url"]:
            results.append(item)

    return results


def normalize_with_llm(listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Заглушка для майбутньої інтеграції LLM: нормалізація location/date, ціни.
    Поля raw_snippet у кожному оголошенні можна передавати в LLM окремим скриптом.
    Зараз повертає вхідний список без змін.
    """
    return listings


def parse_listings_page(html: str, use_llm: bool = False) -> List[Dict[str, Any]]:
    """
    Парсить HTML сторінки списку оголошень.
    use_llm: зараз не використовується (нормалізація через LLM — майбутня).
    """
    return parse_listing_cards(html)


def detect_antibot_page(html: str) -> Dict[str, Any]:
    """
    Шукає на сторінці ознаки антиботу / перевірки браузера / капчі.
    Повертає {"is_antibot": bool, "hints": [str]} — список підказок для логу.
    """
    if not html or not html.strip():
        return {"is_antibot": True, "hints": ["empty_response"]}
    text = html.replace("\u00a0", " ").replace("\u202f", " ")
    lower = text.lower()
    hints: List[str] = []

    # Текстові ознаки перевірки / капчі / Cloudflare
    antibot_phrases = [
        ("captcha", "captcha"),
        ("cloudflare", "cloudflare"),
        ("перевірка браузера", "browser_check"),
        ("перевірте, що ви не робот", "robot_check"),
        ("please wait", "please_wait"),
        ("just a moment", "just_a_moment"),
        ("enable javascript", "enable_js"),
        ("увімкніть javascript", "enable_js_uk"),
        ("ddos protection", "ddos_protection"),
        ("checking your browser", "browser_check_en"),
        ("ray id", "cloudflare_ray"),
    ]
    for phrase, key in antibot_phrases:
        if phrase in lower:
            hints.append(key)

    # Елементи DOM, типові для сторінок перевірки
    if "cf-browser-verification" in lower or "challenge-running" in lower:
        hints.append("cf_challenge")
    if re.search(r"iframe[^>]*captcha|recaptcha|hcaptcha|turnstile", lower):
        hints.append("captcha_iframe")
    if re.search(r"data-cy=\"(captcha|challenge|verification)", lower):
        hints.append("data_cy_challenge")

    # Сторінка дуже коротка і без контенту оголошення — можливо заглушка антиботу
    soup = BeautifulSoup(html, "lxml")
    body = soup.find("body")
    if body:
        body_text = body.get_text(separator=" ", strip=True)
        has_ad_content = "ad_description" in html or "ad_description_content" in html or "data-cy=\"ad-price\"" in html
        if len(body_text) < 200 and not has_ad_content:
            if "script" in lower and ("challenge" in lower or "captcha" in lower or "cloudflare" in lower):
                hints.append("short_page_with_script")
            elif len(body_text) < 80:
                hints.append("very_short_body")

    return {"is_antibot": len(hints) > 0, "hints": hints}


def is_detail_page_inactive(html: str) -> bool:
    """
    Визначає, чи сторінка деталей оголошення відповідає неактивному/знятому оголошенню:
    пуста сторінка або повідомлення про неактивність.
    Орієнтир: типові фрази OLX для знятих/незнайдених оголошень.
    """
    if not html or not html.strip():
        return True
    text = html.replace("\u00a0", " ").replace("\u202f", " ")
    lower = text.lower()
    # Типові ознаки неактивного оголошення на OLX
    inactive_phrases = [
        "оголошення неактивне",
        "прибрано з публікації",
        "знято з публікації",
        "оголошення не знайдено",
        "ми не знайшли оголошення",
        "таке оголошення не знайдено",
        "об'єкт не знайдено",
        "сторінку не знайдено",
        "404",
    ]
    for phrase in inactive_phrases:
        if phrase in lower:
            return True
    # Дуже мало контенту — можливо порожня/помилкова сторінка
    soup = BeautifulSoup(html, "lxml")
    body = soup.find("body")
    if body:
        body_text = body.get_text(separator=" ", strip=True)
        if len(body_text) < 100 and "data-cy=\"ad_description\"" not in html and "ad_description" not in html:
            return True
    return False


def parse_detail_page(html: str) -> Dict[str, Any]:
    """
    Парсить HTML сторінки оголошення (деталі): ціна, опис, параметри.
    Повертає словник: price_text, price_value, currency, description, parameters, location, contact, fetched_at.
    """
    if is_detail_page_inactive(html):
        return {"_inactive": True, "fetched_at": datetime.now(timezone.utc).isoformat()}

    soup = BeautifulSoup(html, "lxml")
    result: Dict[str, Any] = {
        "price_text": None,
        "price_value": None,
        "currency": None,
        "description": None,
        "parameters": [],
        "location": None,
        "contact": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # Ціна: data-cy="ad-price" або схожий елемент (на сторінці деталей ціна відображається окремо)
    price_el = soup.select_one('[data-cy="ad-price"], [data-aut-id="itemPrice"]')
    if not price_el:
        # Шукаємо h2/h3 з коротким текстом ціни (наприклад "2 000 000 $") — не в title
        for tag in soup.find_all(["h2", "h3", "h4"]):
            txt = tag.get_text(strip=True) if hasattr(tag, "get_text") else ""
            if txt and len(txt) < 50 and re.search(r"[\d\s.,·]+\s*[$€]|[\d\s.,·]+\s*грн", txt, re.I):
                price_el = tag
                break
    if not price_el:
        price_str = soup.find(string=re.compile(r"грн|Договірна|\$|€", re.I))
        if price_str:
            parent = price_str.find_parent()
            if parent and parent.name not in ("title", "head"):
                price_el = parent
    if price_el:
        if hasattr(price_el, "get_text"):
            price_text = price_el.get_text(strip=True)
        else:
            price_text = str(price_el).strip() if price_el else ""
        if price_text:
            result["price_text"] = price_text
            result["price_value"] = _extract_price_value(price_text)
            if "грн" in price_text.lower():
                result["currency"] = "UAH"
            elif "$" in price_text:
                result["currency"] = "USD"
            elif "€" in price_text:
                result["currency"] = "EUR"

    # Опис оголошення: конфігуровані селектори + розширені data-testid, потім fallback за старою логікою
    desc_text: Optional[str] = None

    # 1) Конфіг з YAML (config/olx_detail_selectors.yaml)
    try:
        selectors_cfg = _load_olx_detail_selectors().get("description")  # type: ignore[union-attr]
    except Exception:
        selectors_cfg = None
    selectors: List[str] = []
    min_len = 0
    max_len = 50000
    if isinstance(selectors_cfg, dict):
        selectors = list(selectors_cfg.get("selectors") or [])
        try:
            min_len = int(selectors_cfg.get("min_length", 0) or 0)
        except Exception:
            min_len = 0
        try:
            max_len = int(selectors_cfg.get("max_length", 50000) or 50000)
        except Exception:
            max_len = 50000

    for css_selector in selectors:
        try:
            el = soup.select_one(css_selector)
        except Exception:
            el = None
        if el:
            txt = el.get_text(separator="\n", strip=True) or ""
            if txt and len(txt) >= min_len and len(txt) <= max_len:
                desc_text = txt
                break

    # 2) Вбудовані селектори (оновлені під актуальну розмітку OLX)
    if desc_text is None:
        desc_el = (
            soup.select_one('[data-cy="ad_description"]')
            or soup.select_one('[data-cy="ad_description_content"]')
            or soup.select_one('[data-testid="ad-description"]')
            or soup.select_one('[data-testid="ad_description"]')
            or soup.select_one('[data-testid="ad-description-content"]')
            or soup.select_one('[data-testid="ad_description_content"]')
            or soup.find("div", {"id": re.compile(r"description|content", re.I)})
            or soup.find("div", class_=re.compile(r"description|content|text", re.I))
        )
        if desc_el:
            desc_text = desc_el.get_text(separator="\n", strip=True) or None

    if desc_text:
        result["description"] = desc_text

    # Параметри: пари лейбл/значення (наприклад Площа, Поверх)
    def _parse_param_item(text: str) -> Optional[Dict[str, str]]:
        """Парсить рядок у форматі 'Лейбл: значення' або повертає None."""
        if not text or len(text) > 1000:
            return None
        parts = re.split(r"\s*:\s*", text, 1)
        if len(parts) == 2:
            return {"label": parts[0].strip(), "value": parts[1].strip()}
        if text.strip():
            return {"label": text[:100].strip(), "value": ""}
        return None

    params_by_label: Dict[str, str] = {}  # label -> value (зберігаємо найповніше значення)

    # 1) Класичний блок параметрів: data-cy="ad_parameters"
    params_container = (
        soup.select_one('[data-cy="ad_parameters"]')
        or soup.find("ul", class_=re.compile(r"param|detail|list", re.I))
        or soup.find("div", class_=re.compile(r"param|detail", re.I))
    )
    if params_container:
        items = params_container.select("li") or params_container.find_all(["div", "p"], class_=re.compile(r"item|row", re.I))
        for item in items:
            parsed = _parse_param_item(item.get_text(strip=True))
            if parsed and parsed["label"]:
                # Якщо вже є — оновлюємо лише якщо нове значення довше (більш повне)
                existing = params_by_label.get(parsed["label"])
                if existing is None or len(parsed.get("value", "")) > len(existing):
                    params_by_label[parsed["label"]] = parsed.get("value", "")

    # 2) Розширений блок характеристик: data-testid="ad-parameters-container"
    # Містить: відстань до міста, тип нерухомості, кадастровий номер, площа ділянки,
    # комунікації, інфраструктура тощо
    ad_params_container = soup.select_one('[data-testid="ad-parameters-container"]')
    if ad_params_container:
        for p_el in ad_params_container.find_all("p"):
            parsed = _parse_param_item(p_el.get_text(strip=True))
            if parsed and parsed["label"]:
                existing = params_by_label.get(parsed["label"])
                if existing is None or len(parsed.get("value", "")) > len(existing):
                    params_by_label[parsed["label"]] = parsed.get("value", "")

    if params_by_label:
        result["parameters"] = [{"label": k, "value": v} for k, v in params_by_label.items()]

    # Локація (місто/область) – блок «Місцезнаходження»
    full_text = ""
    try:
        loc_label = soup.find(string=re.compile(r"Місцезнаходження", re.I))
        if loc_label:
            container = loc_label.find_parent()
            if container:
                texts = [t.strip() for t in container.stripped_strings if t.strip()]
                # Очікуваний формат:
                # ["Місцезнаходження", "Коломия", "Івано-Франківська область", "Переглянути розташування на карті"]
                if len(texts) >= 2:
                    city = texts[1] if len(texts) >= 2 else None
                    region = texts[2] if len(texts) >= 3 else None
                    raw = " | ".join(texts[1:])
                    result["location"] = {
                        "city": city,
                        "region": region,
                        "raw": raw,
                    }
    except Exception:
        # Не ламаємо парсинг, якщо структура сторінки інша
        pass

    # Контакти продавця: ім'я, профіль, прев'ю телефону (якщо видно)
    try:
        contact: Dict[str, Any] = {
            "name": None,
            "profile_url": None,
            "phone_preview": None,
            "phones": [],
        }

        user_link = soup.find("a", href=re.compile(r"/list/user/"))
        if user_link:
            href = user_link.get("href")
            contact["profile_url"] = _normalize_url(href) if href else None
            # Ім'я зазвичай у заголовку безпосередньо перед посиланням на профіль
            heading = user_link.find_previous(["h4", "h5", "h6"])
            if heading:
                name_text = heading.get_text(strip=True)
                contact["name"] = name_text or None

            # Прев'ю телефону (якщо є) шукаємо поруч
            parent = user_link.find_parent()
            if parent:
                for s in parent.stripped_strings:
                    # На OLX часто показують «xxx xxx xxx» або частину номера
                    if "xxx" in s or re.search(r"\d[\d\s\-]{5,}", s):
                        contact["phone_preview"] = s
                        break

        # Телефони: tel: посилання та патерни в HTML (0XX XXX XX XX, 0XXXXXXXXX)
        phones: List[str] = []
        for a in soup.find_all("a", href=re.compile(r"^tel:", re.I)):
            href = a.get("href") or ""
            num = re.sub(r"^tel:", "", href, flags=re.I).strip()
            if num and num not in phones:
                phones.append(_format_phone(num))
        if not phones:
            phone_pattern = re.compile(
                r"\+38\s*\d{2}\s*\d{3}\s*\d{2}\s*\d{2}|"
                r"0\d{2}\s*\d{3}\s*\d{2}\s*\d{2}|"
                r"0\d{2}\s*\d{3}\s*\d{4}|"
                r"0\d{9}"
            )
            for m in phone_pattern.finditer(html):
                num = m.group(0).strip()
                digits = re.sub(r"\D", "", num)
                if num == "000 000 000":
                    continue
                if len(digits) == 9 and " " not in num and "+" not in num:
                    continue
                formatted = _format_phone(num)
                if formatted and formatted not in phones:
                    phones.append(formatted)
        if phones:
            contact["phones"] = phones
        if any(contact.values()):
            result["contact"] = contact
    except Exception:
        pass

    # Якщо опис не знайдено за селекторами — шукаємо великий текстовий блок
    if not result["description"]:
        for div in soup.select("div[class*='description'], div[class*='content']"):
            t = div.get_text(separator="\n", strip=True)
            if t and len(t) > 50 and len(t) < 50000:
                result["description"] = t
                break

    # Додатковий fallback: парсимо повний текст сторінки.
    if not full_text:
        full_text = soup.get_text(separator="\n", strip=True)

    # 1) Опис між заголовком "Опис" і службовими блоками (ID, контакти тощо).
    if not result["description"] and full_text:
        lines = [ln for ln in full_text.splitlines()]
        start_idx = None
        for i, ln in enumerate(lines):
            if ln.strip().lower().startswith("опис"):
                start_idx = i
                break
        if start_idx is not None:
            tail = lines[start_idx + 1 :]
            stop_markers = [
                "ID:",
                "ID ",
                "Поскаржитися",
                "Зв’язатися з продавцем",
                "Зв'язатися з продавцем",
                "Опубліковано ",
            ]
            stop_idx = len(tail)
            for j, ln in enumerate(tail):
                for m in stop_markers:
                    if m in ln:
                        stop_idx = min(stop_idx, j)
                        break
                if j >= stop_idx:
                    break
            body_lines = [ln.strip() for ln in tail[:stop_idx] if ln.strip()]
            desc_text = "\n".join(body_lines).strip()
            if desc_text and len(desc_text) > 50:
                result["description"] = desc_text

    # 2) Параметри з повного тексту (рядки "Лейбл: значення" до секції "Опис").
    if not result["parameters"] and full_text:
        lines = [ln.strip() for ln in full_text.splitlines()]
        params_section: Dict[str, str] = {}
        for ln in lines:
            if not ln:
                continue
            lower_ln = ln.lower()
            if lower_ln.startswith("опис"):
                break
            parsed = _parse_param_item(ln)
            if parsed and parsed["label"]:
                existing = params_section.get(parsed["label"])
                if existing is None or len(parsed.get("value", "")) > len(existing):
                    params_section[parsed["label"]] = parsed.get("value", "")
        if params_section:
            result["parameters"] = [{"label": k, "value": v} for k, v in params_section.items()]

    # 3) Локація з повного тексту: блок після "Місцезнаходження".
    if not result["location"] and full_text:
        lines = [ln.strip() for ln in full_text.splitlines()]
        loc_idx = None
        for i, ln in enumerate(lines):
            if re.search(r"Місцезнаходження", ln, re.I):
                loc_idx = i
                break
        if loc_idx is not None:
            tail = lines[loc_idx + 1 :]
            city = tail[0] if len(tail) >= 1 else None
            region = tail[1] if len(tail) >= 2 else None
            raw = " | ".join([t for t in tail if t])
            if city or region:
                result["location"] = {
                    "city": city,
                    "region": region,
                    "raw": raw or None,
                }

    return result
