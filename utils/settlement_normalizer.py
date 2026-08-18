# -*- coding: utf-8 -*-
"""
Нормалізація назв населених пунктів для збереження в БД та fuzzy-порівняння.

Правила:
- кирилиця, назва з великої літери (Title Case);
- без префіксів типу м./с./смт./місто/село;
- витяг settlement зі складних рядків (район + НП, DISTRICT/С.НП);
- ключ пошуку — lowercase без префіксів і зайвої пунктуації.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Префікси типу НП на початку або після коми/слеша
_SETTLEMENT_TYPE_PREFIX = re.compile(
    r"^(?:"
    r"(?:у|в|з|на|до)\s+"
    r"|(?:м\.|місто|с\.|село|смт\.?|смт|с-ще|селище|пгт\.?|п\.|"
    r"с\.?т\.|тг\.?|"
    r"обл\.|область)\s*"
    r")+",
    re.IGNORECASE,
)

_COMPOUND_COMMA = re.compile(
    r"^.+?[,\s]+"
    r"(?:м\.|місто|с\.|село|смт\.?|смт|с-ще|селище|пгт\.?|п\.|"
    r"с\.?т\.|тг\.?)\s*(.+)$",
    re.IGNORECASE,
)

_COMPOUND_SLASH = re.compile(
    r"^.+?/(?:"
    r"м\.|с\.|смт\.?|смт|с\.?т\.|село|місто|"
    r"обл\.|область"
    r")\.?\s*(.+)$",
    re.IGNORECASE,
)

_DISTRICT_ONLY = re.compile(
    r"^.+(?:"
    r"ський|цький|нський|зький"
    r")\s+(?:район|р-н|р\.)\s*$",
    re.IGNORECASE,
)

_DISTRICT_SUFFIX = re.compile(
    r"(?:^|[,\s]+).+(?:ський|цький|нський|зький)\s+(?:район|р-н|р\.)\s*$",
    re.IGNORECASE,
)

_MULTI_SPACE = re.compile(r"\s+")

# Префікси типу НП у сирих адресах оголошень (для regex геофільтра)
SETTLEMENT_PREFIX_REGEX = (
    r"(?:м\.\s*|смт\.?\s*|смт|с\.?\s*|село\s*|селище\s*|с-ще\s*|пгт\.?\s*)?"
)

# Поодинокі латинські літери, що часто трапляються замість кирилиці в сирих даних
_LATIN_TO_CYRILLIC = str.maketrans(
    {
        "A": "А",
        "a": "а",
        "B": "В",
        "E": "Е",
        "e": "е",
        "I": "І",
        "i": "і",
        "K": "К",
        "k": "к",
        "M": "М",
        "m": "м",
        "H": "Н",
        "h": "н",
        "O": "О",
        "o": "о",
        "P": "Р",
        "p": "р",
        "C": "С",
        "c": "с",
        "T": "Т",
        "t": "т",
        "X": "Х",
        "x": "х",
        "Y": "У",
        "y": "у",
    }
)


def _unicode_normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value or "").strip()


def _to_cyrillic(value: str) -> str:
    """Підміняє очевидні латинські homoglyphs на кирилицю."""
    return value.translate(_LATIN_TO_CYRILLIC)


def extract_settlement_name(raw: Optional[str]) -> Optional[str]:
    """
    Витягує назву населеного пункту зі сирого рядка.

    Приклади:
    - «смт Любешів» → «Любешів»
    - «Вишгородський район, смт Димер» → «Димер»
    - «ХОРОВЕЦЬКА/С.ХОРОВЕЦЬ» → «ХОРОВЕЦЬ»
    """
    if not raw or not isinstance(raw, str):
        return None

    value = _unicode_normalize(raw)
    if not value:
        return None

    value = _to_cyrillic(value)

    slash_match = _COMPOUND_SLASH.match(value)
    if slash_match:
        value = slash_match.group(1).strip()

    comma_match = _COMPOUND_COMMA.match(value)
    if comma_match:
        value = comma_match.group(1).strip()

    value = _SETTLEMENT_TYPE_PREFIX.sub("", value).strip()
    value = _MULTI_SPACE.sub(" ", value).strip(" .,-/")

    if not value:
        return None

    if is_district_only_name(value):
        return None

    return value


def is_district_only_name(name: Optional[str]) -> bool:
    """Чи це лише назва району без конкретного населеного пункту."""
    if not name or not isinstance(name, str):
        return False
    value = _unicode_normalize(name)
    if not value:
        return False
    if _DISTRICT_ONLY.match(value):
        return True
    if _DISTRICT_SUFFIX.match(value) and not _COMPOUND_COMMA.match(value) and not _COMPOUND_SLASH.match(value):
        return True
    lower = value.lower()
    if lower.endswith(" р.") or lower.endswith(" р"):
        return True
    if lower.endswith(" район") or lower.endswith(" р-н") or lower.endswith(" р."):
        if not re.search(
            r"(?:м\.|місто|с\.|село|смт\.?|смт|с-ще|селище|пгт\.?|тг\.?)\s",
            value,
            re.IGNORECASE,
        ):
            return True
    return False


def format_settlement_display_name(name: Optional[str]) -> Optional[str]:
    """
    Форматує назву НП для збереження: кирилиця, кожне слово з великої літери.

    «ЛЮБЕШІВ» → «Любешів», «старе село» → «Старе Село», «кам'янка-бузька» → «Кам'янка-Бузька»
    """
    if not name or not isinstance(name, str):
        return None

    value = _unicode_normalize(name)
    if not value:
        return None

    value = _to_cyrillic(value)
    parts = re.split(r"(\s+|-)", value)
    formatted: list[str] = []
    for part in parts:
        if not part:
            continue
        if part.isspace() or part == "-":
            formatted.append(part)
            continue
        if len(part) == 1:
            formatted.append(part.upper())
        else:
            formatted.append(part[0].upper() + part[1:].lower())
    result = "".join(formatted).strip()
    return result or None


def normalize_settlement_key(name: Optional[str]) -> str:
    """
    Ключ для пошуку/дедуплікації: lowercase, без префіксів і зайвої пунктуації.
    """
    extracted = extract_settlement_name(name)
    if not extracted:
        return ""

    value = extracted.lower()
    value = value.replace("'", "'").replace("`", "'")
    value = re.sub(r"[\"«»„“]", "", value)
    value = _MULTI_SPACE.sub(" ", value).strip(" .,-/")
    return value


def settlement_name_from_mista_url(url: Optional[str]) -> Optional[str]:
    """Назва НП з останнього сегмента шляху mista.ua (fallback при порожньому parsed.name)."""
    if not url:
        return None
    try:
        from urllib.parse import unquote

        path = unquote(str(url).split("mista.ua", 1)[-1].split("?", 1)[0])
        parts = [p for p in path.strip("/").split("/") if p]
        if len(parts) < 3 or parts[0] != "Україна":
            return None
        slug = parts[-1].replace("_", " ").strip()
        if "," in slug:
            slug = slug.split(",", 1)[0].strip()
        return normalize_settlement_name(slug) or slug or None
    except Exception:
        return None


def merge_settlement_alias_keys(*raw_names: Optional[str]) -> List[str]:
    """Унікальні нормалізовані ключі назв для search_aliases."""
    seen: set[str] = set()
    out: List[str] = []
    for raw in raw_names:
        if raw is None:
            continue
        if isinstance(raw, dict):
            raw = raw.get("name")
        key = normalize_settlement_key(str(raw) if raw is not None else "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def resolve_mista_canonical_and_aliases(
    *,
    list_name: Optional[str] = None,
    detail_name: Optional[str] = None,
    mista_url: Optional[str] = None,
    former_names: Optional[Iterable[Any]] = None,
) -> Tuple[Optional[str], List[str]]:
    """
    Канонічна назва НП з mista.ua та аліаси для пошуку.

    Список пошуку mista показує сучасні назви (Дніпро, Кам'янське), тоді як
    URL/поле «Місто» на детальній сторінці часто лишаються зі старими (Дніпропетровськ).
    Пріоритет канону: list → detail → URL-slug. Усі інші варіанти — search_aliases.
    """
    url_name = settlement_name_from_mista_url(mista_url)
    candidates = [
        normalize_settlement_name(list_name) or (str(list_name).strip() if list_name else None),
        normalize_settlement_name(detail_name) or (str(detail_name).strip() if detail_name else None),
        url_name,
    ]
    canonical: Optional[str] = next((c for c in candidates if c), None)

    former_raw: List[Optional[str]] = []
    for item in former_names or []:
        if isinstance(item, dict):
            former_raw.append(item.get("name"))
        else:
            former_raw.append(str(item) if item is not None else None)

    aliases = merge_settlement_alias_keys(
        canonical,
        list_name,
        detail_name,
        url_name,
        *former_raw,
    )
    return canonical, aliases


def settlement_prefix_regex_pattern(normalized_key: str) -> str:
    """
    Префіксний regex для MongoDB: и/і взаємозамінні (різне кодування в БД і вводі).
    """
    if not normalized_key:
        return "^$"
    parts: list[str] = []
    for ch in normalized_key:
        if ch in ("\u0438", "\u0456"):
            parts.append("[\u0438\u0456]")
        else:
            parts.append(re.escape(ch))
    return "^" + "".join(parts)


def settlement_typo_n_variant(normalized_key: str) -> Optional[str]:
    """
  Додатковий ключ для пошуку: зайва «н» перед -ичі/-ичи (напр. іваничі → іваниничі).
    """
    if not normalized_key:
        return None
    m = re.match(r"^(.*?)ич([\u0456\u0438])$", normalized_key)
    if not m:
        return None
    return f"{m.group(1)}инич{m.group(2)}"


def settlement_search_or_clauses(normalized_key: str) -> List[Dict[str, Any]]:
    """Умови $or для prefix-пошуку НП у cities (ім'я, aliases, и/і, typo-н)."""
    if not normalized_key:
        return []
    keys: List[str] = [normalized_key]
    typo = settlement_typo_n_variant(normalized_key)
    if typo and typo not in keys:
        keys.append(typo)
    patterns: List[str] = []
    simple = f"^{re.escape(normalized_key)}"
    flex = settlement_prefix_regex_pattern(normalized_key)
    patterns.append(simple)
    if flex != simple:
        patterns.append(flex)
    if typo:
        patterns.append(f"^{re.escape(typo)}")

    or_clauses: List[Dict[str, Any]] = []
    for pat in patterns:
        or_clauses.append({"name_normalized": {"$regex": pat}})
        or_clauses.append({"search_aliases": {"$regex": pat}})
    or_clauses.append({"name": {"$regex": flex, "$options": "i"}})
    for k in keys:
        or_clauses.append({"search_aliases": k})
        or_clauses.append({"name_normalized": k})
    return or_clauses


def normalize_settlement_name(raw: Optional[str]) -> Optional[str]:
    """
    Повна нормалізація для збереження в БД: витяг + форматування display name.
    """
    extracted = extract_settlement_name(raw)
    if not extracted:
        return None
    return format_settlement_display_name(extracted)


def normalize_settlement_name_for_search(raw: Optional[str]) -> Optional[str]:
    """Alias для зворотної сумісності з toponym_normalizer."""
    return normalize_settlement_name(raw)


def region_short_label(region_name: Optional[str]) -> str:
    """«Полтавська область» → «Полтавська»; «м. Київ» без змін."""
    if not region_name:
        return ""
    s = str(region_name).strip()
    if s.endswith(" область"):
        return s[: -len(" область")].strip()
    return s


def format_settlement_picker_label(
    name: str,
    region_name: Optional[str] = None,
) -> str:
    """Підпис для autocomplete: «Іваничі, Волинська обл.»."""
    base = normalize_settlement_name(name) or (name or "").strip()
    if not base:
        return ""
    if not region_name:
        return base
    short = region_short_label(region_name)
    if not short:
        return base
    if not short.lower().endswith(" обл.") and "область" not in short.lower():
        short = f"{short} обл."
    return f"{base}, {short}"


def parse_settlement_picker_label(label: str) -> tuple[str, Optional[str]]:
    """
    Розбір підпису picker → (назва НП, фрагмент області для resolve).
    Підтримує «Назва, Область обл.» та legacy «Назва · Область».
    """
    if not label:
        return "", None
    s = str(label).strip()
    idx = s.rfind(" (")
    if idx > 0 and "ос." in s[idx:]:
        s = s[:idx].strip()
    if ", " in s:
        name_part, reg_part = s.rsplit(", ", 1)
        return name_part.strip(), (reg_part.strip() or None)
    if " · " in s:
        return parse_settlement_filter_label(s)
    return s, None


def resolve_region_from_picker_fragment(region_part: Optional[str]) -> Optional[str]:
    """«Волинська обл.» → «Волинська область» (канон для geoRegion)."""
    if not region_part:
        return None
    from utils.ukraine_regions import normalize_region_to_canonical

    raw = str(region_part).strip()
    if not raw:
        return None
    canon = normalize_region_to_canonical(raw)
    if canon:
        return canon
    if "область" not in raw.lower() and not raw.lower().endswith(" обл."):
        canon = normalize_region_to_canonical(f"{raw} область")
        if canon:
            return canon
    return raw


def build_settlement_picker_options(
    cities: Iterable[Dict[str, Any]],
    *,
    regions_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Опції для autocomplete НП з дизамбігуацією області."""
    regions_by_id = regions_by_id or {}
    options: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for doc in cities:
        if not doc:
            continue
        cid = str(doc.get("_id") or doc.get("id") or "")
        name = normalize_settlement_name(doc.get("name")) or (doc.get("name") or "").strip()
        if not name:
            continue
        rid = str(doc.get("region_id") or "")
        reg_doc = regions_by_id.get(rid) if rid else None
        reg = (reg_doc or {}).get("name") or doc.get("region_name") or doc.get("region")
        dedupe_key = (cid or f"{rid}:{normalize_settlement_key(name)}", rid)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        picker_label = format_settlement_picker_label(name, reg)
        aliases = doc.get("search_aliases") or []
        if isinstance(aliases, str):
            aliases = [aliases]
        options.append({
            "id": cid,
            "name": name,
            "region": reg,
            "region_id": rid or None,
            "picker_label": picker_label,
            "label": picker_label,
            "population": doc.get("population"),
            "search_aliases": list(aliases) if aliases else [],
        })

    options.sort(
        key=lambda o: (
            -(o.get("population") or 0),
            (o.get("name") or "").casefold(),
            (o.get("region") or "").casefold(),
        )
    )
    return options


def format_settlement_filter_label(
    name: str,
    *,
    region_name: Optional[str] = None,
    population: Optional[int] = None,
    disambiguate_region: bool = False,
) -> str:
    """
    Підпис для combobox: за потреби «Назва · Область» та/або населення.
    disambiguate_region=True — додати область (для списків без попереднього вибору регіону).
    """
    base = normalize_settlement_name(name) or (name or "").strip()
    if not base:
        return ""
    parts = [base]
    if disambiguate_region and region_name:
        short = region_short_label(region_name)
        if short:
            parts.append(short)
    label = " · ".join(parts)
    if population is not None:
        label = f"{label} ({population:,} ос.)".replace(",", " ")
    return label


def parse_settlement_filter_label(label: str) -> tuple[str, Optional[str]]:
    """
    Розбір підпису combobox → (назва НП, коротка назва області або None).
    Підтримує «Назва (N ос.)», «Назва · Область».
    """
    if not label:
        return "", None
    s = str(label).strip()
    idx = s.rfind(" (")
    if idx > 0 and "ос." in s[idx:]:
        s = s[:idx].strip()
    if " · " in s:
        name_part, reg_part = s.rsplit(" · ", 1)
        return name_part.strip(), reg_part.strip() or None
    return s, None


def build_city_filter_options(
    cities: Iterable[Dict[str, Any]],
    *,
    region_name: Optional[str] = None,
    disambiguate_region: bool = False,
) -> List[Dict[str, Any]]:
    """
    Опції НП для UI/API: унікальність за (_id) або (region_id, name_normalized).
    """
    options: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_keys: set[tuple] = set()

    for doc in cities:
        if not doc:
            continue
        cid = str(doc.get("_id") or doc.get("id") or "")
        name = normalize_settlement_name(doc.get("name")) or (doc.get("name") or "").strip()
        if not name:
            continue
        rid = str(doc.get("region_id") or "")
        key = normalize_settlement_key(name)
        dedupe_key = (rid, key) if rid and key else (cid or name.casefold(),)
        if cid and cid in seen_ids:
            continue
        if dedupe_key in seen_keys:
            continue
        if cid:
            seen_ids.add(cid)
        seen_keys.add(dedupe_key)

        reg = region_name or doc.get("region_name") or doc.get("region")
        label = format_settlement_filter_label(
            name,
            region_name=reg,
            population=doc.get("population"),
            disambiguate_region=disambiguate_region,
        )
        aliases = doc.get("search_aliases") or []
        if isinstance(aliases, str):
            aliases = [aliases]
        expanded_aliases: List[str] = []
        seen_alias: set[str] = set()
        for raw_alias in aliases:
            if not raw_alias:
                continue
            for variant in (str(raw_alias), format_settlement_display_name(str(raw_alias)) or ""):
                key = (variant or "").casefold()
                if variant and key not in seen_alias:
                    seen_alias.add(key)
                    expanded_aliases.append(variant)
        options.append({
            "id": cid,
            "name": name,
            "region": reg,
            "region_id": rid or None,
            "label": label,
            "population": doc.get("population"),
            "search_aliases": expanded_aliases,
        })

    # Не сортувати за label (суфікс «(N ос.)» ламає порядок). Для однієї області — спочатку
    # за населенням (як у CitiesRepository.get_by_region), інакше — за назвою.
    if disambiguate_region:
        options.sort(key=lambda o: (o.get("name") or "").casefold())
    else:
        options.sort(
            key=lambda o: (
                o.get("population") is None,
                -(o.get("population") or 0),
                (o.get("name") or "").casefold(),
            )
        )
    return options


def dedupe_settlement_labels(names: Iterable[str]) -> List[str]:
    """
    Унікальні назви НП для UI/API: один ключ — одна канонічна форма (Title Case).

    «ЛЮБЕШІВ» і «Любешів» → один пункт «Любешів».
    """
    by_key: dict[str, str] = {}
    for raw in names:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        name_part, _ = parse_settlement_filter_label(text)
        key = normalize_settlement_key(name_part or text)
        if not key:
            continue
        display = normalize_settlement_name(name_part or text) or name_part or text
        by_key[key] = display
    return sorted(by_key.values(), key=lambda s: s.casefold())
