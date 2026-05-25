# -*- coding: utf-8 -*-
"""Парсинг HTML mista.ua (список НП та детальна сторінка)."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup

from utils.coordinates_dms import parse_dms_coordinates
from utils.settlement_normalizer import normalize_settlement_key, normalize_settlement_name

_BASE = "https://mista.ua"

_STATUS_PREFIX = re.compile(r"^(м\.|с\.|смт\.?|смт|с-ще|селище)\s*", re.IGNORECASE)
_SETCITY_RE = re.compile(r"[?&]setcity=(\d+)", re.IGNORECASE)
_INT_RE = re.compile(r"(\d+)")

# Мітки на детальній сторінці (порядок важливий для split)
_DETAIL_LABELS = [
    "Область",
    "Місто",
    "Населення",
    "Щільність населення",
    "Поштовий індекс",
    "Телефонний код",
    "Координати",
    "Висота над рівнем моря",
    "Площа",
    "Річка, озеро (море)",
    "Рік заснування",
    "День міста",
    "Колишня назва",
]


def _abs_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href.split("?")[0].rstrip("/") if "?" in href else href.rstrip("/")
    return urljoin(_BASE, href.split("?")[0]).rstrip("/")


def _extract_mista_id_from_href(href: str) -> Optional[int]:
    """setcity лише з href конкретного посилання (не з HTML сторінки)."""
    if not href:
        return None
    m = _SETCITY_RE.search(href)
    return int(m.group(1)) if m else None


def _extract_mista_id_from_detail_page(soup: BeautifulSoup, mista_url: str) -> Optional[int]:
    """
    setcity з посилань на сторінці НП: пріоритет — лінки з slug поточного URL,
    інакше найчастіший setcity у <a href> (не з усього тексту сторінки).
    """
    from_id = _extract_mista_id_from_href(mista_url)
    if from_id is not None:
        return from_id

    path_slug = ""
    try:
        path_slug = unquote(mista_url.split("mista.ua", 1)[-1].split("?", 1)[0]).strip("/").split("/")[-1].lower()
    except Exception:
        pass

    counts: Dict[int, int] = {}
    slug_match_id: Optional[int] = None
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        m = _SETCITY_RE.search(href)
        if not m:
            continue
        cid = int(m.group(1))
        counts[cid] = counts.get(cid, 0) + 1
        if path_slug and path_slug in unquote(href).lower():
            slug_match_id = cid

    if slug_match_id is not None:
        return slug_match_id
    if not counts:
        return None
    return max(counts.items(), key=lambda item: item[1])[0]


def _parse_status_prefix(display_name: str) -> tuple[str, Optional[str]]:
    """Повертає (назва без префікса, settlement_status)."""
    raw = (display_name or "").strip()
    m = _STATUS_PREFIX.match(raw)
    if not m:
        return raw, None
    prefix = m.group(1).lower().replace(".", "")
    status_map = {"м": "місто", "с": "село", "смт": "селище", "с-ще": "селище", "селище": "селище"}
    status = status_map.get(prefix, prefix)
    name = _STATUS_PREFIX.sub("", raw).strip()
    return name, status


def _parse_region_rayon_from_path(url: str) -> tuple[Optional[str], Optional[str]]:
    """З URL /Україна/Область/Район/назва витягує область і район."""
    try:
        path = unquote(url.split("mista.ua", 1)[-1].split("?", 1)[0])
        parts = [p for p in path.strip("/").split("/") if p]
        if len(parts) < 2 or parts[0] != "Україна":
            return None, None
        region = parts[1].replace("_", " ").strip() if len(parts) > 1 else None
        rayon = parts[2].replace("_", " ").strip() if len(parts) > 2 else None
        if region and not region.endswith("область"):
            if "крим" in region.lower():
                region = "Автономна Республіка Крим"
            else:
                region = f"{region} область" if "область" not in region.lower() else region
        return region, rayon
    except Exception:
        return None, None


def parse_former_names(text: str) -> List[Dict[str, Optional[str]]]:
    """Парсить «Єлизаветград > 1924: Зінов'євськ > …»."""
    if not text or not str(text).strip():
        return []
    parts = re.split(r"\s*>\s*", str(text).strip())
    out: List[Dict[str, Optional[str]]] = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        m = re.match(r"^(\d{4}(?:\s*\([^)]+\))?|\d{2}\.\d{2}\.\d{4}):\s*(.+)$", part)
        if m:
            out.append({"name": m.group(2).strip(), "period_label": m.group(1).strip()})
        else:
            out.append({"name": part, "period_label": None})
    return out


def build_search_aliases(
    canonical_name: str,
    former_names: List[Dict[str, Optional[str]]],
) -> List[str]:
    keys: set[str] = set()
    for raw in [canonical_name] + [fn.get("name") or "" for fn in former_names]:
        k = normalize_settlement_key(raw)
        if k:
            keys.add(k)
    return sorted(keys)


def _extract_label_block(soup: BeautifulSoup) -> str:
    """Текст блоку з метаданими НП (h1 + сусідні елементи)."""
    h1 = soup.find("h1")
    chunks: List[str] = []
    if h1:
        chunks.append(h1.get_text(" ", strip=True))
        for sib in h1.find_all_next(["p", "div", "span"], limit=8):
            t = sib.get_text(" ", strip=True)
            if t and any(lbl in t for lbl in _DETAIL_LABELS):
                chunks.append(t)
                break
        if len(chunks) == 1:
            parent = h1.find_parent(["div", "section", "article"])
            if parent:
                chunks.append(parent.get_text(" ", strip=True))
    if not chunks:
        chunks.append(soup.get_text(" ", strip=True)[:8000])
    return " ".join(chunks)


def _parse_labeled_fields(text: str) -> Dict[str, str]:
    """Розбиває текст за мітками полів."""
    result: Dict[str, str] = {}
    if not text:
        return result
    pattern = "|".join(re.escape(lbl) for lbl in _DETAIL_LABELS)
    parts = re.split(rf"(?=(?:{pattern}):)", text)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        for lbl in _DETAIL_LABELS:
            prefix = f"{lbl}:"
            if part.startswith(prefix):
                value = part[len(prefix) :].strip()
                for other in _DETAIL_LABELS:
                    if other != lbl:
                        idx = value.find(f"{other}:")
                        if idx >= 0:
                            value = value[:idx].strip()
                result[lbl] = value
                break
    return result


_OBL_LINK_RE = re.compile(
    r'href="(?:/|%2F)?(?:%D0%9F%D0%BE%D1%88%D1%83%D0%BA_%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%85_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%96%D0%B2|Пошук_населених_пунктів)/?\?obl=(\d+)"[^>]*>([^<]+)',
    re.IGNORECASE,
)


def parse_region_filter_options(html: str) -> List[Dict[str, Any]]:
    """
    Парсить опції фільтра «Область» зі сторінки пошуку НП.
    Повертає [{obl_id, region_name}, ...] у порядку меню mista.ua.
    """
    seen: set[int] = set()
    out: List[Dict[str, Any]] = []
    for m in _OBL_LINK_RE.finditer(html or ""):
        obl_id = int(m.group(1))
        if obl_id in seen:
            continue
        seen.add(obl_id)
        name = re.sub(r"\s+", " ", m.group(2)).strip()
        name = re.sub(r"\s*<[^>]+>.*$", "", name).strip()
        if not name or name.lower() == "область":
            continue
        if not name.endswith("область") and "крим" not in name.lower():
            if "АР" not in name:
                name = f"{name} область"
        out.append({"obl_id": obl_id, "region_name": name})
    return out


def parse_list_max_page(html: str) -> Optional[int]:
    """
    Максимальний номер сторінки списку (1-based) з пагінації mista.ua.

    У HTML є input max=\"69\" (повна кількість сторінок) і data-pages=\"?citySPG=N\"
    лише для вікна 1–5 — його не можна використовувати як верхню межу.
    """
    soup = BeautifulSoup(html, "lxml")
    candidates: List[int] = []

    inp = soup.select_one("input[name='page'][max]")
    if inp and inp.get("max"):
        try:
            candidates.append(int(inp["max"]))
        except (TypeError, ValueError):
            pass

    max_spg = -1
    for el in soup.select("[data-pages*='citySPG']"):
        m = re.search(r"citySPG=(\d+)", el.get("data-pages") or "", re.IGNORECASE)
        if m:
            max_spg = max(max_spg, int(m.group(1)))
    if max_spg >= 0:
        candidates.append(max_spg + 1)

    return max(candidates) if candidates else None


def parse_list_page(html: str, *, list_page: int = 1) -> List[Dict[str, Any]]:
    """Парсить таблицю пошуку НП."""
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for a in soup.select("table a[href*='/Україна/'], table a[href*='/%D0%A3%D0%BA%D1%80%D0%B0%D1%97%D0%BD%D0%B0/']"):
        href = a.get("href") or ""
        if "/Україна/" not in href and "%D0%A3%D0%BA%D1%80%D0%B0%D1%97%D0%BD%D0%B0" not in href:
            continue
        url = _abs_url(href)
        if url in seen_urls:
            continue
        seen_urls.add(url)

        display = a.get_text(strip=True)
        name, status = _parse_status_prefix(display)
        name = normalize_settlement_name(name) or name
        region, rayon = _parse_region_rayon_from_path(url)
        mista_id = _extract_mista_id_from_href(href)

        tr = a.find_parent("tr")
        population = None
        area_sq_km = None
        if tr:
            cells = tr.find_all("td")
            nums = []
            for td in cells[1:]:
                t = td.get_text(strip=True).replace(" ", "")
                if t.isdigit():
                    nums.append(int(t))
            if nums:
                population = nums[0]
            if len(nums) > 1:
                area_sq_km = float(nums[1]) if nums[1] else None

        rows.append({
            "name": name,
            "display_name": display,
            "settlement_status": status,
            "region_name": region,
            "oblast_rayon_name": rayon,
            "mista_url": url,
            "mista_id": mista_id,
            "population": population,
            "area_sq_km": area_sq_km,
            "list_page": list_page,
        })
    return rows


def parse_detail_page(html: str, *, mista_url: str = "") -> Dict[str, Any]:
    """Парсить детальну сторінку населеного пункту."""
    soup = BeautifulSoup(html, "lxml")
    block = _extract_label_block(soup)
    fields = _parse_labeled_fields(block)

    region_name = fields.get("Область", "").strip()
    city_raw = fields.get("Місто", "").strip()
    city_name = normalize_settlement_name(city_raw) or city_raw

    pop_m = _INT_RE.search(fields.get("Населення", ""))
    population = int(pop_m.group(1)) if pop_m else None

    dens_m = re.search(r"(\d+(?:[.,]\d+)?)", fields.get("Щільність населення", ""))
    population_density = float(dens_m.group(1).replace(",", ".")) if dens_m else None

    area_m = re.search(r"(\d+(?:[.,]\d+)?)", fields.get("Площа", ""))
    area_sq_km = float(area_m.group(1).replace(",", ".")) if area_m else None

    elev_m = _INT_RE.search(fields.get("Висота над рівнем моря", ""))
    elevation_m = int(elev_m.group(1)) if elev_m else None

    year_m = _INT_RE.search(fields.get("Рік заснування", ""))
    founded_year = int(year_m.group(1)) if year_m else None

    coords = parse_dms_coordinates(fields.get("Координати", ""))
    coordinates = {"lat": coords[0], "lon": coords[1]} if coords else None

    former_raw = fields.get("Колишня назва", "")
    former_names = parse_former_names(former_raw)
    search_aliases = build_search_aliases(city_name, former_names)

    region_from_url, rayon_from_url = _parse_region_rayon_from_path(mista_url)
    if not region_name and region_from_url:
        region_name = region_from_url

    mista_id = _extract_mista_id_from_detail_page(soup, mista_url)

    return {
        "region_name": region_name or region_from_url,
        "oblast_rayon_name": rayon_from_url,
        "name": city_name,
        "population": population,
        "population_density": population_density,
        "postal_code": fields.get("Поштовий індекс", "").strip() or None,
        "phone_code": fields.get("Телефонний код", "").strip() or None,
        "coordinates": coordinates,
        "elevation_m": elevation_m,
        "area_sq_km": area_sq_km,
        "water_body": fields.get("Річка, озеро (море)", "").strip() or None,
        "founded_year": founded_year,
        "city_day": fields.get("День міста", "").strip() or None,
        "former_names": former_names,
        "search_aliases": search_aliases,
        "mista_url": mista_url,
        "mista_id": mista_id,
    }
