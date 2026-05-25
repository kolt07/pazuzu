# -*- coding: utf-8 -*-
"""Тести парсера mista.ua."""

from scripts.mista_scraper import config as mista_config
from scripts.mista_scraper.parser import (
    build_search_aliases,
    parse_detail_page,
    parse_former_names,
    parse_list_max_page,
    parse_list_page,
    parse_region_filter_options,
)


DETAIL_HTML_SNIPPET = """
<html><body>
<h1>Сайт міста Кропивницький</h1>
<p>Область: Кіровоградська областьМісто: Кропивницький Населення: 250629 осіб
Щільність населення: 2433 осіб/кв. км. Поштовий індекс: 25000-25490
Телефонний код: +380 522 Координати: 48°30'34″ пн. ш. 32°16'01″ сх. д.
Висота над рівнем моря: 113 м. Площа: 103 кв. км.
Річка, озеро (море): р. Інгул Рік заснування: 1765 р.
День міста: 26 вересня (2026 р.)
Колишня назва: Єлизаветград > 1924: Зінов'євськ > 1934: Кірове > 1939: Кіровоград > 14.07.2016: Кропивницький</p>
<a href="/Оголошення/?setcity=458">x</a>
</body></html>
"""

LIST_HTML_SNIPPET = """
<html><body><table>
<tr><td><a href="/Україна/Кіровоградська_область/Кіровоградський_район/кіровоград">м. Кропивницький</a></td>
<td>Кіровоградська</td><td>250629</td><td>103</td></tr>
</table></body></html>
"""


def test_parse_former_names():
    names = parse_former_names("Єлизаветград > 1924: Зінов'євськ > 1934: Кірове")
    assert len(names) == 3
    assert names[0]["name"] == "Єлизаветград"
    assert names[1]["period_label"] == "1924"


def test_parse_detail_kropyvnytskyi():
    url = "https://mista.ua/Україна/Кіровоградська_область/Кіровоградський_район/кіровоград"
    data = parse_detail_page(DETAIL_HTML_SNIPPET, mista_url=url)
    assert data["name"] == "Кропивницький"
    assert data["population"] == 250629
    assert data["area_sq_km"] == 103.0
    assert data["elevation_m"] == 113
    assert data["founded_year"] == 1765
    assert data["coordinates"]["lat"] == 48.509444
    assert data["coordinates"]["lon"] == 32.266944
    assert "кіровоград" in data["search_aliases"]
    assert "єлизаветград" in data["search_aliases"]
    assert data["mista_id"] == 458


def test_parse_list_page():
    rows = parse_list_page(LIST_HTML_SNIPPET, list_page=1)
    assert len(rows) >= 1
    assert rows[0]["name"] == "Кропивницький"
    assert rows[0]["population"] == 250629


def test_list_page_ignores_global_setcity_in_html():
    """setcity=1 у шапці сторінки не повинен потрапляти в усі рядки таблиці."""
    html = (
        '<html><body><a href="/?setcity=1">home</a><table>'
        '<tr><td><a href="/Україна/Вінницька_область/вінниця">м. Вінниця</a></td></tr>'
        '<tr><td><a href="/Україна/Львівська_область/львів">м. Львів</a></td></tr>'
        "</table></body></html>"
    )
    rows = parse_list_page(html, list_page=1)
    assert len(rows) == 2
    assert all(r.get("mista_id") is None for r in rows)


def test_build_search_aliases():
    aliases = build_search_aliases("Кропивницький", [{"name": "Кіровоград"}])
    assert "кропивницький" in aliases
    assert "кіровоград" in aliases


def test_settlements_list_ajax_params():
    assert mista_config.settlements_list_ajax_params(1)["citySPG"] == "0"
    assert mista_config.settlements_list_ajax_params(2)["citySPG"] == "1"
    assert mista_config.settlements_list_ajax_params(1)["reload"] == "ajax"
    assert mista_config.settlements_list_ajax_params(1, obl_id=862)["obl"] == "862"
    assert "obl=862" in mista_config.settlements_list_url(862)


def test_parse_region_filter_options():
    snippet = (
        '<a href="/Пошук_населених_пунктів/?obl=862">Волинська область</a>'
        '<a href="/Пошук_населених_пунктів/?obl=129">Вінницька область</a>'
    )
    opts = parse_region_filter_options(snippet)
    assert len(opts) == 2
    assert opts[0]["obl_id"] == 862
    assert "Волинська" in opts[0]["region_name"]


def test_parse_list_max_page():
    html = (
        '<ul class="pagination">'
        '<li><a data-pages="?citySPG=0">1</a></li>'
        '<li><a data-pages="?citySPG=68">69</a></li>'
        '<li class="withinput"><form class="gotopage">'
        '<input name="page" min="1" max="69" type="number"></form></li>'
        "</ul>"
    )
    assert parse_list_max_page(html) == 69


def test_parse_list_max_page_ajax_window_not_total():
    """AJAX-фрагмент показує лише стор. 1–5, але input max=69 — беремо 69."""
    html = (
        '<ul class="pagination">'
        '<li><a data-pages="?citySPG=0">1</a></li>'
        '<li><a data-pages="?citySPG=4">5</a></li>'
        '<form class="gotopage"><input name="page" min="1" max="69" type="number"></form>'
        "</ul>"
    )
    assert parse_list_max_page(html) == 69
