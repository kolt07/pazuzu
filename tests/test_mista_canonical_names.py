# -*- coding: utf-8 -*-
"""Канонічна назва НП з mista: list vs URL/detail."""

from utils.settlement_normalizer import resolve_mista_canonical_and_aliases


def test_dnipro_list_beats_url_slug():
    url = "https://mista.ua/Україна/Дніпропетровська_область/Дніпропетровський_район/дніпропетровськ"
    canon, aliases = resolve_mista_canonical_and_aliases(
        list_name="Дніпро",
        detail_name="Дніпропетровськ",
        mista_url=url,
    )
    assert canon == "Дніпро"
    assert "дніпро" in aliases
    assert "дніпропетровськ" in aliases


def test_kamyanske_list_beats_url():
    url = "https://mista.ua/Україна/Дніпропетровська_область/Пятихатський_район/Дніпродзержинськ"
    canon, aliases = resolve_mista_canonical_and_aliases(
        list_name="Кам'янське",
        detail_name="Дніпродзержинськ",
        mista_url=url,
    )
    assert canon == "Кам'янське"
    assert "дніпродзержинськ" in aliases


def test_fallback_to_url_when_list_empty():
    url = "https://mista.ua/Україна/Дніпропетровська_область/Дніпропетровський_район/дніпропетровськ"
    canon, aliases = resolve_mista_canonical_and_aliases(
        list_name="",
        detail_name="",
        mista_url=url,
    )
    assert canon == "Дніпропетровськ"
    assert "дніпропетровськ" in aliases
