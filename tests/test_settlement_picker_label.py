# -*- coding: utf-8 -*-
"""Підписи autocomplete НП: «Назва, Область обл.»."""

from utils.settlement_normalizer import (
    format_settlement_picker_label,
    parse_settlement_picker_label,
    resolve_region_from_picker_fragment,
)


def test_format_settlement_picker_label():
    assert format_settlement_picker_label("Іваничі", "Волинська область") == (
        "Іваничі, Волинська обл."
    )


def test_parse_and_resolve_picker_label():
    name, reg_frag = parse_settlement_picker_label("Іваничі, Волинська обл.")
    assert name == "Іваничі"
    assert reg_frag == "Волинська обл."
    assert resolve_region_from_picker_fragment(reg_frag) == "Волинська область"


def test_homonym_picker_labels_differ():
    vol = format_settlement_picker_label("Іваничі", "Волинська область")
    riv = format_settlement_picker_label("Іваничі", "Рівненська область")
    assert vol != riv
    assert "Волинська" in vol
    assert "Рівненська" in riv
