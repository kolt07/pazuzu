# -*- coding: utf-8 -*-
"""Тести нормалізації та fuzzy-порівняння населених пунктів."""

import unittest

from business.services.settlement_matching_service import SettlementMatchingService, similarity_score
from utils.settlement_normalizer import (
    build_city_filter_options,
    dedupe_settlement_labels,
    extract_settlement_name,
    format_settlement_display_name,
    is_district_only_name,
    normalize_settlement_key,
    normalize_settlement_name,
)
from utils.toponym_normalizer import normalize_settlement


class SettlementNormalizerTests(unittest.TestCase):
    def test_caps_and_prefix(self):
        self.assertEqual(normalize_settlement_name("ЛЮБЕШІВ"), "Любешів")
        self.assertEqual(normalize_settlement_name("смт Любешів"), "Любешів")
        self.assertEqual(normalize_settlement_name("м. Дрогобич"), "Дрогобич")

    def test_compound_formats(self):
        self.assertEqual(
            normalize_settlement_name("Вишгородський район, смт Димер"),
            "Димер",
        )
        self.assertEqual(
            normalize_settlement_name("ХОРОВЕЦЬКА/С.ХОРОВЕЦЬ"),
            "Хоровець",
        )
        self.assertEqual(
            normalize_settlement_name("ДНІПРОПЕТРОВСЬКА ОБЛАСТЬ/М.ДНІПРО"),
            "Дніпро",
        )

    def test_same_key_for_variants(self):
        variants = ["Любешів", "ЛЮБЕШІВ", "смт Любешів"]
        keys = {normalize_settlement_key(v) for v in variants}
        self.assertEqual(len(keys), 1)
        self.assertEqual(keys.pop(), "любешів")

    def test_dedupe_settlement_labels(self):
        labels = dedupe_settlement_labels(["Боголюби", "ЛЮБЕШІВ", "Любешів"])
        self.assertEqual(labels, ["Боголюби", "Любешів"])

    def test_dedupe_strips_population_suffix(self):
        labels = dedupe_settlement_labels(["Іваничі (6 918 ос.)", "Іваничі"])
        self.assertEqual(labels, ["Іваничі"])

    def test_build_city_filter_options_population_before_name_in_region(self):
        docs = [
            {"_id": "1", "name": "Якушів", "region_id": "r1", "population": 100},
            {"_id": "2", "name": "Іваничі", "region_id": "r1", "population": 6918},
        ]
        opts = build_city_filter_options(docs, region_name="Волинська область")
        names = [o["name"] for o in opts]
        self.assertEqual(names[0], "Іваничі")

    def test_build_city_filter_options_disambiguate_sorts_by_name(self):
        docs = [
            {"_id": "1", "name": "Берестечко", "region_id": "r1"},
            {"_id": "2", "name": "Антонівка", "region_id": "r2"},
        ]
        opts = build_city_filter_options(docs, disambiguate_region=True)
        names = [o["name"] for o in opts]
        self.assertEqual(names, ["Антонівка", "Берестечко"])

    def test_district_only(self):
        self.assertTrue(is_district_only_name("Дрогobицький район"))
        self.assertFalse(is_district_only_name("Вишгородський район, смт Димер"))

    def test_toponym_settlement_inflection(self):
        self.assertEqual(normalize_settlement("у Києві"), "Київ")
        self.assertEqual(normalize_settlement("в Львові"), "Львів")


class SettlementMatchingTests(unittest.TestCase):
    def test_fuzzy_score(self):
        self.assertEqual(similarity_score("Хоровець", "ХОРОВЕЦЬ"), 1.0)
        self.assertEqual(similarity_score("м. Дрогобич", "Дрогобич"), 1.0)

    def test_find_best_match(self):
        matcher = SettlementMatchingService()
        candidates = [
            {"_id": "1", "region_id": "r1", "name": "Любешів", "name_normalized": "любешів"},
            {"_id": "2", "region_id": "r1", "name": "Одеса", "name_normalized": "одеса"},
        ]
        match = matcher.find_best_match("ЛЮБЕШІВ", "r1", candidates)
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(match.match_type, "exact_key")
        self.assertEqual(match.city["name"], "Любешів")


if __name__ == "__main__":
    unittest.main()
