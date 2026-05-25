# -*- coding: utf-8 -*-
import unittest

from utils.rayon_city_resolver import pick_target_city_for_stub


class RayonCityResolverTests(unittest.TestCase):
    def test_fuzzy_lviv_rayon(self):
        cities = [
            {"_id": "1", "name": "Львів", "name_normalized": "львів"},
            {"_id": "2", "name": "Буск", "name_normalized": "буск"},
        ]
        t, score, reason = pick_target_city_for_stub(
            cities,
            region_name="Львівська",
            rayon_display_name="Львівський район",
        )
        self.assertIsNotNone(t)
        assert t is not None
        self.assertEqual(t["name"], "Львів")
        self.assertGreaterEqual(score, 0.78)

    def test_fallback_kyiv_village(self):
        cities = [
            {"_id": "1", "name": "Ірпінь", "name_normalized": "ірпінь"},
            {"_id": "2", "name": "Васильків", "name_normalized": "васильків"},
        ]
        t, score, reason = pick_target_city_for_stub(
            cities,
            region_name="Київська",
            rayon_display_name="Києво-Святошинський р.",
        )
        self.assertIsNotNone(t)
        self.assertEqual(t["name"], "Ірпінь")
        self.assertEqual(reason, "fallback")


if __name__ == "__main__":
    unittest.main()
