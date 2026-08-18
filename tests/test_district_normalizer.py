# -*- coding: utf-8 -*-
import unittest

from utils.district_normalizer import (
    extract_district_from_query,
    is_obsolete_kyiv_sviatoshyn_rayon,
    normalize_district_for_kyiv,
    sanitize_llm_address_districts,
    split_city_and_district,
)
from utils.address_geo_enrichment import format_address_line_from_llm


class TestDistrictNormalizer(unittest.TestCase):
    def test_obsolete_name_detected(self):
        self.assertTrue(is_obsolete_kyiv_sviatoshyn_rayon("Києво-Святошинський"))
        self.assertTrue(is_obsolete_kyiv_sviatoshyn_rayon("Києво Святошинський р."))
        self.assertFalse(is_obsolete_kyiv_sviatoshyn_rayon("Святошинський"))

    def test_extract_does_not_confuse_oblast_with_city(self):
        self.assertIsNone(
            extract_district_from_query("Києво-Святошинський район Київської області", city="Київ")
        )
        self.assertEqual(
            extract_district_from_query("Святошинський район Києва", city="Київ"),
            "Святошинський",
        )

    def test_normalize_rejects_obsolete(self):
        self.assertIsNone(normalize_district_for_kyiv("Києво-Святошинський"))
        self.assertEqual(normalize_district_for_kyiv("Святошинський"), "Святошинський")

    def test_sanitize_kyiv_city(self):
        out = sanitize_llm_address_districts(
            {
                "region": "Київська",
                "settlement": "Київ",
                "district": "Києво-Святошинський",
            }
        )
        self.assertEqual(out.get("settlement_district"), "Святошинський")
        self.assertIsNone(out.get("district"))
        # Київська → місто Київ
        self.assertEqual(out.get("region"), "місто Київ")

    def test_sanitize_kyiv_city_rewrites_oblast_region(self):
        out = sanitize_llm_address_districts(
            {
                "region": "Київська область",
                "settlement": "Київ",
                "district": "Печерський",
                "street": "Предславинська",
            }
        )
        self.assertEqual(out.get("region"), "місто Київ")
        self.assertEqual(out.get("settlement"), "Київ")

    def test_sanitize_oblast_settlement(self):
        out = sanitize_llm_address_districts(
            {
                "region": "Київська",
                "settlement": "Ірпінь",
                "district": "Києво-Святошинський",
            }
        )
        self.assertEqual(out.get("district"), "Бучанський")

    def test_split_city_and_district(self):
        city, dist = split_city_and_district("Київ, Києво-Святошинський")
        self.assertEqual(city, "Київ")
        self.assertEqual(dist, "Святошинський")

    def test_geocode_line_sanitized(self):
        line = format_address_line_from_llm(
            {
                "region": "Київська",
                "settlement": "Київ",
                "district": "Києво-Святошинський",
                "street": "Святошинська",
            }
        )
        self.assertNotIn("Києво-Святошинський", line)
        self.assertIn("Святошинський", line)
        self.assertIn("Київ", line)


if __name__ == "__main__":
    unittest.main()
