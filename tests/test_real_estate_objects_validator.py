# -*- coding: utf-8 -*-
"""Тести фільтрації ОНМ після LLM-екстракції."""

import unittest

from utils.real_estate_objects_validator import (
    extract_declared_object_count,
    filter_extracted_objects,
    object_is_grounded_in_evidence,
)
from utils.listing_regex_extractor import extract_cadastral_numbers


class RealEstateObjectsValidatorTests(unittest.TestCase):
    def test_declared_plot_count(self):
        title = "Продам або обміняю на автомобіль є 5 ділянок"
        self.assertEqual(extract_declared_object_count(title), 5)

    def test_filters_hallucinated_buildings_for_vague_land_listing(self):
        evidence = "Продам або обміняю на автомобіль є 5 ділянок\nЛьвівська область, с. Підгірне"
        llm_objects = [
            {
                "type": "building",
                "description": "Об'єкт 1",
                "address": {
                    "region": "Львівська",
                    "settlement": "Підгірне",
                    "street": "Шкільна",
                    "building": "12",
                },
            },
            {
                "type": "building",
                "description": "Об'єкт 2",
                "address": {
                    "region": "Львівська",
                    "settlement": "Підгірне",
                    "street": "Центральна",
                    "building": "5",
                },
            },
        ]
        filtered = filter_extracted_objects(llm_objects, evidence)
        self.assertEqual(filtered, [])

    def test_keeps_land_with_cadastral_in_text(self):
        cad = "6320685503:03:000:0202"
        evidence = f"Продаю дві ділянки. Кадастрові номери: {cad} та 6320685503:03:000:0203"
        llm_objects = [
            {"type": "land_plot", "description": "Ділянка біля лісу", "cadastral_number": cad},
            {
                "type": "land_plot",
                "description": "Вигадана ділянка",
                "cadastral_number": "1111111111:11:111:1111",
            },
        ]
        filtered = filter_extracted_objects(llm_objects, evidence)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["cadastral_number"], cad)

    def test_keeps_building_when_street_in_text(self):
        evidence = "Офіс на вул. Хрещатик, 15, м. Київ. Площа 120 м²."
        obj = {
            "type": "building",
            "description": "Офісне приміщення",
            "address": {
                "settlement": "Київ",
                "street": "Хрещатик",
                "building": "15",
            },
        }
        self.assertTrue(object_is_grounded_in_evidence(obj, evidence))
        self.assertEqual(len(filter_extracted_objects([obj], evidence)), 1)

    def test_caps_to_declared_count(self):
        cad1 = "6320685503:03:000:0201"
        cad2 = "6320685503:03:000:0202"
        evidence = f"Продаю 2 ділянки. Кадастрові номери: {cad1}, {cad2}"
        llm_objects = [
            {"type": "land_plot", "cadastral_number": cad1, "description": "Ділянка 1"},
            {"type": "land_plot", "cadastral_number": cad2, "description": "Ділянка 2"},
            {
                "type": "land_plot",
                "cadastral_number": "6320685503:03:000:0999",
                "description": "Ділянка 3",
            },
        ]
        filtered = filter_extracted_objects(llm_objects, evidence)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(
            {o["cadastral_number"] for o in filtered},
            {cad1, cad2},
        )


if __name__ == "__main__":
    unittest.main()
