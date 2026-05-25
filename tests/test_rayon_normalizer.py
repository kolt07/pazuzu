# -*- coding: utf-8 -*-
"""Тести нормалізації назв районів області та geo_circles."""

import unittest

from utils.rayon_normalizer import (
    build_geo_circle_scope_bucket,
    format_oblast_rayon_display_name,
    normalize_oblast_rayon_key,
)


class RayonNormalizerTests(unittest.TestCase):
    def test_oblast_rayon_key(self):
        self.assertEqual(
            normalize_oblast_rayon_key("Дрогобицький район"),
            normalize_oblast_rayon_key("Дрогобицький р-н"),
        )
        self.assertEqual(normalize_oblast_rayon_key("Києво-Святошинський р."), "києво-святошинський")

    def test_geo_circle_scope(self):
        self.assertEqual(
            build_geo_circle_scope_bucket("r1", None, None),
            "r:r1",
        )
        self.assertEqual(
            build_geo_circle_scope_bucket("r1", "c1", None),
            "r:r1|c:c1",
        )

    def test_display(self):
        self.assertEqual(
            format_oblast_rayon_display_name("ЛЬВІВСЬКИЙ РАЙОН"),
            "Львівський",
        )


if __name__ == "__main__":
    unittest.main()
