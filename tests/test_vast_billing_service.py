# -*- coding: utf-8 -*-
"""Тести підрахунку Vast billing charges (UTC-доби та діапазони)."""

import unittest
from datetime import date

from business.services.vast_billing_service import (
    _utc_day_unix_bounds,
    sum_billed_usd_last_n_calendar_days,
    sum_vast_billing_day_rows_usd,
    sum_vast_billing_range_rows_usd,
)


class TestVastBillingService(unittest.TestCase):
    def test_day_rows_sum_amount_from_api_window(self):
        # Для /charges/ з day-filter amount вже відноситься до запитаного вікна.
        gte, lte = _utc_day_unix_bounds(date(2026, 5, 1))
        day_len = lte - gte + 1
        row = {
            "type": "instance",
            "source": "instance-1",
            "start": gte,
            "end": gte + 4 * day_len - 1,
            "amount": 40.0,
            "items": [
                {
                    "type": "gpu",
                    "start": gte,
                    "end": gte + 4 * day_len - 1,
                    "amount": 40.0,
                }
            ],
        }
        d = date(2026, 5, 1)
        one_day = sum_vast_billing_day_rows_usd(iter([row]), day=d)
        self.assertAlmostEqual(one_day, 40.0, places=6)

    def test_range_dedupes_duplicate_contract_rows(self):
        rows = [
            {"type": "instance", "source": "instance-1", "start": 1, "end": 2, "amount": 10.0},
            {"type": "instance", "source": "instance-1", "start": 1, "end": 2, "amount": 10.0},
        ]
        self.assertAlmostEqual(sum_vast_billing_range_rows_usd(iter(rows)), 10.0, places=6)

    def test_sum_last_n_calendar_days_by_utc_end_date(self):
        billed = {"2026-05-01": 1.0, "2026-05-31": 2.0}
        end = date(2026, 5, 31)
        # Рівно 2 календарні дні: 31-е та 30-е (немає в dict → 0)
        self.assertAlmostEqual(
            sum_billed_usd_last_n_calendar_days(billed, n=2, end_date=end),
            2.0,
            places=6,
        )

    def test_sum_last_n_includes_missing_days_as_zero(self):
        billed = {"2026-06-01": 5.0}
        end = date(2026, 6, 3)
        self.assertAlmostEqual(
            sum_billed_usd_last_n_calendar_days(billed, n=3, end_date=end),
            5.0,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
