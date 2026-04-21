# -*- coding: utf-8 -*-
"""
Витрати Vast.ai з офіційного billing API: GET /api/v0/charges/ (per-instance GPU/storage/bandwidth).

Документація: https://docs.vast.ai/api-reference/billing/show-charges
(окремо від /api/v1/invoices/ — там поповнення Stripe тощо).
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from business.services.vast_ai_client import VastAiClient


def _norm_instance_source(instance_id: str) -> str:
    return f"instance-{str(instance_id).strip()}"


def iter_charge_rows(
    client: VastAiClient,
    select_filters: Dict[str, Any],
    *,
    max_pages: int = 200,
) -> Iterator[Dict[str, Any]]:
    """Пагінація results до вичерпання next_token."""
    token: Optional[str] = None
    for _ in range(max(1, int(max_pages))):
        data = client.list_charges(
            select_filters,
            after_token=token,
            limit=500,
        )
        if isinstance(data, dict) and data.get("success") is False:
            msg = str(data.get("msg") or data.get("error") or "charges_failed")
            raise RuntimeError(msg)
        rows = data.get("results") if isinstance(data, dict) else None
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    yield row
        token = data.get("next_token") if isinstance(data, dict) else None
        if not token:
            break


def sum_instance_rows_usd(rows: Iterator[Dict[str, Any]], *, instance_id: Optional[str] = None) -> float:
    want_source = _norm_instance_source(instance_id) if instance_id else None
    total = 0.0
    for row in rows:
        if str(row.get("type") or "").lower() != "instance":
            continue
        src = str(row.get("source") or "").strip()
        if want_source is not None and src != want_source:
            continue
        try:
            total += float(row.get("amount") or 0.0)
        except (TypeError, ValueError):
            continue
    return round(total, 6)


def fetch_instance_contract_charges_usd(
    api_key: str,
    instance_id: str,
    unix_gte: int,
    unix_lte: int,
    *,
    timeout_sec: int = 90,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Сума charges для контракту інстанса у вікні часу [unix_gte, unix_lte] (UTC epoch seconds).

    Повертає (сума USD, None) або (None, повідомлення_про_помилку).
    """
    key = (api_key or "").strip()
    if not key:
        return None, "vast_api_key_empty"
    filters: Dict[str, Any] = {
        "day": {"gte": int(unix_gte), "lte": int(unix_lte)},
        "type": {"in": ["instance"]},
    }
    try:
        client = VastAiClient(api_key=key, timeout_sec=timeout_sec)
        total = sum_instance_rows_usd(iter_charge_rows(client, filters), instance_id=instance_id)
        return total, None
    except Exception as e:
        return None, str(e)


def fetch_gpu_instance_charges_by_calendar_day_usd(
    api_key: str,
    *,
    days: int,
    sleep_between_sec: float = 0.06,
    timeout_sec: int = 60,
) -> Tuple[Dict[str, float], Optional[str]]:
    """
    Для кожної календарної дати UTC (останні ``days`` днів) — сума instance-charges за цей день.

    Окремий запит на день (вимога API: фільтр day з gte/lte).
    """
    key = (api_key or "").strip()
    out: Dict[str, float] = {}
    if not key:
        return {}, "vast_api_key_empty"
    d0 = datetime.now(timezone.utc).date()
    last_err: Optional[str] = None
    try:
        client = VastAiClient(api_key=key, timeout_sec=timeout_sec)
        for i in range(max(1, int(days))):
            current = d0 - timedelta(days=(days - 1 - i))
            gte, lte = _utc_day_unix_bounds(current)
            filters: Dict[str, Any] = {
                "day": {"gte": gte, "lte": lte},
                "type": {"in": ["instance"]},
            }
            day_total = 0.0
            try:
                day_total = sum_instance_rows_usd(iter_charge_rows(client, filters), instance_id=None)
            except Exception as e:
                last_err = str(e)
                day_total = 0.0
            out[current.strftime("%Y-%m-%d")] = day_total
            if sleep_between_sec > 0:
                time.sleep(float(sleep_between_sec))
        return out, last_err
    except Exception as e:
        return out, str(e)


def _utc_day_unix_bounds(d: date) -> Tuple[int, int]:
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end_inclusive = start + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp()), int(end_inclusive.timestamp())


def sum_billed_usd_last_n_calendar_days(
    billed_by_day: Dict[str, float],
    *,
    n: int,
) -> float:
    """Сума останніх n календарних днів за ключами YYYY-MM-DD (UTC у billed_by_day)."""
    if not billed_by_day or n <= 0:
        return 0.0
    keys = sorted(billed_by_day.keys())
    tail = keys[-min(n, len(keys)) :]
    return round(sum(float(billed_by_day.get(k) or 0.0) for k in tail), 6)
