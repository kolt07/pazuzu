# -*- coding: utf-8 -*-
"""
Сервіс реєстру стратегій: fingerprint запиту, оновлення EWMA-score, top-K retrieval.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config.settings import Settings

from data.repositories.strategy_registry_repository import StrategyRegistryRepository

logger = logging.getLogger(__name__)


def normalize_query_for_fingerprint(query: str) -> str:
    q = (query or "").lower().strip()
    q = re.sub(r"\s+", " ", q)
    return q[:4000]


def task_fingerprint_from_query(query: str) -> str:
    """Стабільний короткий відбиток тексту запиту."""
    n = normalize_query_for_fingerprint(query)
    return hashlib.sha256(n.encode("utf-8")).hexdigest()[:32]


def strategy_signature_from_candidate(candidate: Dict[str, Any]) -> str:
    tools = sorted(str(t) for t in (candidate.get("planned_tools") or []) if str(t).strip())
    sid = str(candidate.get("strategy_id") or candidate.get("name") or "unknown")
    h = hashlib.sha256((sid + "|" + ",".join(tools)).encode("utf-8")).hexdigest()[:24]
    return h


class StrategyRegistryService:
    """Читання/оновлення реєстру стратегій після завершення дослідження."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._repo = StrategyRegistryRepository()
        try:
            self._repo.ensure_indexes()
        except Exception:
            logger.debug("strategy registry ensure_indexes failed (non-fatal)", exc_info=True)

    def fingerprint(self, user_query: str) -> str:
        return task_fingerprint_from_query(user_query)

    @staticmethod
    def format_hints_for_prompt(hints: List[Dict[str, Any]]) -> str:
        if not hints:
            return "(немає збережених стратегій для схожого відбитка запиту)"
        lines: List[str] = []
        for i, h in enumerate(hints[:8], start=1):
            sid = str(h.get("strategy_id") or "")
            name = str(h.get("strategy_name") or h.get("name") or "")
            roll = h.get("rolling_score")
            tools = h.get("strategy_template", {}).get("planned_tools") or h.get("planned_tools") or []
            tprev = ", ".join(str(t) for t in tools[:6])
            lines.append(
                f"{i}. id={sid} name={name} rolling_score={roll} tools=[{tprev}]"
            )
        return "\n".join(lines)

    def retrieve_hints_for_planner(
        self,
        user_query: str,
        *,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Повертає записи для підказки Planner (історичні кращі стратегії)."""
        if not getattr(self.settings, "flx_strategy_registry_enabled", False):
            return []
        fp = self.fingerprint(user_query)
        rows = self._repo.find_by_task_fingerprint(fp, limit=max(4, top_k))
        if len(rows) >= top_k:
            return rows[:top_k]
        extra = self._repo.find_global_top(limit=top_k)
        seen = {(r.get("strategy_signature"), r.get("strategy_id")) for r in rows}
        for r in extra:
            key = (r.get("strategy_signature"), r.get("strategy_id"))
            if key not in seen:
                rows.append(r)
                seen.add(key)
            if len(rows) >= top_k:
                break
        return rows[:top_k]

    def record_session_outcome(
        self,
        *,
        user_query: str,
        ranked_results: List[Dict[str, Any]],
        winner_strategy_id: Optional[str] = None,
    ) -> None:
        """Оновлює EWMA / лічильники після ранжування стратегій."""
        if not getattr(self.settings, "flx_strategy_registry_enabled", False):
            return
        if not ranked_results:
            return

        fp = self.fingerprint(user_query)
        alpha = float(getattr(self.settings, "flx_strategy_registry_ewma_alpha", 0.35))

        for rank, row in enumerate(ranked_results):
            sid = str(row.get("strategy_id") or "")
            sig = str(row.get("strategy_signature") or "")
            if not sid or not sig:
                continue
            score = float(row.get("score") or 0.0)
            tool_calls = row.get("tool_calls") or []
            tc_count = len(tool_calls) if isinstance(tool_calls, list) else 0
            cost_eff = 1.0 / max(1, tc_count)
            is_win = winner_strategy_id and sid == winner_strategy_id

            existing_list = self._repo.find_by_task_fingerprint(fp, limit=50)
            existing = next(
                (x for x in existing_list if x.get("strategy_signature") == sig),
                None,
            )

            prev_roll = float(existing.get("rolling_score") or 0.0) if existing else score
            new_roll = alpha * score + (1 - alpha) * prev_roll

            usage = int(existing.get("usage_count") or 0) + 1
            wins = int(existing.get("wins") or 0) + (1 if is_win else 0)
            total_runs = int(existing.get("total_runs") or 0) + 1
            success_rate = wins / max(1, total_runs)

            prev_ce = float(existing.get("cost_efficiency") or cost_eff) if existing else cost_eff
            new_ce = alpha * cost_eff + (1 - alpha) * prev_ce

            template = {
                "strategy_id": sid,
                "name": row.get("name"),
                "planned_tools": row.get("planned_tools") or [],
                "hypothesis": row.get("hypothesis"),
            }

            self._repo.upsert_aggregate(
                task_fingerprint=fp,
                strategy_signature=sig,
                doc_updates={
                    "strategy_id": sid,
                    "strategy_name": str(row.get("name") or "")[:300],
                    "strategy_template": template,
                    "domain_tags": list(row.get("domain_tags") or [])[:12],
                    "rolling_score": round(new_roll, 6),
                    "success_rate": round(success_rate, 6),
                    "cost_efficiency": round(new_ce, 6),
                    "usage_count": usage,
                    "wins": wins,
                    "total_runs": total_runs,
                    "last_rank": rank + 1,
                    "last_score": round(score, 6),
                    "last_used_at": datetime.now(timezone.utc),
                    "query_exemplar": (user_query or "")[:400],
                },
            )
