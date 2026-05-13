# -*- coding: utf-8 -*-
"""
Фіналізація когнітивного запиту: compression Long Chain + дистиляція в semantic memory.

Викликається best-effort після обробки запиту LangChain-агентом.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from business.domain.cognitive_agent_models import SemanticMemoryRecord
from data.repositories.agent_activity_log_repository import AgentActivityLogRepository
from data.repositories.agent_reasoning_chain_repository import AgentReasoningChainRepository
from data.repositories.agent_semantic_memory_repository import AgentSemanticMemoryRepository

logger = logging.getLogger(__name__)


class CognitiveSupervisorService:
    """Легкий supervisor: стиснення chain + збереження semantic memory."""

    def __init__(self):
        self._chain = AgentReasoningChainRepository()
        self._semantic = AgentSemanticMemoryRepository()
        self._activity = AgentActivityLogRepository()

    def compress_chain_if_needed(
        self,
        *,
        request_id: str,
        threshold: int,
        llm_invoke,
    ) -> None:
        """Якщо кроків більше threshold — саммарі старих через LLM і позначка compressed."""
        if threshold <= 0 or not request_id:
            return
        try:
            n = self._chain.count_by_request(request_id)
            if n < threshold:
                return
            steps = self._chain.list_by_request(request_id, limit=min(n, 80))
            if len(steps) < threshold:
                return
            # Беремо префікс (старі) для саммарі
            cut = max(1, len(steps) // 2)
            old = steps[:cut]
            from datetime import datetime, timezone

            boundary = old[-1].get("created_at") or datetime.now(timezone.utc)
            lines = []
            for s in old:
                lines.append(
                    f"- {s.get('action','')}: {str(s.get('observation',''))[:200]}"
                )
            prompt = (
                "Стисло (2-5 речень українською) підсумуй ці кроки міркування агента для архіву. "
                "Без вигаданих фактів, лише узагальнення:\n"
                + "\n".join(lines[:60])
            )
            summary = ""
            try:
                msg = llm_invoke(prompt)
                summary = (msg or "").strip()[:2000]
            except Exception as e:
                logger.debug("compress_chain llm: %s", e)
                summary = "Автостиснення без LLM: " + "; ".join(lines[:10])[:1800]
            self._chain.mark_compressed_before(request_id, boundary, summary)
        except Exception as e:
            logger.debug("compress_chain_if_needed: %s", e)

    def distill_and_store(
        self,
        *,
        request_id: str,
        user_id: Optional[str],
        user_query: str,
        final_answer: str,
        cognitive: Optional[Dict[str, Any]],
        llm_invoke,
    ) -> None:
        """Дистиляція у semantic memory + лог у agent_activity_log."""
        if not request_id:
            return
        try:
            cog_excerpt = ""
            if cognitive:
                cog_excerpt = json.dumps(
                    {k: cognitive.get(k) for k in ("goal", "known_facts", "completed_steps", "confidence") if cognitive.get(k)},
                    ensure_ascii=False,
                    default=str,
                )[:2500]
            prompt = (
                "Ти аналітик пам'яті. Запит користувача та відповідь асистента — збережи лише стійкі факти та евристики для майбутніх задач.\n"
                f"Запит: {user_query[:1200]}\n\nВідповідь: {final_answer[:2500]}\n\n"
                f"Внутрішній стан (JSON): {cog_excerpt}\n\n"
                "Поверни СТРОГО JSON об'єкт з ключами: "
                '`summary_uk` (string), `facts` (array of strings), `tool_heuristics` (array), `failures` (array). Українською.'
            )
            raw = llm_invoke(prompt)
            parsed: Dict[str, Any] = {}
            if raw:
                try:
                    s = raw.strip()
                    if s.startswith("```"):
                        s = s.split("\n", 1)[-1]
                        if "```" in s:
                            s = s.rsplit("```", 1)[0]
                    parsed = json.loads(s)
                except Exception:
                    parsed = {"summary_uk": (raw or "")[:2000], "facts": [], "tool_heuristics": [], "failures": []}
            rec = SemanticMemoryRecord(
                request_id=request_id,
                user_id=user_id,
                user_query_excerpt=user_query[:500],
                summary_uk=str(parsed.get("summary_uk") or "")[:4000],
                facts=list(parsed.get("facts") or []) if isinstance(parsed.get("facts"), list) else [],
                tool_heuristics=list(parsed.get("tool_heuristics") or [])
                if isinstance(parsed.get("tool_heuristics"), list)
                else [],
                failures=list(parsed.get("failures") or []) if isinstance(parsed.get("failures"), list) else [],
            )
            self._semantic.save_record(rec.to_mongo_doc())
            self._activity.log(
                request_id=request_id,
                user_id=user_id,
                agent_name="cognitive_supervisor",
                step=AgentActivityLogRepository.STEP_ACTION,
                payload={"kind": "semantic_memory_distilled", "summary_len": len(rec.summary_uk)},
            )
        except Exception as e:
            logger.debug("distill_and_store: %s", e)
