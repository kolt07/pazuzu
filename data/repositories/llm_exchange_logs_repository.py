# -*- coding: utf-8 -*-
"""
Репозиторій для колекції llm_exchange_logs — повні запити та відповіді до LLM (Gemini, Ollama тощо).
"""

import hashlib
from typing import Optional, Dict, Any, List
from datetime import datetime
from data.repositories.base_repository import BaseRepository


class LLMExchangeLogsRepository(BaseRepository):
    """Репозиторій для логів обміну з LLM (окрема колекція)."""

    COLLECTION_NAME = "llm_exchange_logs"

    def __init__(self):
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("timestamp")
            self.collection.create_index("source")
            self.collection.create_index("provider")
            self.collection.create_index([("timestamp", -1)])
            self.collection.create_index([("provider", 1), ("timestamp", -1)])
            self._indexes_created = True
        except Exception:
            pass

    def add(
        self,
        request_text: str,
        response_text: str,
        input_tokens: int,
        output_tokens: int,
        source: str,
        request_id: Optional[str] = None,
        initiator: Optional[str] = None,
        provider: Optional[str] = None,
        duration_ms: Optional[int] = None,
        gpu_seconds: Optional[float] = None,
        gpu_cost_usd: Optional[float] = None,
    ) -> str:
        """
        Зберігає один запис обміну запит–відповідь LLM.

        Args:
            request_text: Повний текст запиту (промпт / серіалізовані повідомлення).
            response_text: Повний текст відповіді LLM.
            input_tokens: Кількість вхідних токенів.
            output_tokens: Кількість вихідних токенів.
            source: Джерело виклику (напр. langchain_agent_main, llm_service.parse_auction_description).
            request_id: Ідентифікатор запиту (correlation id).
            initiator: Ініціатор (user_id або 'system').
            provider: Провайдер LLM: 'gemini', 'ollama' тощо (опційно).

        Returns:
            ID створеного запису.
        """
        self._ensure_indexes()
        req_str = (request_text or "") if isinstance(request_text, str) else str(request_text or "")
        resp_str = (response_text or "") if isinstance(response_text, str) else str(response_text or "")
        req_bytes = req_str.encode("utf-8", errors="replace")
        resp_bytes = resp_str.encode("utf-8", errors="replace")
        request_hash = hashlib.sha256(req_bytes).hexdigest()
        response_hash = hashlib.sha256(resp_bytes).hexdigest()
        document = {
            "timestamp": datetime.utcnow(),
            "source": source,
            "request_id": request_id,
            "initiator": initiator or "system",
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "request_hash": request_hash,
            "response_hash": response_hash,
            "request_text": req_str,
            "response_text": resp_str,
        }
        if provider:
            document["provider"] = provider
        if duration_ms is not None:
            document["duration_ms"] = int(duration_ms)
        if gpu_seconds is not None:
            document["gpu_seconds"] = float(gpu_seconds)
        if gpu_cost_usd is not None:
            document["gpu_cost_usd"] = float(gpu_cost_usd)
        result = self.collection.insert_one(document)
        return str(result.inserted_id)

    def find_recent(
        self,
        limit: int = 50,
        provider: Optional[str] = None,
        source: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Останні записи з опційним фільтром по provider/source."""
        self._ensure_indexes()
        filt = {}
        if provider:
            filt["provider"] = provider
        if source:
            filt["source"] = source
        return self.find_many(filter=filt, sort=[("timestamp", -1)], limit=limit)
