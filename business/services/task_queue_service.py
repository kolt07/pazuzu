# -*- coding: utf-8 -*-
"""
Сервіс диспетчеризації та моніторингу черг RabbitMQ/Celery.
"""

from __future__ import annotations

import logging
import socket
import time
from typing import Any, Dict, Iterable, List, Optional

try:
    from celery.result import AsyncResult
except ImportError:  # pragma: no cover - Celery is optional in local fallback mode.
    AsyncResult = None

from config.settings import Settings
from data.repositories.background_task_repository import BackgroundTaskRepository
from data.repositories.task_queue_controls_repository import TaskQueueControlsRepository

logger = logging.getLogger(__name__)


class TaskQueueService:
    """Єдина точка enqueue/status для source-load та LLM processing."""

    SOURCE_LOAD_QUEUE = "source_load"
    LLM_QUEUE = "llm_processing"
    ACTIVE_STATES = ["queued", "received", "started", "running", "retry"]
    TERMINAL_STATES = {"success", "failed", "revoked"}
    CONTROL_STATES = {"running", "paused", "disabled"}

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or Settings()
        self._repo = BackgroundTaskRepository()
        self._controls = TaskQueueControlsRepository()
        self._celery = None
        try:
            from business.celery_app import create_celery_app
            self._celery = create_celery_app(self.settings)
        except Exception:
            self._celery = None

    def is_enabled(self) -> bool:
        return bool(getattr(self.settings, "task_queue_enabled", False) and self.settings.task_queue_broker_url and self._celery is not None)

    def _rabbitmq_passive_queue_message_count(self, queue_name: str) -> Optional[int]:
        """Кількість повідомлень у черзі RabbitMQ (passive declare)."""
        url = (getattr(self.settings, "task_queue_broker_url", None) or "").strip()
        if not url:
            return None
        try:
            from kombu import Connection

            with Connection(url, connect_timeout=4) as conn:
                conn.ensure_connection(max_retries=2)
                ch = conn.channel()
                try:
                    declare_ok = ch.queue_declare(queue=queue_name, passive=True)
                finally:
                    try:
                        ch.close()
                    except Exception:
                        pass
            if declare_ok is None:
                return None
            mc = getattr(declare_ok, "message_count", None)
            if mc is not None:
                return int(mc)
            if isinstance(declare_ok, (tuple, list)) and len(declare_ok) >= 2:
                return int(declare_ok[1])
        except Exception as e:
            logger.debug("Passive queue_declare для %s: %s", queue_name, e)
        return None

    def get_llm_queue_snapshot(self) -> Dict[str, Any]:
        """Орієнтир черги llm_processing: Rabbit + активні записи у Mongo."""
        snap: Dict[str, Any] = {
            "rabbit_messages": None,
            "mongo_active_tasks": None,
            "mongo_queued_only": None,
        }
        try:
            snap["rabbit_messages"] = self._rabbitmq_passive_queue_message_count(self.LLM_QUEUE)
        except Exception:
            pass
        try:
            snap["mongo_active_tasks"] = int(
                self._repo.count_by_queue_states(self.LLM_QUEUE, self.ACTIVE_STATES)
            )
        except Exception:
            pass
        try:
            snap["mongo_queued_only"] = int(
                self._repo.count_by_queue_states(self.LLM_QUEUE, ["queued", "received"])
            )
        except Exception:
            pass
        return snap

    def get_task_doc(self, task_id: str) -> Optional[Dict[str, Any]]:
        return self._repo.get_by_task_id(str(task_id or "").strip())

    def get_llm_batch_progress(self, batch_id: str, total_hint: Optional[int] = None) -> Dict[str, int]:
        bid = str(batch_id or "").strip()
        if not bid:
            total = int(total_hint or 0)
            return {"total": max(0, total), "processed": 0, "success": 0, "failed": 0, "in_progress": max(0, total)}
        total = self._repo.count_by_batch_id(bid)
        if total_hint is not None:
            total = max(total, int(total_hint))
        success = self._repo.count_by_batch_id(bid, states=["success"])
        failed = self._repo.count_by_batch_id(bid, states=["failed", "revoked"])
        processed = success + failed
        in_progress = max(0, total - processed)
        return {
            "total": int(total),
            "processed": int(processed),
            "success": int(success),
            "failed": int(failed),
            "in_progress": int(in_progress),
        }

    def enqueue_source_load(
        self,
        *,
        days: Optional[int],
        sources: Optional[List[str]],
        regions: Optional[List[str]] = None,
        listing_types: Optional[List[str]] = None,
        use_browser_olx: Optional[bool] = None,
        olx_phase1_max_threads: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if self.get_queue_control_state(self.SOURCE_LOAD_QUEUE) == "disabled":
            raise RuntimeError("Queue source_load is disabled by admin.")
        payload = {
            "days": days,
            "sources": list(sources or []),
            "regions": list(regions or []),
            "listing_types": list(listing_types or []),
            "use_browser_olx": use_browser_olx,
            "olx_phase1_max_threads": olx_phase1_max_threads,
        }
        if self._celery is None:
            raise RuntimeError("Task queue is not available because Celery is not installed.")
        async_result = self._celery.send_task(
            "business.tasks.run_source_load_pipeline_task",
            kwargs=payload,
            queue=self.SOURCE_LOAD_QUEUE,
        )
        self._repo.register_task(
            async_result.id,
            "run_source_load_pipeline_task",
            self.SOURCE_LOAD_QUEUE,
            payload=payload,
            metadata=metadata or {},
        )
        return {"task_id": async_result.id, "queue": self.SOURCE_LOAD_QUEUE}

    def enqueue_olx_llm(self, listing_url: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        if self.get_queue_control_state(self.LLM_QUEUE) == "disabled":
            raise RuntimeError("Queue llm_processing is disabled by admin.")
        payload = {"listing_url": listing_url}
        if self._celery is None:
            raise RuntimeError("Task queue is not available because Celery is not installed.")
        async_result = self._celery.send_task(
            "business.tasks.process_olx_llm_task",
            kwargs=payload,
            queue=self.LLM_QUEUE,
        )
        self._repo.register_task(
            async_result.id,
            "process_olx_llm_task",
            self.LLM_QUEUE,
            payload=payload,
            metadata=metadata or {},
        )
        return async_result.id

    def enqueue_prozorro_llm(self, auction_id: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        if self.get_queue_control_state(self.LLM_QUEUE) == "disabled":
            raise RuntimeError("Queue llm_processing is disabled by admin.")
        payload = {"auction_id": auction_id}
        if self._celery is None:
            raise RuntimeError("Task queue is not available because Celery is not installed.")
        async_result = self._celery.send_task(
            "business.tasks.process_prozorro_llm_task",
            kwargs=payload,
            queue=self.LLM_QUEUE,
        )
        self._repo.register_task(
            async_result.id,
            "process_prozorro_llm_task",
            self.LLM_QUEUE,
            payload=payload,
            metadata=metadata or {},
        )
        return async_result.id

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        doc = self._repo.get_by_task_id(task_id)
        async_result = AsyncResult(task_id, app=self._celery) if (AsyncResult and self._celery is not None) else None
        state = str((async_result.state if async_result else None) or (doc or {}).get("state") or "PENDING").lower()
        response = {
            "task_id": task_id,
            "state": state,
            "ready": async_result.ready() if async_result else state in self.TERMINAL_STATES,
            "successful": async_result.successful() if async_result and async_result.ready() else state == "success",
        }
        if doc:
            response["task"] = doc
        try:
            if async_result and async_result.ready():
                response["result"] = async_result.result
        except Exception as e:
            response["error"] = str(e)
        return response

    def wait_for_all(
        self,
        task_ids: Iterable[str],
        timeout_sec: int = 3600,
        poll_interval_sec: float = 2.0,
        heartbeat_fn=None,
        progress_fn=None,
    ) -> List[Dict[str, Any]]:
        ids = [str(task_id).strip() for task_id in list(task_ids or []) if str(task_id).strip()]
        if not ids:
            return []
        deadline = time.time() + max(1, int(timeout_sec))
        while time.time() < deadline:
            docs = self._repo.list_by_task_ids(ids)
            by_id = {doc.get("task_id"): doc for doc in docs}
            if callable(progress_fn):
                progress_fn(docs, ids)
            if all(str((by_id.get(task_id) or {}).get("state") or "").lower() in self.TERMINAL_STATES for task_id in ids):
                return docs
            if callable(heartbeat_fn):
                heartbeat_fn()
            time.sleep(max(0.5, float(poll_interval_sec)))
        raise TimeoutError(f"Timed out waiting for tasks: {', '.join(ids)}")

    def has_pending_llm_tasks(self) -> bool:
        return self._repo.count_by_queue_states(self.LLM_QUEUE, self.ACTIVE_STATES) > 0

    def get_queue_control_state(self, queue_name: str) -> str:
        doc = self._controls.get_control(str(queue_name or "").strip())
        state = str((doc or {}).get("state") or "running").strip().lower()
        if state not in self.CONTROL_STATES:
            return "running"
        return state

    def set_queue_control_state(
        self,
        queue_name: str,
        state: str,
        updated_by: Optional[str] = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        st = str(state or "").strip().lower()
        if st not in self.CONTROL_STATES:
            raise ValueError(f"Unsupported queue state: {state}")
        qn = str(queue_name or "").strip()
        if qn not in (self.SOURCE_LOAD_QUEUE, self.LLM_QUEUE):
            raise ValueError(f"Unsupported queue: {queue_name}")
        return self._controls.set_control(qn, st, updated_by=updated_by, reason=reason)

    def get_queues_status_snapshot(self) -> Dict[str, Any]:
        queues = [self.SOURCE_LOAD_QUEUE, self.LLM_QUEUE]
        out: Dict[str, Any] = {"task_queue_enabled": self.is_enabled(), "queues": []}
        for queue_name in queues:
            state = self.get_queue_control_state(queue_name)
            counts = self._repo.get_queue_state_counts(queue_name)
            latest = self._repo.get_latest_task_for_queue(queue_name) or {}
            out["queues"].append(
                {
                    "queue_name": queue_name,
                    "control_state": state,
                    "counts": counts,
                    "rabbit_messages": self._rabbitmq_passive_queue_message_count(queue_name),
                    "latest_task": {
                        "task_id": latest.get("task_id"),
                        "state": latest.get("state"),
                        "updated_at": latest.get("updated_at"),
                        "message": latest.get("message") or latest.get("error") or "",
                    }
                    if latest
                    else None,
                }
            )
        return out

    def has_active_source_load_tasks(self, within_sec: int = 20 * 60) -> bool:
        return self._repo.has_recent_activity(
            self.SOURCE_LOAD_QUEUE,
            ["running", "started"],
            within_seconds=within_sec,
        )

    def mark_task_started(self, task_id: str) -> None:
        self._repo.mark_started(task_id, worker_id=self._worker_id())

    def heartbeat(self, task_id: str, patch: Optional[Dict[str, Any]] = None) -> None:
        self._repo.heartbeat(task_id, patch=patch)

    def mark_task_success(self, task_id: str, result: Optional[Dict[str, Any]] = None) -> None:
        self._repo.mark_finished(task_id, "success", result=result or {})

    def mark_task_failed(self, task_id: str, error: str) -> None:
        self._repo.mark_finished(task_id, "failed", error=error)

    @staticmethod
    def _worker_id() -> str:
        return f"{socket.gethostname()}"
