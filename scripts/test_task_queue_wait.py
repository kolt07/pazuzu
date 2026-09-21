# -*- coding: utf-8 -*-
"""
Тести очікування фонових задач: коротке повідомлення таймауту, pause abort.

Запуск:
    py -m unittest scripts.test_task_queue_wait
"""

from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from business.services.task_queue_service import TaskQueueService, TaskWaitTimeoutError


class _FakeRepo:
    def __init__(self, docs):
        self.docs = list(docs)

    def list_by_task_ids(self, ids):
        wanted = set(ids)
        return [d for d in self.docs if d.get("task_id") in wanted]


class _FakeControls:
    def __init__(self, state="running"):
        self.state = state

    def get_control(self, queue_name):
        return {"state": self.state, "queue_name": queue_name}


def _svc(docs, control="running"):
    svc = TaskQueueService.__new__(TaskQueueService)
    svc._repo = _FakeRepo(docs)
    svc._controls = _FakeControls(control)
    svc._celery = None
    svc.settings = None
    return svc


class TaskQueueWaitTest(unittest.TestCase):
    def test_returns_when_all_terminal(self):
        docs = [
            {"task_id": "a", "state": "success", "queue_name": "llm_processing"},
            {"task_id": "b", "state": "failed", "queue_name": "llm_processing"},
        ]
        out = _svc(docs).wait_for_all(["a", "b"], timeout_sec=5, poll_interval_sec=0.5)
        self.assertEqual(len(out), 2)

    def test_timeout_message_is_short(self):
        ids = [f"aaaaaaaa-bbbb-cccc-dddd-{i:012d}" for i in range(60)]
        docs = [{"task_id": tid, "state": "queued", "queue_name": "llm_processing"} for tid in ids]
        with patch.object(time, "sleep", return_value=None):
            with patch.object(time, "time", side_effect=[0.0, 0.0, 2.0]):
                with self.assertRaises(TaskWaitTimeoutError) as ctx:
                    _svc(docs).wait_for_all(ids, timeout_sec=1, poll_interval_sec=0.5)
        msg = str(ctx.exception)
        self.assertLess(len(msg), 400)
        self.assertIn("готово 0/60", msg)
        self.assertIn("queued=60", msg)
        self.assertNotIn(ids[10], msg)
        self.assertEqual(len(ctx.exception.pending_ids), 60)
        self.assertEqual(ctx.exception.reason, "timeout")

    def test_paused_queue_aborts(self):
        docs = [{"task_id": "a", "state": "queued", "queue_name": "llm_processing"}]
        with patch.object(time, "sleep", return_value=None):
            with self.assertRaises(TaskWaitTimeoutError) as ctx:
                _svc(docs, control="paused").wait_for_all(
                    ["a"], timeout_sec=3600, poll_interval_sec=0.5
                )
        self.assertEqual(ctx.exception.reason, "paused")
        self.assertIn("призупинена", str(ctx.exception))
        self.assertLess(len(str(ctx.exception)), 400)


if __name__ == "__main__":
    unittest.main()
