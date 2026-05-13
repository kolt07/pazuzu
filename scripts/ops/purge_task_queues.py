# -*- coding: utf-8 -*-
"""
Операційний скрипт: очистка черг RabbitMQ, опційно revoke Celery task_id з Mongo,
опційно скасування активних FLX-сесій.

Запуск (з кореня репо, UTF-8 консоль):
  py scripts/ops/purge_task_queues.py --dry-run
  py scripts/ops/purge_task_queues.py --purge-queues --queues llm_processing,source_load,flx_investigation,celery
  py scripts/ops/purge_task_queues.py --purge-queues --revoke-mongo-active --cancel-flx-sessions

Увага: purge видаляє лише повідомлення в черзі; running tasks у воркері можуть
допрацювати — комбінуйте з рестартом worker за потреби.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# Windows: UTF-8 stdout
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _parse_vhost(broker_url: str) -> str:
    p = urlparse(str(broker_url or "").replace("amqp://", "http://", 1))
    path = (p.path or "").strip("/")
    return path if path else "/"


def _purge_queues(broker_url: str, queue_names: List[str], dry_run: bool) -> None:
    from kombu import Connection

    vhost = _parse_vhost(broker_url)
    print(f"[purge] broker vhost={vhost!r} queues={queue_names} dry_run={dry_run}")
    if dry_run:
        return
    with Connection(broker_url, connect_timeout=10) as conn:
        conn.ensure_connection(max_retries=3)
        ch = conn.channel()
        try:
            for qn in queue_names:
                qn = str(qn or "").strip()
                if not qn:
                    continue
                ch.queue_declare(queue=qn, passive=True)
                ch.queue_purge(queue=qn)
                print(f"[purge] purged queue={qn}")
        finally:
            try:
                ch.close()
            except Exception:
                pass


def _revoke_mongo_active(settings: Any, dry_run: bool) -> int:
    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.celery_app import create_celery_app

    if not isinstance(settings, Settings):
        settings = Settings()
    MongoDBConnection.initialize(settings)
    db = MongoDBConnection.get_database()
    coll = db["background_tasks"]
    terminal = {"success", "failed", "revoked"}
    cur = coll.find({"state": {"$nin": list(terminal)}})
    ids: List[str] = []
    for doc in cur:
        tid = str(doc.get("task_id") or "").strip()
        if tid:
            ids.append(tid)
    print(f"[revoke] mongo non-terminal tasks: {len(ids)}")
    if dry_run or not ids:
        return len(ids)
    app = create_celery_app(settings)
    for tid in ids:
        try:
            app.control.revoke(tid, terminate=True, signal="SIGTERM")
        except Exception as e:
            print(f"[revoke] warn task_id={tid}: {e}")
        coll.update_one(
            {"task_id": tid},
            {"$set": {"state": "revoked", "error": "revoked_by_purge_task_queues_script"}},
        )
    return len(ids)


def _cancel_flx_sessions(settings: Any, dry_run: bool) -> int:
    from config.settings import Settings
    from data.database.connection import MongoDBConnection

    if not isinstance(settings, Settings):
        settings = Settings()
    MongoDBConnection.initialize(settings)
    db = MongoDBConnection.get_database()
    active = {"created", "planning", "running", "awaiting_user", "awaiting_sources"}
    n = db["investigation_sessions"].count_documents({"state": {"$in": list(active)}})
    print(f"[flx] active sessions to cancel: {n}")
    if dry_run or n == 0:
        return int(n)
    res = db["investigation_sessions"].update_many(
        {"state": {"$in": list(active)}},
        {
            "$set": {
                "state": "cancelled",
                "pending_source_wait": None,
                "last_error": "cancelled_by_purge_task_queues_script",
            }
        },
    )
    return int(res.modified_count)


def main() -> int:
    from config.settings import Settings

    ap = argparse.ArgumentParser(description="Purge RabbitMQ queues / revoke tasks / cancel FLX")
    ap.add_argument("--dry-run", action="store_true", help="Only print actions")
    ap.add_argument("--purge-queues", action="store_true", help="Purge listed Rabbit queues")
    ap.add_argument(
        "--queues",
        default="llm_processing,source_load,flx_investigation,celery",
        help="Comma-separated queue names",
    )
    ap.add_argument("--revoke-mongo-active", action="store_true", help="Revoke all non-terminal background_tasks")
    ap.add_argument("--cancel-flx-sessions", action="store_true", help="Set all active FLX sessions to cancelled")
    args = ap.parse_args()
    dry = bool(args.dry_run)
    settings = Settings()
    broker = (getattr(settings, "task_queue_broker_url", None) or "").strip()
    if args.purge_queues:
        if not broker:
            print("[purge] task_queue_broker_url empty — skip purge")
        else:
            names = [x.strip() for x in str(args.queues or "").split(",") if x.strip()]
            _purge_queues(broker, names, dry_run=dry)
    if args.revoke_mongo_active:
        _revoke_mongo_active(settings, dry_run=dry)
    if args.cancel_flx_sessions:
        _cancel_flx_sessions(settings, dry_run=dry)
    if not (args.purge_queues or args.revoke_mongo_active or args.cancel_flx_sessions):
        print("Nothing to do. Use --purge-queues / --revoke-mongo-active / --cancel-flx-sessions")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
