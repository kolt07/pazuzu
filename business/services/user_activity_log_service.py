# -*- coding: utf-8 -*-
"""Сервіс журналу активності користувачів Mini App."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from data.repositories.user_activity_log_repository import UserActivityLogRepository

# Категорії
CATEGORY_AUTH = "auth"
CATEGORY_SEARCH = "search"
CATEGORY_LISTING = "listing"
CATEGORY_REPORT = "report"

# Дії
ACTION_AUTH_SUCCESS = "auth_success"
ACTION_AUTH_DENIED = "auth_denied"
ACTION_AUTH_BLOCKED = "auth_blocked"

ACTION_SEARCH_QUERY = "search_query"
ACTION_SEARCH_EXPORT = "search_export"

ACTION_LISTING_VIEW = "listing_view"

ACTION_REPORT_GENERATE = "report_generate"

CATEGORY_LABELS_UK: Dict[str, str] = {
    CATEGORY_AUTH: "Авторизація",
    CATEGORY_SEARCH: "Пошук",
    CATEGORY_LISTING: "Оголошення",
    CATEGORY_REPORT: "Звіт",
}

ACTION_LABELS_UK: Dict[str, str] = {
    ACTION_AUTH_SUCCESS: "Успішний вхід",
    ACTION_AUTH_DENIED: "Відмова (не в білому списку)",
    ACTION_AUTH_BLOCKED: "Заблокований користувач",
    ACTION_SEARCH_QUERY: "Пошук",
    ACTION_SEARCH_EXPORT: "Експорт пошуку",
    ACTION_LISTING_VIEW: "Відкриття оголошення",
    ACTION_REPORT_GENERATE: "Формування звіту",
}


def _preview_text(value: Optional[str], max_len: int = 240) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


class UserActivityLogService:
    def __init__(self):
        self.repository = UserActivityLogRepository()

    def log(
        self,
        *,
        user_id: int,
        category: str,
        action: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> Optional[str]:
        try:
            return self.repository.create_entry(
                user_id=user_id,
                category=category,
                action=action,
                message=message,
                metadata=metadata,
                error=error,
            )
        except Exception:
            return None

    def log_auth_success(self, user_id: int, *, is_admin: bool = False) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_AUTH,
            action=ACTION_AUTH_SUCCESS,
            message=f"Користувач {user_id} відкрив Mini App",
            metadata={"is_admin": bool(is_admin)},
        )

    def log_auth_denied(self, user_id: int) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_AUTH,
            action=ACTION_AUTH_DENIED,
            message=f"Користувач {user_id} не в білому списку",
        )

    def log_auth_blocked(self, user_id: int) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_AUTH,
            action=ACTION_AUTH_BLOCKED,
            message=f"Заблокований користувач {user_id} намагався увійти",
        )

    def log_search_query(
        self,
        user_id: int,
        *,
        total: int,
        filter_string: Optional[str] = None,
        limit: int = 0,
        skip: int = 0,
    ) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_SEARCH,
            action=ACTION_SEARCH_QUERY,
            message=f"Пошук: {total} результатів",
            metadata={
                "total": int(total),
                "limit": int(limit),
                "skip": int(skip),
                "filter_string": _preview_text(filter_string),
            },
        )

    def log_search_export(
        self,
        user_id: int,
        *,
        rows_count: int,
        via_bot: bool = False,
        filter_string: Optional[str] = None,
    ) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_SEARCH,
            action=ACTION_SEARCH_EXPORT,
            message=f"Експорт пошуку: {rows_count} рядків",
            metadata={
                "rows_count": int(rows_count),
                "via_bot": bool(via_bot),
                "filter_string": _preview_text(filter_string),
            },
        )

    def log_listing_view(
        self,
        user_id: int,
        *,
        source: str,
        source_id: str,
    ) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_LISTING,
            action=ACTION_LISTING_VIEW,
            message=f"Відкрито {source}: {source_id}",
            metadata={"source": source, "source_id": _preview_text(source_id, 500)},
        )

    def log_report_generate(
        self,
        user_id: int,
        *,
        template_id: str,
        template_name: str,
        rows_count: int,
        via_bot: bool = False,
        output_format: Optional[str] = None,
    ) -> None:
        self.log(
            user_id=user_id,
            category=CATEGORY_REPORT,
            action=ACTION_REPORT_GENERATE,
            message=f"Звіт «{template_name}»: {rows_count} рядків",
            metadata={
                "template_id": template_id,
                "template_name": template_name,
                "rows_count": int(rows_count),
                "via_bot": bool(via_bot),
                "output_format": output_format,
            },
        )

    def _format_event(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(doc)
        if out.get("_id") is not None:
            out["_id"] = str(out["_id"])
        ts = out.get("timestamp")
        if hasattr(ts, "isoformat"):
            out["timestamp"] = ts.isoformat()
        out["category_label"] = CATEGORY_LABELS_UK.get(out.get("category", ""), out.get("category"))
        out["action_label"] = ACTION_LABELS_UK.get(out.get("action", ""), out.get("action"))
        return out

    def list_for_admin(
        self,
        *,
        category: Optional[str] = None,
        action: Optional[str] = None,
        user_id: Optional[int] = None,
        days: int = 7,
        limit: int = 20,
        skip: int = 0,
        user_profiles: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        grouped = self.repository.find_entries_grouped_by_user(
            category=category,
            action=action,
            user_id=user_id,
            days=days,
            users_limit=limit,
            users_skip=skip,
        )
        total_users = self.repository.count_distinct_users(
            category=category,
            action=action,
            user_id=user_id,
            days=days,
        )
        total_events = self.repository.count_entries(
            category=category,
            action=action,
            user_id=user_id,
            days=days,
        )
        profiles = user_profiles or {}
        users_out: List[Dict[str, Any]] = []
        for row in grouped:
            uid = int(row.get("user_id", 0))
            profile = profiles.get(uid) or {}
            nickname = profile.get("nickname")
            last_ts = row.get("last_activity")
            if hasattr(last_ts, "isoformat"):
                last_ts = last_ts.isoformat()
            events = [self._format_event(ev) for ev in (row.get("events") or [])]
            users_out.append({
                "user_id": uid,
                "nickname": nickname,
                "role": profile.get("role"),
                "is_blocked": profile.get("is_blocked", False),
                "events_count": int(row.get("events_count", len(events))),
                "last_activity": last_ts,
                "events": events,
            })
        return {
            "users": users_out,
            "total_users": total_users,
            "total_events": total_events,
            "limit": limit,
            "skip": skip,
            "category_labels": CATEGORY_LABELS_UK,
            "action_labels": ACTION_LABELS_UK,
        }
