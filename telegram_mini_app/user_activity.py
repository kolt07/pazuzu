# -*- coding: utf-8 -*-
"""Допоміжні функції запису активності користувача з маршрутів Mini App."""

from __future__ import annotations

from typing import Optional

from fastapi import Request


def _service(request: Request):
    return getattr(request.app.state, "user_activity_service", None)


def record_auth_check(request: Request, user_id: int, user_service) -> None:
    svc = _service(request)
    if not svc:
        return
    user = user_service.get_user(user_id)
    if not user:
        svc.log_auth_denied(user_id)
        return
    if user.get("is_blocked"):
        svc.log_auth_blocked(user_id)
        return
    svc.log_auth_success(user_id, is_admin=user_service.is_admin(user_id))


def record_search_query(
    request: Request,
    user_id: int,
    *,
    total: int,
    filter_string: Optional[str] = None,
    limit: int = 0,
    skip: int = 0,
) -> None:
    svc = _service(request)
    if svc:
        svc.log_search_query(
            user_id,
            total=total,
            filter_string=filter_string,
            limit=limit,
            skip=skip,
        )


def record_search_export(
    request: Request,
    user_id: int,
    *,
    rows_count: int,
    via_bot: bool = False,
    filter_string: Optional[str] = None,
) -> None:
    svc = _service(request)
    if svc:
        svc.log_search_export(
            user_id,
            rows_count=rows_count,
            via_bot=via_bot,
            filter_string=filter_string,
        )


def record_listing_view(
    request: Request,
    user_id: int,
    *,
    source: str,
    source_id: str,
) -> None:
    svc = _service(request)
    if svc:
        svc.log_listing_view(user_id, source=source, source_id=source_id)


def record_report_generate(
    request: Request,
    user_id: int,
    *,
    template_id: str,
    template_name: str,
    rows_count: int,
    via_bot: bool = False,
    output_format: Optional[str] = None,
) -> None:
    svc = _service(request)
    if svc:
        svc.log_report_generate(
            user_id,
            template_id=template_id,
            template_name=template_name,
            rows_count=rows_count,
            via_bot=via_bot,
            output_format=output_format,
        )
