# -*- coding: utf-8 -*-
"""
MCP сервер агента-інвестігейтора Flx.

Експортує тулзи для роботи з нотатками сесії, lessons-learned, рендером Static Maps,
постановкою питань користувачу, компонуванням звіту та (заглушкою) web-search.

Усередині процесу orchestrator (InvestigationService) викликає ті самі функції напряму
через tools registry — MCP сервер потрібен для зовнішніх клієнтів і документації схем.
"""

import logging
import sys
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.flx_lessons_repository import FlxLessonsRepository
from data.repositories.investigation_event_repository import InvestigationEventRepository
from data.repositories.investigation_notes_repository import InvestigationNotesRepository
from data.repositories.investigation_session_repository import InvestigationSessionRepository
from business.services.static_map_service import StaticMapService

logger = logging.getLogger(__name__)
mcp = FastMCP("flx-mcp", json_response=True)

_settings: Optional[Settings] = None
_session_repo: Optional[InvestigationSessionRepository] = None
_notes_repo: Optional[InvestigationNotesRepository] = None
_lessons_repo: Optional[FlxLessonsRepository] = None
_events_repo: Optional[InvestigationEventRepository] = None
_static_map: Optional[StaticMapService] = None


def _init() -> None:
    """Лінива ініціалізація."""
    global _settings, _session_repo, _notes_repo, _lessons_repo, _events_repo, _static_map
    if _session_repo is not None:
        return
    try:
        _settings = Settings()
        MongoDBConnection.initialize(_settings)
        _session_repo = InvestigationSessionRepository()
        _notes_repo = InvestigationNotesRepository()
        _lessons_repo = FlxLessonsRepository()
        _events_repo = InvestigationEventRepository()
        _static_map = StaticMapService(_settings)
    except Exception as e:
        print(f"flx-mcp init error: {e}", file=sys.stderr)
        raise


@mcp.tool()
def note_write(
    session_id: str,
    kind: str,
    text: str,
    step_index: int = 0,
    tool_call_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Записує нотатку (думку/спостереження/гіпотезу/рішення) для активного розслідування.

    Args:
        session_id: Ідентифікатор сесії розслідування.
        kind: Тип нотатки: thought | observation | hypothesis | decision | question.
        text: Сама нотатка українською. До 8000 символів.
        step_index: Номер поточного кроку плану (0 — поза кроками).
        tool_call_summary: Опційне коротке резюме викликаного раніше інструмента.

    Returns:
        {ok, seq} — seq це монотонний номер нотатки в межах сесії.
    """
    try:
        _init()
        doc = _notes_repo.append(
            session_id=session_id,
            kind=kind,
            text=text,
            step_index=step_index,
            tool_call_summary=tool_call_summary,
        )
        # Дублюємо в потік подій, щоб UI міг показувати нотатки в реальному часі
        try:
            _events_repo.push(session_id=session_id, event_type="note", payload={
                "kind": doc.get("kind"),
                "text": doc.get("text"),
                "step_index": doc.get("step_index"),
                "seq": doc.get("seq"),
            })
        except Exception:
            pass
        return {"ok": True, "seq": doc.get("seq")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def notes_read(session_id: str, top_k: int = 12) -> Dict[str, Any]:
    """Повертає короткий summary останніх top_k нотаток сесії (для контексту LLM)."""
    try:
        _init()
        return {"ok": True, "summary": _notes_repo.summary(session_id, top_k=top_k)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def lessons_search(query: str, user_id: Optional[str] = None, top_k: int = 5) -> Dict[str, Any]:
    """Шукає схожі lessons-learned по тексту запиту (MongoDB $text)."""
    try:
        _init()
        items = _lessons_repo.search_text(query=query, user_id=user_id, top_k=top_k)
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def lessons_save(
    user_id: Optional[str],
    topic_tags: List[str],
    query_pattern: str,
    what_worked: List[str],
    what_failed: List[str],
    recommendations: List[str],
    related_session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Зберігає lesson-learned. Зазвичай викликається в кінці розслідування з результату Reflection."""
    try:
        _init()
        lesson_id = _lessons_repo.add_lesson(
            user_id=user_id,
            topic_tags=topic_tags or [],
            query_pattern=query_pattern,
            what_worked=what_worked or [],
            what_failed=what_failed or [],
            recommendations=recommendations or [],
            related_session_id=related_session_id,
        )
        return {"ok": True, "lesson_id": lesson_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def static_map_render(
    session_id: str,
    user_id: Optional[str] = None,
    center_lat: Optional[float] = None,
    center_lng: Optional[float] = None,
    bbox: Optional[List[float]] = None,
    markers: Optional[List[Dict[str, Any]]] = None,
    heatmap_points: Optional[List[Dict[str, Any]]] = None,
    paths: Optional[List[Dict[str, Any]]] = None,
    size: Optional[str] = None,
    zoom: Optional[int] = None,
) -> Dict[str, Any]:
    """Рендерить Google Static Maps PNG і повертає artifact_id для подальшого вбудовування у звіт.

    Args:
        session_id: ID розслідування (для прив'язки артефакта в логах).
        user_id: Користувач-власник (для TTL та доступу).
        center_lat / center_lng: Центр карти (опц.; альтернатива bbox).
        bbox: [min_lat, min_lng, max_lat, max_lng] (опц.; якщо без center).
        markers: [{lat, lng, color, label}].
        heatmap_points: [{lat, lng, value}] — value у будь-яких одиницях, бакетується автоматично.
        paths: [{points: [[lat,lng], ...], color, weight}].
        size: "WIDTHxHEIGHT" (за замовчуванням з settings).
        zoom: цілочисельний зум 1..20.
    """
    try:
        _init()
        center = (float(center_lat), float(center_lng)) if center_lat is not None and center_lng is not None else None
        bbox_t = tuple(bbox) if isinstance(bbox, list) and len(bbox) == 4 else None
        result = _static_map.render_to_artifact(
            user_id=user_id,
            center=center,
            bbox=bbox_t,
            markers=markers or [],
            heatmap_points=heatmap_points or [],
            paths=paths or [],
            size=size,
            zoom=zoom,
        )
        if result.get("ok"):
            try:
                _events_repo.push(session_id=session_id, event_type="status", payload={
                    "message": "Карту згенеровано",
                    "artifact_id": result.get("artifact_id"),
                })
            except Exception:
                pass
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def ask_user(
    session_id: str,
    question: str,
    options: Optional[List[str]] = None,
    allow_freeform: bool = True,
) -> Dict[str, Any]:
    """Записує відкрите питання у потік подій і переводить сесію в стан awaiting_user.

    Викликач (executor розслідування) має після цього зупинити цикл і чекати на
    submit_user_answer. Цей метод сам по собі не блокує виконання.
    """
    try:
        _init()
        payload = {
            "question": str(question)[:1000],
            "options": [str(o)[:200] for o in (options or []) if str(o).strip()][:6],
            "allow_freeform": bool(allow_freeform),
        }
        _session_repo.set_pending_question(session_id, payload)
        _events_repo.push(session_id=session_id, event_type="question", payload=payload)
        return {"ok": True, "state": "awaiting_user"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def report_compose(
    session_id: str,
    sections: List[Dict[str, Any]],
    title: str = "",
    executive_summary: str = "",
    warnings: Optional[List[str]] = None,
    sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Компонує фінальну структуру звіту (без HTML). Сам HTML рендерить InvestigationService.

    Цей tool існує для повноти MCP-інтерфейсу: при виклику з orchestrator він просто
    зберігає structured-JSON у service_data сесії, щоб подальший крок зрендерив HTML.
    """
    try:
        _init()
        report_doc = {
            "title": str(title)[:200] or "Звіт розслідування Flx",
            "executive_summary": str(executive_summary)[:2000],
            "sections": sections or [],
            "warnings": warnings or [],
            "sources": sources or [],
        }
        _session_repo.update_fields(session_id, {"report_structured": report_doc})
        return {"ok": True, "ready_to_render": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def web_search(query: str, top_k: int = 5) -> Dict[str, Any]:
    """Заглушка веб-пошуку. Поки що використовується лише Gemini Grounding всередині LLM.

    Якщо буде вибрано окремого провайдера (Tavily / Serper / Google PSE) — реалізацію
    додамо без зміни сигнатури.
    """
    return {
        "ok": False,
        "error": "web_search_not_configured",
        "message": (
            "Окремий web-search провайдер не налаштовано. "
            "Використовуй Gemini Grounding (вмикається через settings.llm_investigator_google_grounding)."
        ),
        "items": [],
    }


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
