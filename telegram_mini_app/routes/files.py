# -*- coding: utf-8 -*-
"""
API: звіти (скачати з БД, запустити формування, статус, завантажити файл).
Артефакти чату (Excel з LLM): зберігання, завантаження за token, видалення при закритті чату.
Для мобільних пристроїв підтримуються одноразові посилання з токеном (без заголовків авторизації).
"""

import asyncio
import base64
import uuid
import zipfile
from io import BytesIO
from datetime import timedelta
from urllib.parse import quote
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List
from telegram_mini_app.auth import validate_telegram_init_data
from utils.date_utils import format_datetime_display

router = APIRouter(prefix="/api/files", tags=["files"])

# Одноразові токени для скачування (мобільні кліки не передають заголовки)
_report_download_tokens: dict = {}  # token -> {"user_id": int, "days": int}
_TOKEN_TTL_SECONDS = 300  # 5 хвилин


def _content_disposition_attachment(filename: str) -> str:
    """Повертає значення Content-Disposition з UTF-8 ім'ям файлу (RFC 5987)."""
    ascii_fallback = "report.zip"
    encoded = quote(filename, safe="")
    return f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded}"


def _get_user_and_services(request: Request):
    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        raise HTTPException(status_code=403, detail="X-Telegram-Init-Data required")
    token = getattr(request.app.state, "bot_token", None)
    if not token:
        raise HTTPException(status_code=503, detail="Mini app not configured")
    validated = validate_telegram_init_data(init_data, token)
    if not validated:
        raise HTTPException(status_code=403, detail="Invalid init data")
    user_obj = validated.get("user")
    if not user_obj or not isinstance(user_obj, dict):
        raise HTTPException(status_code=403, detail="User data missing")
    user_id = int(user_obj.get("id", 0))
    user_service = request.app.state.user_service
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    return user_id, request.app.state.prozorro_service, request.app.state.logging_service


def _get_user_id_from_request(request: Request):
    """Повертає user_id якщо є валідний init data; інакше None."""
    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        return None
    token = getattr(request.app.state, "bot_token", None)
    if not token:
        return None
    validated = validate_telegram_init_data(init_data, token)
    if not validated:
        return None
    user_obj = validated.get("user")
    if not user_obj or not isinstance(user_obj, dict):
        return None
    user_id = int(user_obj.get("id", 0))
    user_service = request.app.state.user_service
    if not user_service.is_user_authorized(user_id):
        return None
    return user_id


class ReportDownloadUrlRequest(BaseModel):
    days: int = 1


@router.post("/send-report-via-bot")
def send_report_via_bot(request: Request, body: ReportDownloadUrlRequest):
    """
    Генерує звіт та надсилає його користувачу в чат бота.
    Для мобільних застосунків, де пряме скачування не працює.
    """
    if body.days not in (1, 7):
        raise HTTPException(status_code=400, detail="days must be 1 or 7")
    user_id, prozorro, logging_service = _get_user_and_services(request)
    excel_bytes = prozorro.generate_excel_from_db(body.days)
    if not excel_bytes:
        raise HTTPException(status_code=404, detail=f"Дані за {body.days} днів не знайдено в БД.")
    try:
        from data.repositories.app_data_repository import AppDataRepository
        app_data_repo = AppDataRepository()
        update_date = app_data_repo.get_update_date(body.days)
    except Exception:
        update_date = None
    if update_date:
        date_from = update_date - timedelta(days=body.days)
        zip_filename = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).zip"
    else:
        zip_filename = f"Звіт по нерухомості ({body.days} днів).zip"
    zip_bytes = BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as zf:
        excel_bytes.seek(0)
        internal = zip_filename.replace(".zip", ".xlsx")
        zf.writestr(internal, excel_bytes.read())
    zip_bytes.seek(0)
    bot_token = getattr(request.app.state, "bot_token", None) or ""
    from telegram_mini_app.send_via_bot import send_file_via_telegram
    ok = send_file_via_telegram(user_id, zip_bytes.getvalue(), zip_filename, bot_token)
    if not ok:
        raise HTTPException(status_code=500, detail="Не вдалося надіслати файл через бота")
    logging_service.log_user_action(
        user_id=user_id,
        action="download_file",
        message=f"Mini App: файл за {body.days} днів надіслано через бота",
        metadata={"days": body.days, "via_bot": True},
    )
    return {"success": True, "message": "Файл надіслано в чат бота"}


@router.post("/report-download-url")
def get_report_download_url(request: Request, body: ReportDownloadUrlRequest):
    """
    Повертає одноразове посилання для скачування звіту (для мобільних: клік по посиланню не передає заголовки).
    Токен дійсний 5 хвилин.
    """
    if body.days not in (1, 7):
        raise HTTPException(status_code=400, detail="days must be 1 or 7")
    user_id, _, _ = _get_user_and_services(request)
    download_token = str(uuid.uuid4())
    _report_download_tokens[download_token] = {"user_id": user_id, "days": body.days}
    url = f"/api/files/report?days={body.days}&token={download_token}"
    return {"url": url, "days": body.days}


@router.get("/report")
def get_report(request: Request, days: int = 1, token: str = None):
    """
    Повертає ZIP з Excel-звітом за вказану кількість днів (з БД).
    days: 1 або 7. Якщо передано token (одноразове посилання з report-download-url), авторизація по заголовках не потрібна.
    """
    if days not in (1, 7):
        raise HTTPException(status_code=400, detail="days must be 1 or 7")
    if token:
        if token not in _report_download_tokens:
            raise HTTPException(status_code=403, detail="Invalid or expired download link")
        stored = _report_download_tokens.pop(token)
        if stored["days"] != days:
            raise HTTPException(status_code=400, detail="Token days mismatch")
        user_id = stored["user_id"]
        prozorro = request.app.state.prozorro_service
        logging_service = request.app.state.logging_service
    else:
        user_id, prozorro, logging_service = _get_user_and_services(request)
    excel_bytes = prozorro.generate_excel_from_db(days)
    if not excel_bytes:
        raise HTTPException(status_code=404, detail=f"Дані за {days} днів не знайдено в БД.")
    try:
        from data.repositories.app_data_repository import AppDataRepository
        app_data_repo = AppDataRepository()
        update_date = app_data_repo.get_update_date(days)
    except Exception:
        update_date = None
    if update_date:
        date_from = update_date - timedelta(days=days)
        internal_name = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).xlsx"
        zip_filename = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).zip"
    else:
        internal_name = f"Звіт по нерухомості ({days} днів).xlsx"
        zip_filename = f"Звіт по нерухомості ({days} днів).zip"
    zip_bytes = BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as zf:
        excel_bytes.seek(0)
        zf.writestr(internal_name, excel_bytes.read())
    zip_bytes.seek(0)
    logging_service.log_user_action(
        user_id=user_id,
        action="download_file",
        message=f"Mini App: скачано файл за {days} днів з БД",
        metadata={"days": days},
    )
    return Response(
        content=zip_bytes.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": _content_disposition_attachment(zip_filename)},
    )


def _ensure_tasks_store(request: Request):
    if not hasattr(request.app.state, "mini_app_tasks"):
        request.app.state.mini_app_tasks = {}
    return request.app.state.mini_app_tasks


@router.post("/generate")
async def start_generate(request: Request, days: int = 7):
    """
    Запускає формування даних та звіту за вказану кількість днів.
    Повертає task_id для перевірки статусу та завантаження файлу.
    """
    if days not in (1, 7):
        raise HTTPException(status_code=400, detail="days must be 1 or 7")
    user_id, prozorro, logging_service = _get_user_and_services(request)
    tasks = _ensure_tasks_store(request)
    task_id = str(uuid.uuid4())
    settings = request.app.state.settings

    async def run_generate():
        tasks[task_id] = {"status": "running", "message": "Формування файлу..."}
        try:
            loop = asyncio.get_event_loop()
            if days == 7:
                result = await loop.run_in_executor(
                    None,
                    lambda: prozorro.fetch_and_save_real_estate_auctions(days=days, user_id=user_id),
                )
            else:
                auctions = await loop.run_in_executor(
                    None,
                    lambda: prozorro.get_real_estate_auctions(days),
                )
                if not auctions:
                    tasks[task_id] = {"status": "error", "message": "Аукціони не знайдено."}
                    return
                result = await loop.run_in_executor(
                    None,
                    lambda: prozorro.fetch_and_save_real_estate_auctions(
                        days=days, user_id=user_id, auctions=auctions
                    ),
                )
            if not result.get("success"):
                tasks[task_id] = {"status": "error", "message": result.get("message", "Невідома помилка")}
                return
            try:
                from business.services.source_data_load_service import run_full_pipeline
                run_full_pipeline(settings=settings, sources=["olx", "prozorro"], days=days)
            except Exception as _err:
                pass
            excel_bytes = prozorro.generate_excel_from_db(days)
            if not excel_bytes:
                tasks[task_id] = {"status": "error", "message": "Не вдалося згенерувати Excel."}
                return
            update_date = result.get("update_date")
            if update_date:
                date_from = update_date - timedelta(days=days)
                fn = f"Звіт по нерухомості ({format_datetime_display(date_from, '%d.%m.%Y')}-{format_datetime_display(update_date, '%d.%m.%Y')}).zip"
            else:
                fn = f"Звіт по нерухомості ({days} днів).zip"
            zip_bytes = BytesIO()
            with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as zf:
                excel_bytes.seek(0)
                internal = fn.replace(".zip", ".xlsx")
                zf.writestr(internal, excel_bytes.read())
            zip_bytes.seek(0)
            download_token = str(uuid.uuid4())
            tasks[task_id] = {
                "status": "done",
                "message": f"Знайдено {result.get('count', 0)} аукціонів.",
                "filename": fn,
                "bytes": zip_bytes.getvalue(),
                "download_token": download_token,
                "user_id": user_id,
            }
            logging_service.log_user_action(
                user_id=user_id,
                action="generate_file",
                message=f"Mini App: дані за {days} днів успішно сформовано",
                metadata={"days": days, "count": result.get("count")},
            )
        except Exception as e:
            tasks[task_id] = {"status": "error", "message": str(e)}

    asyncio.create_task(run_generate())
    return {"task_id": task_id, "status": "started", "days": days}


@router.get("/generate/status")
def generate_status(request: Request, task_id: str):
    """Повертає статус задачі формування файлу. При status=done повертає download_url для мобільних (клікабельне посилання)."""
    _get_user_and_services(request)
    tasks = _ensure_tasks_store(request)
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = tasks[task_id]
    out = {
        "task_id": task_id,
        "status": t["status"],
        "message": t.get("message"),
        "filename": t.get("filename"),
    }
    if t.get("status") == "done" and t.get("download_token"):
        out["download_url"] = f"/api/files/download?task_id={task_id}&token={t['download_token']}"
        out["download_token"] = t["download_token"]
    return out


class SendArtifactRequest(BaseModel):
    artifact_id: str
    token: str


@router.post("/send-artifact-via-bot")
def send_artifact_via_bot(request: Request, body: SendArtifactRequest):
    """Надсилає артефакт (Excel з чату) через бота. Для мобільних."""
    artifact_service = getattr(request.app.state, "artifact_service", None)
    if not artifact_service:
        from business.services.artifact_service import ArtifactService
        request.app.state.artifact_service = ArtifactService()
        artifact_service = request.app.state.artifact_service
    doc = artifact_service.get_artifact(body.artifact_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Файл не знайдено або термін дії закінчено")
    stored_token = (doc.get("metadata") or {}).get("download_token")
    if stored_token != body.token:
        raise HTTPException(status_code=403, detail="Недійсне або прострочене посилання")
    content_b64 = doc.get("content_base64")
    if not content_b64:
        raise HTTPException(status_code=404, detail="Файл порожній")
    try:
        content = base64.b64decode(content_b64)
    except Exception:
        raise HTTPException(status_code=500, detail="Помилка декодування")
    user_id = doc.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="User not found")
    filename = (doc.get("metadata") or {}).get("filename", "report.xlsx")
    bot_token = getattr(request.app.state, "bot_token", None) or ""
    from telegram_mini_app.send_via_bot import send_file_via_telegram
    ok = send_file_via_telegram(int(user_id), content, filename, bot_token)
    if not ok:
        raise HTTPException(status_code=500, detail="Не вдалося надіслати файл через бота")
    return {"success": True, "message": "Файл надіслано в чат бота"}


@router.get("/artifact/{artifact_id}")
def download_artifact(request: Request, artifact_id: str, token: str = None):
    """
    Завантажує артефакт (Excel з чату) за ID.
    Якщо передано token (з посилання для мобільних) — авторизація по заголовках не потрібна.
    Інакше потрібен X-Telegram-Init-Data та відповідність user_id.
    """
    artifact_service = getattr(request.app.state, "artifact_service", None)
    if not artifact_service:
        from business.services.artifact_service import ArtifactService
        request.app.state.artifact_service = ArtifactService()
        artifact_service = request.app.state.artifact_service
    doc = artifact_service.get_artifact(artifact_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Файл не знайдено або термін дії закінчено")
    if token:
        stored_token = (doc.get("metadata") or {}).get("download_token")
        if stored_token != token:
            raise HTTPException(status_code=403, detail="Недійсне або прострочене посилання")
    else:
        user_id = _get_user_id_from_request(request)
        if user_id is None:
            raise HTTPException(status_code=403, detail="Потрібна авторизація або посилання з токеном")
        artifact_user = doc.get("user_id")
        if artifact_user is not None and str(artifact_user) != str(user_id):
            raise HTTPException(status_code=403, detail="Немає доступу")
    content_b64 = doc.get("content_base64")
    if not content_b64:
        raise HTTPException(status_code=404, detail="Файл порожній")
    filename = (doc.get("metadata") or {}).get("filename", "report.xlsx")
    try:
        content = base64.b64decode(content_b64)
    except Exception:
        raise HTTPException(status_code=500, detail="Помилка декодування")
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": _content_disposition_attachment(filename)},
    )


class DeleteArtifactsRequest(BaseModel):
    artifact_ids: List[str]


@router.post("/artifacts/delete")
def delete_artifacts(request: Request, body: DeleteArtifactsRequest):
    """Видаляє артефакти за списком ID (при видаленні чату). Перевіряє, що user_id збігається."""
    user_id, _, _ = _get_user_and_services(request)
    artifact_service = getattr(request.app.state, "artifact_service", None)
    if not artifact_service:
        from business.services.artifact_service import ArtifactService
        request.app.state.artifact_service = ArtifactService()
        artifact_service = request.app.state.artifact_service
    deleted = artifact_service.delete_by_ids(body.artifact_ids or [], user_id=str(user_id))
    return {"deleted": deleted}


class SendGeneratedRequest(BaseModel):
    task_id: str
    token: str


@router.post("/send-generated-via-bot")
def send_generated_via_bot(request: Request, body: SendGeneratedRequest):
    """Надсилає згенерований файл через бота (для мобільних)."""
    tasks = _ensure_tasks_store(request)
    if body.task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = tasks[body.task_id]
    if t.get("download_token") != body.token:
        raise HTTPException(status_code=403, detail="Invalid or expired link")
    if t["status"] != "done" or "bytes" not in t:
        raise HTTPException(status_code=400, detail="File not ready or task failed")
    user_id = t.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="User not found")
    bot_token = getattr(request.app.state, "bot_token", None) or ""
    from telegram_mini_app.send_via_bot import send_file_via_telegram
    ok = send_file_via_telegram(user_id, t["bytes"], t.get("filename", "report.zip"), bot_token)
    if not ok:
        raise HTTPException(status_code=500, detail="Не вдалося надіслати файл через бота")
    return {"success": True, "message": "Файл надіслано в чат бота"}


@router.get("/download")
def download_generated(request: Request, task_id: str, token: str = None):
    """Повертає згенерований ZIP-файл після завершення задачі. Якщо передано token (з generate/status), авторизація по заголовках не потрібна — для клікабельних посилань на мобільних."""
    tasks = _ensure_tasks_store(request)
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = tasks[task_id]
    if token:
        if t.get("download_token") != token:
            raise HTTPException(status_code=403, detail="Invalid or expired download link")
    else:
        _get_user_and_services(request)
    if t["status"] != "done" or "bytes" not in t:
        raise HTTPException(status_code=400, detail="File not ready or task failed")
    return Response(
        content=t["bytes"],
        media_type="application/zip",
        headers={"Content-Disposition": _content_disposition_attachment(t["filename"])},
    )
