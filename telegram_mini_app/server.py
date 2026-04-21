# -*- coding: utf-8 -*-
"""
Сервер FastAPI для Telegram Mini App.
Валідує initData, надає API для профілю, чату з LLM, адмін-дій та файлів.
"""

from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
import hashlib
import os

from config.settings import Settings
from business.services.user_service import UserService
from business.services.prozorro_service import ProZorroService
from business.services.logging_service import LoggingService

from telegram_mini_app.routes import me, llm, admin, files, search, feedback, report_templates, analytics


def _get_static_file_version(static_dir: Path) -> str:
    """Обчислює версію статичних файлів на основі модифікації app.js та styles.css."""
    version_parts = []
    for filename in ["app.js", "styles.css", "index.html"]:
        file_path = static_dir / filename
        if file_path.exists():
            mtime = file_path.stat().st_mtime
            version_parts.append(str(int(mtime)))
    if version_parts:
        combined = "".join(version_parts)
        return hashlib.md5(combined.encode()).hexdigest()[:8]
    return "1"


class CacheControlMiddleware(BaseHTTPMiddleware):
    """Middleware для встановлення правильних заголовків Cache-Control."""
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        
        # HTML - завжди no-cache для Telegram Web App
        if path == "/" or path.endswith(".html"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        # CSS та JS - короткий TTL або no-cache для розробки
        elif path.endswith((".css", ".js")):
            # Для розробки - no-cache, для продакшну можна встановити короткий TTL
            response.headers["Cache-Control"] = "no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        
        return response


def create_app(settings: Settings) -> FastAPI:
    """Створює FastAPI додаток з маршрутами та станом."""
    app = FastAPI(
        title="Pazuzu Mini App API",
        description="API для Telegram Mini App (дубль функціональності бота)",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # Middleware для cache control (після CORS)
    app.add_middleware(CacheControlMiddleware)

    app.state.settings = settings
    app.state.bot_token = settings.telegram_bot_token or ""
    app.state.user_service = UserService(settings.telegram_users_config_path)
    app.state.prozorro_service = ProZorroService(settings)
    app.state.logging_service = LoggingService()
    app.state.multi_agent_service = None  # леніва ініціалізація в routes/llm.py

    app.include_router(me.router)
    app.include_router(llm.router)
    app.include_router(admin.router)
    app.include_router(files.router)
    app.include_router(search.router)
    app.include_router(feedback.router)
    app.include_router(report_templates.router)
    app.include_router(analytics.router)

    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        @app.get("/")
        def index():
            # Читаємо HTML та додаємо версію до посилань на статичні файли
            static_version = _get_static_file_version(static_dir)
            html_path = static_dir / "index.html"
            if html_path.exists():
                html_content = html_path.read_text(encoding="utf-8")
                # Додаємо версію до посилань на CSS та JS
                html_content = html_content.replace(
                    'href="/static/styles.css"',
                    f'href="/static/styles.css?v={static_version}"'
                )
                html_content = html_content.replace(
                    'src="/static/app.js"',
                    f'src="/static/app.js?v={static_version}"'
                )
                return Response(content=html_content, media_type="text/html")
            return FileResponse(html_path)

        @app.get("/favicon.ico")
        def favicon():
            return Response(status_code=204)
    return app


def run_server(settings: Settings, host: str = "0.0.0.0", port: int = None):
    """Запускає uvicorn сервер для Mini App."""
    port = port or getattr(settings, "mini_app_port", 8000)
    import uvicorn
    app = create_app(settings)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    run_server(Settings())
