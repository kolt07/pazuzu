# -*- coding: utf-8 -*-
# Використовуємо офіційний образ Python
FROM python:3.11-slim

# Встановлюємо робочу директорію
WORKDIR /app

# Встановлюємо змінні середовища (PYTHONPATH — імпорт business.* для Celery)
# PLAYWRIGHT_BROWSERS_PATH — браузери в спільній директорії (доступні для appuser після chown)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8 \
    PYTHONPATH=/app \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Базові пакети; APT для Chromium додає `playwright install --with-deps`
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssh-client \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Копіюємо файл залежностей
COPY requirements.txt .

# Встановлюємо залежності Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    python -c "import celery,kombu; print('celery', celery.__version__, 'kombu', kombu.__version__)"

# Chromium для OLX (clicker / browser_fetcher): системні залежності + завантаження браузера
RUN python -m playwright install --with-deps chromium && \
    chmod -R a+rx /ms-playwright

# Лише runtime-код (не venv/дампи/docs — див. .dockerignore).
# data/ і temp/ — порожні в образі; у compose монтуються з хоста.
COPY business/ business/
COPY config/ config/
COPY domain/ domain/
COPY mcp_servers/ mcp_servers/
COPY scripts/ scripts/
COPY telegram_mini_app/ telegram_mini_app/
COPY transport/ transport/
COPY utils/ utils/
COPY main.py .
RUN mkdir -p data temp

# Створюємо користувача для безпеки (не root)
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app /ms-playwright
USER appuser

# Вказуємо точку входу
CMD ["python", "main.py"]
