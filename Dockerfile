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
    && rm -rf /var/lib/apt/lists/*

# Копіюємо файл залежностей
COPY requirements.txt .

# Встановлюємо залежності Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir "celery>=5.3.0" && \
    python -c "import celery; print('celery', celery.__version__)"

# Chromium для OLX (clicker / browser_fetcher): системні залежності + завантаження браузера
RUN python -m playwright install --with-deps chromium && \
    chmod -R a+rx /ms-playwright

# Копіюємо весь код проекту
COPY . .

# Створюємо користувача для безпеки (не root)
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app /ms-playwright
USER appuser

# Вказуємо точку входу
CMD ["python", "main.py"]
