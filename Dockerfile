# -*- coding: utf-8 -*-
# Використовуємо офіційний образ Python
FROM python:3.11-slim

# Встановлюємо робочу директорію
WORKDIR /app

# Встановлюємо змінні середовища
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8

# Оновлюємо систему та встановлюємо необхідні пакети
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Копіюємо файл залежностей
COPY requirements.txt .

# Встановлюємо залежності Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копіюємо весь код проекту
COPY . .

# Створюємо користувача для безпеки (не root)
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Вказуємо точку входу
CMD ["python", "main.py"]
