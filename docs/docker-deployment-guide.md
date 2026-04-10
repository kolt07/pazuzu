# Docker Deployment Guide (App + MongoDB)

Цей гайд описує розгортання `pazuzu` у Docker, включно з MongoDB, від клонування репозиторію до запуску готового стеку.

## 1) Перевірка поточного стану готовності

На момент перевірки:

- `Dockerfile` присутній і збирає застосунок.
- `docker-compose.yml` присутній, але містить лише сервіс застосунку.
- Сервіс MongoDB у `docker-compose.yml` **не описаний**.
- Команда запуску міграцій БД окремо не інтегрована в compose.

Висновок: для повноцінного контейнерного деплою (app + DB) потрібен розширений compose-сценарій.

## 2) Передумови (Windows CMD)

Встановіть:

- Docker Desktop (або Docker Engine + Compose plugin)
- Git

Перевірка:

```cmd
docker --version
docker compose version
git --version
```

## 3) Клонування репозиторію

```cmd
git clone <YOUR_REPO_URL> pazuzu
cd pazuzu
```

## 4) Створення runtime-конфігурації

Застосунок читає налаштування з `config/config.yaml` (має пріоритет над env).
Створіть файл на основі прикладу:

```cmd
copy config\config.example.yaml config\config.yaml
```

Заповніть мінімум:

- `telegram.bot_token` (якщо запускаєте бота)
- `llm.api_keys.*` (за потрібним провайдером)
- `mongodb.host`, `mongodb.port`, `mongodb.database_name`

Для Docker Compose:

- `mongodb.host` має бути `mongodb` (назва сервісу в мережі compose)
- `mongodb.port` має бути `27017`

## 5) Docker Compose для повного стеку (app + MongoDB + ngrok)

У репозиторії вже є готовий `docker-compose.yml`.
Він піднімає:

- `mongodb`
- `pazuzu-app`
- `ngrok` (еквівалент `ngrok http 8000`)

Перед стартом створіть `.env` у корені проєкту:

```cmd
copy NUL .env
```

Додайте в `.env`:

```env
NGROK_AUTHTOKEN=your_real_ngrok_token
```

Актуальна структура `docker-compose.yml`:

```yaml
version: "3.9"

services:
  mongodb:
    image: mongo:7
    container_name: pazuzu-mongodb
    restart: unless-stopped
    ports:
      - "27017:27017"
    volumes:
      - pazuzu_mongo_data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"]
      interval: 10s
      timeout: 5s
      retries: 10

  pazuzu-app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pazuzu-app
    restart: unless-stopped
    depends_on:
      mongodb:
        condition: service_healthy
    volumes:
      - ./config/config.yaml:/app/config/config.yaml:ro
      - ./data:/app/data
      - ./temp:/app/temp
    command: ["python", "main.py"]

  ngrok:
    image: ngrok/ngrok:latest
    container_name: pazuzu-ngrok
    restart: unless-stopped
    depends_on:
      - pazuzu-app
    environment:
      - NGROK_AUTHTOKEN=${NGROK_AUTHTOKEN}
    command: ["http", "pazuzu-app:8000"]
    ports:
      - "4040:4040"

volumes:
  pazuzu_mongo_data:
```

Примітка: `main.py` за замовчуванням запускає Telegram-бот; для batch-режиму використовуйте `--generate-file`.

## 6) Збірка і запуск стеку

```cmd
docker compose up -d --build
```

Перевірка:

```cmd
docker compose ps
docker compose logs -f pazuzu-app
docker compose logs -f ngrok
```

Щоб отримати публічний URL ngrok:

```cmd
docker compose logs ngrok
```

## 7) Запуск міграцій БД (після першого старту)

Проєкт містить міграції в `scripts/migrations`.
Запустіть їх у контейнері застосунку:

```cmd
docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
```

## 8) Оновлення версії застосунку

```cmd
git pull
docker compose up -d --build
docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
```

## 9) Бекап і відновлення MongoDB

Бекап:

```cmd
docker exec pazuzu-mongodb sh -c "mongodump --out /data/db/dump"
```

Відновлення:

```cmd
docker exec pazuzu-mongodb sh -c "mongorestore /data/db/dump"
```

## 10) Зупинка

```cmd
docker compose down
```

Щоб зупинити і видалити том БД (обережно, це видалить дані):

```cmd
docker compose down -v
```

