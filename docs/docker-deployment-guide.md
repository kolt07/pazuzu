# Docker Deployment Guide (повний стек)

Цей гайд описує розгортання `pazuzu` у Docker: застосунок, MongoDB, RabbitMQ, Celery-воркери та (опційно) ngrok — від клонування репозиторію до запуску стеку.

## 1) Склад стеку

| Компонент | Контейнер | Призначення |
|-----------|-----------|-------------|
| **MongoDB** | `pazuzu-mongodb` | Основна БД (`mongo:7`) |
| **RabbitMQ** | `pazuzu-rabbitmq` | Брокер черг для Celery (`rabbitmq:4-management`) |
| **Застосунок** | `pazuzu-app` | `main.py` (Telegram-бот тощо), `TASK_QUEUE_ENABLED=true` |
| **Celery: source** | `pazuzu-source-worker` | Черга `source_load` (пайплайн завантаження джерел) |
| **Celery: LLM** | `pazuzu-llm-worker` | Черга `llm_processing` (OLX / ProZorro LLM-задачі) |
| **ngrok** | `pazuzu-ngrok` | Публічний HTTP-тунель до `pazuzu-app:8000` (за потреби) |

Порти на хості (за замовчуванням):

- `27017` — MongoDB
- `5672` — AMQP (RabbitMQ)
- `15672` — веб-інтерфейс керування RabbitMQ (Management UI)
- `4040` — локальний інспектор ngrok

Повна конфігурація — у кореневому файлі `docker-compose.yml` у репозиторії.

## 2) Передумови (Windows)

Встановіть:

- Docker Desktop (або Docker Engine + Compose plugin)
- Git

Перевірка:

```cmd
docker --version
docker compose version
git --version
```

У PowerShell для порожнього `.env` можна виконати: `New-Item .env -ItemType File` (у CMD — `copy NUL .env`).

## 3) Клонування репозиторію

```cmd
git clone <YOUR_REPO_URL> pazuzu
cd pazuzu
```

## 4) Конфігурація застосунку

Застосунок читає налаштування з `config/config.yaml` (має пріоритет над env для багатьох полів; див. `config/settings.py`).

Створіть файл на основі прикладу:

```cmd
copy config\config.example.yaml config\config.yaml
```

Заповніть мінімум:

- `telegram.bot_token` (якщо запускаєте бота)
- `llm.api_keys.*` (за потрібним провайдером)
- `mongodb.host`, `mongodb.port`, `mongodb.database_name`

Для Docker Compose:

- `mongodb.host` = `mongodb` (ім’я сервісу в мережі compose)
- `mongodb.port` = `27017`

### Черга задач (RabbitMQ / Celery)

У `docker-compose.yml` для `pazuzu-app` і воркерів уже задані змінні середовища: `TASK_QUEUE_ENABLED=true`, `RABBITMQ_HOST=rabbitmq` та облікові дані брокера. За потреби узгодьте їх із секцією `task_queue` у `config.yaml` (див. закоментований приклад у `config.example.yaml`).

Якщо змінюєте логін/пароль/vhost RabbitMQ, задайте їх у `.env` (див. нижче), щоб усі сервіси використовували однакові значення.

## 5) Файл `.env` у корені проєкту

Створіть `.env` (наприклад `copy NUL .env`) і додайте змінні.

**Обов’язково для ngrok** (якщо піднімаєте сервіс `ngrok`):

```env
NGROK_AUTHTOKEN=your_real_ngrok_token
```

**Опційно — облікові дані RabbitMQ** (мають збігатися з тим, що очікує застосунок у `config.yaml` або з дефолтами compose):

```env
RABBITMQ_DEFAULT_USER=pazuzu
RABBITMQ_DEFAULT_PASS=your_secure_password
RABBITMQ_DEFAULT_VHOST=pazuzu
```

Якщо ці змінні не задані, Compose використовує значення за замовчуванням `pazuzu` / `pazuzu` / `pazuzu` (див. `docker-compose.yml`).

## 6) Актуальна структура `docker-compose.yml`

Нижче — узгоджений з репозиторієм вміст (перевіряйте при оновленні гілки):

```yaml
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

  rabbitmq:
    image: rabbitmq:4-management
    container_name: pazuzu-rabbitmq
    restart: unless-stopped
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_DEFAULT_USER:-pazuzu}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_DEFAULT_PASS:-pazuzu}
      RABBITMQ_DEFAULT_VHOST: ${RABBITMQ_DEFAULT_VHOST:-pazuzu}
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - pazuzu_rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  pazuzu-app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pazuzu-app
    working_dir: /app
    restart: unless-stopped
    depends_on:
      mongodb:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./config/config.yaml:/app/config/config.yaml:ro
      - ./data:/app/data
      - ./temp:/app/temp
    environment:
      PYTHONPATH: /app
      TASK_QUEUE_ENABLED: "true"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: ${RABBITMQ_DEFAULT_USER:-pazuzu}
      RABBITMQ_PASSWORD: ${RABBITMQ_DEFAULT_PASS:-pazuzu}
      RABBITMQ_VHOST: ${RABBITMQ_DEFAULT_VHOST:-pazuzu}
    command: ["python", "main.py"]

  pazuzu-source-worker:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pazuzu-source-worker
    working_dir: /app
    restart: unless-stopped
    depends_on:
      mongodb:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./config/config.yaml:/app/config/config.yaml:ro
      - ./data:/app/data
      - ./temp:/app/temp
    environment:
      PYTHONPATH: /app
      TASK_QUEUE_ENABLED: "true"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: ${RABBITMQ_DEFAULT_USER:-pazuzu}
      RABBITMQ_PASSWORD: ${RABBITMQ_DEFAULT_PASS:-pazuzu}
      RABBITMQ_VHOST: ${RABBITMQ_DEFAULT_VHOST:-pazuzu}
    command: ["python", "-m", "celery", "-A", "business.celery_worker_entry:celery_app", "worker", "-Q", "source_load", "--loglevel=info", "--concurrency=1"]

  pazuzu-llm-worker:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pazuzu-llm-worker
    working_dir: /app
    restart: unless-stopped
    depends_on:
      mongodb:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./config/config.yaml:/app/config/config.yaml:ro
      - ./data:/app/data
      - ./temp:/app/temp
    environment:
      PYTHONPATH: /app
      TASK_QUEUE_ENABLED: "true"
      RABBITMQ_HOST: rabbitmq
      RABBITMQ_PORT: 5672
      RABBITMQ_USER: ${RABBITMQ_DEFAULT_USER:-pazuzu}
      RABBITMQ_PASSWORD: ${RABBITMQ_DEFAULT_PASS:-pazuzu}
      RABBITMQ_VHOST: ${RABBITMQ_DEFAULT_VHOST:-pazuzu}
    command: ["python", "-m", "celery", "-A", "business.celery_worker_entry:celery_app", "worker", "-Q", "llm_processing", "--loglevel=info", "--concurrency=1"]

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
  pazuzu_rabbitmq_data:
```

Примітки:

- Воркери викликають `python -m celery -A business.celery_worker_entry:celery_app` (модуль `business/celery_worker_entry.py` лише реекспортує `celery_app` для стабільного імпорту в Docker). `PYTHONPATH=/app` задано в `Dockerfile` і в `environment`.
- `main.py` за замовчуванням запускає Telegram-бот; для batch-режиму використовуйте `--generate-file`.
- Якщо **ngrok не потрібен**, можна тимчасово зупинити лише цей сервіс: `docker compose stop ngrok` або закоментувати сервіс у локальній копії compose (не комітьте випадково, якщо це лише ваш експеримент).

## 7) Перший запуск: збірка і старт

```cmd
docker compose up -d --build
```

Перевірка статусу:

```cmd
docker compose ps
```

Логи (за потреби):

```cmd
docker compose logs -f pazuzu-app
docker compose logs -f pazuzu-source-worker
docker compose logs -f pazuzu-llm-worker
docker compose logs -f ngrok
```

RabbitMQ Management UI: у браузері відкрийте `http://localhost:15672` (логін/пароль — як у `.env` або дефолти `pazuzu` / `pazuzu`).

Публічний URL ngrok (якщо сервіс запущений):

```cmd
docker compose logs ngrok
```

Або інспектор: `http://localhost:4040`.

## 8) Міграції БД (після першого старту)

```cmd
docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
```

## 9) Оновлення встановлення (новий код і залежності)

Порядок дій після `git pull`:

1. Перевірте `config/config.example.yaml` і при потребі злийте зміни у свій `config.yaml`.
2. Перебудуйте образи та перезапустіть сервіси (щоб підтягнути зміни в `requirements.txt`, `Dockerfile` і коді):

```cmd
git pull
docker compose up -d --build
```

3. Застосуйте міграції БД:

```cmd
docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
```

4. Якщо після оновлення щось виглядає «застряглим», перезапустіть воркери:

```cmd
docker compose restart pazuzu-source-worker pazuzu-llm-worker
```

5. Перевірте логи застосунку й воркерів (розділ 7).

### Додавання нових компонентів у Docker

Коли в репозиторій додають новий сервіс (наприклад, ще один Celery worker, sidecar або БД):

1. Оновіть `docker-compose.yml` у репозиторії та перезберіть стек: `docker compose up -d --build`.
2. Додайте потрібні змінні в `.env` і задокументуйте їх тут або в `config.example.yaml`.
3. Якщо новий компонент залежить від мережі compose, використовуйте **ім’я сервісу** як hostname (як `mongodb` чи `rabbitmq`).
4. Після оновлення гайду звіряйте вставлений фрагмент YAML з фактичним `docker-compose.yml`.

## 10) Бекап і відновлення MongoDB

Бекап:

```cmd
docker exec pazuzu-mongodb sh -c "mongodump --out /data/db/dump"
```

Відновлення:

```cmd
docker exec pazuzu-mongodb sh -c "mongorestore /data/db/dump"
```

## 11) Зупинка

```cmd
docker compose down
```

Щоб зупинити й видалити томи БД і RabbitMQ (**дані будуть втрачені**):

```cmd
docker compose down -v
```
