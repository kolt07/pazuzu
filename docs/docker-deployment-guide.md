# Docker Deployment Guide (повний стек)

Гайд описує розгортання **pazuzu** у Docker: MongoDB, RabbitMQ, застосунок, Celery-воркери та **опційний** сервіс **ngrok** (публічний HTTP до `pazuzu-app:8000`). Ngrok увімкнюється профілем Compose `ngrok`, щоб можна було працювати без токена; налаштування URL у Telegram / mini app — у `docs/mini_app_setup.md`.

**Зміст:** [огляд](#огляд-стеку-і-портів) · [передумови](#передумови) · [**встановлення з нуля**](#встановлення-з-нуля) · [**оновлення**](#оновлення-існуючого-розгортання) · [довідка `docker-compose`](#довідка-файл-docker-composeyml) · [міграції](#міграції-бд) · [бекап](#бекап-і-відновлення-mongodb) · [зупинка](#зупинка) · [нові сервіси в compose](#додавання-нових-компонентів-у-docker)

---

## Огляд стеку і портів

| Компонент | Контейнер | Призначення |
|-----------|-----------|-------------|
| **MongoDB** | `pazuzu-mongodb` | Основна БД (`mongo:7`) |
| **RabbitMQ** | `pazuzu-rabbitmq` | Брокер черг для Celery (`rabbitmq:4-management`) |
| **Застосунок** | `pazuzu-app` | `main.py` (Telegram-бот тощо), `TASK_QUEUE_ENABLED=true` |
| **Celery: source** | `pazuzu-source-worker` | Черга `source_load` |
| **Celery: LLM** | `pazuzu-llm-worker` | Черга `llm_processing` (паралелізм з `task_queue.llm_worker_threads`) |
| **ngrok** | `pazuzu-ngrok` | Тунель до `pazuzu-app:8000` (лише з профілем `ngrok`) |

**Порти на хості:** `27017` (MongoDB), `5672` / `15672` (RabbitMQ), `4040` (веб-інспектор ngrok — якщо запущено сервіс ngrok). HTTP застосунку всередині мережі — `pazuzu-app:8000` (для ngrok-команди в compose).

Актуальний повний файл — у корені репозиторію: `docker-compose.yml`.

---

## Передумови

- **Docker Desktop** (або Docker Engine + Compose plugin v2)
- **Git**

Перевірка:

```cmd
docker --version
docker compose version
git --version
```

Далі в прикладах — **CMD**; у PowerShell для порожнього `.env`: `New-Item .env -ItemType File -Force`.

---

## Встановлення з нуля

Кроки для **нової** машини або **першого** клону репозиторію (немає локального `config.yaml`, томів і контейнерів pazuzu).

### Крок 1. Клонування

```cmd
git clone <YOUR_REPO_URL> pazuzu
cd pazuzu
```

### Крок 2. Конфігурація застосунку

Застосунок читає `config/config.yaml` (пріоритет над багатьма env; див. `config/settings.py`).

```cmd
copy config\config.example.yaml config\config.yaml
```

**Мінімум для Docker:**

- `mongodb.host` = `mongodb`, `mongodb.port` = `27017`, `mongodb.database_name` — за потреби
- `telegram.bot_token` — якщо потрібен бот
- `llm.api_keys.*` — за обраним провайдером

### Крок 3. Файл `.env` у корені проєкту

Створіть `.env` (у CMD: `copy NUL .env`; у PowerShell: `New-Item .env -ItemType File -Force`).

| Змінна | Обов’язково? | Примітка |
|--------|----------------|----------|
| `NGROK_AUTHTOKEN` | Так, якщо піднімаєте ngrok | [Authtoken](https://dashboard.ngrok.com/get-started/your-authtoken). Без нього контейнер ngrok видасть `ERR_NGROK_4018`. |
| `RABBITMQ_DEFAULT_USER` / `PASS` / `VHOST` | Ні | Якщо не задати, Compose використовує `pazuzu` / `pazuzu` / `pazuzu`. Мають збігатися з очікуваннями у `config.yaml` (секція `task_queue`, якщо розкоментована). |

Приклад фрагмента `.env`:

```env
# Для docker compose --profile ngrok
# NGROK_AUTHTOKEN=ваш_токен_з_dashboard

# RABBITMQ_DEFAULT_USER=pazuzu
# RABBITMQ_DEFAULT_PASS=your_secure_password
# RABBITMQ_DEFAULT_VHOST=pazuzu
```

У `docker-compose.yml` для app і воркерів уже задані `TASK_QUEUE_ENABLED=true` та змінні підключення до RabbitMQ.

### Крок 4. Каталоги на хості

Переконайтеся, що існують (або створіться при першому записі): `data\`, `temp\`. У compose вони змонтовані в `/app/data` та `/app/temp`.

### Крок 5. Перша збірка і запуск

Базовий стек (MongoDB, RabbitMQ, app, воркери):

```cmd
docker compose up -d --build
```

Разом із **ngrok** (спочатку додайте `NGROK_AUTHTOKEN` у `.env`):

```cmd
docker compose --profile ngrok up -d --build
```

### Крок 6. Міграції БД

Після успішного старту:

```cmd
docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
```

### Крок 7. Перевірка

```cmd
docker compose ps
docker compose logs -f pazuzu-app
```

За потреби логи воркерів: `pazuzu-source-worker`, `pazuzu-llm-worker`. Якщо запускали з `--profile ngrok`: `docker compose logs -f ngrok`.

**RabbitMQ Management UI:** `http://localhost:15672` (логін/пароль — з `.env` або `pazuzu` / `pazuzu`).

### Крок 8. Публічний HTTPS (опційно)

1. **Через compose:** крок 5 з `--profile ngrok`, токен у `.env`. Подивитися URL: `docker compose logs ngrok` або `http://localhost:4040`.
2. **Або** окремий тунель на хості (ngrok/cloudflared без Docker) — якщо так зручніше.
3. Виданий `https://…` URL пропишіть у `config.yaml` / mini app; інструкція — `docs/mini_app_setup.md`.

---

## Оновлення існуючого розгортання

Коли репозиторій уже склоновано, `config/config.yaml` налаштовано, контейнерів запускали раніше.

### Короткий чеклист

1. Зберегти або закомітити локальні правки (або `git stash`), щоб `git pull` не блокувався.
2. `git pull` (або `git pull origin main` — залежно від гілки).
3. Порівняти **`config/config.example.yaml`** з вашим **`config/config.yaml`** і перенести нові ключі вручну (не перезаписуйте весь файл сліпо — втратите секрети).
4. Перебудувати образи й перезапустити сервіси:
   ```cmd
   docker compose up -d --build
   ```
   Якщо підозра на кеш Docker:  
   `docker compose build --no-cache`  
   потім `docker compose up -d`.
5. Застосувати міграції:
   ```cmd
   docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
   ```
6. За потреби перезапустити лише воркери:
   ```cmd
   docker compose restart pazuzu-source-worker pazuzu-llm-worker
   ```
7. Якщо використовуєте ngrok: `NGROK_AUTHTOKEN` у `.env` і запуск з профілем  
   `docker compose --profile ngrok up -d --build`.

### Повне вирівнювання коду з remote (обережно)

Якщо потрібно **скинути всі локальні зміни** і зробити робочу копію ідентичною `origin/main`:

```cmd
git fetch origin
git reset --hard origin/main
git clean -fd
```

`git clean -fd` видаляє **не відстежувані** файли — зробіть резерв копій, якщо там щось важливе. Після цього знову `docker compose up -d --build` і міграції.

### Типові проблеми після оновлення

- **Старі образи:** після змін у `Dockerfile` або `requirements.txt` виконуйте `docker compose build --no-cache` для відповідних сервісів.
- **Конфлікти при `git pull`:** вирішіть у файлах, приберіть маркери `<<<<<<<`, `git add`, `git commit`.
- **ngrok і `ERR_NGROK_4018`:** перевірте `NGROK_AUTHTOKEN` у `.env` і що піднімаєте з `--profile ngrok`. Без токена не запускайте профіль або приберіть сервіс з локального override.

---

## Довідка: файл `docker-compose.yml`

Нижче — узгоджений з репозиторієм зміст (при оновленні гілки звіряйте з файлом у корені):

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
    command: ["python", "-m", "business.celery_worker_runner", "source_load"]

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
    command: ["python", "-m", "business.celery_worker_runner", "llm_processing"]

  ngrok:
    profiles:
      - ngrok
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

**Примітки:** ключ `version` у Compose v2 не потрібен. Сервіс **ngrok** має `profiles: [ngrok]` — звичайний `docker compose up` його не стартує. Воркери: `business.celery_worker_entry:celery_app`; `PYTHONPATH=/app` у `Dockerfile` і в `environment`. `main.py` за замовчуванням — Telegram-бот; batch: `--generate-file`.

---

## Міграції БД

Перший запуск і після кожного оновлення коду, де з’являються нові скрипти в `scripts/migrations`:

```cmd
docker compose exec pazuzu-app python scripts/migrations/run_migrations.py
```

---

## Бекап і відновлення MongoDB

Бекап:

```cmd
docker exec pazuzu-mongodb sh -c "mongodump --out /data/db/dump"
```

Відновлення:

```cmd
docker exec pazuzu-mongodb sh -c "mongorestore /data/db/dump"
```

---

## Зупинка

```cmd
docker compose down
```

Зупинити й **видалити томи** MongoDB і RabbitMQ (дані зникнуть):

```cmd
docker compose down -v
```

---

## Додавання нових компонентів у Docker

1. Оновіть `docker-compose.yml` у репозиторії; локально: `docker compose up -d --build`.
2. Додайте змінні в `.env` і за потреби — у `config/config.example.yaml`.
3. Нові сервіси в одній мережі compose звертаються один до одного за **ім’ям сервісу** (як `mongodb`, `rabbitmq`).
4. Оновіть цей гайд або коментарі в compose, щоб наступне розгортання було відтворюваним.
