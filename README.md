# Prozzorro Parser

Парсер для роботи з даними Prozzorro.

## Структура проекту

```
.
├── main.py                 # Головний клас застосунку (вхідна точка)
├── data/                   # Шар роботи з даними
│   ├── repositories/      # Репозиторії
│   ├── models/            # Моделі даних
│   └── database/          # Робота з БД
├── business/               # Шар бізнес-логіки
│   ├── services/          # Бізнес-сервіси
│   └── use_cases/         # Use cases
├── transport/              # Транспортний шар
│   ├── dto/               # Data Transfer Objects
│   ├── requests/          # Моделі запитів
│   └── responses/         # Моделі відповідей
├── config/                # Конфігурація
│   └── settings.py        # Налаштування
└── utils/                  # Утиліти
```

## Запуск

### Локальний запуск

```bash
py main.py
```

### Запуск у Docker

#### Збірка образу

```bash
docker build -t prozzorro-parser .
```

#### Запуск контейнера

```bash
docker run --rm prozzorro-parser
```

#### Запуск з docker-compose

```bash
docker-compose up
```

Для запуску у фоновому режимі:```bash
docker-compose up -d
```Для зупинки:```bash
docker-compose down
```
