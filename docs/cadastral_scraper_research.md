# Дослідження kadastrova-karta.com для скрапера кадастру

## Мета

Визначити, як kadastrova-karta.com завантажує дані про земельні ділянки, щоб реалізувати скрапер з прямими HTTP-запитами.

## Результати дослідження (18.02.2026)

### Джерело даних: Mapbox Vector Tiles (MVT/PBF)

Сайт використовує **векторні тайли** у форматі Mapbox Vector Tile (.pbf).

**Capabilities:** https://kadastrova-karta.com/tiles/capabilities/kadastr.json

```json
{
  "tiles": ["https://kadastrova-karta.com/tiles/maps/kadastr/{z}/{x}/{y}.pbf"],
  "vector_layers": [
    {"id": "polygons", "minzoom": 3, "maxzoom": 11, "geometry_type": "polygon",
     "tiles": [".../kadastr/polygons/{z}/{x}/{y}.pbf"]},
    {"id": "land_polygons", "minzoom": 11, "maxzoom": 16, "geometry_type": "polygon",
     "tiles": [".../kadastr/land_polygons/{z}/{x}/{y}.pbf"]},
    {"id": "centroids", "minzoom": 3, "maxzoom": 7, "geometry_type": "point",
     "tiles": [".../kadastr/centroids/{z}/{x}/{y}.pbf"]}
  ]
}
```

### URL для детальних ділянок (land_polygons)

```
https://kadastrova-karta.com/tiles/maps/kadastr/land_polygons/{z}/{x}/{y}.pbf
```

- **Zoom:** 11–16 (ділянки з'являються при zoom ≥ 11)
- **Формат:** Mapbox Vector Tile (Protobuf)
- **Схема:** XYZ (Web Mercator, EPSG:3857)

### Додаткові endpoint'и

| URL | Призначення |
|-----|-------------|
| `/search?q=...` | Пошук за кадастровим номером або адресою. Повертає HTML (turbo-stream) з cadastral_number, data-lat, data-lng |
| `/map/styles.json` | Стилі карти |
| `/tiles/capabilities/kadastr.json` | Опис шарів тайлів |

### Стратегія скрапінгу

1. **Ітерація по тайлах:** для кожного (z, x, y) у межах України завантажувати `land_polygons/{z}/{x}/{y}.pbf`
2. **Парсинг MVT:** використати бібліотеку `mapbox-vector-tile` для декодування .pbf
3. **Екстракція:** з кожної feature — geometry (polygon), properties (cadastral_number, area, purpose, category тощо)
4. **Дедуплікація:** за `cadastral_number` (upsert у MongoDB)

### Властивості features у land_polygons

| Поле MVT | Опис |
|----------|------|
| cadnum | Кадастровий номер |
| address | Адреса ділянки (якщо вказана в кадастрі) |
| purpose_code | Код призначення (14.02 тощо) |
| purpose | Текстова назва призначення |
| category | Категорія земель |
| area | Площа (у гектарах, 0.0222 = 222 м²) |
| ownership | Форма власності |

### Конвертація координат

Тайли в Web Mercator (EPSG:3857). Формули для lat/lon → tile (x, y):

```
n = 2^z
x = floor((lon + 180) / 360 * n)
y = floor((1 - ln(tan(rad(lat)) + sec(rad(lat))) / π) / 2 * n)
```

## Структура кадастрового номера (для індексації)

Формат: **НКЗ:НКК:НЗД** або **10:2:3:4** (6310138500:10:012:0045)

- **НКЗ** (12 цифр): номер кадастрової зони. Перші 10 = код КОАТУУ.
- **КОАТУУ** (10 цифр): 1–2 = область, 3–5 = район, 6–8 = місто, 9–10 = село.
- **НКК** (3 цифри): кадастровий квартал.
- **НЗД** (4 цифри): номер земельної ділянки.

З номера можна визначити область (63=Харківська, 80=Київ, 32=Київська тощо). Маппінг: `config/koatuu_oblast_codes.yaml`. Індекс: `cadastral_parcel_location_index`.

## Поточний стан

Дослідження завершено. Скрапер оновлено для роботи з vector tiles:
- **fetcher.py** — завантаження `land_polygons/{z}/{x}/{y}.pbf`
- **parser.py** — парсинг MVT через `mapbox-vector-tile`
- **grid_iterator.py** — генерація комірок у форматі тайлів (z, x, y)
