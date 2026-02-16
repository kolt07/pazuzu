# -*- coding: utf-8 -*-
"""
Domain layer: об'єкти даних, що ізольовані від БД.
- Entities: обгортають сирі документи, надають методи отримання властивостей.
- Managers: CollectionManager (робота з колекцією), ObjectManager (робота з записом).
- Services: GeoFilterService (формування геофільтрів).
- Models: FilterGroup, FilterElement, GeoFilter, FindQuery.
"""
