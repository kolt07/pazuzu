# -*- coding: utf-8 -*-
"""
Міграція 012: Створення колекцій для географічних даних (regions, cities, streets, buildings).
"""

from data.database.connection import MongoDBConnection


def up():
    """Створює колекції та індекси для географічних даних."""
    db = MongoDBConnection.get_database()
    
    # Створюємо колекції (вони будуть створені автоматично при першому записі)
    # Але ми можемо створити індекси заздалегідь
    
    # Regions
    regions_collection = db["regions"]
    regions_collection.create_index("name", unique=True)
    regions_collection.create_index("name_normalized")
    print("✓ Created indexes for regions collection")
    
    # Cities
    cities_collection = db["cities"]
    cities_collection.create_index([("region_id", 1), ("name_normalized", 1)], unique=True)
    cities_collection.create_index("region_id")
    cities_collection.create_index("name_normalized")
    print("✓ Created indexes for cities collection")
    
    # Streets
    streets_collection = db["streets"]
    streets_collection.create_index([("city_id", 1), ("name_normalized", 1)], unique=True)
    streets_collection.create_index("city_id")
    streets_collection.create_index("name_normalized")
    print("✓ Created indexes for streets collection")
    
    # Buildings
    buildings_collection = db["buildings"]
    buildings_collection.create_index([("street_id", 1), ("number", 1)], unique=True)
    buildings_collection.create_index("street_id")
    print("✓ Created indexes for buildings collection")
    
    print("Migration 012 completed: Geography collections created")


def down():
    """Видаляє колекції географічних даних."""
    db = MongoDBConnection.get_database()
    
    db["regions"].drop()
    db["cities"].drop()
    db["streets"].drop()
    db["buildings"].drop()
    
    print("Migration 012 rolled back: Geography collections dropped")


if __name__ == "__main__":
    up()
