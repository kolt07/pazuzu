# -*- coding: utf-8 -*-
"""
Міграція 013: Перетворення існуючих текстових адрес у посилання на топоніми.
"""

from typing import Dict, Any, List
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.geography_service import GeographyService


def migrate_olx_listings(geography_service: GeographyService, limit: int = None):
    """Мігрує адреси з OLX оголошень."""
    repo = OlxListingsRepository()
    
    # Отримуємо всі оголошення з адресами
    filter_query = {
        "$or": [
            {"detail.llm.addresses": {"$exists": True, "$ne": []}},
            {"detail.resolved_locations": {"$exists": True, "$ne": []}}
        ]
    }
    
    docs = repo.find_many(filter=filter_query, sort=None, limit=limit, skip=None)
    print(f"Found {len(docs)} OLX listings with addresses")
    
    updated_count = 0
    for doc in docs:
        try:
            address_refs_list = []
            
            # Обробляємо адреси з LLM
            llm = doc.get("detail", {}).get("llm", {})
            addresses = llm.get("addresses", [])
            
            for addr in addresses:
                if isinstance(addr, dict):
                    address_refs = geography_service.resolve_address(addr)
                    if address_refs.get("region_id") or address_refs.get("city_id"):
                        address_refs_list.append(address_refs["address_refs"])
            
            # Оновлюємо документ
            if address_refs_list:
                repo.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"detail.address_refs": address_refs_list}}
                )
                updated_count += 1
        except Exception as e:
            print(f"Error processing OLX listing {doc.get('url', 'unknown')}: {e}")
            continue
    
    print(f"Updated {updated_count} OLX listings")
    return updated_count


def migrate_prozorro_auctions(geography_service: GeographyService, limit: int = None):
    """Мігрує адреси з Prozorro аукціонів."""
    repo = ProZorroAuctionsRepository()
    
    # Отримуємо всі аукціони з адресами
    filter_query = {
        "$or": [
            {"auction_data.items.address": {"$exists": True}},
            {"auction_data.address_refs": {"$exists": True}}
        ]
    }
    
    docs = repo.find_many(filter=filter_query, sort=None, limit=limit, skip=None)
    print(f"Found {len(docs)} Prozorro auctions with addresses")
    
    updated_count = 0
    for doc in docs:
        try:
            auction_data = doc.get("auction_data", {})
            
            # Перевіряємо чи вже є address_refs
            if auction_data.get("address_refs"):
                continue
            
            # Отримуємо адреси з items
            items = auction_data.get("items", [])
            address_refs_list = []
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                address = item.get("address", {})
                if not address:
                    continue
                
                # Формуємо адресу з даних ProZorro
                addr_data = {}
                
                region_obj = address.get("region", {})
                if isinstance(region_obj, dict):
                    region_ua = region_obj.get("uk_UA", "")
                    if region_ua:
                        addr_data["region"] = region_ua.replace(" область", "").replace(" обл.", "").strip()
                
                locality_obj = address.get("locality", {})
                if isinstance(locality_obj, dict):
                    addr_data["settlement"] = locality_obj.get("uk_UA", "")
                
                street_address_obj = address.get("streetAddress", {})
                if isinstance(street_address_obj, dict):
                    street_ua = street_address_obj.get("uk_UA", "")
                    if street_ua:
                        parts = street_ua.split(" ", 1)
                        if len(parts) > 1:
                            addr_data["street_type"] = parts[0]
                            addr_data["street"] = parts[1]
                        else:
                            addr_data["street"] = street_ua
                
                if addr_data.get("region") or addr_data.get("settlement"):
                    address_refs = geography_service.resolve_address(addr_data)
                    if address_refs.get("region_id") or address_refs.get("city_id"):
                        address_refs_list.append(address_refs["address_refs"])
            
            # Оновлюємо документ
            if address_refs_list:
                repo.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"auction_data.address_refs": address_refs_list}}
                )
                updated_count += 1
        except Exception as e:
            print(f"Error processing Prozorro auction {doc.get('auction_id', 'unknown')}: {e}")
            continue
    
    print(f"Updated {updated_count} Prozorro auctions")
    return updated_count


def up():
    """Виконує міграцію адрес."""
    print("Starting migration 013: Converting addresses to references...")
    
    geography_service = GeographyService()
    
    # Мігруємо OLX оголошення
    print("\nMigrating OLX listings...")
    olx_count = migrate_olx_listings(geography_service)
    
    # Мігруємо Prozorro аукціони
    print("\nMigrating Prozorro auctions...")
    prozorro_count = migrate_prozorro_auctions(geography_service)
    
    print(f"\nMigration 013 completed:")
    print(f"  - OLX listings updated: {olx_count}")
    print(f"  - Prozorro auctions updated: {prozorro_count}")


def down():
    """Відкатує міграцію (видаляє address_refs)."""
    print("Rolling back migration 013: Removing address references...")
    
    olx_repo = OlxListingsRepository()
    prozorro_repo = ProZorroAuctionsRepository()
    
    # Видаляємо address_refs з OLX
    olx_repo.collection.update_many(
        {"detail.address_refs": {"$exists": True}},
        {"$unset": {"detail.address_refs": ""}}
    )
    
    # Видаляємо address_refs з Prozorro
    prozorro_repo.collection.update_many(
        {"auction_data.address_refs": {"$exists": True}},
        {"$unset": {"auction_data.address_refs": ""}}
    )
    
    print("Migration 013 rolled back")


if __name__ == "__main__":
    up()
