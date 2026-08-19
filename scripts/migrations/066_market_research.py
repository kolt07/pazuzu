# -*- coding: utf-8 -*-
"""
Міграція 066: deal_type на unified_listings (backfill sale) + колекція market_research_runs.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.market_research_repository import MarketResearchRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        unified = UnifiedListingsRepository()
        unified._ensure_indexes()
        try:
            unified.collection.create_index("deal_type")
        except Exception:
            pass
        result = unified.collection.update_many(
            {"$or": [{"deal_type": {"$exists": False}}, {"deal_type": None}, {"deal_type": ""}]},
            {"$set": {"deal_type": "sale"}},
        )
        MarketResearchRepository()._ensure_indexes()
        print(
            "Міграція 066: deal_type=sale для %s оголошень; market_research_runs ok."
            % result.modified_count
        )
        return True
    except Exception as e:
        print("Помилка міграції 066:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
