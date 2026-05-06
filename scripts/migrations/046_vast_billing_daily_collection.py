# -*- coding: utf-8 -*-
"""
Міграція 046: колекція vast_billing_daily — кеш добових витрат Vast (instance charges) для admin usage-stats.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.vast_billing_daily_repository import VastBillingDailyRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        VastBillingDailyRepository()._ensure_indexes()
        print("Міграція 046: vast_billing_daily готова.")
        return True
    except Exception as e:
        print("Помилка міграції 046:", e)
        return False


if __name__ == "__main__":
    import sys

    sys.exit(0 if run_migration() else 1)
