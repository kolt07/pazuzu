# -*- coding: utf-8 -*-
"""
Одноразовий запуск профілювання колекцій (дослідження даних).
Заповнює колекцію collection_knowledge статистикою по полях для prozorro_auctions, olx_listings, llm_cache.
Запуск: py scripts/run_data_profiling.py
Перед запуском переконайтесь, що міграція 016 виконана та MongoDB доступна.
"""

import sys
from pathlib import Path

# Додаємо корінь проекту в шлях
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.collection_knowledge_service import CollectionKnowledgeService


def main() -> int:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        service = CollectionKnowledgeService(sample_size=5000)
        result = service.run_profiling()
        for name, res in result.items():
            status = "OK" if res.get("success") else "FAIL"
            detail = res.get("total_documents", res.get("error", ""))
            print(f"  {name}: {status} — {detail}", flush=True)
        return 0
    except Exception as e:
        print(f"Помилка: {e}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
