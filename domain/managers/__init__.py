# -*- coding: utf-8 -*-
"""Domain managers — CollectionManager та ObjectManager."""

from domain.managers.collection_manager import (
    BaseCollectionManager,
    UnifiedListingsCollectionManager,
)
from domain.managers.object_manager import ObjectManager

__all__ = [
    "BaseCollectionManager",
    "UnifiedListingsCollectionManager",
    "ObjectManager",
]
