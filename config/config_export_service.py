# -*- coding: utf-8 -*-
"""
Сервіс експорту та імпорту конфігураційного bundle.
"""

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import yaml

from config.config_loader import get_config_loader, compute_bundle_checksum, PLATFORM_VERSION


def _get_config_path() -> Path:
    """Шлях до каталогу config."""
    return Path(__file__).parent


def _get_project_root() -> Path:
    """Корінь проекту."""
    return Path(__file__).parent.parent


# Файли bundle (без секретів)
BUNDLE_FILES = [
    "bundle_metadata.yaml",
    "prompts.yaml",
    "data_dictionary.yaml",
    "app_metadata.yaml",
    "ProZorro_clasification_codes.yaml",
]

# Конфіг для експорту — секрети замінюються на порожні
SECRET_KEYS = ["bot_token", "api_key", "password"]


def _mask_secrets(data: dict) -> dict:
    """Рекурсивно маскує секретні поля."""
    if not isinstance(data, dict):
        return data
    result = {}
    for k, v in data.items():
        key_lower = k.lower()
        if key_lower == "api_keys" and isinstance(v, dict):
            result[k] = {kk: "" for kk in v}
        elif any(sec in key_lower for sec in SECRET_KEYS) or key_lower == "password":
            result[k] = ""
        elif isinstance(v, dict):
            result[k] = _mask_secrets(v)
        else:
            result[k] = v
    return result


def build_config_zip() -> Tuple[bytes, str]:
    """
    Збирає конфігураційний bundle у ZIP.

    Returns:
        (zip_bytes, suggested_filename)
    """
    config_path = _get_config_path()
    project_root = _get_project_root()
    meta = get_config_loader().get_bundle_metadata() or {}
    meta["created_at"] = datetime.now(timezone.utc).isoformat()
    meta["checksum"] = compute_bundle_checksum(config_path)
    meta_bytes = yaml.dump(meta, allow_unicode=True, default_flow_style=False, sort_keys=False).encode("utf-8")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("bundle_metadata.yaml", meta_bytes)
        for filename in BUNDLE_FILES:
            if filename == "bundle_metadata.yaml":
                continue
            path = config_path / filename
            if path.exists():
                content = path.read_bytes()
                zf.writestr(filename, content)

        # settings.yaml — з config.yaml, секрети замасковані
        config_yaml = config_path / "config.yaml"
        if config_yaml.exists():
            with open(config_yaml, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if data:
                data = _mask_secrets(data)
            settings_content = yaml.dump(data or {}, allow_unicode=True, default_flow_style=False, sort_keys=False)
            zf.writestr("settings.yaml", settings_content.encode("utf-8"))

        # developer_glossary.md
        for subpath in ("developer_glossary.md", "docs/developer_glossary.md"):
            path = project_root / subpath
            if path.exists():
                zf.writestr("developer_glossary.md", path.read_bytes())
                break

    buffer.seek(0)
    version = meta.get("config_version", "1.0")
    date_str = datetime.now().strftime("%Y%m%d")
    filename = f"pazuzu_config_v{version}_{date_str}.zip"
    return buffer.getvalue(), filename


def import_config_from_zip(zip_bytes: bytes) -> Tuple[bool, str]:
    """
    Імпортує конфіг з ZIP.

    Returns:
        (success, message)
    """
    config_path = _get_config_path()
    project_root = _get_project_root()
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            if "bundle_metadata.yaml" not in names:
                return False, "Відсутній bundle_metadata.yaml"

            # Валідація версії
            meta_content = zf.read("bundle_metadata.yaml").decode("utf-8")
            meta = yaml.safe_load(meta_content)
            min_ver = meta.get("min_platform_version", "0")
            if min_ver and min_ver > PLATFORM_VERSION:
                return False, f"Конфіг потребує платформу >= {min_ver}, поточна: {PLATFORM_VERSION}"

            # Збереження файлів
            for filename in names:
                if filename.startswith("__") or "/" in filename:
                    continue
                content = zf.read(filename)
                if filename == "settings.yaml":
                    dest = config_path / "config.yaml"
                else:
                    dest = config_path / filename
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(content)

        # Очищаємо кеш config loader
        get_config_loader().clear_cache()
        return True, "Конфігурацію імпортовано. Застосується після перезапуску."
    except Exception as e:
        return False, str(e)


# Колекції для експорту даних (без логів, тимчасових, артефактів)
DATA_EXPORT_COLLECTIONS = [
    "prozorro_auctions",
    "olx_listings",
    "unified_listings",
    "users",
    "regions",
    "cities",
    "collection_knowledge",
    "report_templates",
]


def build_data_zip(limit_per_collection: int = 10000) -> Tuple[bytes, str]:
    """
    Експортує дані з основних колекцій у ZIP (JSON файли).

    Returns:
        (zip_bytes, suggested_filename)
    """
    from bson import json_util
    from config.settings import Settings
    from data.database.connection import MongoDBConnection

    MongoDBConnection.initialize(Settings())
    db = MongoDBConnection.get_database()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for coll_name in DATA_EXPORT_COLLECTIONS:
            try:
                coll = db[coll_name]
                cursor = coll.find().limit(limit_per_collection)
                docs = list(cursor)
                json_str = json_util.dumps(docs, ensure_ascii=False, indent=2)
                zf.writestr(f"data/{coll_name}.json", json_str.encode("utf-8"))
            except Exception:
                pass
    buffer.seek(0)
    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"pazuzu_data_{date_str}.zip"
    return buffer.getvalue(), filename


def build_full_zip(limit_per_collection: int = 5000) -> Tuple[bytes, str]:
    """Конфіг + дані в одному архіві."""
    config_bytes, config_name = build_config_zip()
    data_bytes, data_name = build_data_zip(limit_per_collection)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("config.zip", config_bytes)
        zf.writestr("data.zip", data_bytes)
    buffer.seek(0)
    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"pazuzu_full_{date_str}.zip"
    return buffer.getvalue(), filename
