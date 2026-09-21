# -*- coding: utf-8 -*-
"""
Вивантаження вмісту колекцій MongoDB застосунку в локальний каталог.

Підключення — те саме, що в застосунку (Settings + MongoDBConnection).
Документи пишуться потоково у JSONL (Canonical Extended JSON), щоб зберегти
ObjectId, дати та інші BSON-типи. Індекси й опції колекції — у сусідньому
.meta.json. Маніфест запуску — manifest.json.

Запуск (з кореня репо, UTF-8 консоль):
  py scripts/ops/dump_mongodb_collections.py
  py scripts/ops/dump_mongodb_collections.py --gzip
  py scripts/ops/dump_mongodb_collections.py --collections users,regions
  py scripts/ops/dump_mongodb_collections.py --exclude cadastral_parcels,logs,llm_cache
  py scripts/ops/dump_mongodb_collections.py --out data/mongo_dumps/manual
  py scripts/ops/dump_mongodb_collections.py --dry-run

Відновлення: py scripts/ops/restore_mongodb_collections.py --dir <каталог>
"""


from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, TextIO

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DUMP_ROOT = PROJECT_ROOT / "data" / "mongo_dumps"
UNSAFE_FILENAME_RE = re.compile(r"[^0-9A-Za-z._-]+")
LARGE_COLLECTION_WARN_DOCS = 1_000_000

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _safe_filename(name: str) -> str:
    cleaned = UNSAFE_FILENAME_RE.sub("_", (name or "").strip())
    return cleaned.strip("._") or "collection"


def _parse_collection_names(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    names = [part.strip() for part in str(raw).split(",") if part.strip()]
    return names or None


def _open_text(path: Path, gzip_enabled: bool) -> TextIO:
    if gzip_enabled:
        return gzip.open(path, "wt", encoding="utf-8")  # type: ignore[return-value]
    return path.open("w", encoding="utf-8", newline="\n")


def _list_dumpable_collections(db: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for spec in db.list_collections():
        name = str(spec.get("name") or "").strip()
        if not name or name.startswith("system."):
            continue
        items.append({
            "name": name,
            "type": spec.get("type") or "collection",
            "options": spec.get("options") or {},
        })
    items.sort(key=lambda item: item["name"])
    return items


def _dump_collection_documents(
    coll: Any,
    dest_path: Path,
    gzip_enabled: bool,
    json_options: Any,
) -> Dict[str, Any]:
    count = 0
    with _open_text(dest_path, gzip_enabled) as handle:
        cursor = coll.find({}, no_cursor_timeout=True).batch_size(500)
        try:
            from bson import json_util

            for doc in cursor:
                handle.write(json_util.dumps(doc, json_options=json_options, ensure_ascii=False))
                handle.write("\n")
                count += 1
        finally:
            cursor.close()
    return {
        "file": dest_path.name,
        "documents": count,
        "bytes": dest_path.stat().st_size,
    }


def _write_collection_meta(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")


def dump_database(
    db: Any,
    out_dir: Path,
    *,
    collection_filter: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
    gzip_enabled: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    from bson.json_util import CANONICAL_JSON_OPTIONS

    out_dir = out_dir.resolve()
    available = _list_dumpable_collections(db)
    by_name = {item["name"]: item for item in available}
    excluded = {name.strip() for name in (exclude or []) if str(name).strip()}

    requested = list(collection_filter) if collection_filter else [item["name"] for item in available]
    missing = [name for name in requested if name not in by_name]
    selected = [by_name[name] for name in requested if name in by_name and name not in excluded]
    skipped_excluded = [name for name in requested if name in excluded]
    skipped_views = [item["name"] for item in selected if item["type"] == "view"]
    to_dump = [item for item in selected if item["type"] != "view"]

    manifest: Dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "database": db.name,
        "output_dir": str(out_dir),
        "gzip": gzip_enabled,
        "dry_run": dry_run,
        "collections": [],
        "skipped_views": skipped_views,
        "excluded_collections": skipped_excluded,
        "missing_collections": missing,
    }

    if dry_run:
        for item in to_dump:
            estimated = db[item["name"]].estimated_document_count()
            manifest["collections"].append({
                "name": item["name"],
                "type": item["type"],
                "estimated_documents": estimated,
            })
        return manifest

    out_dir.mkdir(parents=True, exist_ok=True)

    for item in to_dump:
        name = item["name"]
        coll = db[name]
        file_stem = _safe_filename(name)
        data_suffix = ".jsonl.gz" if gzip_enabled else ".jsonl"
        data_path = out_dir / f"{file_stem}{data_suffix}"
        meta_path = out_dir / f"{file_stem}.meta.json"
        estimated = coll.estimated_document_count()
        if estimated >= LARGE_COLLECTION_WARN_DOCS:
            print(
                f"[dump] увага: {name} ≈ {estimated} документів — вивантаження може зайняти багато місця і часу",
                flush=True,
            )
        print(f"[dump] {name} → {data_path.name}", flush=True)
        try:
            doc_stats = _dump_collection_documents(
                coll,
                data_path,
                gzip_enabled=gzip_enabled,
                json_options=CANONICAL_JSON_OPTIONS,
            )
            meta = {
                "name": name,
                "type": item["type"],
                "options": item["options"],
                "indexes": coll.index_information(),
                "documents": doc_stats["documents"],
                "data_file": doc_stats["file"],
            }
            _write_collection_meta(meta_path, meta)
            manifest["collections"].append({
                "name": name,
                "type": item["type"],
                "documents": doc_stats["documents"],
                "file": doc_stats["file"],
                "meta_file": meta_path.name,
                "bytes": doc_stats["bytes"],
            })
            print(f"[dump] {name}: {doc_stats['documents']} документів, {doc_stats['bytes']} байт", flush=True)
        except Exception as exc:
            print(f"[dump] {name}: помилка — {exc}", file=sys.stderr, flush=True)
            manifest["collections"].append({
                "name": name,
                "type": item["type"],
                "error": str(exc),
            })

    manifest_path = out_dir / "manifest.json"
    _write_collection_meta(manifest_path, manifest)
    print(f"[dump] маніфест: {manifest_path}", flush=True)
    return manifest


def _resolve_out_dir(raw_out: Optional[str]) -> Path:
    if raw_out:
        path = Path(raw_out)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DEFAULT_DUMP_ROOT / stamp


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Вивантажити вміст колекцій MongoDB застосунку в локальний каталог поза git.",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Каталог вивантаження (за замовчуванням data/mongo_dumps/<timestamp>)",
    )
    parser.add_argument(
        "--collections",
        default="",
        help="Список колекцій через кому (за замовчуванням усі, крім system.* і views)",
    )
    parser.add_argument(
        "--exclude",
        default="",
        help="Колекції, які пропустити (через кому)",
    )
    parser.add_argument("--gzip", action="store_true", help="Стискати JSONL у .jsonl.gz")
    parser.add_argument("--dry-run", action="store_true", help="Лише показати колекції, без запису файлів")
    args = parser.parse_args()

    from config.settings import Settings
    from data.database.connection import MongoDBConnection

    settings = Settings()
    MongoDBConnection.initialize(settings)
    try:
        db = MongoDBConnection.get_database()
        out_dir = _resolve_out_dir(args.out or None)
        print(
            f"[dump] db={settings.mongodb_database_name} "
            f"host={settings.mongodb_host}:{settings.mongodb_port} "
            f"out={out_dir}",
            flush=True,
        )
        manifest = dump_database(
            db,
            out_dir,
            collection_filter=_parse_collection_names(args.collections),
            exclude=_parse_collection_names(args.exclude),
            gzip_enabled=bool(args.gzip),
            dry_run=bool(args.dry_run),
        )
    finally:
        MongoDBConnection.close()

    if manifest.get("missing_collections"):
        print("[dump] немає в БД:", ", ".join(manifest["missing_collections"]), file=sys.stderr)
    if manifest.get("skipped_views"):
        print("[dump] пропущено views:", ", ".join(manifest["skipped_views"]))
    if manifest.get("excluded_collections"):
        print("[dump] виключено:", ", ".join(manifest["excluded_collections"]))

    dumped = manifest.get("collections") or []
    errors = [item for item in dumped if item.get("error")]
    if args.dry_run:
        print(f"[dump] dry-run: {len(dumped)} колекцій")
        for item in dumped:
            estimated = int(item.get("estimated_documents") or 0)
            warn = "  [велика]" if estimated >= LARGE_COLLECTION_WARN_DOCS else ""
            print(f"  - {item['name']}: ~{estimated} документів{warn}")
        return 0
    print(f"[dump] готово: {len(dumped) - len(errors)} колекцій, помилок: {len(errors)}")
    if errors or manifest.get("missing_collections"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
