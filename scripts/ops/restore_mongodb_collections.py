# -*- coding: utf-8 -*-
"""
Завантаження колекцій MongoDB з каталогу дампу (пара до dump_mongodb_collections.py).

Читає JSONL (Canonical Extended JSON), .meta.json (індекси/опції) і manifest.json.
Підключення — те саме, що в застосунку (Settings + MongoDBConnection).

Без --drop документи з уже існуючим _id пропускаються. З --drop колекція
спочатку видаляється і створюється наново (повна заміна з дампу).

Запуск (з кореня репо, UTF-8 консоль):
  py scripts/ops/restore_mongodb_collections.py
  py scripts/ops/restore_mongodb_collections.py --dir data/mongo_dumps/20260921_081714
  py scripts/ops/restore_mongodb_collections.py --drop
  py scripts/ops/restore_mongodb_collections.py --collections users,regions --drop
  py scripts/ops/restore_mongodb_collections.py --exclude cadastral_parcels,logs
  py scripts/ops/restore_mongodb_collections.py --dry-run
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, TextIO, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DUMP_ROOT = PROJECT_ROOT / "data" / "mongo_dumps"
UNSAFE_FILENAME_RE = re.compile(r"[^0-9A-Za-z._-]+")
LARGE_COLLECTION_WARN_DOCS = 1_000_000
DEFAULT_BATCH_SIZE = 500
DUPLICATE_KEY_CODE = 11000

CREATE_COLLECTION_OPTION_KEYS = frozenset({
    "capped",
    "size",
    "max",
    "validator",
    "validationLevel",
    "validationAction",
    "collation",
    "storageEngine",
    "indexOptionDefaults",
    "timeseries",
    "expireAfterSeconds",
    "changeStreamPreAndPostImages",
    "clusteredIndex",
    "encryptedFields",
})

INDEX_OPTION_KEYS = (
    "unique",
    "sparse",
    "expireAfterSeconds",
    "partialFilterExpression",
    "collation",
    "hidden",
    "wildcardProjection",
    "weights",
    "default_language",
    "language_override",
    "textIndexVersion",
    "2dsphereIndexVersion",
    "bits",
    "min",
    "max",
    "bucketSize",
)

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


def _resolve_project_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def find_latest_dump_dir(root: Path = DEFAULT_DUMP_ROOT) -> Path:
    if not root.exists():
        raise FileNotFoundError(
            f"Каталог дампів не знайдено: {root}. Спочатку запустіть dump_mongodb_collections.py"
        )

    def _has_dump_files(path: Path) -> bool:
        if (path / "manifest.json").is_file():
            return True
        return any(
            child.is_file() and (child.name.endswith(".jsonl") or child.name.endswith(".jsonl.gz"))
            for child in path.iterdir()
        )

    if _has_dump_files(root):
        subdirs = [p for p in root.iterdir() if p.is_dir() and _has_dump_files(p)]
        if subdirs:
            return max(subdirs, key=lambda p: p.stat().st_mtime)
        return root

    subdirs = [p for p in root.iterdir() if p.is_dir() and _has_dump_files(p)]
    if not subdirs:
        raise FileNotFoundError(
            f"У {root} немає дампу з manifest.json або JSONL. Передайте --dir"
        )
    return max(subdirs, key=lambda p: p.stat().st_mtime)


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Очікувано JSON-об'єкт: {path}")
    return payload


def _open_text_read(path: Path) -> TextIO:
    gzip_enabled = path.name.endswith(".gz")
    if gzip_enabled:
        return gzip.open(path, "rt", encoding="utf-8")  # type: ignore[return-value]
    return path.open("r", encoding="utf-8")


def _iter_documents(path: Path) -> Iterator[Any]:
    from bson import json_util

    with _open_text_read(path) as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                yield json_util.loads(line)
            except Exception as exc:
                raise ValueError(f"{path.name}:{line_no}: не вдалося розібрати документ ({exc})") from exc


def _file_stem_from_data_name(file_name: str) -> str:
    name = file_name
    if name.endswith(".jsonl.gz"):
        return name[: -len(".jsonl.gz")]
    if name.endswith(".jsonl"):
        return name[: -len(".jsonl")]
    return Path(name).stem


def _discover_without_manifest(dump_dir: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for path in sorted(dump_dir.iterdir()):
        if not path.is_file():
            continue
        if not (path.name.endswith(".jsonl") or path.name.endswith(".jsonl.gz")):
            continue
        stem = _file_stem_from_data_name(path.name)
        meta_path = dump_dir / f"{stem}.meta.json"
        collection_name = stem
        meta: Dict[str, Any] = {}
        if meta_path.is_file():
            meta = _load_json(meta_path)
            collection_name = str(meta.get("name") or stem)
        items.append({
            "name": collection_name,
            "type": meta.get("type") or "collection",
            "file": path.name,
            "meta_file": meta_path.name if meta_path.is_file() else "",
            "documents": meta.get("documents"),
        })
    return items


def list_dump_collections(dump_dir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    manifest_path = dump_dir / "manifest.json"
    if manifest_path.is_file():
        manifest = _load_json(manifest_path)
        collections = [
            item for item in (manifest.get("collections") or [])
            if isinstance(item, dict) and item.get("name") and not item.get("error")
        ]
        return manifest, collections
    return {}, _discover_without_manifest(dump_dir)


def _resolve_data_path(dump_dir: Path, entry: Dict[str, Any]) -> Path:
    file_name = str(entry.get("file") or "").strip()
    if file_name:
        path = dump_dir / file_name
        if path.is_file():
            return path
    stem = _safe_filename(str(entry.get("name") or ""))
    for candidate in (dump_dir / f"{stem}.jsonl.gz", dump_dir / f"{stem}.jsonl"):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"Немає JSONL для колекції {entry.get('name')}")


def _load_meta(dump_dir: Path, entry: Dict[str, Any]) -> Dict[str, Any]:
    meta_name = str(entry.get("meta_file") or "").strip()
    candidates: List[Path] = []
    if meta_name:
        candidates.append(dump_dir / meta_name)
    stem = _safe_filename(str(entry.get("name") or ""))
    candidates.append(dump_dir / f"{stem}.meta.json")
    for path in candidates:
        if path.is_file():
            return _load_json(path)
    return {}


def _create_collection_kwargs(options: Any) -> Dict[str, Any]:
    if not isinstance(options, dict):
        return {}
    return {key: value for key, value in options.items() if key in CREATE_COLLECTION_OPTION_KEYS}


def _index_keys(raw_key: Any) -> List[Tuple[str, Any]]:
    pairs: List[Tuple[str, Any]] = []
    if not isinstance(raw_key, list):
        return pairs
    for item in raw_key:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            pairs.append((str(item[0]), item[1]))
    return pairs


def _ensure_collection(db: Any, name: str, options: Dict[str, Any], drop: bool) -> Any:
    existing_names = set(db.list_collection_names())
    if drop and name in existing_names:
        db.drop_collection(name)
        existing_names.discard(name)
    if name not in existing_names:
        create_kwargs = _create_collection_kwargs(options)
        if create_kwargs:
            db.create_collection(name, **create_kwargs)
        else:
            db.create_collection(name)
    return db[name]


def _insert_batch(coll: Any, batch: List[Any]) -> Tuple[int, int, int]:
    if not batch:
        return 0, 0, 0
    from pymongo.errors import BulkWriteError

    try:
        result = coll.insert_many(batch, ordered=False)
        return len(result.inserted_ids), 0, 0
    except BulkWriteError as exc:
        details = exc.details or {}
        inserted = int(details.get("nInserted") or 0)
        dupes = 0
        other = 0
        for err in details.get("writeErrors") or []:
            if int(err.get("code") or 0) == DUPLICATE_KEY_CODE:
                dupes += 1
            else:
                other += 1
        return inserted, dupes, other


def _restore_indexes(coll: Any, indexes: Any) -> Tuple[int, int]:
    from pymongo.errors import OperationFailure, PyMongoError

    created = 0
    skipped = 0
    if not isinstance(indexes, dict):
        return created, skipped
    existing = set(coll.index_information().keys())
    for index_name, spec in indexes.items():
        if index_name == "_id_" or not isinstance(spec, dict):
            continue
        if str(index_name) in existing:
            skipped += 1
            continue
        keys = _index_keys(spec.get("key"))
        if not keys:
            skipped += 1
            continue
        kwargs: Dict[str, Any] = {"name": str(index_name)}
        for opt in INDEX_OPTION_KEYS:
            if opt in spec:
                kwargs[opt] = spec[opt]
        try:
            coll.create_index(keys, **kwargs)
            created += 1
        except OperationFailure:
            skipped += 1
        except PyMongoError as exc:
            print(f"[restore] індекс {coll.name}.{index_name}: {exc}", file=sys.stderr, flush=True)
            skipped += 1
    return created, skipped


def restore_database(
    db: Any,
    dump_dir: Path,
    *,
    collection_filter: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
    drop: bool = False,
    restore_indexes: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> Dict[str, Any]:
    dump_dir = dump_dir.resolve()
    manifest, available = list_dump_collections(dump_dir)
    by_name = {str(item["name"]): item for item in available}
    excluded = {name.strip() for name in (exclude or []) if str(name).strip()}
    requested = list(collection_filter) if collection_filter else [item["name"] for item in available]
    missing = [name for name in requested if name not in by_name]
    selected = [by_name[name] for name in requested if name in by_name and name not in excluded]
    skipped_excluded = [name for name in requested if name in excluded]

    report: Dict[str, Any] = {
        "dump_dir": str(dump_dir),
        "database": db.name,
        "dump_database": manifest.get("database"),
        "drop": drop,
        "dry_run": dry_run,
        "collections": [],
        "excluded_collections": skipped_excluded,
        "missing_collections": missing,
    }

    if dry_run:
        existing = set(db.list_collection_names())
        for item in selected:
            estimated = int(item.get("documents") or 0)
            report["collections"].append({
                "name": item["name"],
                "documents": estimated,
                "exists_in_db": item["name"] in existing,
                "file": item.get("file"),
            })
        return report

    chunk = max(1, int(batch_size))
    for item in selected:
        name = str(item["name"])
        try:
            data_path = _resolve_data_path(dump_dir, item)
            meta = _load_meta(dump_dir, item)
            estimated = int(item.get("documents") or meta.get("documents") or 0)
            if estimated >= LARGE_COLLECTION_WARN_DOCS:
                print(
                    f"[restore] увага: {name} ≈ {estimated} документів — завантаження може зайняти багато часу",
                    flush=True,
                )
            print(f"[restore] {name} ← {data_path.name}", flush=True)
            coll = _ensure_collection(db, name, meta.get("options") or {}, drop=drop)
            inserted = 0
            duplicates = 0
            other_errors = 0
            batch: List[Any] = []
            for doc in _iter_documents(data_path):
                batch.append(doc)
                if len(batch) >= chunk:
                    ins, dup, err = _insert_batch(coll, batch)
                    inserted += ins
                    duplicates += dup
                    other_errors += err
                    batch = []
            ins, dup, err = _insert_batch(coll, batch)
            inserted += ins
            duplicates += dup
            other_errors += err

            indexes_created = 0
            indexes_skipped = 0
            if restore_indexes:
                indexes_created, indexes_skipped = _restore_indexes(coll, meta.get("indexes"))

            entry = {
                "name": name,
                "inserted": inserted,
                "duplicates_skipped": duplicates,
                "write_errors": other_errors,
                "indexes_created": indexes_created,
                "indexes_skipped": indexes_skipped,
                "file": data_path.name,
            }
            report["collections"].append(entry)
            print(
                f"[restore] {name}: вставлено {inserted}, "
                f"дублікати {duplicates}, помилки запису {other_errors}, "
                f"індекси +{indexes_created}",
                flush=True,
            )
            if other_errors:
                print(f"[restore] {name}: були помилки запису (не дублікати _id)", file=sys.stderr)
        except Exception as exc:
            print(f"[restore] {name}: помилка — {exc}", file=sys.stderr, flush=True)
            report["collections"].append({"name": name, "error": str(exc)})

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Завантажити колекції MongoDB з каталогу дампу data/mongo_dumps/.",
    )
    parser.add_argument(
        "--dir",
        default="",
        help="Каталог дампу (за замовчуванням найновіший у data/mongo_dumps/)",
    )
    parser.add_argument(
        "--collections",
        default="",
        help="Список колекцій через кому (за замовчуванням усі з дампу)",
    )
    parser.add_argument(
        "--exclude",
        default="",
        help="Колекції, які пропустити (через кому)",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Перед завантаженням видалити колекцію в цільовій БД (повна заміна)",
    )
    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Не створювати індекси з .meta.json",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Розмір пакету insert_many (за замовчуванням {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument("--dry-run", action="store_true", help="Лише показати план, без запису в БД")
    args = parser.parse_args()

    from config.settings import Settings
    from data.database.connection import MongoDBConnection

    try:
        dump_dir = _resolve_project_path(args.dir) if args.dir else find_latest_dump_dir()
        if not dump_dir.is_dir():
            print(f"[restore] каталог не знайдено: {dump_dir}", file=sys.stderr)
            return 1
    except FileNotFoundError as exc:
        print(f"[restore] {exc}", file=sys.stderr)
        return 1

    settings = Settings()
    MongoDBConnection.initialize(settings)
    try:
        db = MongoDBConnection.get_database()
        print(
            f"[restore] db={settings.mongodb_database_name} "
            f"host={settings.mongodb_host}:{settings.mongodb_port} "
            f"dir={dump_dir}",
            flush=True,
        )
        report = restore_database(
            db,
            dump_dir,
            collection_filter=_parse_collection_names(args.collections),
            exclude=_parse_collection_names(args.exclude),
            drop=bool(args.drop),
            restore_indexes=not bool(args.no_indexes),
            batch_size=int(args.batch_size),
            dry_run=bool(args.dry_run),
        )
    finally:
        MongoDBConnection.close()

    if report.get("missing_collections"):
        print("[restore] немає в дампі:", ", ".join(report["missing_collections"]), file=sys.stderr)
    if report.get("excluded_collections"):
        print("[restore] виключено:", ", ".join(report["excluded_collections"]))

    restored = report.get("collections") or []
    errors = [item for item in restored if item.get("error")]
    write_errors = [item for item in restored if item.get("write_errors")]
    if args.dry_run:
        print(f"[restore] dry-run: {len(restored)} колекцій")
        for item in restored:
            exists = "вже є в БД" if item.get("exists_in_db") else "нової в БД немає"
            estimated = int(item.get("documents") or 0)
            warn = "  [велика]" if estimated >= LARGE_COLLECTION_WARN_DOCS else ""
            drop_hint = "; зникне при --drop" if args.drop and item.get("exists_in_db") else ""
            print(f"  - {item['name']}: ~{estimated} документів, {exists}{drop_hint}{warn}")
        return 0

    print(f"[restore] готово: {len(restored) - len(errors)} колекцій, помилок: {len(errors)}")
    if errors or report.get("missing_collections") or write_errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
