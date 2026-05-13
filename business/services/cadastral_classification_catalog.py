# -*- coding: utf-8 -*-
"""
Каталог КВЦПЗ (класифікатор видів цільового призначення земельних ділянок).

Слугує доменним джерелом істини для FLX-агента: переклад
КВЦПЗ-код <-> людська назва, угруповання у бізнес-категорії
(commercial / industrial / residential / agricultural / ...), а також
підказки для нормалізації користувацького запиту в `purpose_codes`.

Архітектурно: статичний реєстр, без зовнішніх IO; YAML читається один раз і
кешується in-memory.
"""

from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import yaml

logger = logging.getLogger(__name__)

_CATALOG_YAML_PATH = (
    Path(__file__).resolve().parent.parent.parent / "config" / "cadastral_kvcpz.yaml"
)


@dataclass(frozen=True)
class BusinessGroup:
    """Бізнес-група КВЦПЗ-кодів (комерція/промисловість/житло/...)."""

    name: str
    label: str
    description: str
    purpose_codes: Tuple[str, ...]
    keywords: Tuple[str, ...]


class CadastralClassificationCatalog:
    """Статичний каталог КВЦПЗ.

    Доступ thread-safe; YAML підвантажується ліниво при першому виклику.
    """

    _lock = threading.Lock()
    _instance: Optional["CadastralClassificationCatalog"] = None

    def __init__(self, catalog_path: Path = _CATALOG_YAML_PATH) -> None:
        self._catalog_path = catalog_path
        self._codes: Dict[str, str] = {}
        self._groups: Dict[str, BusinessGroup] = {}
        # purpose_code → set of business_group names
        self._code_to_groups: Dict[str, Set[str]] = {}
        self._loaded = False
        self._load()

    @classmethod
    def get_instance(cls) -> "CadastralClassificationCatalog":
        """Глобальний синглтон каталога."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    # ------------------------------ Loading ------------------------------

    def _load(self) -> None:
        try:
            with open(self._catalog_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.warning("KVCPZ catalog not found at %s", self._catalog_path)
            data = {}
        except Exception as e:
            logger.exception("Failed to load KVCPZ catalog: %s", e)
            data = {}

        self._codes = {
            str(k).strip(): str(v).strip()
            for k, v in (data.get("codes") or {}).items()
            if k is not None
        }

        for name, payload in (data.get("business_groups") or {}).items():
            if not isinstance(payload, dict):
                continue
            group = BusinessGroup(
                name=str(name),
                label=str(payload.get("label") or name),
                description=str(payload.get("description") or ""),
                purpose_codes=tuple(str(c).strip() for c in (payload.get("purpose_codes") or [])),
                keywords=tuple(str(k).strip().lower() for k in (payload.get("keywords") or [])),
            )
            self._groups[group.name] = group
            for code in group.purpose_codes:
                self._code_to_groups.setdefault(code, set()).add(group.name)
        self._loaded = True

    # ------------------------------ Lookups ------------------------------

    def label_for_code(self, code: Optional[str]) -> Optional[str]:
        """Повертає офіційну назву призначення для коду, або None."""
        if not code:
            return None
        return self._codes.get(str(code).strip())

    def all_codes(self) -> List[Dict[str, Any]]:
        """Список усіх відомих кодів з назвами та групами."""
        return [
            {
                "code": code,
                "label": label,
                "business_groups": sorted(self._code_to_groups.get(code, set())),
            }
            for code, label in sorted(self._codes.items())
        ]

    def all_business_groups(self) -> List[Dict[str, Any]]:
        """Список усіх бізнес-груп з кодами та описом."""
        return [
            {
                "name": g.name,
                "label": g.label,
                "description": g.description,
                "purpose_codes": list(g.purpose_codes),
                "keyword_hints": list(g.keywords),
            }
            for g in sorted(self._groups.values(), key=lambda x: x.name)
        ]

    def business_group(self, name: str) -> Optional[BusinessGroup]:
        return self._groups.get(str(name or "").strip())

    def codes_for_business_groups(self, group_names: Iterable[str]) -> List[str]:
        """Об'єднання purpose_codes для набору груп (deduplicate, відсортовано)."""
        out: Set[str] = set()
        for n in group_names or []:
            g = self.business_group(n)
            if not g:
                continue
            out.update(g.purpose_codes)
        return sorted(out)

    def groups_for_code(self, code: Optional[str]) -> List[str]:
        """Бізнес-групи, до яких належить КВЦПЗ-код."""
        if not code:
            return []
        return sorted(self._code_to_groups.get(str(code).strip(), set()))

    # ---------------------------- Suggestions ----------------------------

    def suggest_groups_for_text(self, text: str) -> List[str]:
        """Підказки бізнес-груп з тексту вільної форми (ключові слова).

        Працює для коротких user-фраз на кшталт "склад і супермаркет".
        Не претендує на точність — для агента це лише hint, остаточний вибір
        робить LLM, маючи `business_groups[]` повний список з keyword_hints.
        """
        if not text:
            return []
        lowered = text.lower()
        out: List[str] = []
        seen: Set[str] = set()
        for g in self._groups.values():
            for kw in g.keywords:
                if not kw:
                    continue
                if kw in lowered and g.name not in seen:
                    out.append(g.name)
                    seen.add(g.name)
                    break
        return out

    def normalize_purpose_filter(
        self,
        *,
        purpose_codes: Optional[Sequence[str]] = None,
        business_groups: Optional[Sequence[str]] = None,
        purpose_label_contains: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Зводить три способи фільтрації призначень у єдину нормалізовану форму.

        Повертає словник:
            {
                "purpose_codes": [...],            # дедуплікований союз
                "business_groups_resolved": [...], # тільки відомі
                "business_groups_unknown": [...],  # ті, що не знайшли в каталозі
                "purpose_label_regex": "...|None"  # case-insensitive regex
            }
        """
        codes: Set[str] = set()
        for c in purpose_codes or []:
            c = str(c or "").strip()
            if c:
                codes.add(c)

        resolved: List[str] = []
        unknown: List[str] = []
        for g in business_groups or []:
            n = str(g or "").strip()
            if not n:
                continue
            if n in self._groups:
                resolved.append(n)
                codes.update(self._groups[n].purpose_codes)
            else:
                unknown.append(n)

        regex: Optional[str] = None
        if purpose_label_contains and str(purpose_label_contains).strip():
            regex = re.escape(str(purpose_label_contains).strip())

        return {
            "purpose_codes": sorted(codes),
            "business_groups_resolved": resolved,
            "business_groups_unknown": unknown,
            "purpose_label_regex": regex,
        }

    @property
    def is_loaded(self) -> bool:
        return self._loaded
