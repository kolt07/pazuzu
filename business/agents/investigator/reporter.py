# -*- coding: utf-8 -*-
"""
InvestigatorReportAgent — компонує структурований JSON фінального звіту.

LLM-агент НЕ пише HTML напряму: ми отримуємо строгу структуру (sections, narrative,
maps, listings, metrics), а HTML генерує Jinja-шаблон у InvestigationService.
"""

import logging
from typing import Any, Dict, List

from config.settings import Settings

from business.agents.investigator._common import (
    call_llm_json,
    get_llm_service,
    get_prompt_template,
    render_template,
)

logger = logging.getLogger(__name__)


class InvestigatorReportAgent:
    """Будує structured-JSON фінального звіту розслідування."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    def compose(
        self,
        *,
        user_query: str,
        notes_summary: str,
        evidence: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Повертає dict зі структурою звіту (готова для Jinja-рендеру).

        evidence — масив observation-нотаток і артефактів (artifact_id, url, listings, тощо).
        У разі помилки LLM повертає мінімальний звіт із executive_summary.
        """
        template = get_prompt_template("investigator_report")
        if not template:
            return self._minimal_report(user_query, notes_summary)

        evidence_block = self._format_evidence(evidence)
        prompt = render_template(
            template,
            user_query=(user_query or "")[:1500],
            notes_summary=(notes_summary or "(порожньо)")[:6000],
            evidence_block=evidence_block[:8000],
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.reporter")
        if not data:
            return self._minimal_report(user_query, notes_summary)

        return {
            "title": str(data.get("title") or "Звіт розслідування Flx")[:200],
            "executive_summary": str(data.get("executive_summary") or "")[:2000],
            "sections": self._normalize_sections(data.get("sections") or []),
            "warnings": [str(w)[:500] for w in (data.get("warnings") or [])][:10],
            "sources": [str(s)[:500] for s in (data.get("sources") or [])][:20],
        }

    def _normalize_sections(self, sections: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for sec in sections[:12]:
            if not isinstance(sec, dict):
                continue
            normalized = {
                "id": str(sec.get("id") or "section")[:60],
                "title": str(sec.get("title") or "")[:200],
                "narrative": str(sec.get("narrative") or "")[:4000],
                "map_artifact_id": (str(sec["map_artifact_id"])[:80] if sec.get("map_artifact_id") else None),
                "bullets": [str(b)[:400] for b in (sec.get("bullets") or [])][:20],
            }
            if isinstance(sec.get("items"), list):
                normalized["items"] = [
                    {
                        "name": str(it.get("name") or "")[:200],
                        "reason": str(it.get("reason") or "")[:500],
                        "score": float(it.get("score") or 0.0),
                    }
                    for it in sec["items"][:20]
                    if isinstance(it, dict)
                ]
            if isinstance(sec.get("listings"), list):
                normalized["listings"] = [
                    {
                        "title": str(l.get("title") or "")[:300],
                        "address": str(l.get("address") or "")[:400],
                        "price_uah": (float(l.get("price_uah")) if l.get("price_uah") not in (None, "") else None),
                        "url": str(l.get("url") or "")[:600],
                    }
                    for l in sec["listings"][:30]
                    if isinstance(l, dict)
                ]
            if isinstance(sec.get("metrics"), list):
                normalized["metrics"] = [
                    {
                        "label": str(m.get("label") or "")[:200],
                        "value": str(m.get("value") or "")[:200],
                    }
                    for m in sec["metrics"][:20]
                    if isinstance(m, dict)
                ]
            out.append(normalized)
        return out

    def _format_evidence(self, evidence: List[Dict[str, Any]]) -> str:
        if not evidence:
            return "(немає evidence)"
        out: List[str] = []
        for i, e in enumerate(evidence[:60], start=1):
            kind = str(e.get("kind") or "fact")
            text = str(e.get("text") or "")
            if len(text) > 400:
                text = text[:400].rstrip() + "…"
            extras: List[str] = []
            if e.get("artifact_id"):
                extras.append(f"artifact_id={e['artifact_id']}")
            if e.get("url"):
                extras.append(f"url={e['url']}")
            if e.get("price_uah"):
                extras.append(f"price={e['price_uah']}")
            extra_str = (" | " + ", ".join(extras)) if extras else ""
            out.append(f"{i}. [{kind}] {text}{extra_str}")
        return "\n".join(out)

    def _minimal_report(self, user_query: str, notes_summary: str) -> Dict[str, Any]:
        return {
            "title": "Звіт розслідування Flx (часткові дані)",
            "executive_summary": (
                "Не вдалося згенерувати повний звіт. Наведено короткий стан розслідування."
            ),
            "sections": [
                {
                    "id": "method_notes",
                    "title": "Хід міркувань",
                    "narrative": (notes_summary or "(нотаток немає)")[:2000],
                    "bullets": [],
                    "map_artifact_id": None,
                }
            ],
            "warnings": ["Цей звіт є мінімальним fallback'ом без даних від LLM."],
            "sources": [],
        }
