# -*- coding: utf-8 -*-
"""
InvestigatorReportAgent — компонує структурований JSON фінального звіту.

Multi-stage pipeline (за замовчуванням):
  1) OUTLINE   — LLM визначає 4-8 секцій звіту з focus + evidence_filter.
  2) SECTIONS  — окремий LLM-виклик НА КОЖНУ секцію з вузьким контекстом
                  (тільки релевантне evidence). Це і є «комплексний документ
                  через окремі запити по п.п.» з рекомендації користувача.
  3) SYNTHESIS — reduce-крок: executive_summary, claims, warnings, sources.

Якщо `settings.flx_multistage_report = False` — fallback на single-pass
(старий `investigator_report` промпт).

LLM-агент НЕ пише HTML напряму: ми отримуємо строгу структуру (sections, narrative,
maps, listings, metrics), а HTML генерує Jinja-шаблон у InvestigationService.
"""

import json
import logging
from typing import Any, Callable, Dict, List, Optional

from config.settings import Settings

from business.agents.investigator._common import (
    call_llm_json,
    get_llm_service,
    get_prompt_template,
    render_template,
)

logger = logging.getLogger(__name__)


class InvestigatorReportAgent:
    """Будує structured-JSON фінального звіту дослідження."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    # ------------------------------------------------------------------
    # PUBLIC
    # ------------------------------------------------------------------
    def compose(
        self,
        *,
        user_query: str,
        notes_summary: str,
        evidence: List[Dict[str, Any]],
        evidence_graph: Optional[Dict[str, Any]] = None,
        coverage_snapshot: Optional[Dict[str, Any]] = None,
        strategy_comparison: Optional[Dict[str, Any]] = None,
        scope_lock: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """Повертає dict зі структурою звіту (готова для Jinja-рендеру).

        `progress_callback(phase, payload)` — push події у SSE для UI
        (`report.outline_ready` / `report.section_ready` / `report.synthesis_done`).
        """
        multistage = bool(getattr(self.settings, "flx_multistage_report", True))
        if multistage:
            try:
                return self._compose_multistage(
                    user_query=user_query,
                    notes_summary=notes_summary,
                    evidence=evidence,
                    evidence_graph=evidence_graph,
                    coverage_snapshot=coverage_snapshot,
                    strategy_comparison=strategy_comparison,
                    scope_lock=scope_lock,
                    progress_callback=progress_callback,
                )
            except Exception as e:
                logger.warning(
                    "Multi-stage report failed (%s) — fallback to single-pass.", e
                )
        return self._compose_single_pass(
            user_query=user_query,
            notes_summary=notes_summary,
            evidence=evidence,
            evidence_graph=evidence_graph,
            coverage_snapshot=coverage_snapshot,
            strategy_comparison=strategy_comparison,
            scope_lock=scope_lock,
        )

    # ------------------------------------------------------------------
    # MULTI-STAGE PIPELINE
    # ------------------------------------------------------------------
    def _compose_multistage(
        self,
        *,
        user_query: str,
        notes_summary: str,
        evidence: List[Dict[str, Any]],
        evidence_graph: Optional[Dict[str, Any]],
        coverage_snapshot: Optional[Dict[str, Any]],
        strategy_comparison: Optional[Dict[str, Any]],
        scope_lock: Optional[Dict[str, Any]],
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]],
    ) -> Dict[str, Any]:
        scope_block = self._format_scope_lock(scope_lock)
        max_sections = int(getattr(self.settings, "flx_report_max_sections", 8))

        # 1) OUTLINE
        outline = self._build_outline(
            user_query=user_query,
            scope_block=scope_block,
            notes_summary=notes_summary,
            evidence=evidence,
            max_sections=max_sections,
        )
        if not outline:
            logger.warning("Outline empty — fallback single-pass.")
            return self._compose_single_pass(
                user_query=user_query,
                notes_summary=notes_summary,
                evidence=evidence,
                evidence_graph=evidence_graph,
                coverage_snapshot=coverage_snapshot,
                strategy_comparison=strategy_comparison,
                scope_lock=scope_lock,
            )
        if progress_callback:
            try:
                progress_callback(
                    "report.outline_ready",
                    {"sections_count": len(outline), "sections": [{"id": s.get("id"), "title": s.get("title")} for s in outline]},
                )
            except Exception:
                pass

        # 2) SECTIONS — по одному виклику на секцію.
        outline_block = self._format_outline(outline)
        sections: List[Dict[str, Any]] = []
        for sec_meta in outline:
            filtered = self._filter_evidence(evidence, sec_meta)
            sec = self._compose_section(
                user_query=user_query,
                scope_block=scope_block,
                outline_block=outline_block,
                section_meta=sec_meta,
                filtered_evidence=filtered,
            )
            if sec:
                sections.append(sec)
                if progress_callback:
                    try:
                        progress_callback(
                            "report.section_ready",
                            {"id": sec.get("id"), "title": sec.get("title"), "index": len(sections), "total": len(outline)},
                        )
                    except Exception:
                        pass

        # 3) SYNTHESIS — executive_summary / claims / warnings / sources.
        synth = self._synthesize(
            user_query=user_query,
            scope_block=scope_block,
            sections=sections,
            evidence_graph=evidence_graph,
            coverage_snapshot=coverage_snapshot,
        )
        if progress_callback:
            try:
                progress_callback("report.synthesis_done", {"claims": len(synth.get("claims") or [])})
            except Exception:
                pass

        return {
            "title": str(synth.get("title") or "Звіт дослідження Flx")[:200],
            "executive_summary": str(synth.get("executive_summary") or "")[:2000],
            "sections": self._normalize_sections(sections),
            "warnings": [str(w)[:500] for w in (synth.get("warnings") or [])][:10],
            "sources": [str(s)[:500] for s in (synth.get("sources") or [])][:20],
            "claims": self._normalize_claims(synth.get("claims") or []),
        }

    # --- stage 1: outline ---------------------------------------------
    def _build_outline(
        self,
        *,
        user_query: str,
        scope_block: str,
        notes_summary: str,
        evidence: List[Dict[str, Any]],
        max_sections: int,
    ) -> List[Dict[str, Any]]:
        template = get_prompt_template("investigator_report_outline")
        if not template:
            return []
        inv = self._evidence_inventory(evidence)
        prompt = render_template(
            template,
            user_query=(user_query or "")[:1500],
            scope_lock_block=scope_block[:1500],
            notes_summary=(notes_summary or "(порожньо)")[:4000],
            evidence_inventory_block=inv[:2000],
        )
        data = call_llm_json(self.llm, prompt=prompt, caller="flx.reporter.outline")
        if not data:
            return []
        sections_raw = data.get("sections") or []
        outline: List[Dict[str, Any]] = []
        for s in sections_raw[:max_sections]:
            if not isinstance(s, dict):
                continue
            outline.append(
                {
                    "id": str(s.get("id") or "section")[:60],
                    "title": str(s.get("title") or "")[:200],
                    "focus": str(s.get("focus") or "")[:600],
                    "evidence_kinds": [
                        str(k)[:40] for k in (s.get("evidence_kinds") or []) if str(k).strip()
                    ][:10],
                    "evidence_keywords": [
                        str(k)[:60] for k in (s.get("evidence_keywords") or []) if str(k).strip()
                    ][:10],
                }
            )
        return outline

    # --- stage 2: section ---------------------------------------------
    def _compose_section(
        self,
        *,
        user_query: str,
        scope_block: str,
        outline_block: str,
        section_meta: Dict[str, Any],
        filtered_evidence: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        template = get_prompt_template("investigator_report_section")
        if not template:
            return None
        ev_block = self._format_evidence(filtered_evidence)
        prompt = render_template(
            template,
            user_query=(user_query or "")[:1000],
            scope_lock_block=scope_block[:1500],
            outline_block=outline_block[:1500],
            section_id=str(section_meta.get("id") or "")[:60],
            section_title=str(section_meta.get("title") or "")[:200],
            section_focus=str(section_meta.get("focus") or "")[:600],
            filtered_evidence_block=ev_block[:12000],
        )
        data = call_llm_json(self.llm, prompt=prompt, caller=f"flx.reporter.section.{section_meta.get('id')}")
        if not data:
            return None
        # ensure id/title fields keep stable values regardless of LLM output
        data["id"] = str(section_meta.get("id") or data.get("id") or "section")[:60]
        data["title"] = str(section_meta.get("title") or data.get("title") or "")[:200]
        return data

    # --- stage 3: synthesis -------------------------------------------
    def _synthesize(
        self,
        *,
        user_query: str,
        scope_block: str,
        sections: List[Dict[str, Any]],
        evidence_graph: Optional[Dict[str, Any]],
        coverage_snapshot: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        template = get_prompt_template("investigator_report_synthesis")
        if not template:
            return {"title": "Звіт дослідження Flx", "executive_summary": "", "warnings": [], "sources": [], "claims": []}
        sections_brief = []
        for s in sections[:12]:
            sections_brief.append(
                {
                    "id": s.get("id"),
                    "title": s.get("title"),
                    "narrative": (str(s.get("narrative") or ""))[:600],
                    "bullets": (s.get("bullets") or [])[:5],
                    "items_count": len(s.get("items") or []),
                    "listings_count": len(s.get("listings") or []),
                    "parcels_count": len(s.get("parcels") or []),
                }
            )
        sections_brief_block = json.dumps(sections_brief, ensure_ascii=False, default=str)[:8000]
        eg_block = json.dumps(evidence_graph or {}, ensure_ascii=False, default=str)[:5000]
        cov_block = json.dumps(coverage_snapshot or {}, ensure_ascii=False, default=str)[:2000]
        prompt = render_template(
            template,
            user_query=(user_query or "")[:1000],
            scope_lock_block=scope_block[:1500],
            sections_brief_block=sections_brief_block,
            evidence_graph_block=eg_block,
            coverage_snapshot_block=cov_block,
        )
        data = call_llm_json(self.llm, prompt=prompt, caller="flx.reporter.synthesis")
        return data or {}

    # ------------------------------------------------------------------
    # EVIDENCE filtering / inventory / formatting
    # ------------------------------------------------------------------
    def _evidence_inventory(self, evidence: List[Dict[str, Any]]) -> str:
        counts: Dict[str, int] = {}
        for e in evidence:
            k = str((e or {}).get("kind") or "fact")
            counts[k] = counts.get(k, 0) + 1
        if not counts:
            return "(немає evidence)"
        lines = [f"- {k}: {n}" for k, n in sorted(counts.items(), key=lambda x: -x[1])]
        return "\n".join(lines)

    @staticmethod
    def _format_outline(outline: List[Dict[str, Any]]) -> str:
        lines: List[str] = []
        for i, s in enumerate(outline, start=1):
            lines.append(
                f"{i}. id={s.get('id')} title={s.get('title')!r} focus={(s.get('focus') or '')[:180]}"
            )
        return "\n".join(lines)

    @staticmethod
    def _format_scope_lock(scope_lock: Optional[Dict[str, Any]]) -> str:
        if not isinstance(scope_lock, dict):
            return "(scope_lock відсутній)"
        loc = scope_lock.get("location") or {}
        prop = scope_lock.get("property") or {}
        lines: List[str] = []
        if loc.get("city"):
            lines.append(f"city = «{loc['city']}»")
        if loc.get("region"):
            lines.append(f"region = «{loc['region']}» (без слова «область»)")
        if prop.get("type_keyword"):
            lines.append(f"property_type_contains = «{prop['type_keyword']}»")
        if prop.get("intent"):
            lines.append(f"intent = «{prop['intent']}»")
        if prop.get("min_area_sqm"):
            lines.append(f"min area = {prop['min_area_sqm']} м²")
        if prop.get("max_area_sqm"):
            lines.append(f"max area = {prop['max_area_sqm']} м²")
        if not lines:
            return "(scope_lock порожній)"
        return "\n".join(f"- {ln}" for ln in lines)

    def _filter_evidence(
        self,
        evidence: List[Dict[str, Any]],
        section_meta: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Залишає тільки те evidence, що корисне поточній секції.

        Правила:
        - якщо `evidence_kinds` задано — лишаємо тільки з цими kind'ами;
        - інакше — лишаємо все;
        - додатково ранжуємо за збігом `evidence_keywords` у тексті/payload-ключах.
        """
        kinds = set([str(k).lower() for k in (section_meta.get("evidence_kinds") or [])])
        keywords = [str(k).lower() for k in (section_meta.get("evidence_keywords") or []) if k]
        if not kinds and not keywords:
            return evidence[:60]

        scored: List[tuple] = []
        for e in evidence:
            if not isinstance(e, dict):
                continue
            k = str(e.get("kind") or "").lower()
            if kinds and k not in kinds:
                continue
            text = (str(e.get("text") or "") + " " + json.dumps(e.get("payload") or {}, ensure_ascii=False, default=str)).lower()
            score = sum(1 for kw in keywords if kw in text)
            scored.append((score, e))
        # У випадку коли kinds задано, але evidence порожнє — повертаємо top
        # observation-нотатки (фолбек), щоб секція не лишилась без контексту.
        if not scored and kinds:
            for e in evidence:
                if str((e or {}).get("kind") or "").lower() == "observation":
                    scored.append((0, e))
        scored.sort(key=lambda x: -x[0])
        return [e for _, e in scored[:50]]

    # ------------------------------------------------------------------
    # SINGLE-PASS (legacy fallback)
    # ------------------------------------------------------------------
    def _compose_single_pass(
        self,
        *,
        user_query: str,
        notes_summary: str,
        evidence: List[Dict[str, Any]],
        evidence_graph: Optional[Dict[str, Any]],
        coverage_snapshot: Optional[Dict[str, Any]],
        strategy_comparison: Optional[Dict[str, Any]],
        scope_lock: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        template = get_prompt_template("investigator_report")
        if not template:
            return self._minimal_report(user_query, notes_summary)

        evidence_block = self._format_evidence(evidence)
        comparison_block = (
            json.dumps(strategy_comparison, ensure_ascii=False, default=str)[:6000]
            if strategy_comparison
            else "(немає порівняння стратегій — одиночний прохід)"
        )
        evidence_graph_block = json.dumps(evidence_graph or {}, ensure_ascii=False, default=str)[:7000]
        coverage_block = json.dumps(coverage_snapshot or {}, ensure_ascii=False, default=str)[:3000]
        prompt = render_template(
            template,
            user_query=(user_query or "")[:1500],
            notes_summary=(notes_summary or "(порожньо)")[:6000],
            evidence_block=evidence_block[:16000],
            evidence_graph_block=evidence_graph_block,
            coverage_snapshot_block=coverage_block,
            strategy_comparison_block=comparison_block,
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.reporter")
        if not data:
            return self._minimal_report(user_query, notes_summary)

        return {
            "title": str(data.get("title") or "Звіт дослідження Flx")[:200],
            "executive_summary": str(data.get("executive_summary") or "")[:2000],
            "sections": self._normalize_sections(data.get("sections") or []),
            "warnings": [str(w)[:500] for w in (data.get("warnings") or [])][:10],
            "sources": [str(s)[:500] for s in (data.get("sources") or [])][:20],
            "claims": self._normalize_claims(data.get("claims") or []),
        }

    def _normalize_claims(self, claims: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for claim in claims[:40]:
            if not isinstance(claim, dict):
                continue
            out.append(
                {
                    "statement": str(claim.get("statement") or "")[:1000],
                    "confidence": float(claim.get("confidence") or 0.0),
                    "evidence_count": int(claim.get("evidence_count") or 0),
                    "source_diversity": int(claim.get("source_diversity") or 0),
                    "contradiction_status": str(claim.get("contradiction_status") or "none")[:40],
                }
            )
        return out

    def _normalize_sections(self, sections: List[Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for sec in sections[:12]:
            if not isinstance(sec, dict):
                continue
            normalized: Dict[str, Any] = {
                "id": str(sec.get("id") or "section")[:60],
                "title": str(sec.get("title") or "")[:200],
                "narrative": str(sec.get("narrative") or "")[:4000],
                "map_artifact_id": (str(sec["map_artifact_id"])[:80] if sec.get("map_artifact_id") else None),
                "bullets": [str(b)[:400] for b in (sec.get("bullets") or [])][:20],
            }
            if sec.get("applied_strategy"):
                normalized["applied_strategy"] = str(sec.get("applied_strategy"))[:60]
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
            if isinstance(sec.get("parcels"), list):
                normalized["parcels"] = [
                    self._normalize_parcel(p) for p in sec["parcels"][:50] if isinstance(p, dict)
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

    @staticmethod
    def _normalize_parcel(p: Dict[str, Any]) -> Dict[str, Any]:
        def _f(v: Any) -> Optional[float]:
            if v in (None, ""):
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        centroid = p.get("centroid") if isinstance(p.get("centroid"), dict) else None
        out_p: Dict[str, Any] = {
            "cadastral_number": str(p.get("cadastral_number") or "")[:60],
            "area_sqm": _f(p.get("area_sqm")),
            "purpose_label": str(p.get("purpose_label") or "")[:400],
            "ownership_form": str(p.get("ownership_form") or "")[:200],
            "cluster_id": (str(p.get("cluster_id"))[:80] if p.get("cluster_id") else None),
            "cluster_total_area_sqm": _f(p.get("cluster_total_area_sqm")),
            "cluster_parcel_count": (
                int(p.get("cluster_parcel_count"))
                if str(p.get("cluster_parcel_count") or "").strip().isdigit()
                else None
            ),
        }
        if centroid:
            out_p["centroid"] = {
                "latitude": _f(centroid.get("latitude")),
                "longitude": _f(centroid.get("longitude")),
            }
        return out_p

    def _format_evidence(self, evidence: List[Dict[str, Any]]) -> str:
        if not evidence:
            return "(немає evidence)"
        out: List[str] = []
        for i, e in enumerate(evidence[:80], start=1):
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
            payload = e.get("payload")
            if not isinstance(payload, dict):
                continue
            if kind == "cadastral_finding":
                out.append(self._format_cadastral_payload(payload, indent=f"   {i}."))
            elif kind == "listings_finding":
                out.append(self._format_listings_payload(payload, indent=f"   {i}."))
        return "\n".join(out)

    @staticmethod
    def _format_cadastral_payload(payload: Dict[str, Any], *, indent: str) -> str:
        lines: List[str] = []
        clusters = payload.get("clusters") or []
        singles = payload.get("single_parcels") or []
        if clusters:
            lines.append(f"{indent} CLUSTERS={len(clusters)}:")
            for c in clusters:
                cns = ", ".join(c.get("cadastral_numbers") or [])
                if c.get("cadastral_numbers_total") and c["cadastral_numbers_total"] > len(c.get("cadastral_numbers") or []):
                    cns += f", … +{c['cadastral_numbers_total'] - len(c.get('cadastral_numbers') or [])} інших"
                lines.append(
                    f"{indent}   - cluster_id={c.get('cluster_id')} parcels={c.get('parcel_count')} "
                    f"total_area_sqm={c.get('total_area_sqm')} purpose_label={c.get('purpose_label')!r} "
                    f"ownership={c.get('ownership_form')} centroid={c.get('centroid')} cns=[{cns}]"
                )
        if singles:
            lines.append(f"{indent} SINGLE_PARCELS={len(singles)}:")
            for s in singles:
                cns = ", ".join(s.get("cadastral_numbers") or [])
                lines.append(
                    f"{indent}   - cn={cns} area_sqm={s.get('total_area_sqm')} "
                    f"purpose_label={s.get('purpose_label')!r} ownership={s.get('ownership_form')} "
                    f"centroid={s.get('centroid')}"
                )
        af = payload.get("applied_filters")
        if af:
            lines.append(f"{indent} applied_filters={af}")
        return "\n".join(lines)

    @staticmethod
    def _format_listings_payload(payload: Dict[str, Any], *, indent: str) -> str:
        lines: List[str] = []
        lines.append(
            f"{indent} applied_strategy={payload.get('applied_strategy')!r} count={payload.get('count')}"
        )
        items = payload.get("items") or []
        for it in items[:15]:
            lines.append(
                f"{indent}   - source={it.get('source')} url={it.get('url')} "
                f"title={it.get('title')!r} addr={it.get('address')!r} "
                f"price_uah={it.get('price_uah')} city={it.get('city')!r} region={it.get('region')!r} "
                f"property_type={it.get('property_type')!r} building_area_sqm={it.get('building_area_sqm')} "
                f"land_area_sqm={it.get('land_area_sqm')}"
            )
        ls = payload.get("landscape") or {}
        if ls:
            lines.append(f"{indent} landscape_brief={str(ls)[:300]}")
        return "\n".join(lines)

    def _minimal_report(self, user_query: str, notes_summary: str) -> Dict[str, Any]:
        return {
            "title": "Звіт дослідження Flx (часткові дані)",
            "executive_summary": (
                "Не вдалося згенерувати повний звіт. Наведено короткий стан дослідження."
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
            "claims": [],
        }
