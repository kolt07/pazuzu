# -*- coding: utf-8 -*-
"""
InvestigatorPlannerAgent — будує початковий JSON-план дослідження Flx.

LLM-only: жодних викликів MCP, жодних побічних ефектів. Вихід — структура з
переліком кроків, які потім виконує InvestigatorStepAgent через executor.

Підтримка Strategy Engine: опційно `strategy_candidates[]` — паралельні стратегії
з власними кроками (крос-порівняння на боці оркестратора).
"""

import logging
import re
import uuid
from typing import Any, Dict, List, Optional

from config.settings import Settings

from business.agents.investigator._common import (
    call_llm_json,
    get_llm_service,
    get_prompt_template,
    render_template,
)

logger = logging.getLogger(__name__)


class InvestigatorPlannerAgent:
    """Початковий планувальник дослідження."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    def build_plan(
        self,
        *,
        user_query: str,
        allowed_tools: List[str],
        relevant_lessons: Optional[List[Dict[str, Any]]] = None,
        registry_hints_block: Optional[str] = None,
        strategy_engine_enabled: bool = False,
    ) -> Dict[str, Any]:
        """Будує JSON-план дослідження.

        Повертає dict з ключами: objective, assumptions, open_questions, steps[]
        та опційно strategy_candidates[] (якщо увімкнено Strategy Engine).
        """
        template = get_prompt_template("investigator_planner")
        if not template:
            plan = self._fallback_plan(user_query)
            return self._ensure_strategy_candidates(
                plan,
                user_query=user_query,
                allowed_tools=allowed_tools,
                strategy_engine_enabled=strategy_engine_enabled,
            )

        lessons_block = self._format_lessons(relevant_lessons or [])
        hints = registry_hints_block or "(немає історичних стратегій)"
        prompt = render_template(
            template,
            user_query=(user_query or "").strip()[:2000],
            lessons_block=lessons_block,
            registry_hints_block=hints,
            allowed_tools=", ".join(allowed_tools or []),
            strategy_engine_note=(
                "Обов'язково заповни strategy_candidates (2–4 стратегії), кожна з власним масивом steps."
                if strategy_engine_enabled
                else "Поле strategy_candidates можна опустити; достатньо єдиного масиву steps."
            ),
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.planner")
        if not data:
            plan = self._fallback_plan(user_query)
            return self._ensure_strategy_candidates(
                plan,
                user_query=user_query,
                allowed_tools=allowed_tools,
                strategy_engine_enabled=strategy_engine_enabled,
            )

        steps = data.get("steps")
        if not isinstance(steps, list):
            steps = []

        plan: Dict[str, Any] = {
            "objective": str(data.get("objective") or "")[:500],
            "assumptions": [str(x)[:500] for x in (data.get("assumptions") or []) if x][:10],
            "open_questions": [str(x)[:500] for x in (data.get("open_questions") or []) if x][:10],
            "steps": [self._normalize_step(idx + 1, s) for idx, s in enumerate(steps)][:12],
            "scope_lock": self._normalize_scope_lock(data.get("scope_lock"), user_query),
        }

        raw_candidates = data.get("strategy_candidates")
        if isinstance(raw_candidates, list) and raw_candidates:
            plan["strategy_candidates"] = [
                self._normalize_strategy_candidate(i, c, allowed_tools)
                for i, c in enumerate(raw_candidates)
                if isinstance(c, dict)
            ][: int(getattr(self.settings, "flx_strategy_max_candidates", 4) or 4)]

        return self._ensure_strategy_candidates(
            plan,
            user_query=user_query,
            allowed_tools=allowed_tools,
            strategy_engine_enabled=strategy_engine_enabled,
        )

    def _ensure_strategy_candidates(
        self,
        plan: Dict[str, Any],
        *,
        user_query: str,
        allowed_tools: List[str],
        strategy_engine_enabled: bool,
    ) -> Dict[str, Any]:
        """Гарантує наявність кількох стратегій при увімкненому рушії або лишає план як є."""
        if not strategy_engine_enabled:
            return plan

        max_c = int(getattr(self.settings, "flx_strategy_max_candidates", 4) or 4)
        existing = plan.get("strategy_candidates")
        if isinstance(existing, list) and len(existing) >= 2:
            plan["strategy_candidates"] = existing[:max_c]
            plan["steps"] = (plan["strategy_candidates"][0].get("steps") or plan.get("steps") or [])[:12]
            plan["strategy_engine_version"] = 1
            return plan

        poi_cand = self._poi_proxy_candidate(allowed_tools)
        if isinstance(existing, list) and len(existing) == 1 and isinstance(existing[0], dict):
            one = self._normalize_strategy_candidate(0, existing[0], allowed_tools)
            plan["strategy_candidates"] = [one, poi_cand][:max_c]
            plan["steps"] = (plan["strategy_candidates"][0].get("steps") or plan.get("steps") or [])[:12]
            plan["strategy_engine_version"] = 1
            plan.setdefault(
                "objective",
                str(plan.get("objective") or "Порівняти підходи до відповіді на запит."),
            )
            return plan

        base_steps = plan.get("steps") or self._fallback_plan(user_query).get("steps") or []
        main_id = "internal_data_baseline"

        cand_main: Dict[str, Any] = {
            "strategy_id": main_id,
            "name": "Базова вибірка та аналітика БД",
            "hypothesis": "Розподіл цін можна оцінити напряму з оголошень і метрик.",
            "planned_tools": [
                t
                for t in (
                    "query_builder.execute_query",
                    "analytics.execute_analytics",
                    "flx.targeted_source_search",
                )
                if t in (allowed_tools or [])
            ],
            "expected_signals": ["кількість оголошень", "квантилі цін", "типовий діапазон"],
            "risk_level": "medium",
            "estimated_cost": "medium",
            "stop_conditions": "Достатня кількість точок або підтверджена рідкість даних.",
            "steps": [self._normalize_step(i + 1, s) for i, s in enumerate(base_steps)][:12],
        }

        cand_poi = poi_cand

        plan["strategy_candidates"] = [cand_main, cand_poi][:max_c]
        plan["steps"] = plan["strategy_candidates"][0].get("steps") or []
        plan["strategy_engine_version"] = 1
        plan.setdefault(
            "objective",
            str(plan.get("objective") or "Порівняти розподіл цін і альтернативні сигнали (POI) для локації."),
        )
        return plan

    def _poi_proxy_candidate(self, allowed_tools: List[str]) -> Dict[str, Any]:
        """Друга стратегія за замовчуванням: геокод + nearby places (проксі «ціновості» району)."""
        poi_id = "poi_proxy_pricing"
        poi_steps = [
            {
                "step_id": 1,
                "goal": "Закодувати ключову локацію запиту (місто/район) для подальшого пошуку POI.",
                "candidate_tools": [t for t in ("geocoding.geocode_address", "flx.web_search") if t in allowed_tools][:4],
                "success_criteria": "Є координати або підтверджений топонім для точки огляду.",
            },
            {
                "step_id": 2,
                "goal": "Зібрати POI (ресторани, кафе, fast-food) в околицях для проксі-профілю «цінового» середовища.",
                "candidate_tools": [
                    t for t in ("geocoding.search_nearby_places", "flx.static_map_render") if t in allowed_tools
                ][:4],
                "success_criteria": "Є список місць або типів закладів для порівняння районів.",
            },
            {
                "step_id": 3,
                "goal": "Зіставити POI-сигнали з наявними даними по нерухомості (якщо є) та сформулювати обережний висновок.",
                "candidate_tools": [t for t in ("query_builder.execute_query", "analytics.execute_analytics") if t in allowed_tools][:4],
                "success_criteria": "Є пояснення обмежень POI як проксі та узгодження з фактами з БД.",
            },
        ]
        return {
            "strategy_id": poi_id,
            "name": "POI як проксі цінового профілю (мало оголошень)",
            "hypothesis": "Щільність і тип закладів харчування корелює з ціновим сегментом району.",
            "planned_tools": [
                t
                for t in ("geocoding.geocode_address", "geocoding.search_nearby_places", "flx.static_map_render")
                if t in (allowed_tools or [])
            ],
            "expected_signals": ["poi_premium_mix", "fastfood_density", "різноманіття типів"],
            "risk_level": "high",
            "estimated_cost": "medium",
            "stop_conditions": "Отримано репрезентативну вибірку POI або зрозумілі обмеження API.",
            "steps": [self._normalize_step(i + 1, s) for i, s in enumerate(poi_steps)],
        }

    def _normalize_strategy_candidate(
        self,
        idx: int,
        raw: Dict[str, Any],
        allowed_tools: List[str],
    ) -> Dict[str, Any]:
        sid = str(raw.get("strategy_id") or "").strip() or f"strategy_{idx+1}_{uuid.uuid4().hex[:6]}"
        steps_in = raw.get("steps")
        if not isinstance(steps_in, list) or not steps_in:
            steps_in = [
                {
                    "step_id": 1,
                    "goal": str(raw.get("name") or "Виконати крок стратегії")[:500],
                    "candidate_tools": raw.get("planned_tools") or [],
                    "success_criteria": "Є результат інструментів або зафіксовано блокер.",
                }
            ]
        steps_norm = [self._normalize_step(i + 1, s) for i, s in enumerate(steps_in)][:12]
        cand_tools = raw.get("planned_tools") if isinstance(raw.get("planned_tools"), list) else []
        filtered_tools = [str(t)[:80] for t in cand_tools if str(t).strip() and str(t) in (allowed_tools or [])][:12]
        alt_planned = [
            str(x)[:80] for x in (raw.get("candidate_tools") or [])
            if str(x) in (allowed_tools or [])
        ][:8]

        return {
            "strategy_id": sid[:120],
            "name": str(raw.get("name") or f"Стратегія {idx+1}")[:300],
            "hypothesis": str(raw.get("hypothesis") or "")[:800],
            "planned_tools": filtered_tools or alt_planned,
            "expected_signals": [str(x)[:200] for x in (raw.get("expected_signals") or []) if x][:12],
            "risk_level": str(raw.get("risk_level") or "medium")[:20],
            "estimated_cost": str(raw.get("estimated_cost") or "medium")[:20],
            "stop_conditions": str(raw.get("stop_conditions") or "")[:500],
            "steps": steps_norm,
        }

    def _format_lessons(self, lessons: List[Dict[str, Any]]) -> str:
        if not lessons:
            return "(немає релевантних попередніх кейсів)"
        lines: List[str] = []
        for i, lsn in enumerate(lessons[:5], start=1):
            tags = ", ".join((lsn.get("topic_tags") or [])[:5])
            recs = "; ".join((lsn.get("recommendations") or [])[:3])
            ww = "; ".join((lsn.get("what_worked") or [])[:3])
            lines.append(
                f"{i}. tags=[{tags}] recommendations: {recs}; what_worked: {ww}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # scope_lock — авторитетна структура з параметрами запиту, яку executor
    # ПРИМУСОВО накладає на listings/cadastral tool args. Це bypasses
    # галюцинації LLM-крокувальника (gemini-flash хронічно вигадує місто).
    # ------------------------------------------------------------------
    _UA_CITY_TO_REGION = {
        "турійськ": "Волинська",
        "турійск": "Волинська",
        "турийск": "Волинська",
        "ковель": "Волинська",
        "луцьк": "Волинська",
        "нововолинськ": "Волинська",
        "володимир-волинський": "Волинська",
        "ужгород": "Закарпатська",
        "мукачево": "Закарпатська",
        "хуст": "Закарпатська",
        "львів": "Львівська",
        "дрогобич": "Львівська",
        "стрий": "Львівська",
        "турка": "Львівська",
        "київ": "Київська",
        "буча": "Київська",
        "ірпінь": "Київська",
        "бровари": "Київська",
        "одеса": "Одеська",
        "ізмаїл": "Одеська",
        "харків": "Харківська",
        "дніпро": "Дніпропетровська",
        "запоріжжя": "Запорізька",
        "полтава": "Полтавська",
        "вінниця": "Вінницька",
        "житомир": "Житомирська",
        "чернігів": "Чернігівська",
        "суми": "Сумська",
        "черкаси": "Черкаська",
        "тернопіль": "Тернопільська",
        "хмельницький": "Хмельницька",
        "рівне": "Рівненська",
        "івано-франківськ": "Івано-Франківська",
        "чернівці": "Чернівецька",
        "миколаїв": "Миколаївська",
        "херсон": "Херсонська",
    }

    _CITY_NORMALIZE = {
        "турийск": "Турійськ",
        "турійск": "Турійськ",
    }

    def _normalize_scope_lock(self, raw: Any, user_query: str) -> Dict[str, Any]:
        loc_in: Dict[str, Any] = {}
        prop_in: Dict[str, Any] = {}
        tk: List[str] = []
        if isinstance(raw, dict):
            if isinstance(raw.get("location"), dict):
                loc_in = raw["location"]
            if isinstance(raw.get("property"), dict):
                prop_in = raw["property"]
            tk = [str(k).strip() for k in (raw.get("task_keywords") or []) if str(k).strip()][:8]

        city = (str(loc_in.get("city") or "").strip() or None)
        region = (str(loc_in.get("region") or "").strip() or None)
        raw_loc = (str(loc_in.get("raw") or "").strip() or None)

        # Regex-fallback (на випадок якщо LLM не повернув scope_lock)
        q = (user_query or "").strip()
        if not city and q:
            m = re.search(r"\bм\.\s*([A-ZА-ЯІЇЄҐ][a-zа-яіїєґʼ'`\-]{2,40})", q)
            if m:
                city = m.group(1).strip()
        if not city and q:
            # Перебираємо словник відомих міст у нижньому регістрі
            qlow = q.lower()
            for known in self._UA_CITY_TO_REGION.keys():
                if known in qlow:
                    city = known
                    break

        if city:
            key = city.lower()
            city = self._CITY_NORMALIZE.get(key, city)
            # Для regex-fallback (повернули lower-case ключ зі словника) — capitalize
            if city == key:
                city = key[:1].upper() + key[1:]
            if not region:
                region = self._UA_CITY_TO_REGION.get(city.lower())

        if region:
            region = re.sub(r"\s*область\s*$", "", region, flags=re.IGNORECASE).strip()

        # property
        type_keyword = (str(prop_in.get("type_keyword") or "").strip() or None)
        intent = (str(prop_in.get("intent") or "").strip() or None)
        min_area = self._to_float(prop_in.get("min_area_sqm"))
        max_area = self._to_float(prop_in.get("max_area_sqm"))

        if (not type_keyword or not min_area) and q:
            qlow = q.lower()
            if not type_keyword:
                if any(x in qlow for x in ["склад", "супермаркет", "магазин", "тц", "торгов", "комерц", "офіс", "офис", "азс"]):
                    type_keyword = "комерц"
                elif "земельн" in qlow or "ділянк" in qlow or "ділянок" in qlow:
                    type_keyword = "земельн"
                elif "квартир" in qlow:
                    type_keyword = "квартир"
                elif "будин" in qlow:
                    type_keyword = "будин"
            if not min_area:
                m = re.search(r"(?:від|>|більше)\s*(\d{2,6})\s*(?:м2|м²|кв\.?\s*м|sqm)", q, flags=re.IGNORECASE)
                if m:
                    try:
                        min_area = float(m.group(1))
                    except Exception:
                        pass
            if not max_area:
                m = re.search(r"(?:до|<|менше)\s*(\d{2,6})\s*(?:м2|м²|кв\.?\s*м|sqm)", q, flags=re.IGNORECASE)
                if m:
                    try:
                        max_area = float(m.group(1))
                    except Exception:
                        pass

        if not tk and q:
            ql = q.lower()
            for k in ("склад", "супермаркет", "магазин", "тц", "офіс", "квартир", "будинок", "азс", "виробництв", "ферма", "земельна ділянка"):
                if k in ql:
                    tk.append(k)
            tk = tk[:8]

        return {
            "location": {
                "city": (city[:80] if city else None),
                "region": (region[:80] if region else None),
                "raw": (raw_loc[:200] if raw_loc else (q[:200] if q else None)),
            },
            "property": {
                "type_keyword": (type_keyword[:40] if type_keyword else None),
                "intent": (intent[:20] if intent else None),
                "min_area_sqm": min_area,
                "max_area_sqm": max_area,
            },
            "task_keywords": tk,
        }

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            f = float(value)
            return f if f > 0 else None
        except Exception:
            return None

    def _normalize_step(self, idx: int, step: Any) -> Dict[str, Any]:
        if not isinstance(step, dict):
            step = {}
        return {
            "step_id": int(step.get("step_id") or idx),
            "goal": str(step.get("goal") or "")[:500],
            "candidate_tools": [
                str(t)[:80]
                for t in (step.get("candidate_tools") or [])
                if str(t).strip()
            ][:8],
            "success_criteria": str(step.get("success_criteria") or "")[:500],
        }

    def _fallback_plan(self, user_query: str) -> Dict[str, Any]:
        """Мінімальний план, якщо LLM не доступний/відмовив. Дозволяє оркестратору не зависнути."""
        return {
            "objective": "Зібрати початкові дані щодо запиту користувача.",
            "assumptions": [],
            "open_questions": [str(user_query or "")[:500]],
            "steps": [
                {
                    "step_id": 1,
                    "goal": "Уточнити запит у користувача та зібрати базові дані по локації.",
                    "candidate_tools": ["flx.ask_user", "schema.get_data_dictionary"],
                    "success_criteria": "Є чітка локація і тип нерухомості.",
                }
            ],
        }
