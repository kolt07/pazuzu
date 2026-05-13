# -*- coding: utf-8 -*-
"""
Агенти Flx — інвестігейтор-досліджувач для глибокої гео-аналітики нерухомості.

Чотири LLM-only агенти, без побічних ефектів:
- InvestigatorPlannerAgent — будує початковий JSON-план дослідження.
- InvestigatorStepAgent — на кожному кроці вирішує: tool_call / ask_user / final_for_step.
- InvestigatorReflectionAgent — наприкінці генерує lessons-learned.
- InvestigatorReportAgent — компонує структурований JSON фінального звіту.
"""

from business.agents.investigator.planner import InvestigatorPlannerAgent
from business.agents.investigator.step import InvestigatorStepAgent
from business.agents.investigator.reflection import InvestigatorReflectionAgent
from business.agents.investigator.reporter import InvestigatorReportAgent
from business.agents.investigator.specialized import (
    InvestigatorContrarianAgent,
    InvestigatorCriticAgent,
    InvestigatorDataExtractorAgent,
    InvestigatorGeoAnalystAgent,
    InvestigatorRiskAnalystAgent,
    InvestigatorTrendAgent,
    InvestigatorValuationAgent,
)

__all__ = [
    "InvestigatorPlannerAgent",
    "InvestigatorStepAgent",
    "InvestigatorReflectionAgent",
    "InvestigatorReportAgent",
    "InvestigatorCriticAgent",
    "InvestigatorContrarianAgent",
    "InvestigatorDataExtractorAgent",
    "InvestigatorRiskAnalystAgent",
    "InvestigatorValuationAgent",
    "InvestigatorTrendAgent",
    "InvestigatorGeoAnalystAgent",
]
