# -*- coding: utf-8 -*-
"""Specialized FLX investigator agents for critic/contrarian/domain workers."""

from __future__ import annotations

from typing import Any, Dict

from config.settings import Settings

from business.agents.investigator._common import call_llm_json, get_llm_service, get_prompt_template, render_template


class _BaseSpecializedAgent:
    prompt_name = ""
    caller_name = "flx.specialized"

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    def run(self, **kwargs: Any) -> Dict[str, Any]:
        template = get_prompt_template(self.prompt_name)
        if not template:
            return {}
        prompt = render_template(template, **kwargs)
        return call_llm_json(self.llm, prompt=prompt, caller=self.caller_name)


class InvestigatorCriticAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_critic"
    caller_name = "flx.critic"


class InvestigatorContrarianAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_contrarian"
    caller_name = "flx.contrarian"


class InvestigatorDataExtractorAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_data_extractor"
    caller_name = "flx.data_extractor"


class InvestigatorRiskAnalystAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_risk_analyst"
    caller_name = "flx.risk_analyst"


class InvestigatorValuationAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_valuation"
    caller_name = "flx.valuation"


class InvestigatorTrendAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_trend"
    caller_name = "flx.trend"


class InvestigatorGeoAnalystAgent(_BaseSpecializedAgent):
    prompt_name = "investigator_geo_analyst"
    caller_name = "flx.geo_analyst"
