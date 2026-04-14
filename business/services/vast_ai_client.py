# -*- coding: utf-8 -*-
"""
Клієнт Vast.ai REST API для lifecycle оренди GPU.
"""

from typing import Any, Dict, List, Optional

import requests


class VastAiClient:
    """Мінімальний клієнт Vast.ai для search/create/start/stop/destroy instance."""

    BASE_URL = "https://console.vast.ai/api/v0"

    def __init__(self, api_key: str, timeout_sec: int = 30) -> None:
        self.api_key = (api_key or "").strip()
        self.timeout_sec = timeout_sec
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        response = self.session.request(method=method, url=url, timeout=self.timeout_sec, **kwargs)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            details = ""
            try:
                body = response.json()
                if isinstance(body, dict):
                    details = str(body.get("msg") or body.get("error") or body)
                else:
                    details = str(body)
            except Exception:
                details = (response.text or "").strip()
            if details:
                raise requests.HTTPError(f"{e} | vast_response={details}", response=response) from e
            raise
        data = response.json() if response.content else {}
        return data if isinstance(data, dict) else {"data": data}

    def search_offers(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if filters:
            data = self._request("POST", "/bundles/", json=filters)
        else:
            params: Dict[str, Any] = {}
            if query:
                params["q"] = query
            data = self._request("GET", "/bundles/", params=params)
        offers = data.get("offers") or data.get("results") or data.get("data") or []
        return offers if isinstance(offers, list) else []

    def create_instance(self, ask_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PUT", f"/asks/{ask_id}/", json=payload or {})

    def show_instance(self, instance_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/instances/{instance_id}/")

    def start_instance(self, instance_id: str) -> Dict[str, Any]:
        return self._request("PUT", f"/instances/{instance_id}/", json={"method": "start"})

    def stop_instance(self, instance_id: str) -> Dict[str, Any]:
        return self._request("PUT", f"/instances/{instance_id}/", json={"method": "stop"})

    def destroy_instance(self, instance_id: str) -> Dict[str, Any]:
        return self._request("DELETE", f"/instances/{instance_id}/")
