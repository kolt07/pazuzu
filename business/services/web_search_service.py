# -*- coding: utf-8 -*-
"""
Веб-пошук для Flx.

- duckduckgo: HTML endpoint (без ключа);
- gemini_grounding: Gemini з Google Search grounding (google-genai);
- при помилці / порожньому результаті — fallback на DuckDuckGo.
"""

from __future__ import annotations

import html
import json
import logging
import re
from typing import Any, Dict, List
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen

from config.settings import Settings

logger = logging.getLogger(__name__)

_A_TAG_RE = re.compile(
    r'<a[^>]*class="[^"]*result__a[^"]*"[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_SNIPPET_RE = re.compile(
    r'<a[^>]*class="[^"]*result__a[^"]*"[^>]*>.*?</a>.*?<a[^>]*class="[^"]*result__snippet[^"]*"[^>]*>(?P<snippet>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")
_JSON_ARRAY_RE = re.compile(r"\[[\s\S]*\]")


def _strip_html(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(_TAG_RE.sub("", text or ""))).strip()


def _unwrap_ddg_url(raw_href: str) -> str:
    href = html.unescape(raw_href or "")
    if href.startswith("//"):
        href = "https:" + href
    parsed = urlparse(href)
    if "duckduckgo.com" in (parsed.netloc or "") and parsed.path.startswith("/l/"):
        q = parse_qs(parsed.query or "")
        uddg = (q.get("uddg") or [""])[0]
        if uddg:
            return unquote(uddg)
    return href


class WebSearchService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def search(self, query: str, top_k: int = 5) -> Dict[str, Any]:
        q = str(query or "").strip()
        if not q:
            return {"ok": False, "error": "empty_query", "items": []}
        if not getattr(self.settings, "llm_investigator_web_search_enabled", True):
            return {"ok": False, "error": "web_search_disabled", "items": []}

        limit = max(1, min(int(top_k or 5), 10))
        provider = str(getattr(self.settings, "llm_investigator_web_search_provider", "duckduckgo") or "duckduckgo").lower()
        if provider in ("gemini_grounding", "gemini", "google_grounding"):
            gr = self._search_gemini_grounding(q, limit)
            if gr.get("ok") and gr.get("items"):
                return gr
            logger.info("WebSearchService: gemini_grounding empty or failed, fallback duckduckgo")
        elif provider != "duckduckgo":
            logger.warning("WebSearchService: unknown provider %s, using duckduckgo", provider)
        return self._search_duckduckgo(q, limit)

    def _search_gemini_grounding(self, q: str, top_k: int) -> Dict[str, Any]:
        try:
            from google import genai
            from google.genai import types
        except ImportError:
            return {"ok": False, "error": "google_genai_missing", "items": []}

        api_key = (self.settings.llm_api_keys or {}).get("gemini") or ""
        if not str(api_key).strip():
            return {"ok": False, "error": "missing_gemini_api_key", "items": []}

        model = str(getattr(self.settings, "llm_investigator_web_search_model_name", "") or "").strip()
        if not model:
            model = str(getattr(self.settings, "llm_assistant_model_name", "") or "gemini-2.5-flash").strip()

        client = genai.Client(api_key=api_key)
        prompt = (
            f"Use Google Search. Query: {q}\n\n"
            f"Return ONLY a JSON array (no markdown fences) of up to {top_k} objects with string fields: "
            f"title, url, snippet. Ukrainian context is fine."
        )
        try:
            cfg = types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
                temperature=0.15,
                max_output_tokens=2048,
            )
            resp = client.models.generate_content(model=model, contents=prompt, config=cfg)
        except Exception as e:
            logger.warning("Gemini grounded search request failed: %s", e)
            return {"ok": False, "error": str(e), "items": []}

        items: List[Dict[str, Any]] = []
        seen_urls: set = set()

        try:
            cands = getattr(resp, "candidates", None) or []
            if cands:
                gm = getattr(cands[0], "grounding_metadata", None)
                chunks = getattr(gm, "grounding_chunks", None) or [] if gm else []
                for ch in chunks:
                    web = getattr(ch, "web", None)
                    if web is None:
                        continue
                    uri = str(getattr(web, "uri", None) or "").strip()
                    title = str(getattr(web, "title", None) or "").strip()
                    if not uri and not title:
                        continue
                    key = uri or title
                    if key in seen_urls:
                        continue
                    seen_urls.add(key)
                    items.append({"title": (title or uri)[:300], "url": uri[:2000], "snippet": ""})
        except Exception as e:
            logger.debug("grounding_metadata parse: %s", e)

        text = (getattr(resp, "text", None) or "").strip()
        if text:
            for m in _JSON_ARRAY_RE.finditer(text):
                chunk = m.group(0).strip()
                try:
                    arr = json.loads(chunk)
                    if isinstance(arr, list):
                        for el in arr:
                            if not isinstance(el, dict):
                                continue
                            url = str(el.get("url") or "").strip()
                            title = str(el.get("title") or "").strip()
                            snip = str(el.get("snippet") or "").strip()
                            if not url and not title:
                                continue
                            key = url or title
                            if key in seen_urls:
                                continue
                            seen_urls.add(key)
                            items.append(
                                {
                                    "title": (title or url)[:300],
                                    "url": url[:2000],
                                    "snippet": snip[:600],
                                }
                            )
                except json.JSONDecodeError:
                    continue

        if not items and text:
            items.append({"title": "Підсумок пошуку", "url": "", "snippet": text[:1500]})

        items = items[:top_k]
        return {
            "ok": True,
            "provider": "gemini_grounding",
            "query": q,
            "items": items,
            "count": len(items),
        }

    def _search_duckduckgo(self, q: str, limit: int) -> Dict[str, Any]:
        url = f"https://duckduckgo.com/html/?q={quote_plus(q)}"
        req = Request(
            url=url,
            headers={
                "User-Agent": str(getattr(self.settings, "user_agent", "") or "Mozilla/5.0 Pazuzu-Flx"),
                "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
            },
        )
        try:
            with urlopen(req, timeout=12) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("WebSearchService DDG request failed: %s", e)
            return {"ok": False, "error": f"request_failed:{e}", "items": []}

        snippets = [_strip_html(m.group("snippet")) for m in _SNIPPET_RE.finditer(body)]
        items: List[Dict[str, Any]] = []
        for idx, match in enumerate(_A_TAG_RE.finditer(body)):
            title = _strip_html(match.group("title"))
            link = _unwrap_ddg_url(match.group("href"))
            if not title or not link:
                continue
            items.append(
                {
                    "title": title[:300],
                    "url": link,
                    "snippet": (snippets[idx] if idx < len(snippets) else "")[:600],
                }
            )
            if len(items) >= limit:
                break

        return {
            "ok": True,
            "provider": "duckduckgo",
            "query": q,
            "items": items,
            "count": len(items),
        }
