# -*- coding: utf-8 -*-
"""
Легковаговий веб-пошук для Flx.

Поточна реалізація:
- duckduckgo html endpoint (без API-ключа);
- повертає топ результатів із title/url/snippet.
"""

from __future__ import annotations

import html
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

        provider = str(getattr(self.settings, "llm_investigator_web_search_provider", "duckduckgo") or "duckduckgo").lower()
        if provider != "duckduckgo":
            return {"ok": False, "error": f"unsupported_provider:{provider}", "items": []}

        limit = max(1, min(int(top_k or 5), 10))
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
            logger.warning("WebSearchService request failed: %s", e)
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
