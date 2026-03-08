# -*- coding: utf-8 -*-
"""
Форматування посилань у відповідях користувачу: клікабельні посилання
без виведення сирого URL, з коротким представленням (наприклад «Посилання»).
Використовується в Telegram-боті та міні-застосунку.
"""

import re
import html
from typing import Optional, Tuple

# Текст замість URL при відображенні (клікабельний)
LINK_LABEL = "Посилання"
LINK_LABEL_OPEN_IN_APP = "Відкрити в застосунку"

# Регулярка для HTTP/HTTPS URL (без пробілів у кінці)
_URL_PATTERN = re.compile(
    r"https?://[^\s<>\]\)\"]+",
    re.IGNORECASE,
)

# Markdown-посилання [текст](url) — замінюємо цілим блоком, щоб уникнути дублювання
_MARKDOWN_LINK_PATTERN = re.compile(
    r"\[[^\]]*\]\s*\(\s*(https?://[^\s<>\)]+)\s*\)",
    re.IGNORECASE,
)

# Патерни для внутрішніх посилань (відкриваються в застосунку)
_PROZORRO_AUCTION_PATTERN = re.compile(
    r"https?://(?:www\.)?prozorro\.sale/auction/([^\s/]+)",
    re.IGNORECASE,
)
_OLX_LISTING_PATTERN = re.compile(
    r"https?://(?:www\.)?olx\.(?:ua|com\.ua)(?:/uk)?/[^\s\"'<>]+",
    re.IGNORECASE,
)


def _escape_html(text: str) -> str:
    """Екранує HTML-спецсимволи в тексті."""
    return html.escape(text, quote=True)


def format_message_links_for_telegram(text: str) -> str:
    """
    Повертає текст, придатний для відправки в Telegram з parse_mode=HTML:
    URL замінюються на клікабельні посилання з підписом «Посилання» (без виведення самого URL).

    Args:
        text: сирий текст відповіді (може містити URL та довільний текст).

    Returns:
        Текст з HTML-тегами для Telegram; решта тексту екранована.
    """
    if not text or not text.strip():
        return text
    text = _collapse_markdown_links(text)
    parts = []
    last_end = 0
    for m in _URL_PATTERN.finditer(text):
        # Текст до посилання — екрануємо
        if m.start() > last_end:
            segment = text[last_end : m.start()]
            parts.append(_escape_html(segment))
        url = m.group(0)
        # Вирізаємо можливі trailing punctuation з URL (часті помилки LLM)
        url = url.rstrip(".,;:!?)")
        parts.append(f'<a href="{_escape_html(url)}">{LINK_LABEL}</a>')
        last_end = m.end()
    if last_end < len(text):
        parts.append(_escape_html(text[last_end:]))
    return "".join(parts)


def _parse_internal_listing_url(url: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    Перевіряє, чи URL є посиланням на оголошення в системі (ProZorro або OLX).
    Повертає (source, source_id, label) або (None, None, label) для зовнішнього посилання.
    """
    url_clean = url.rstrip(".,;:!?)")
    m = _PROZORRO_AUCTION_PATTERN.match(url_clean)
    if m:
        auction_id = m.group(1)
        return ("prozorro", auction_id, LINK_LABEL_OPEN_IN_APP)
    if _OLX_LISTING_PATTERN.match(url_clean):
        return ("olx", url_clean, LINK_LABEL_OPEN_IN_APP)
    return (None, None, LINK_LABEL)


def _collapse_markdown_links(text: str) -> str:
    """
    Замінює markdown [текст](url) на сам url, щоб уникнути дублювання посилань.
    LLM часто повертає [Відкрити в застосунку](https://...), і без цього кроку
    ми б отримали подвійне посилання.
    """
    return _MARKDOWN_LINK_PATTERN.sub(r"\1", text)


def format_message_links_for_mini_app(text: str) -> str:
    """
    Повертає текст з HTML-тегами посилань для безпечного відображення у веб-клієнті:
    - ProZorro/OLX URL → внутрішнє посилання (data-source, data-source-id) для відкриття в застосунку
    - Інші URL → зовнішнє посилання target="_blank"

    Args:
        text: сирий текст відповіді.

    Returns:
        Текст з посиланнями в HTML для вставки в innerHTML (решта екранована).
    """
    if not text or not text.strip():
        return text
    text = _collapse_markdown_links(text)
    parts = []
    last_end = 0
    for m in _URL_PATTERN.finditer(text):
        if m.start() > last_end:
            parts.append(_escape_html(text[last_end : m.start()]))
        url = m.group(0).rstrip(".,;:!?)")
        source, source_id, label = _parse_internal_listing_url(url)
        if source and source_id:
            sid_escaped = _escape_html(str(source_id))
            parts.append(
                f'<a href="#" class="chat-link chat-link-internal" '
                f'data-source="{_escape_html(source)}" data-source-id="{sid_escaped}">{_escape_html(label)}</a>'
            )
        else:
            parts.append(
                f'<a href="{_escape_html(url)}" target="_blank" rel="noopener noreferrer" class="chat-link">{_escape_html(label)}</a>'
            )
        last_end = m.end()
    if last_end < len(text):
        parts.append(_escape_html(text[last_end:]))
    return "".join(parts)
