# -*- coding: utf-8 -*-
"""
Форматування посилань у відповідях користувачу: клікабельні посилання
без виведення сирого URL, з коротким представленням (наприклад «Посилання»).
Використовується в Telegram-боті та міні-застосунку.
"""

import re
import html

# Текст замість URL при відображенні (клікабельний)
LINK_LABEL = "Посилання"

# Регулярка для HTTP/HTTPS URL (без пробілів у кінці)
_URL_PATTERN = re.compile(
    r"https?://[^\s<>\]\)\"]+",
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


def format_message_links_for_mini_app(text: str) -> str:
    """
    Повертає текст з HTML-тегами посилань для безпечного відображення у веб-клієнті:
    URL замінюються на <a href="..." target="_blank" rel="noopener">Посилання</a>,
    решта тексту екранована.

    Args:
        text: сирий текст відповіді.

    Returns:
        Текст з посиланнями в HTML для вставки в innerHTML (решта екранована).
    """
    if not text or not text.strip():
        return text
    parts = []
    last_end = 0
    for m in _URL_PATTERN.finditer(text):
        if m.start() > last_end:
            parts.append(_escape_html(text[last_end : m.start()]))
        url = m.group(0).rstrip(".,;:!?)")
        parts.append(f'<a href="{_escape_html(url)}" target="_blank" rel="noopener noreferrer" class="chat-link">{LINK_LABEL}</a>')
        last_end = m.end()
    if last_end < len(text):
        parts.append(_escape_html(text[last_end:]))
    return "".join(parts)
