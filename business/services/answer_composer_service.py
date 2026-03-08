# -*- coding: utf-8 -*-
"""
Answer Composer: перетворює результат виконання (дані, тип запиту, розмір) у єдиний
контракт відповіді для клієнта (Telegram, Mini App). Відповідь відокремлена від агента.
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Типи відповіді за контрактом
RESPONSE_TYPE_LIST = "list"
RESPONSE_TYPE_TABLE = "table"
RESPONSE_TYPE_CHART = "chart"
RESPONSE_TYPE_FILE = "file"
RESPONSE_TYPE_TEXT = "text"

# Канали клієнта
CHANNEL_TELEGRAM = "telegram"
CHANNEL_MINI_APP = "mini_app"

# Обмеження довжини тексту по каналу (символів)
MAX_TEXT_LENGTH = {
    CHANNEL_TELEGRAM: 4000,
    CHANNEL_MINI_APP: 8000,
}


class AnswerComposerService:
    """
    Формує контракт відповіді: type, title, items, attachments, summary.
    Логіка: ≤10 рядків → текст/список; таблиця → markdown або список; 50+ → summary + файл; тренд → опис.
    """

    def __init__(self, max_text_length_by_channel: Optional[Dict[str, int]] = None):
        self.max_text_length = max_text_length_by_channel or MAX_TEXT_LENGTH

    def compose(
        self,
        execution_result: Dict[str, Any],
        client_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Повертає контракт відповіді для клієнта.

        Args:
            execution_result: містить data (list), query_type (analytical | report | export),
                              row_count, has_attachment (bool), title (optional), presentation (list | table | chart),
                              response_format (text_answer | data_export | analytical_text | out_of_scope).
            client_context: channel (telegram | mini_app), max_text_length (optional override).

        Returns:
            Контракт: { type, title, items, attachments, summary }.
        """
        data = execution_result.get("data") or []
        query_type = execution_result.get("query_type", "analytical")
        row_count = execution_result.get("row_count", len(data))
        has_attachment = execution_result.get("has_attachment", False)
        title = execution_result.get("title") or ""
        presentation = execution_result.get("presentation") or "list"
        attachments = execution_result.get("attachments") or []
        response_format = execution_result.get("response_format")  # Новий формат відповіді

        client_context = client_context or {}
        channel = client_context.get("channel", CHANNEL_TELEGRAM)
        max_len = client_context.get("max_text_length") or self.max_text_length.get(channel, 4000)

        items: List[Any] = []
        summary_parts: List[str] = []
        intent_info = execution_result.get("intent_info") or {}
        response_template = intent_info.get("response_template", "")
        intent_str = str(intent_info.get("intent", "")).lower()
        is_count_query = bool(
            response_template
            and response_format == "text_answer"
            and ("X" in response_template or "кількість" in intent_str or "[кількість]" in response_template)
        )

        # Обробка формату out_of_scope
        if response_format == "out_of_scope":
            return {
                "type": RESPONSE_TYPE_TEXT,
                "title": "Запит не стосується системи",
                "items": [],
                "attachments": [],
                "summary": "Ваш запит не стосується функціональності цієї системи. Система працює тільки з даними про нерухомість з джерел ProZorro та OLX.",
            }

        if row_count == 0:
            # Формуємо інформативніше повідомлення про відсутність результатів
            filter_info = execution_result.get("filter_info", {})
            sources = execution_result.get("sources", [])
            diagnostic_info = execution_result.get("diagnostic_info", {})
            
            summary_parts = ["За вказаними умовами результатів не знайдено."]
            
            # Додаємо інформацію про джерела даних
            if sources:
                sources_str = ", ".join(sources)
                summary_parts.append(f"\nПеревірено джерела: {sources_str}.")
            
            # Додаємо інформацію про застосовані фільтри
            if filter_info:
                filter_details = []
                if filter_info.get("region"):
                    filter_details.append(f"регіон: {filter_info['region']}")
                if filter_info.get("city"):
                    filter_details.append(f"місто: {filter_info['city']}")
                if filter_info.get("date_range"):
                    filter_details.append(f"період: {filter_info['date_range']}")
                if filter_details:
                    summary_parts.append(f"\nЗастосовані фільтри: {', '.join(filter_details)}.")
            
            # Додаємо діагностичну інформацію
            if diagnostic_info:
                if diagnostic_info.get("total_documents_in_collection"):
                    summary_parts.append(f"\nЗагальна кількість документів у колекції: {diagnostic_info['total_documents_in_collection']}.")
                if diagnostic_info.get("addresses_available") is False:
                    summary_parts.append("\n⚠️ Увага: поле адрес відсутнє в даних. Можливо, дані ще не оброблені.")
                if diagnostic_info.get("suggestions"):
                    summary_parts.append(f"\n💡 Рекомендації: {diagnostic_info['suggestions']}")
            
            # Загальні рекомендації
            summary_parts.append("\n💡 Спробуйте:")
            summary_parts.append("- Розширити період пошуку")
            summary_parts.append("- Перевірити правильність написання назви регіону/міста")
            summary_parts.append("- Використати менш строгі фільтри")
            
            return {
                "type": RESPONSE_TYPE_TEXT,
                "title": title or "Результат",
                "items": [],
                "attachments": attachments,
                "summary": "\n".join(summary_parts),
            }

        # Визначення типу відповіді на основі response_format
        if response_format == "data_export":
            # Вибірка у файл - завжди файл
            response_type = RESPONSE_TYPE_FILE
        elif response_format == "analytical_text":
            # Аналітичний текст - текст з висновками
            response_type = RESPONSE_TYPE_TEXT
        elif response_format == "text_answer":
            # Перевірка на запит кількості — використовуємо response_template
            if is_count_query:
                # Запит на кількість — коротка текстова відповідь з числом
                response_type = RESPONSE_TYPE_TEXT
            elif row_count <= 10:
                response_type = RESPONSE_TYPE_LIST
            else:
                response_type = RESPONSE_TYPE_TABLE
        else:
            # Стара логіка для сумісності
            if presentation == "chart" or query_type == "trend":
                response_type = RESPONSE_TYPE_CHART
            elif has_attachment and row_count > 20:
                response_type = RESPONSE_TYPE_FILE
            elif presentation == "table" or row_count > 10:
                response_type = RESPONSE_TYPE_TABLE
            else:
                response_type = RESPONSE_TYPE_LIST

        # Формування відповіді залежно від формату
        if response_format == "analytical_text":
            # Аналітичний текст - формуємо текст з висновками
            summary = self._format_analytical_text(data, title, row_count)
        elif response_format == "text_answer" and is_count_query:
            # Запит на кількість — підставляємо число в шаблон
            summary = self._format_count_answer(response_template, row_count, intent_info)
        elif response_format == "text_answer":
            # Текстова відповідь - список з посиланнями та цифрами
            summary = self._format_text_answer(data, title, row_count, max_len)
        else:
            # Стандартне форматування
            for i, row in enumerate(data[:100]):
                if isinstance(row, dict):
                    item = self._row_to_display_item(row, i + 1)
                    items.append(item)
                    line = item.get("text") or str(item)
                    summary_parts.append(line)
                else:
                    items.append({"text": str(row)})
                    summary_parts.append(str(row))

            summary = "\n".join(summary_parts)
            if len(summary) > max_len:
                summary = summary[: max_len - 50] + "\n… (обрізано)"
            if row_count > 100:
                summary += f"\n\nУсього записів: {row_count}."

        return {
            "type": response_type,
            "title": title or "Результат запиту",
            "items": items,
            "attachments": attachments,
            "summary": summary,
        }
    
    def _format_count_answer(
        self,
        response_template: str,
        row_count: int,
        intent_info: Dict[str, Any],
    ) -> str:
        """Формує відповідь на запит про кількість — підставляє число в шаблон."""
        # Підставляємо X, [кількість], [count] тощо
        result = response_template.replace("X", str(row_count))
        result = result.replace("[кількість]", str(row_count))
        result = result.replace("[count]", str(row_count))
        return result.strip()

    def _format_text_answer(
        self,
        data: List[Dict[str, Any]],
        title: str,
        row_count: int,
        max_len: int
    ) -> str:
        """Формує текстову відповідь з посиланнями та цифрами."""
        parts = []
        
        if title:
            parts.append(f"## {title}")
            parts.append("")
        
        # Додаємо результати з посиланнями
        for i, row in enumerate(data[:20], 1):  # Обмежуємо до 20 для текстової відповіді
            item_parts = []
            
            # Ціна
            if "price" in row:
                item_parts.append(f"{row['price']} грн")
            elif "auction_data" in row and "value" in row["auction_data"]:
                amount = row["auction_data"]["value"].get("amount")
                if amount:
                    item_parts.append(f"{amount:,.0f} грн")
            
            # Кількість учасників (для запитів про учасників)
            if "bidders_count" in row:
                item_parts.append(f"{int(row['bidders_count'])} учасників")
            elif "bids_count" in row:
                item_parts.append(f"{int(row['bids_count'])} заявок")
            elif "auction_data" in row and "bids" in row["auction_data"]:
                bids = row["auction_data"].get("bids", [])
                if bids:
                    # Рахуємо унікальних учасників
                    unique_bidders = set()
                    for bid in bids:
                        for bidder in bid.get("bidders", []):
                            identifier = bidder.get("identifier", {})
                            bidder_id = identifier.get("id")
                            if bidder_id:
                                unique_bidders.add(bidder_id)
                    if unique_bidders:
                        item_parts.append(f"{len(unique_bidders)} учасників")
            
            # Площа
            if "area" in row:
                item_parts.append(f"{row['area']} м²")
            
            # Регіон/місто
            region = self._extract_region_from_row(row)
            if region:
                item_parts.append(region)
            
            # Посилання
            url = self._extract_url_from_row(row)
            if url:
                item_parts.append(f"[Посилання]({url})")
            elif not url and "auction_data" in row:
                # Спробуємо знайти auction_id в auction_data
                auction_id = row["auction_data"].get("auction_id") or row.get("auction_id")
                if auction_id:
                    url = f"https://prozorro.sale/auction/{auction_id}"
                    item_parts.append(f"[Посилання]({url})")
            
            if item_parts:
                parts.append(f"{i}. {' | '.join(item_parts)}")
        
        if row_count > 20:
            parts.append(f"\n... та ще {row_count - 20} результатів")
        
        summary = "\n".join(parts)
        if len(summary) > max_len:
            summary = summary[:max_len - 50] + "\n… (обрізано)"
        
        return summary
    
    def _format_analytical_text(
        self,
        data: List[Dict[str, Any]],
        title: str,
        row_count: int
    ) -> str:
        """Формує аналітичний текст з висновками."""
        parts = []
        
        if title:
            parts.append(f"## {title}")
            parts.append("")
        
        parts.append("### Результати аналізу:")
        parts.append("")
        
        # Аналізуємо дані та формуємо висновки
        if data:
            # Підрахунок середніх значень
            prices = []
            for row in data:
                price = self._extract_price_from_row(row)
                if price:
                    prices.append(price)
            
            if prices:
                avg_price = sum(prices) / len(prices)
                min_price = min(prices)
                max_price = max(prices)
                
                parts.append(f"- Середня ціна: {avg_price:,.0f} грн")
                parts.append(f"- Мінімальна ціна: {min_price:,.0f} грн")
                parts.append(f"- Максимальна ціна: {max_price:,.0f} грн")
                parts.append("")
            
            # Розподіл по регіонах
            regions = {}
            for row in data:
                region = self._extract_region_from_row(row)
                if region:
                    regions[region] = regions.get(region, 0) + 1
            
            if regions:
                parts.append("### Розподіл по регіонах:")
                for region, count in sorted(regions.items(), key=lambda x: x[1], reverse=True)[:5]:
                    parts.append(f"- {region}: {count} об'єктів")
                parts.append("")
        
        parts.append(f"### Висновки:")
        parts.append(f"Проаналізовано {row_count} об'єктів нерухомості.")
        
        return "\n".join(parts)
    
    def _extract_price_from_row(self, row: Dict[str, Any]) -> Optional[float]:
        """Витягує ціну з рядка."""
        if "price" in row:
            try:
                return float(row["price"])
            except (ValueError, TypeError):
                pass
        if "value" in row and row["value"] is not None:
            try:
                return float(row["value"])
            except (ValueError, TypeError):
                pass
        if "auction_data" in row and "value" in row["auction_data"]:
            amount = row["auction_data"]["value"].get("amount")
            if amount:
                try:
                    return float(amount)
                except (ValueError, TypeError):
                    pass
        
        return None
    
    def _extract_region_from_row(self, row: Dict[str, Any]) -> Optional[str]:
        """Витягує регіон з рядка."""
        if "region" in row:
            return str(row["region"])
        
        if "address_refs" in row:
            for ref in row.get("address_refs", []):
                if "region" in ref and "name" in ref["region"]:
                    return ref["region"]["name"]
        
        if "auction_data" in row and "address_refs" in row["auction_data"]:
            for ref in row["auction_data"].get("address_refs", []):
                if "region" in ref and "name" in ref["region"]:
                    return ref["region"]["name"]
        
        return None
    
    def _extract_url_from_row(self, row: Dict[str, Any]) -> Optional[str]:
        """Витягує URL з рядка."""
        if "url" in row:
            url = row["url"]
            if url and str(url) != "Посилання":
                return str(url)
        
        # Спробуємо знайти auction_id (формат LSE001-UA-..., не MongoDB _id)
        auction_id = row.get("auction_id")
        if not auction_id and "auction_data" in row:
            auction_id = row["auction_data"].get("auctionId") or row["auction_data"].get("auction_id")
        # НЕ використовуємо _id — це MongoDB ObjectId, не валідний для prozorro.sale/auction/{id}

        if auction_id:
            return f"https://prozorro.sale/auction/{auction_id}"
        
        return None

    def _row_to_display_item(self, row: Dict[str, Any], index: int) -> Dict[str, Any]:
        """Перетворює один ряд результату на елемент для відображення (текст + опційні поля)."""
        parts = []
        if isinstance(row.get("_id"), dict):
            parts.append(", ".join(f"{k}: {v}" for k, v in row["_id"].items()))
        elif row.get("_id") is not None:
            parts.append(str(row["_id"]))
        if row.get("search_data", {}).get("title"):
            parts.append(str(row["search_data"]["title"])[:80])
        if row.get("search_data", {}).get("price") is not None:
            parts.append(f"{row['search_data']['price']} грн")
        if row.get("auction_data", {}).get("value", {}).get("amount") is not None:
            parts.append(f"{row['auction_data']['value']['amount']} грн")
        if "count" in row and row["count"] is not None:
            parts.append(f"кільк. {row['count']}")
        if "avg" in row and row["avg"] is not None:
            try:
                parts.append(f"сер. {float(row['avg']):.2f}")
            except (TypeError, ValueError):
                parts.append(f"сер. {row['avg']}")
        if "sum" in row and row["sum"] is not None:
            parts.append(f"сума {row['sum']}")
        text = f"{index}. {' | '.join(parts)}" if parts else str(row)
        return {"text": text, "index": index}
