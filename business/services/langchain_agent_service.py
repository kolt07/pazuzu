# -*- coding: utf-8 -*-
"""
Сервіс LangChain-агента (найвищий шар застосунку).

Шарова логіка: агент використовує виключно інструменти шару бізнес-логіки; прямого
доступу до БД, API чи файлової системи немає. Інструменти згруповані за категоріями:
ініціація обмінів, вибірка/агрегація/метрики, збереження у файл та відправка користувачу.
Діалог зберігається через сервіси пам'яті; контекст попередніх бесід та глосарій
надаються агенту. Нові сервіси додаються поступово через нові інструменти.
"""

import base64
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable, Tuple, Union, TYPE_CHECKING

from config.settings import Settings
from data.repositories.agent_temp_exports_repository import AgentTempExportsRepository
from data.repositories.export_daily_count_repository import ExportDailyCountRepository
from data.repositories.pending_export_repository import PendingExportRepository
from business.services.artifact_service import ArtifactService
from utils.data_dictionary import DataDictionary
from utils.date_utils import format_date_display, format_datetime_display
from utils.analytics_builder import AnalyticsBuilder
from utils.query_builder import QueryBuilder
from domain.services.analytics_extracts_service import AnalyticsExtractsService
from utils.report_generator import ReportGenerator
from utils.file_utils import generate_excel_in_memory, ensure_directory_exists
from business.services.geocoding_service import GeocodingService
from business.services.logging_service import LoggingService
from business.services.places_service import PlacesService
from business.services.collection_knowledge_service import CollectionKnowledgeService
from business.services.prozorro_service import ProZorroService
from data.database.connection import MongoDBConnection
from business.services.source_data_load_service import run_full_pipeline

# LangChain imports
try:
    from langchain_core.tools import StructuredTool
    from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage, AIMessage
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_openai import ChatOpenAI
    from langchain_anthropic import ChatAnthropic
    LANGCHAIN_AVAILABLE = True
except ImportError:
    StructuredTool = None  # type: ignore
    HumanMessage = None  # type: ignore
    SystemMessage = None  # type: ignore
    ToolMessage = None  # type: ignore
    AIMessage = None  # type: ignore
    ChatGoogleGenerativeAI = None  # type: ignore
    ChatOpenAI = None  # type: ignore
    ChatAnthropic = None  # type: ignore
    LANGCHAIN_AVAILABLE = False

# Ollama (локальна LLM) — без API ключа
try:
    from langchain_ollama import ChatOllama
    CHAT_OLLAMA_AVAILABLE = True
except ImportError:
    try:
        from langchain_community.chat_models import ChatOllama
        CHAT_OLLAMA_AVAILABLE = True
    except ImportError:
        ChatOllama = None  # type: ignore
        CHAT_OLLAMA_AVAILABLE = False

# Опціонально: embeddings для VectorStoreRetrieverMemory-стилю пошуку
try:
    from langchain_google_genai import GoogleGenerativeAIEmbeddings
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    GoogleGenerativeAIEmbeddings = None  # type: ignore
    EMBEDDINGS_AVAILABLE = False

# Налаштування логування
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '[LangChain Agent] %(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.propagate = False


def _extract_usage_from_aimessage(msg: Any) -> Optional[Dict[str, int]]:
    """Витягує input_tokens, output_tokens з AIMessage (LangChain Google GenAI). Повертає None якщо немає."""
    if msg is None:
        return None
    try:
        um = getattr(msg, "usage_metadata", None) or (getattr(msg, "response_metadata", None) or {}).get("usage_metadata")
        if not um:
            return None
        if isinstance(um, dict):
            inp = um.get("input_tokens") or um.get("prompt_token_count")
        else:
            inp = getattr(um, "input_tokens", None) or getattr(um, "prompt_token_count", None)
        if isinstance(um, dict):
            out = um.get("output_tokens") or um.get("candidates_token_count")
        else:
            out = getattr(um, "output_tokens", None) or getattr(um, "candidates_token_count", None)
        if inp is not None or out is not None:
            return {"input_tokens": int(inp or 0), "output_tokens": int(out or 0)}
    except (TypeError, ValueError, AttributeError):
        pass
    return None


def _message_content_to_str(content: Any) -> str:
    """Перетворює content повідомлення (рядок або список блоків) на один рядок для логування."""
    if content is None:
        return ""
    if isinstance(content, bytes):
        try:
            return content.decode("utf-8", errors="replace")
        except Exception:
            return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                # Різні формати: {"type": "text", "text": "..."}, {"thinking": "..."}, {"content": "..."}
                part = (
                    item.get("text")
                    or item.get("content")
                    or item.get("thinking", "")
                )
                if part:
                    parts.append(str(part))
            else:
                # Об'єкти (наприклад блоки від SDK): .text, .content, .thinking
                part = getattr(item, "text", None) or getattr(item, "content", None) or getattr(item, "thinking", None)
                if part is not None and str(part).strip():
                    parts.append(str(part))
                else:
                    parts.append(str(item))
        return "\n".join(parts) if parts else ""
    return str(content)


def _messages_to_request_text(messages: List[Any]) -> str:
    """Серіалізує список повідомлень (HumanMessage, SystemMessage, AIMessage, ToolMessage) у один рядок для логування."""
    if not messages:
        return ""
    lines = []
    for msg in messages:
        role = type(msg).__name__
        content = getattr(msg, "content", None)
        if content is None:
            content = getattr(msg, "text", None)
        part = _message_content_to_str(content)
        if not part.strip() and content is not None:
            # Контент є, але не вдалося витягти текст (напр. невідомий формат)
            part = f"(content type: {type(content).__name__}, repr: {repr(content)[:200]})"
        lines.append(f"[{role}]\n{part}")
    result = "\n---\n".join(lines)
    if not result.strip():
        return f"(серіалізація: {len(messages)} повідомлень, контент не витягнуто)"
    return result


def _aimessage_to_response_text(response: Any) -> str:
    """Витягує повний текст відповіді з AIMessage для логування (включаючи thinking-блоки як текст)."""
    if response is None:
        return ""
    content = getattr(response, "content", None)
    if content is None:
        content = getattr(response, "text", None)
    # Частина інтеграцій (наприклад langchain_core) зберігає блоки в content_blocks
    if (content is None or (isinstance(content, list) and not content)) and hasattr(response, "content_blocks"):
        blocks = getattr(response, "content_blocks", None)
        if blocks:
            content = blocks
    text = _message_content_to_str(content)
    if not text.strip() and hasattr(response, "tool_calls") and getattr(response, "tool_calls", None):
        tool_calls = response.tool_calls or []
        names = [getattr(t, "name", t.get("name", "?")) if not isinstance(t, dict) else t.get("name", "?") for t in tool_calls]
        text = f"(відповідь: виклики інструментів: {', '.join(str(n) for n in names)})"
    return text


# Параметри пам'яті (елементи ConversationBufferMemory, ConversationSummaryMemory, VectorStoreRetrieverMemory)
CONVERSATION_BUFFER_MAX_MESSAGES = 20
CONVERSATION_SUMMARY_TRIM_SIZE = 10
VECTOR_RETRIEVE_TOP_K = 5
VECTOR_STORE_MAX_DOCS = 100

# Retry при тимчасових помилках LLM (503, 429, timeout, connection)
AGENT_LLM_RETRY_ATTEMPTS = 2
AGENT_LLM_RETRY_BACKOFF_SECONDS = (1.0, 2.0)


def _extract_and_send_thinking(response: Any, thinking_callback: Callable[[str], None]) -> None:
    """Витягує thinking-блоки з відповіді AIMessage та викликає callback."""
    content = getattr(response, 'content', None)
    if not content:
        return
    parts = content if isinstance(content, list) else [content]
    thinking_parts = []
    for item in parts:
        if isinstance(item, dict) and item.get('type') == 'thinking':
            text = item.get('thinking') or item.get('reasoning') or ''
            if text:
                thinking_parts.append(str(text).strip())
        elif isinstance(item, dict) and item.get('type') == 'reasoning':
            text = item.get('reasoning') or ''
            if text:
                thinking_parts.append(str(text).strip())
    if thinking_parts:
        try:
            thinking_callback('\n\n'.join(thinking_parts))
        except Exception as e:
            logger.debug("thinking_callback error: %s", e)

# Логічні групи tools за маршрутом (None = усі інструменти)
TOOL_ROUTES = {
    "free_form": None,
    "query_export": {
        "get_database_schema", "get_collection_info", "get_data_dictionary", "get_allowed_collections",
        "get_distinct_values", "execute_query", "execute_aggregation", "save_query_to_temp_collection",
        "export_from_temp_collection", "export_listings_to_file", "save_query_results_to_excel",
        "trigger_data_update", "list_templates", "generate_report",
        "generate_search_filter_string", "search_unified_listings",
    },
    "analytics": {
        "get_allowed_collections", "get_data_dictionary", "execute_analytics", "list_metrics",
        "analytics_extracts_aggregate", "analytics_extracts_search", "analytics_extracts_list_metrics",
        "analytics_extracts_list_dimensions", "analytics_extracts_get_distinct",
        "generate_report", "list_templates", "execute_query", "execute_aggregation",
        "save_query_to_temp_collection", "export_from_temp_collection",
        "generate_search_filter_string", "search_unified_listings",
    },
    "geo": {"geocode_address", "search_nearby_places", "get_allowed_collections", "get_collection_info"},
    "geo_assessment": {"geocode_address", "search_nearby_places", "get_collection_info", "execute_query"},
    "listing_detail": {"get_listing_details", "geocode_address", "get_collection_info"},
}


def _is_transient_llm_error(exc: Exception) -> bool:
    """Чи є помилка тимчасовою (доцільно повторити запит)."""
    msg = (str(exc) or "").lower()
    if "503" in msg or "429" in msg or "quota" in msg or "rate" in msg:
        return True
    if "timeout" in msg or "timed out" in msg or "deadline" in msg:
        return True
    if "connection" in msg or "connect" in msg or "unavailable" in msg:
        return True
    return False


class UserConversationMemory:
    """
    Пам'ять розмови на одного користувача: буфер останніх повідомлень (ConversationBufferMemory),
    саммарі старішої частини (ConversationSummaryMemory), опційно векторний пошук релевантних фрагментів (VectorStoreRetrieverMemory).
    """
    __slots__ = ('buffer', 'summary', 'vector_docs', 'embeddings')

    def __init__(self):
        self.buffer: List[Tuple[str, str]] = []  # (human, ai) пари
        self.summary: str = ""
        self.vector_docs: List[Dict[str, Any]] = []  # [{"content": str, "embedding": list}, ...]
        self.embeddings = None  # опційно: модель для embeddings

    def add_exchange(self, human: str, ai: str) -> None:
        self.buffer.append((human, ai))
        if len(self.buffer) > CONVERSATION_BUFFER_MAX_MESSAGES:
            self.buffer = self.buffer[-CONVERSATION_BUFFER_MAX_MESSAGES:]

    def get_buffer_messages(self) -> List[Any]:
        """Повертає список HumanMessage/AIMessage з буфера для вставки в історію."""
        if not HumanMessage or not AIMessage:
            return []
        messages = []
        for h, a in self.buffer:
            messages.append(HumanMessage(content=h))
            messages.append(AIMessage(content=a))
        return messages

    def trim_and_summarize(self, summarize_fn: Callable[[List[Tuple[str, str]]], str]) -> None:
        """Обрізає найстаріші обміни і додає їх саммарі до self.summary."""
        if len(self.buffer) <= CONVERSATION_SUMMARY_TRIM_SIZE:
            return
        to_summarize = self.buffer[:CONVERSATION_SUMMARY_TRIM_SIZE]
        self.buffer = self.buffer[CONVERSATION_SUMMARY_TRIM_SIZE:]
        new_summary = summarize_fn(to_summarize)
        if new_summary:
            self.summary = (self.summary + "\n\n" + new_summary).strip() if self.summary else new_summary

    def add_to_vector_store(self, content: str, embed_fn: Optional[Callable[[str], List[float]]]) -> None:
        if not embed_fn or len(self.vector_docs) >= VECTOR_STORE_MAX_DOCS:
            return
        try:
            emb = embed_fn(content)
            self.vector_docs.append({"content": content, "embedding": emb})
            if len(self.vector_docs) > VECTOR_STORE_MAX_DOCS:
                self.vector_docs = self.vector_docs[-VECTOR_STORE_MAX_DOCS:]
        except Exception as e:
            logger.debug("Vector store add failed: %s", e)

    def retrieve_relevant(self, query: str, embed_fn: Optional[Callable[[str], List[float]]], k: int = VECTOR_RETRIEVE_TOP_K) -> List[str]:
        """Повертає до k найрелевантніших фрагментів з vector_docs за запитом (cosine similarity)."""
        if not embed_fn or not self.vector_docs:
            return []
        try:
            import math
            q_emb = embed_fn(query)
            scores = []
            for doc in self.vector_docs:
                emb = doc["embedding"]
                dot = sum(a * b for a, b in zip(q_emb, emb))
                na = math.sqrt(sum(x * x for x in q_emb)) or 1e-10
                nb = math.sqrt(sum(x * x for x in emb)) or 1e-10
                cos = dot / (na * nb)
                scores.append((cos, doc["content"]))
            scores.sort(key=lambda x: -x[0])
            return [s[1] for s in scores[:k]]
        except Exception as e:
            logger.debug("Vector retrieve failed: %s", e)
            return []


class LangChainAgentService:
    """
    Сервіс для роботи з LangChain агентом з підтримкою MCP інструментів.
    
    Архітектурні принципи:
    - Явний цикл агента (plan → act → observe)
    - Всі операції з даними через MCP tools
    - Детальне логування всіх дій
    - Безпека на рівні сервісу
    """
    
    # Максимальна кількість ітерацій агента
    MAX_ITERATIONS = 10
    
    # Максимальна кількість викликів tools за одну ітерацію (збільшено для аналітики по днях/батч-операцій)
    MAX_TOOL_CALLS_PER_ITERATION = 10
    
    def __init__(self, settings: Settings):
        """
        Ініціалізація сервісу.
        
        Args:
            settings: Налаштування застосунку
        """
        if not LANGCHAIN_AVAILABLE:
            raise ImportError(
                "Для використання LangChain потрібно встановити залежності: "
                "pip install langchain langchain-google-genai langchain-openai langchain-anthropic"
            )
        
        self.settings = settings
        
        # Ініціалізуємо утиліти (те саме, що використовують MCP-сервери)
        self.data_dictionary = DataDictionary()
        self.analytics_builder = AnalyticsBuilder()
        self.query_builder = QueryBuilder()
        self.analytics_extracts_service = AnalyticsExtractsService()
        self.report_generator = ReportGenerator()
        self.prozorro_service = ProZorroService(settings)
        self.geocoding_service = GeocodingService(settings)
        self.logging_service = LoggingService()
        self.places_service = PlacesService(settings)
        self.temp_exports_repo = AgentTempExportsRepository()
        self.export_daily_count_repo = ExportDailyCountRepository()
        self.pending_export_repo = PendingExportRepository()
        self.artifact_service = ArtifactService()

        # Ініціалізуємо підключення до БД
        MongoDBConnection.initialize(settings)
        
        # Ініціалізуємо LLM: базовий (summarize, fallback) та асистент (thinking + grounding)
        self.llm = self._create_llm()
        self.llm_assistant = self._create_assistant_llm() if self.settings.llm_assistant_provider.lower() == 'gemini' else self.llm
        
        # Створюємо tools
        self.tools = self._create_tools()
        
        # Ініціалізуємо сервіс метаданих
        from business.services.app_metadata_service import AppMetadataService
        self.metadata_service = AppMetadataService(settings)
        
        # Створюємо систему промптів
        self.system_prompt = self._get_system_prompt()
        
        # Історія розмови (лише для поточного запиту в межах одного виклику process_query)
        self.conversation_history: List[Any] = []
        
        # Пам'ять користувачів: буфер + саммарі + опційно векторний пошук (ConversationBufferMemory, ConversationSummaryMemory, VectorStoreRetrieverMemory)
        self._user_memories: Dict[str, UserConversationMemory] = {}
        
        # Опційно: модель для embeddings (для векторного пошуку релевантних фрагментів)
        self._embed_fn: Optional[Callable[[str], List[float]]] = None

        # Correlation ID поточного запиту (встановлюється в process_query або ззовні при multi-agent run_tool)
        self._current_request_id: Optional[str] = None
        # User ID поточного запиту (для лімітів експорту та артефактів)
        self._current_user_id: Optional[str] = None
        # Chat ID поточного діалогу (Mini App)
        self._current_chat_id: Optional[str] = None
        # Контекст оголошення для маршруту listing_detail (get_listing_details використовує його, якщо не передано source/source_id)
        self._current_listing_context: Optional[Dict[str, Any]] = None
        if EMBEDDINGS_AVAILABLE and self.settings.llm_assistant_provider.lower() == 'gemini':
            api_key = self.settings.llm_api_keys.get('gemini', '')
            if api_key:
                try:
                    _emb = GoogleGenerativeAIEmbeddings(
                        model="models/gemini-embedding-001",
                        google_api_key=api_key
                    )
                    self._embed_fn = _emb.embed_query
                except Exception as e:
                    logger.info("Embeddings не ініціалізовано: %s", e)
        
        # Флаг для відстеження, чи вже згенеровано Excel файл для поточного запиту
        self.excel_generated = False
        # Метрики останнього запиту (для observability): iterations, duration_seconds, tool_failures_count, tool_recovery_attempted
        self._last_request_metrics: Dict[str, Any] = {}
    
    def _create_llm(self):
        """Створює базовий LLM (без thinking) для summarize, fallback та інших випадків."""
        return self._create_llm_internal(use_assistant_config=False)

    def _create_assistant_llm(self):
        """Створює LLM для AI-асистента з Thinking mode (лише Gemini). Google Search — через bind_tools."""
        return self._create_llm_internal(use_assistant_config=True)

    def _create_llm_internal(self, use_assistant_config: bool = False):
        """Створює LLM для асистента. use_assistant_config=True — thinking_budget + include_thoughts (лише Gemini)."""
        provider_name = self.settings.llm_assistant_provider.lower()
        api_key = self.settings.llm_api_keys.get(provider_name, '')

        # Ollama — локальна LLM, API ключ не потрібен
        if provider_name == 'ollama':
            if not CHAT_OLLAMA_AVAILABLE or ChatOllama is None:
                raise ImportError(
                    "Для використання Ollama встановіть: pip install langchain-ollama або langchain-community"
                )
            model_name = getattr(self.settings, 'llm_assistant_model_name', 'gemma3:27b')
            temperature = getattr(self.settings, 'llm_agent_temperature', 0.7)
            max_tokens = getattr(self.settings, 'llm_agent_max_output_tokens', 8192)
            return ChatOllama(
                model=model_name,
                temperature=temperature,
                num_predict=max_tokens,
            )

        if not api_key:
            raise ValueError(f"API ключ для провайдера {provider_name} не вказано")

        model_name = getattr(self.settings, 'llm_assistant_model_name', 'gemini-2.5-flash')
        temperature = getattr(self.settings, 'llm_agent_temperature', 0.7)
        max_tokens = getattr(self.settings, 'llm_agent_max_output_tokens', 8192)
        
        if provider_name == 'gemini':
            kwargs = dict(
                model=model_name,
                google_api_key=api_key,
                temperature=temperature,
                max_output_tokens=max_tokens,
            )
            if use_assistant_config:
                thinking_budget = getattr(self.settings, 'llm_agent_thinking_budget', 0)
                include_thoughts = getattr(self.settings, 'llm_agent_include_thoughts', False)
                if thinking_budget > 0:
                    kwargs['thinking_budget'] = thinking_budget
                    kwargs['include_thoughts'] = include_thoughts
            return ChatGoogleGenerativeAI(**kwargs)
        elif provider_name == 'openai':
            return ChatOpenAI(
                model=model_name,
                api_key=api_key,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        elif provider_name == 'anthropic':
            return ChatAnthropic(
                model=model_name,
                api_key=api_key,
                temperature=temperature,
                max_tokens=max_tokens,
            )
        else:
            raise ValueError(f"Невідомий провайдер LLM: {provider_name}")
    
    def _get_or_create_memory(self, user_id: str) -> UserConversationMemory:
        """Повертає пам'ять розмови для користувача (буфер + саммарі + векторний пошук)."""
        if user_id not in self._user_memories:
            self._user_memories[user_id] = UserConversationMemory()
        return self._user_memories[user_id]

    def _build_request_metrics(
        self,
        iteration: int,
        start_time: float,
        time_budget_exceeded: bool = False,
    ) -> Dict[str, Any]:
        """Формує словник метрик запиту для observability (recovery metrics включено)."""
        m = {
            "iterations": iteration,
            "duration_seconds": time.perf_counter() - start_time,
            "tool_failures_count": getattr(self, "_tool_failures_this_request", 0),
            "tool_recovery_attempted": getattr(self, "_had_tool_failure_before_success", False),
        }
        if time_budget_exceeded:
            m["time_budget_exceeded"] = True
        return m

    def _summarize_exchanges(self, exchanges: List[Tuple[str, str]]) -> str:
        """Саммарізує список обмінів (ConversationSummaryMemory-стиль) через LLM."""
        if not exchanges:
            return ""
        text = "\n\n".join(
            f"Користувач: {h}\nАсистент: {a}" for h, a in exchanges
        )
        prompt = (
            "Summarize this dialogue fragment in 2-4 sentences. Output must be in Ukrainian. "
            "Preserve key facts and requests:\n\n" + text[:4000]
        )
        try:
            messages = [HumanMessage(content=prompt)]
            resp = self.llm.invoke(messages)
            try:
                usage = _extract_usage_from_aimessage(resp)
                meta = {"prompt_preview": prompt[:100] + ("..." if len(prompt) > 100 else "")}
                if usage:
                    meta["input_tokens"] = usage.get("input_tokens", 0)
                    meta["output_tokens"] = usage.get("output_tokens", 0)
                self.logging_service.log_api_usage(
                    service="llm",
                    source="langchain_agent_summarize",
                    from_cache=False,
                    metadata=meta,
                )
                self.logging_service.log_llm_exchange(
                    request_text=_messages_to_request_text(messages),
                    response_text=_aimessage_to_response_text(resp),
                    input_tokens=meta.get("input_tokens", 0),
                    output_tokens=meta.get("output_tokens", 0),
                    source="langchain_agent_summarize",
                    provider=(getattr(self.settings, "llm_assistant_provider", None) or "gemini"),
                )
            except Exception as e:
                logger.warning("Не вдалося записати llm_exchange (summarize): %s", e)
            content = getattr(resp, "content", None) or ""
            return content.strip() if isinstance(content, str) else ""
        except Exception as e:
            logger.warning("Помилка саммарізації історії: %s", e)
            return ""

    def _save_response_to_memory(
        self,
        user_id: Optional[str],
        user_query: str,
        response: str,
        chat_id: Optional[str] = None,
    ) -> None:
        """Зберігає пару (запит, відповідь) у пам'ять: in-memory буфер та опційно ChatSessionRepository."""
        if not user_id:
            return
        memory_key = f"{user_id}:{chat_id}" if (user_id and chat_id) else user_id
        memory = self._get_or_create_memory(memory_key)
        memory.add_exchange(user_query, response)
        memory.trim_and_summarize(self._summarize_exchanges)
        exchange_text = f"Користувач: {user_query}\nАсистент: {response}"
        memory.add_to_vector_store(exchange_text[:3000], self._embed_fn)
        if user_id and chat_id:
            try:
                from data.repositories.chat_session_repository import ChatSessionRepository
                chat_repo = ChatSessionRepository()
                chat_repo.append_message(str(user_id), chat_id, "user", user_query)
                chat_repo.append_message(str(user_id), chat_id, "assistant", response)
            except Exception as e:
                logger.warning("chat session save: %s", e)

    def _load_glossary(self) -> str:
        """Завантажує глосарій розробника з config або docs/."""
        try:
            from config.config_loader import get_config_loader
            return get_config_loader().get_glossary()
        except Exception:
            return ""
    
    def _get_system_prompt(self) -> str:
        """Формує системний промпт з урахуванням архітектурних правил та трьох категорій інструментів."""
        # Додаємо метаданні застосунку
        metadata_text = self.metadata_service.get_metadata_for_llm(max_length=3000)
        
        # Завантажуємо base промпт з prompts.yaml (fallback на дефолт з коду)
        from config.config_loader import get_config_loader
        loader = get_config_loader()
        base_from_config = loader.get_prompt("langchain_system")
        if base_from_config:
            base_prompt = metadata_text + "\n\n" + base_from_config.strip()
        else:
            # Fallback — hardcoded prompt (legacy). Instructions in English; answer and all text output in Ukrainian only.
            base_prompt = metadata_text + """

---
You are a data analysis assistant for listings (ProZorro auctions, OLX). You work in the app top layer: all data operations go through tools (MCP); no direct DB/API/filesystem access. Use only the tools provided. You must answer the user and any text output in Ukrainian only.

---
## ROLE AND ARCHITECTURE CONTEXT

**Who handles the request:** SecurityAgent first. If the user explicitly asks for a "report for day/week" or "export for period", the multi-agent pipeline runs (planner → analyst). All other requests go to you (LangChain agent).

**Your role:** You receive free-form requests. Build a plan from available tools (get_collection_info, get_distinct_values, execute_query, execute_aggregation, execute_analytics, etc.) and respond. For «найдорожчу/найдешевшу нерухомість», «топ за ціною», «по регіону/області» завжди враховуй обидва джерела даних, якщо користувач не вказав одне: викликай execute_query або execute_aggregation для prozorro_auctions і окремо для olx_listings (з відповідними фільтрами за регіоном/містом через addresses). Результати оформлюй у відповіді двома блоками: «За даними ProZorro …» та «За даними OLX …» — у кожному вкажи найвищу ціну (абсолютну), за можливості ціну за м², та коротко об’єкт/лот. Якщо в одному з джерел немає відповідних записів — явно напиши «немає відповідних лотів/оголошень», але все одно покажи другий блок. Планувальник і аналітик викликаються системою лише для явних звітів/експортів за період; Present two blocks in Ukrainian: \"За даними ProZorro …\" and \"За даними OLX …\". If one source has no data, say so and still show the other.

**Approach:** Understand the request → clarify if needed → use tools and data (schema, collections, metrics) → respond in Ukrainian. For selection/aggregation use tools and support the answer with numbers. For file/export use save_query_to_temp_collection and export_from_temp_collection; otherwise give a short text answer with numbers.

---
**How to work:** Understand the request from dialogue context. Clarify with the user if needed. Build a tool plan and execute it or explain limitations. Answer only in Ukrainian.


---
## 1. DATA UPDATE (triggering imports)

**trigger_data_update** — initiates data refresh (with parameters).
- source="olx": update OLX listings (non-residential, land). Optional param days: 1 or 7.
- source="prozorro": reminder that ProZorro updates run via main pipeline/Telegram.
Call when the user asks to update or reload data.

---
## 2. SELECTION, AGGREGATION AND ANALYTICS

**Schema and context:** get_database_schema, get_collection_info(collection_name), get_data_dictionary — explore structure. Collections: prozorro_auctions, llm_cache, olx_listings.

**analytics_extracts (priority for avg price per m² and geo-aggregations):** For "average price per m²" always use **analytics_extracts_aggregate**. Collection has price_per_m2_uah in UAH. Examples: (1) avg price commercial in Kyiv area >500 m² → metric=price_per_m2_uah, aggregation=avg, filters city "Київ", property_type "Комерційна нерухомість", building_area_sqm $gt 500. (2) Kyiv district with highest avg price per m² → group_by=city_district, filters region "Київська", city "Київ". (3) Solomianskyi district → city_district regex. Kyiv districts may have apostrophe (Солом'янський). Operators: eq, gt, gte, lt, lte, in, regex. Do not use execute_query/execute_aggregation for avg price per m² — prices there may be in USD.

**Metrics and filters:** execute_analytics — aggregated metrics. Default collection: prozorro_auctions. Use list_metrics or custom metric { name, formula, aggregation }; formula may use auction_data.*, llm_result.result.* (+, -, *, /). Filter by status, region, city, property_type, building_area_sqm, land_area_ha, auction_data.dateModified. Region without "область" (e.g. Київська, Львівська). City and region: $or. For "by days" reports use groupBy: ["date"]. By region: ProZorro execute_analytics groupBy: ["region"]; OLX only groupBy: ["date"], for region use execute_aggregation (OLX region fields: detail.resolved_locations, search_data.location). If ProZorro returns 0, try execute_analytics collection "olx_listings" with same filters.

**Critical — address search (region/city):** Use **unified_listings** and **addresses**. addresses[].region, addresses[].settlement.
- unified_listings: $elemMatch on region or settlement (e.g. "Київська", "Київ" — region without "область").
- "Kyiv and oblast": $or on settlement "Київ" and region "Київська".
- Before filtering call get_distinct_values for exact values.

**ProZorro — nested entities (mandatory):**
- bids and bidders: auction_data.numberOfBids does not exist. Bids are in auction_data.bids[]. Each bid has bids[].bidders[]; unique participant ID is bids[].bidders[].identifier.id. Number of bids: $addFields bids_count = $size($ifNull(auction_data.bids, [])). Unique participants: collect identifier.id from all bids[].bidders[], then $setUnion or distinct count.
- Financial: starting price auction_data.value.amount (UAH); final price auction_data.contracts[].value.amount or awards[].value.amount. Use auction_data.value.amount for analytics/sorting; not bids[].value.amount.
- items: areas in items[].quantity.value (m² or ha). Lot address items[].address; classification items[].classification.id. For region aggregations use $unwind on items and filter by items.address.

**Queries and aggregations:** execute_query — simple queries (default limit 100; for "all for period" export always pass limit 100). execute_aggregation — pipeline (групування, $unwind, $lookup, $match, $project, $limit). Для OLX використовуй execute_query/execute_aggregation з collection "olx_listings" (поля: url, search_data.*, detail.*, updated_at). get_allowed_collections — список дозволених колекцій. Для запитів типу «найдорожча/найдешевша нерухомість по області/регіону» або «топ за ціною» виконуй окремі виклики для prozorro_auctions та olx_listings і у відповіді завжди дай два блоки: «За даними ProZorro …» та «За даними OLX …» (ціна, за можливостю ціна за м², коротко об’єкт). **ProZorro — кількість учасників:** поля auction_data.numberOfBids у документах НЕМАЄ. Для «топ-N аукціонів за кількістю зареєстрованих учасників» використовуй pipeline: [{\"$addFields\": {\"bids_count\": {\"$size\": {\"$ifNull\": [\"$auction_data.bids\", []]}}}}, {\"$match\": {\"bids_count\": {\"$gt\": 0}}}, {\"$sort\": {\"bids_count\": -1}}, {\"$limit\": N}, {\"$project\": {\"auction_id\": 1, \"bids_count\": 1, \"auction_data.title\": 1, \"auction_data.value\": 1, \"_id\": 0}}]. **get_distinct_values(collection_name, field_path, unwrap_array=False)** — унікальні значення поля; для полів-масивів (наприклад detail.llm.tags) передай unwrap_array=True. **Перед фільтрацією за регіоном/локацією/текстовим полем обов'язково викликай get_distinct_values**; перед фільтром за тегами (крамниця, аптека, газ, вода тощо) — get_distinct_values(olx_listings, detail.llm.tags, unwrap_array=True), потім $match з "detail.llm.tags": {"$in": ["тег1", "тег2"]}. Теги в OLX зберігаються в detail.llm.tags (призначення та комунікації з парсингу опису).

**Dates in MongoDB:** In prozorro_auctions auction_data.dateModified/dateCreated are ISO 8601 strings; use string comparison in $match. Do not use $date. In olx_listings updated_at is BSON Date; pass ISO strings in $match, server converts.

**Security:** $regex in query-builder is forbidden. Use execute_analytics for status/region filtering for ProZorro.

**Geocoding:** geocode_address(address, region="ua") — address or toponym to coordinates and formatted_address (response in Ukrainian); result cached.

**Places:** search_nearby_places(lat, lon, place_types, radius_meters=500) — pharmacy, hospital, bus_station, etc. Use for suitability of premises: geocode_address then search_nearby_places.

**Unified search:** For search across OLX/ProZorro, "generate filters", "search string by region/city": use generate_search_filter_string (region, city, source, property_type, price_min, price_max, etc.) and search_unified_listings (filter_string or flat params: limit, skip, sort_field, sort_order).

---
## 3. SAVING DATA TO FILE AND SENDING TO USER

**Export to file:** Do not load large query results into chat. Use save_query_to_temp_collection(query) then export_from_temp_collection(temp_collection_id, format=xlsx, filename_prefix). Supported: prozorro_auctions, olx_listings. For "listings for period" run for each collection (two files). export_listings_to_file when you already have a list of ids. save_query_results_to_excel for existing data/results. generate_report from analytics-mcp or query-builder-mcp; list_templates for templates. If the user asks for a period report or export to file, use save_query_to_temp_collection and export_from_temp_collection; otherwise a short text answer with numbers in Ukrainian is enough.

---
## DIALOGUE, CONTEXT AND GLOSSARY

- Dialogue context is stored via memory services (buffer, summary, relevant fragments). Use it for follow-up and "reply to specific message". Project glossary is provided separately — use terms consistently.

---
## GENERAL RULES

- In JSON use "null", not None. Allowed aggregation stages: $match, $project, $group, $unwind, $sort, $limit, $lookup, $addFields; forbidden: $out, $merge.
- Do not invent fields or collections; if data is missing, say so explicitly. Answer the user only in Ukrainian.

**Links and lists:**
- When the user asks for links, only provide URLs that exist. Do not promise "N listings" if you have fewer.
- Each link must be a full URL (https://...). No placeholders; either provide the URL or omit the item.
- Do not use markdown links [text](url). Write plain URL after the description. Do not duplicate the same URL. Format: number, short description (address/title), URL."""
        
        glossary = self._load_glossary()
        if glossary:
            base_prompt += """

## Project Terminology (Developer Glossary)

Use this terminology when responding and working with data:

""" + glossary + """

Important: Use terms from the glossary correctly."""
        
        return base_prompt
    
    def _create_tools(self) -> List[Any]:  # type: ignore
        """Створює LangChain Tools для всіх MCP-серверів."""
        tools = []
        
        # Schema MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._get_database_schema,
                name="get_database_schema",
                description="[Схема] Повна схема метаданих колекцій БД.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_collection_info,
                name="get_collection_info",
                description="[Схема] Інформація про колекцію: prozorro_auctions, llm_cache, olx_listings.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_data_dictionary,
                name="get_data_dictionary",
                description="[Схема] Data Dictionary — опис колекцій та полів.",
                return_direct=False
            )
        ])
        
        # Query Builder MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._execute_query,
                name="execute_query",
                description="[Вибірка] Безпечний запит: collection, filters, projection, limit (за замовчуванням 100). Для експорту «усіх» за період передавай limit: 100. $regex заборонено. Для пошуку за адресами використовуй unified_listings: {\"addresses\": {\"$elemMatch\": {\"region\": \"Київська\"}}} або {\"settlement\": \"Київ\"}.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._execute_aggregation,
                name="execute_aggregation",
                description="[Вибірка] Aggregation pipeline: $match, $addFields, $project, $group, $unwind, $sort, $limit. collection_name, pipeline, limit. Для prozorro_auctions: поле auction_data.numberOfBids НЕ існує — для кількості учасників використовуй $addFields з bids_count: {$size: {$ifNull: [\"$auction_data.bids\", []]}}, потім $sort по bids_count (-1).",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_allowed_collections,
                name="get_allowed_collections",
                description="[Вибірка] Список дозволених колекцій з коротким описом полів (для olx_listings — url, search_data, detail.llm, updated_at). Повна схема — get_collection_info(collection_name).",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_distinct_values,
                name="get_distinct_values",
                description="[Вибірка] Унікальні значення поля в колекції — для аналізу перед фільтрацією. Параметри: collection_name, field_path (наприклад search_data.location для OLX; для тегів — detail.llm.tags), опційно unwrap_array=True для полів-масивів (теги). Повертає values (список). Перед фільтром за регіоном/локацією завжди викликай get_distinct_values; перед фільтром за тегами (крамниця, аптека, газ, вода тощо) — get_distinct_values з field_path=detail.llm.tags та unwrap_array=True, потім $match з \"detail.llm.tags\": {\"$in\": [обраний тег/теги]}.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._save_query_to_temp_collection,
                name="save_query_to_temp_collection",
                description="[Вибірка→Експорт] Виконує запит та зберігає результати в тимчасову вибірку. Повертає temp_collection_id. Далі викликай export_from_temp_collection(temp_collection_id). Колекції: unified_listings, prozorro_auctions, olx_listings. Якщо користувач НЕ вказав явно ProZorro чи OLX — використовуй unified_listings (зведена таблиця, один файл). Для «оголошення за період» з явним джерелом — prozorro_auctions та/або olx_listings. Може приймати (1) collection + filters + limit, або (2) collection + aggregation_pipeline + limit.",
                return_direct=False
            )
        ])
        
        # Analytics MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._execute_analytics,
                name="execute_analytics",
                description="[Метрики] Агреговані метрики. collection: prozorro_auctions (за замовч.) або olx_listings. ProZorro: metric з list_metrics або кастомна {{ name, formula, aggregation (опціонально) }}; formula — auction_data.*, llm_result.result.*. Для ціни за м² по регіону (Київ, область): якщо ProZorro повернув 0 — викликай з collection: 'olx_listings', ті самі filters (auction_data.dateModified $gte/$lte, $or [region, city]) та groupBy: ['date']. OLX: лише metric average_price_per_m2, groupBy: ['date'], фільтри за датою та регіоном/містом.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._list_metrics,
                name="list_metrics",
                description="[Метрики] Список доступних метрик аналітики.",
                return_direct=False
            )
        ])

        # Analytics Extracts tools (колекція виокремлених даних для швидких агрегацій)
        tools.extend([
            StructuredTool.from_function(
                func=self._analytics_extracts_aggregate,
                name="analytics_extracts_aggregate",
                description="""[Аналітика extracts] Агрегація по метриці. Ціни вже в UAH (price_per_m2_uah). Для «середня ціна за м² комерційної нерухомості в Києві з площею понад 500 м²»: metric=price_per_m2_uah, aggregation=avg, filters={city: 'Київ', property_type: 'Комерційна нерухомість', building_area_sqm: {$gt: 500}}. Для «район міста з найвищою ціною»: group_by=[city_district], filters={region: 'Київська', city: 'Київ'}. Оператори: eq, gt, gte, lt, lte, in. filters — dict або список [{\"field\": \"building_area_sqm\", \"operator\": \"gt\", \"value\": 500}].""",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._analytics_extracts_search,
                name="analytics_extracts_search",
                description="[Аналітика extracts] Пошук з логічними умовами. filters — простий dict або список умов з $and/$or/$not. sort: [{\"field\": \"price_per_m2_uah\", \"order\": -1}].",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._analytics_extracts_list_metrics,
                name="analytics_extracts_list_metrics",
                description="[Аналітика extracts] Список метрик для агрегації: price_per_m2_uah, price_per_ha_uah, building_area_sqm, land_area_sqm, price_uah тощо.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._analytics_extracts_list_dimensions,
                name="analytics_extracts_list_dimensions",
                description="[Аналітика extracts] Список полів для групування: region, city, city_district, oblast_raion, settlement, street, property_type, source тощо.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._analytics_extracts_get_distinct,
                name="analytics_extracts_get_distinct",
                description="[Аналітика extracts] Унікальні значення поля (напр. city_district для Києва). Викликай перед фільтрацією.",
                return_direct=False
            )
        ])
        
        # Report MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._generate_report,
                name="generate_report",
                description="[Звіт] Звіт з джерела dataSource (analytics-mcp або query-builder-mcp), format, columns.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._list_templates,
                name="list_templates",
                description="[Звіт] Список шаблонів звітів.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._save_query_results_to_excel,
                name="save_query_results_to_excel",
                description="[Експорт] Зберегти вже отримані results (data/results) у Excel з указаними columns та column_headers. Для довільної структури таблиці.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._export_listings_to_file,
                name="export_listings_to_file",
                description="[Експорт] Excel за списком ids (коли вже є ids). ids, collection, format=xlsx, filename_prefix. Для експорту результатів запиту краще: save_query_to_temp_collection → export_from_temp_collection.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._export_from_temp_collection,
                name="export_from_temp_collection",
                description="[Експорт] Експорт у файл з тимчасової вибірки. Викликай після save_query_to_temp_collection. Параметри: temp_collection_id (обов'язково), format=xlsx, filename_prefix (релевантний запиту, напр. mista_kyiv, zvit_za_tyzhden).",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._generate_search_filter_string,
                name="generate_search_filter_string",
                description="[Пошук] Генерує рядок фільтрів для зведеного пошуку (unified_listings) з структурованих параметрів. Параметри: region, city, source (olx/prozorro), property_type, price_min, price_max, date_filter_days (1/7/30), title_contains, description_contains. Гео: geo('Область' INSIDE 'Київська'). Повертає filter_string для вставки на сторінку пошуку або для search_unified_listings.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._search_unified_listings,
                name="search_unified_listings",
                description="[Пошук] Пошук по зведеній таблиці (OLX + ProZorro). Або передай filter_string (рядок фільтрів), або параметри: region, city, source, property_type, price_min, price_max, date_filter_days, title_contains, description_contains. limit (за замовч. 50), skip, sort_field (source_updated_at/price/title), sort_order (asc/desc). Повертає items, total та згенерований filter_string (якщо передані параметри).",
                return_direct=False
            )
        ])
        
        # Data update tool (ініціювання оновлення даних у БД)
        tools.extend([
            StructuredTool.from_function(
                func=self._trigger_data_update,
                name="trigger_data_update",
                description="[Оновлення] Ініціює оновлення даних: source=olx|prozorro|all, days=1|7. Опційно regions (список областей), listing_types (типи оголошень OLX) для точкового оновлення.",
                return_direct=False
            )
        ])
        
        # Geocoding MCP tool
        tools.extend([
            StructuredTool.from_function(
                func=self._geocode_address,
                name="geocode_address",
                description="[Контекст] Адреса/топонім → координати, formatted_address, place_id. region='ua'. Кеш.",
                return_direct=False
            )
        ])
        # Деталі оголошення (зведені + сирі) для аналізу локації/опису — використовуй для визначення місцезнаходження з тексту
        tools.extend([
            StructuredTool.from_function(
                func=self._get_listing_details,
                name="get_listing_details",
                description="[Контекст оголошення] Повертає повні дані оголошення (зведені + сирі): опис, location.raw, llm.addresses, регіон/місто. Викликай без аргументів, якщо розмова про поточне оголошення; або передай source (olx/prozorro) та source_id (url або auction_id). Використовуй для розбору тексту та витягування топонімів, потім geocode_address для визначення координат.",
                return_direct=False
            )
        ])
        
        # Places API tool (пошук місць поблизу для гео-аналізу)
        tools.extend([
            StructuredTool.from_function(
                func=self._search_nearby_places,
                name="search_nearby_places",
                description="[Гео-аналіз] Пошук місць поблизу координат. place_types: pharmacy, hospital, bus_station, transit_station, restaurant, cafe, supermarket, school, apartment_building тощо. radius_meters (за замовч. 500). Використовуй після geocode_address для оцінки оточення.",
                return_direct=False
            )
        ])
        
        return tools

    def get_tools_for_route(self, route: Optional[str] = None) -> List[Any]:
        """
        Повертає підмножину інструментів для маршруту (cost control, less chaos).
        route: free_form (усі інструменти, включно з execute_query та execute_aggregation), query_export, analytics, geo, geo_assessment, listing_detail або None (= free_form).
        """
        if not route or route not in TOOL_ROUTES:
            route = "free_form"
        allowed = TOOL_ROUTES.get(route)
        if allowed is None:
            return list(self.tools)
        name_to_tool = {t.name: t for t in self.tools}
        return [name_to_tool[n] for n in allowed if n in name_to_tool]

    # Schema MCP tool implementations
    def _get_database_schema(self) -> Dict[str, Any]:
        """Отримує повну схему метаданих всіх колекцій."""
        logger.info("🔍 [Schema MCP] Виклик get_database_schema")
        try:
            schema = self.data_dictionary.to_schema_dict()
            schema['generated_at'] = datetime.utcnow().isoformat()
            logger.info("✓ [Schema MCP] Схема отримана успішно")
            return {'success': True, 'schema': schema}
        except Exception as e:
            logger.error(f"✗ [Schema MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """Отримує детальну інформацію про колекцію."""
        logger.info(f"🔍 [Schema MCP] Виклик get_collection_info для {collection_name}")
        try:
            collection = self.data_dictionary.get_collection(collection_name)
            if collection:
                result = {
                    'success': True,
                    'collection': {
                        'collection_name': collection.mongo_collection,
                        'description': collection.description,
                        'schema': self.data_dictionary.to_schema_dict()['collections'][collection_name]['schema'],
                        'indexes': collection.indexes,
                        'relationships': collection.relationships
                    }
                }
                logger.info(f"✓ [Schema MCP] Інформація про колекцію {collection_name} отримана")
                return result
            logger.warning(f"⚠ [Schema MCP] Колекція {collection_name} не знайдена")
            return {'success': False, 'error': f'Колекція {collection_name} не знайдена'}
        except Exception as e:
            logger.error(f"✗ [Schema MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_data_dictionary(self) -> Dict[str, Any]:
        """Отримує повний Data Dictionary."""
        logger.info("🔍 [Schema MCP] Виклик get_data_dictionary")
        try:
            result = {
                'success': True,
                'data_dictionary': self.data_dictionary.to_schema_dict(),
                'metadata': self.data_dictionary.get_metadata()
            }
            logger.info("✓ [Schema MCP] Data Dictionary отримано")
            return result
        except Exception as e:
            logger.error(f"✗ [Schema MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    # Query Builder MCP tool implementations
    def _execute_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Виконує безпечний запит до MongoDB."""
        logger.info(f"🔍 [Query Builder MCP] Виклик execute_query")
        logger.debug(f"Запит: {json.dumps(query, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            # Парсимо addFields та sort якщо вони передані як string
            if 'addFields' in query and isinstance(query['addFields'], str):
                try:
                    add_fields_str = query['addFields'].strip()
                    # Замінюємо Python None на JSON null перед парсингом
                    add_fields_str = re.sub(r'\bNone\b', 'null', add_fields_str)
                    query['addFields'] = json.loads(add_fields_str)
                except json.JSONDecodeError as e:
                    logger.error(f"✗ [Query Builder MCP] Помилка парсингу addFields: {e}")
                    return {'success': False, 'error': f'Помилка парсингу addFields: {str(e)}. Переконайтеся, що використовується null замість None.'}
            
            if 'sort' in query and isinstance(query['sort'], str):
                try:
                    sort_str = query['sort'].strip()
                    # Замінюємо Python None на JSON null перед парсингом
                    sort_str = re.sub(r'\bNone\b', 'null', sort_str)
                    query['sort'] = json.loads(sort_str)
                except json.JSONDecodeError as e:
                    logger.error(f"✗ [Query Builder MCP] Помилка парсингу sort: {e}")
                    return {'success': False, 'error': f'Помилка парсингу sort: {str(e)}. Переконайтеся, що використовується null замість None.'}
            
            result = self.query_builder.execute_query(query)
            if result.get('success'):
                logger.info(f"✓ [Query Builder MCP] Запит виконано успішно. Знайдено записів: {len(result.get('results', []))}")
            else:
                logger.error(f"✗ [Query Builder MCP] Помилка виконання: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Query Builder MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _execute_aggregation(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]],
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Виконує MongoDB aggregation pipeline."""
        logger.info(f"🔍 [Query Builder MCP] Виклик execute_aggregation для колекції {collection_name}")
        logger.debug(f"Pipeline: {json.dumps(pipeline, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            result = self.query_builder.execute_aggregation(
                collection_name=collection_name,
                pipeline=pipeline,
                limit=limit
            )
            if result.get('success'):
                logger.info(f"✓ [Query Builder MCP] Aggregation виконано успішно. Знайдено записів: {len(result.get('results', []))}")
            else:
                logger.error(f"✗ [Query Builder MCP] Помилка виконання: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Query Builder MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_allowed_collections(self) -> Dict[str, Any]:
        """Отримує список дозволених колекцій та короткий опис полів (для OLX — структура та ідентифікатор url)."""
        logger.info("🔍 [Query Builder MCP] Виклик get_allowed_collections")
        try:
            descriptions = {
                "prozorro_auctions": "Аукціони ProZorro.Sale. Поля: auction_id, auction_data (dateModified, dateCreated — рядки ISO 8601; value, status, items, bids — масив заявок). ВАЖЛИВО: поля auction_data.numberOfBids НЕ існує. Кількість зареєстрованих учасників обчислюй через $addFields: {\"bids_count\": {\"$size\": {\"$ifNull\": [\"$auction_data.bids\", []]}}}, потім $sort по bids_count (-1), $limit. Для фільтра за датою: auction_data.dateModified з $gte/$lte (рядки).",
                "olx_listings": "Оголошення OLX (нежитлова нерухомість, земля). Поля: url (ід для експорту), search_data (title, price, location, area_m2), detail (description, llm.property_type, llm.building_area_sqm, llm.land_area_ha, resolved_locations, parameters), updated_at (BSON Date — для фільтра за періодом $gte/$lte). Повна схема — get_collection_info('olx_listings').",
                "llm_cache": "Кеш результатів LLM-парсингу описів; description_hash, result (property_type, addresses, тощо).",
                "unified_listings": "Зведена таблиця оголошень, що об'єднує дані з OLX та ProZorro в єдину структуру. Поля: source (olx/prozorro), source_id, status, property_type, title, description, addresses (масив з region, settlement, coordinates, is_complete), cadastral_numbers, price_uah, price_usd, price_per_m2_uah/usd, price_per_ha_uah/usd, currency_rate, source_updated_at, system_updated_at. Фільтр за регіоном/містом: addresses з $elemMatch. Це основна колекція для пошуку — використовуй її замість olx_listings/prozorro_auctions для уніфікованих даних.",
                "listing_analytics": "LLM-аналітика оголошень (ціна за одиницю, місцезнаходження, оточення). Поля: source, source_id, analysis_text, analysis_at. Зв'язок з unified_listings через source+source_id.",
                "real_estate_objects": "Об'єкти нерухомого майна (ОНМ): land_plot, building, premises. Поля: type, area_sqm, cadastral_info, address, source_listing_ids. unified_listings.real_estate_refs посилається на _id.",
                "price_analytics": "Зведена аналітика цін: метрики за періодами, квартілі по містах.",
            }
            collections = [
                {"id": c, "description": descriptions.get(c, "")}
                for c in sorted(self.query_builder.ALLOWED_COLLECTIONS)
            ]
            result = {
                "success": True,
                "collections": collections,
                "max_results": self.query_builder.MAX_RESULTS,
            }
            logger.info(f"✓ [Query Builder MCP] Дозволені колекції: {[c['id'] for c in collections]}")
            return result
        except Exception as e:
            logger.error(f"✗ [Query Builder MCP] Помилка: {e}")
            return {"success": False, "error": str(e)}

    def _get_distinct_values(
        self,
        collection_name: str,
        field_path: str,
        limit: int = 300,
        unwrap_array: bool = False,
    ) -> Dict[str, Any]:
        """Повертає унікальні значення поля в колекції для аналізу перед фільтрацією. Для масивів (напр. detail.llm.tags) передай unwrap_array=True."""
        logger.info("🔍 [Query Builder MCP] Виклик get_distinct_values: %s, %s, unwrap_array=%s", collection_name, field_path, unwrap_array)
        try:
            result = self.query_builder.get_distinct_values(
                collection_name=collection_name,
                field_path=field_path,
                limit=limit,
                unwrap_array=unwrap_array,
            )
            if result.get("success"):
                logger.info("✓ [Query Builder MCP] Унікальних значень: %s", result.get("count", 0))
            return result
        except Exception as e:
            logger.exception("✗ [Query Builder MCP] get_distinct_values: %s", e)
            return {"success": False, "error": str(e), "values": []}
    
    def _save_query_to_temp_collection(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Виконує запит та зберігає результати в тимчасову вибірку. Повертає temp_collection_id.
        Далі викликай export_from_temp_collection(temp_collection_id). Підтримуються prozorro_auctions, olx_listings.
        Якщо передано aggregation_pipeline — виконується aggregation (фільтрація, сортування, addFields тощо);
        інакше — звичайний запит за filters та limit.
        """
        logger.info("🔍 [Query Builder MCP] Виклик save_query_to_temp_collection")
        try:
            inner = query.get("query", query) if isinstance(query, dict) else {}
            if not isinstance(inner, dict):
                inner = {}
            coll = (
                query.get("collection")
                or query.get("collection_name")
                or inner.get("collection")
                or inner.get("collection_name")
            )
            if not coll or coll not in ("prozorro_auctions", "olx_listings", "unified_listings"):
                return {"success": False, "error": "Поле 'collection' обов'язкове. Допустимі: unified_listings, prozorro_auctions, olx_listings."}

            limit_val = inner.get("limit") or query.get("limit")
            if limit_val is not None and isinstance(limit_val, int) and limit_val > 0:
                limit_val = min(limit_val, 5000)
            else:
                limit_val = 5000

            pipeline = inner.get("aggregation_pipeline") or query.get("aggregation_pipeline")
            if isinstance(pipeline, list) and len(pipeline) > 0:
                result = self.query_builder.execute_aggregation(
                    collection_name=coll,
                    pipeline=pipeline,
                    limit=limit_val,
                )
                if not result.get("success"):
                    return result
                results = result.get("results") or []
            else:
                filters = inner.get("query") if "query" in inner and isinstance(inner.get("query"), dict) else (inner.get("filters") or inner.get("filter") or {})
                if not isinstance(filters, dict):
                    filters = {}
                elif coll:
                    filters = {k: v for k, v in filters.items() if k not in ("project", "projection", "collection", "collection_name", "limit", "filter")}
                query_for_builder = {
                    "collection": coll,
                    "filters": filters,
                    "limit": limit_val,
                }
                if inner.get("join") and isinstance(inner["join"], list):
                    query_for_builder["join"] = inner["join"]
                proj = query.get("project") or query.get("projection") or inner.get("project") or inner.get("projection")
                if proj and isinstance(proj, list) and len(proj) > 0:
                    query_for_builder["projection"] = proj
                result = self.query_builder.execute_query(query_for_builder)
                if not result.get("success"):
                    return result
                results = result.get("results") or []

            batch_id = str(uuid.uuid4())
            count = self.temp_exports_repo.insert_batch(batch_id, coll, results)
            logger.info(f"✓ [Query Builder MCP] Збережено у тимчасову вибірку: temp_collection_id={batch_id}, count={count}")
            user_id = getattr(self, "_current_user_id", None)
            chat_id = getattr(self, "_current_chat_id", None)
            if user_id and chat_id:
                try:
                    from data.repositories.chat_session_repository import ChatSessionRepository
                    ChatSessionRepository().append_temp_collection(str(user_id), chat_id, batch_id, coll, count)
                except Exception as e:
                    logger.debug("chat session append_temp_collection: %s", e)
            return {
                "success": True,
                "temp_collection_id": batch_id,
                "count": count,
                "source_collection": coll,
            }
        except Exception as e:
            logger.exception(f"✗ [Query Builder MCP] save_query_to_temp_collection: {e}")
            return {"success": False, "error": str(e)}
    
    # Analytics MCP tool implementations
    def _execute_analytics(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Виконує аналітичний запит."""
        logger.info(f"🔍 [Analytics MCP] Виклик execute_analytics")
        logger.debug(f"Запит: {json.dumps(query, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            result = self.analytics_builder.execute_analytics_query(query)
            if result.get('success'):
                logger.info(f"✓ [Analytics MCP] Аналітика виконана успішно")
                if result.get('count', 0) == 0 and query.get('collection') != 'olx_listings':
                    metric = query.get('metric')
                    is_price_per_sqm = (
                        (isinstance(metric, str) and 'price' in metric.lower() and ('m2' in metric or 'sqm' in metric or 'м²' in metric))
                        or (isinstance(metric, dict) and ('building_area_sqm' in str(metric.get('formula', '')) or (metric.get('name') or '').lower().find('price') >= 0))
                    )
                    if is_price_per_sqm:
                        result = dict(result)
                        result['_agent_hint'] = (
                            "По ProZorro результатів не знайдено. Викликай execute_analytics ще раз з тими ж параметрами, "
                            "але додай collection: 'olx_listings' — аналітика ціни за м² по OLX підтримується (Київ, область, по днях)."
                        )
            else:
                logger.error(f"✗ [Analytics MCP] Помилка: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Analytics MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _list_metrics(self) -> Dict[str, Any]:
        """Отримує список доступних метрик."""
        logger.info("🔍 [Analytics MCP] Виклик list_metrics")
        try:
            from utils.analytics_metrics import AnalyticsMetrics
            metrics = AnalyticsMetrics.list_metrics()
            result = {
                'success': True,
                'metrics': metrics[:20] if len(metrics) > 20 else metrics,
                'total_count': len(metrics)
            }
            logger.info(f"✓ [Analytics MCP] Знайдено метрик: {len(metrics)}")
            return result
        except Exception as e:
            logger.error(f"✗ [Analytics MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}

    # Analytics Extracts tool implementations
    def _analytics_extracts_aggregate(
        self,
        metric: str,
        aggregation: str = "avg",
        group_by: Optional[List[str]] = None,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Агрегація по метриці з групуванням."""
        logger.info("🔍 [Analytics Extracts] Виклик analytics_extracts_aggregate: metric=%s, aggregation=%s", metric, aggregation)
        try:
            return self.analytics_extracts_service.aggregate_by_metric(
                metric=metric,
                aggregation=aggregation,
                group_by=group_by,
                filters=filters,
                limit=limit,
            )
        except Exception as e:
            logger.exception("✗ [Analytics Extracts] Помилка: %s", e)
            return {"success": False, "error": str(e)}

    def _analytics_extracts_search(
        self,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        fields: Optional[List[str]] = None,
        sort: Optional[List[Dict[str, Any]]] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> Dict[str, Any]:
        """Пошук з логічними умовами."""
        logger.info("🔍 [Analytics Extracts] Виклик analytics_extracts_search")
        try:
            return self.analytics_extracts_service.search(
                filters=filters,
                fields=fields,
                sort=sort,
                limit=limit,
                skip=skip,
            )
        except Exception as e:
            logger.exception("✗ [Analytics Extracts] Помилка: %s", e)
            return {"success": False, "error": str(e)}

    def _analytics_extracts_list_metrics(self) -> Dict[str, Any]:
        """Список метрик для агрегації."""
        try:
            metrics = self.analytics_extracts_service.get_available_metrics()
            return {"success": True, "metrics": metrics}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _analytics_extracts_list_dimensions(self) -> Dict[str, Any]:
        """Список полів для групування."""
        try:
            fields = self.analytics_extracts_service.get_group_by_fields()
            return {"success": True, "dimensions": fields}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _analytics_extracts_get_distinct(
        self,
        field: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Унікальні значення поля."""
        logger.info("🔍 [Analytics Extracts] Виклик analytics_extracts_get_distinct: field=%s", field)
        try:
            return self.analytics_extracts_service.get_distinct_values(field=field, filters=filters)
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    # Report MCP tool implementations
    def _generate_report(self, request: Dict[str, Any], return_base64: bool = True) -> Dict[str, Any]:
        """Генерує звіт."""
        logger.info(f"🔍 [Report MCP] Виклик generate_report")
        logger.debug(f"Запит: {json.dumps(request, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            result = self.report_generator.generate_report(request, return_base64=return_base64)
            if result.get('success'):
                logger.info(f"✓ [Report MCP] Звіт згенеровано успішно")
            else:
                logger.error(f"✗ [Report MCP] Помилка: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Report MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _list_templates(self) -> Dict[str, Any]:
        """Отримує список доступних шаблонів."""
        logger.info("🔍 [Report MCP] Виклик list_templates")
        try:
            from utils.report_templates import ReportTemplates
            templates = ReportTemplates.list_templates()
            result = {'success': True, 'templates': templates}
            logger.info(f"✓ [Report MCP] Знайдено шаблонів: {len(templates)}")
            return result
        except Exception as e:
            logger.error(f"✗ [Report MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}

    def _generate_search_filter_string(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Генерує рядок фільтрів для зведеного пошуку з структурованих параметрів.
        Викликається з одним dict від LLM (kwargs).
        """
        from domain.services.unified_search_service import filter_string_from_flat_params
        logger.info("🔍 [Search] Виклик generate_search_filter_string")
        try:
            # Нормалізація: якщо передано один dict (наприклад params)
            params = dict(kwargs)
            if len(params) == 1 and isinstance(next(iter(params.values())), dict):
                params = next(iter(params.values()))
            s = filter_string_from_flat_params(**params)
            return {"success": True, "filter_string": s}
        except Exception as e:
            logger.exception("✗ [Search] generate_search_filter_string: %s", e)
            return {"success": False, "error": str(e)}

    def _search_unified_listings(
        self,
        filter_string: Optional[str] = None,
        limit: int = 50,
        skip: int = 0,
        sort_field: str = "source_updated_at",
        sort_order: str = "desc",
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Пошук по unified_listings: або за рядком фільтрів, або за структурованими параметрами.
        """
        from domain.services.unified_search_service import (
            find,
            find_by_filter_string,
            build_query_from_flat_params,
            filter_string_from_flat_params,
        )
        logger.info("🔍 [Search] Виклик search_unified_listings")
        try:
            params = dict(kwargs)
            if filter_string and str(filter_string).strip():
                docs, total, err = find_by_filter_string(
                    str(filter_string).strip(),
                    sort=[{"field": sort_field, "order": -1 if sort_order == "desc" else 1}],
                    limit=min(limit, 200),
                    skip=skip,
                )
                if err:
                    return {"success": False, "error": err}
                return {
                    "success": True,
                    "items": docs or [],
                    "total": total or 0,
                    "limit": limit,
                    "skip": skip,
                }
            # Інакше збираємо flat params (можливо один вкладений dict від LLM)
            if len(params) == 1 and isinstance(next(iter(params.values())), dict):
                params = next(iter(params.values()))
            query = build_query_from_flat_params(**params)
            sort_spec = [{"field": sort_field, "order": -1 if sort_order == "desc" else 1}]
            docs, total = find(
                filter_group=query.filters,
                geo_filter=query.geo_filters,
                sort=sort_spec,
                limit=min(limit, 200),
                skip=skip,
            )
            generated_string = filter_string_from_flat_params(**params)
            return {
                "success": True,
                "items": docs,
                "total": total,
                "limit": limit,
                "skip": skip,
                "filter_string": generated_string,
            }
        except Exception as e:
            logger.exception("✗ [Search] search_unified_listings: %s", e)
            return {"success": False, "error": str(e)}
    
    def _normalize_export_ids(
        self, ids: Any, collection: str
    ) -> List[str]:
        """
        Нормалізує ids до списку рядків.
        Якщо агент передав список об'єктів (наприклад [{"auction_id": "..."}] з результатів aggregation),
        витягує значення auction_id або url.
        """
        if not ids:
            return []
        result = []
        id_key = (
            "auction_id" if collection == "prozorro_auctions"
            else "url" if collection == "olx_listings"
            else "identifier"  # unified_listings: identifier або source:source_id
        )
        for item in ids:
            if isinstance(item, str) and item.strip():
                result.append(item.strip())
            elif isinstance(item, dict):
                if collection == "unified_listings":
                    src, sid = item.get("source"), item.get("source_id")
                    val = f"{src}:{sid}" if (src and sid) else item.get("_id")
                else:
                    val = item.get(id_key) or item.get("_id")
                if val is not None and str(val).strip():
                    result.append(str(val).strip())
        return result

    def _export_listings_to_file(
        self,
        ids: List[str],
        collection: str,
        format: str = "xlsx",
        columns: Optional[List[str]] = None,
        column_headers: Optional[Dict[str, str]] = None,
        filename_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Експортує оголошення/аукціони за списком ids у файл.
        Якщо columns не вказано — використовується стандартний формат (як у «файл за день/тиждень»).
        Повертає url, filename та file_base64 для відправки файлу в Telegram.
        """
        logger.info("🔍 [Export MCP] Виклик export_listings_to_file")
        try:
            ids = self._normalize_export_ids(ids, collection or "")
            if not ids and collection in ("prozorro_auctions", "olx_listings", "unified_listings"):
                logger.warning("[Export MCP] ids порожній після нормалізації — буде створено файл «Немає даних»")
            # Стандартний формат (як у формуванні файлу за день/тиждень)
            if format == "xlsx" and collection in ("prozorro_auctions", "olx_listings", "unified_listings") and columns is None:
                sheet = self.prozorro_service.get_standard_sheet_data_for_export(ids, collection)
                if sheet:
                    rows, fieldnames, headers = sheet
                    excel_io = generate_excel_in_memory(rows, fieldnames, headers)
                    from datetime import datetime as dt
                    from utils.date_utils import KYIV_TZ
                    ensure_directory_exists(str(Path(self.settings.temp_directory) / "exports"))
                    export_dir = Path(self.settings.temp_directory) / "exports"
                    ts = dt.now(KYIV_TZ).strftime("%Y%m%d_%H%M%S")
                    prefix = (filename_prefix or "export").strip()
                    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in prefix)
                    filename = f"{safe}_{ts}.xlsx"
                    file_path = export_dir / filename
                    file_path.write_bytes(excel_io.getvalue())
                    result = {
                        "success": True,
                        "url": str(file_path),
                        "filename": filename,
                        "rows_count": len(rows),
                        "columns_count": len(fieldnames),
                        "format": "xlsx",
                    }
                    result["file_base64"] = base64.b64encode(excel_io.getvalue()).decode("ascii")
                    logger.info(f"✓ [Export MCP] Стандартний формат: {result.get('url')}, рядків: {len(rows)}")
                    return result
            result = self.report_generator.export_data_service.export_to_file(
                ids=ids,
                collection=collection,
                file_format=format,
                fields=columns,
                column_headers=column_headers,
                filename_prefix=filename_prefix or "export",
            )
            if result.get("success"):
                logger.info(f"✓ [Export MCP] Файл збережено: {result.get('url')}, рядків: {result.get('rows_count', 0)}")
                file_path = Path(result.get("url", ""))
                if file_path.exists():
                    result["file_base64"] = base64.b64encode(file_path.read_bytes()).decode("ascii")
                result["columns_count"] = len(columns) if columns else 0
            else:
                logger.error(f"✗ [Export MCP] Помилка: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Export MCP] Помилка: {e}")
            return {"success": False, "error": str(e)}
    
    def _export_from_temp_collection(
        self,
        temp_collection_id: str,
        format: str = "xlsx",
        filename_prefix: Optional[str] = None,
        skip_confirm: bool = False,
    ) -> Dict[str, Any]:
        """
        Експортує в файл вибірку, створену save_query_to_temp_collection. Файл відправляється в чат (file_base64).
        Перевіряє ліміти та поріг підтвердження (export_confirm_rows_threshold).
        Реєструє артефакт і додає artifact_id до результату.
        """
        logger.info("🔍 [Export MCP] Виклик export_from_temp_collection")
        try:
            source_collection, docs = self.temp_exports_repo.get_batch(temp_collection_id)
            if source_collection is None and not docs:
                return {"success": False, "error": "Тимчасову вибірку не знайдено або вже видалено."}
            return self._export_docs_to_excel(
                docs=docs,
                source_collection=source_collection or "",
                filename_prefix=filename_prefix or "export",
                temp_collection_id=temp_collection_id,
                skip_confirm=skip_confirm,
                format=format,
            )
        except Exception as e:
            logger.exception("✗ [Export MCP] export_from_temp_collection: %s", e)
            return {"success": False, "error": str(e)}

    def _export_docs_to_excel(
        self,
        docs: List[Dict[str, Any]],
        source_collection: str,
        filename_prefix: str,
        temp_collection_id: Optional[str] = None,
        skip_confirm: bool = False,
        format: str = "xlsx",
    ) -> Dict[str, Any]:
        """
        Експортує документи в Excel у стандартному уніфікованому форматі.
        Використовується як export_from_temp_collection, так і export_results_to_excel_unified.
        """
        try:
            from datetime import datetime as dt
            from utils.date_utils import KYIV_TZ
            user_id = getattr(self, "_current_user_id", None)
            request_id = getattr(self, "_current_request_id", None)
            if user_id:
                max_per_day = getattr(self.settings, "exports_per_user_per_day", 20)
                count = self.export_daily_count_repo.get_count(str(user_id))
                if count >= max_per_day:
                    return {
                        "success": False,
                        "error": f"Денний ліміт експортів ({max_per_day}) вичерпано. Спробуйте завтра.",
                    }
            if not docs:
                return {"success": False, "error": "Немає документів для експорту."}
            sheet = self.prozorro_service.get_standard_sheet_data_for_export_from_docs(docs, source_collection or "")
            if not sheet:
                if temp_collection_id:
                    self.temp_exports_repo.delete_batch(temp_collection_id)
                return {"success": False, "error": "Не вдалося сформувати лист для експорту."}
            rows, fieldnames, column_headers = sheet
            confirm_threshold = getattr(self.settings, "export_confirm_rows_threshold", 50000)
            if not skip_confirm and len(rows) > confirm_threshold and temp_collection_id:
                self.pending_export_repo.create(
                    request_id or "",
                    str(user_id) if user_id else None,
                    temp_collection_id,
                    (filename_prefix or "export").strip(),
                    format or "xlsx",
                )
                return {
                    "success": False,
                    "needs_confirmation": True,
                    "estimated_rows": len(rows),
                    "confirm_token": request_id,
                    "message": f"Експорт приблизно {len(rows)} рядків у Excel. Підтвердити? Відправте: confirm_export:{request_id}",
                }
            max_rows = getattr(self.settings, "export_max_rows", 50000)
            if len(rows) > max_rows:
                if temp_collection_id:
                    self.temp_exports_repo.delete_batch(temp_collection_id)
                return {
                    "success": False,
                    "error": f"Кількість рядків ({len(rows)}) перевищує ліміт ({max_rows}). Звужте вибірку.",
                }
            excel_io = generate_excel_in_memory(rows, fieldnames, column_headers)
            file_bytes = excel_io.getvalue()
            max_mb = getattr(self.settings, "export_max_file_size_mb", 50)
            size_mb = len(file_bytes) / (1024 * 1024)
            if size_mb > max_mb:
                if temp_collection_id:
                    self.temp_exports_repo.delete_batch(temp_collection_id)
                return {
                    "success": False,
                    "error": f"Розмір файлу ({size_mb:.1f} МБ) перевищує ліміт ({max_mb} МБ).",
                }
            ensure_directory_exists(str(Path(self.settings.temp_directory) / "exports"))
            export_dir = Path(self.settings.temp_directory) / "exports"
            ts = dt.now(KYIV_TZ).strftime("%Y%m%d_%H%M%S")
            prefix = (filename_prefix or "export").strip()
            safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in prefix)
            filename = f"{safe}_{ts}.xlsx"
            file_path = export_dir / filename
            file_path.write_bytes(file_bytes)
            if temp_collection_id:
                self.temp_exports_repo.delete_batch(temp_collection_id)
            if skip_confirm and temp_collection_id:
                self.pending_export_repo.delete_by_temp_collection_id(temp_collection_id)
            file_base64_str = base64.b64encode(file_bytes).decode("ascii")
            ttl = getattr(self.settings, "artifact_ttl_seconds", 3600)
            artifact_id = self.artifact_service.register(
                user_id=str(user_id) if user_id else None,
                artifact_type="excel",
                content_base64=file_base64_str,
                metadata={"filename": filename, "rows_count": len(rows), "columns_count": len(fieldnames), "source_collection": source_collection or ""},
                ttl_seconds=ttl,
            )
            if user_id:
                self.export_daily_count_repo.increment(str(user_id))
            result = {
                "success": True,
                "url": str(file_path),
                "filename": filename,
                "rows_count": len(rows),
                "columns_count": len(fieldnames),
                "format": "xlsx",
                "file_base64": file_base64_str,
                "artifact_id": artifact_id,
            }
            logger.info("✓ [Export MCP] Експорт: %s, рядків: %s, artifact_id: %s", result.get("url"), len(rows), artifact_id)
            return result
        except Exception as e:
            logger.exception("✗ [Export MCP] _export_docs_to_excel: %s", e)
            return {"success": False, "error": str(e)}

    def export_results_to_excel_unified(
        self,
        results: List[Dict[str, Any]],
        source_collection: str = "unified_listings",
        filename_prefix: str = "export",
    ) -> Dict[str, Any]:
        """
        Експортує результати пайплайну в Excel у стандартному уніфікованому форматі
        (дата, джерело, адреса, ціна, заголовок, посилання тощо).
        Використовується NEW_FLOW для data_export замість save_query_results_to_excel.
        """
        if not results:
            return {"success": False, "error": "Немає результатів для експорту."}
        return self._export_docs_to_excel(
            docs=results,
            source_collection=source_collection,
            filename_prefix=filename_prefix,
            temp_collection_id=None,
            skip_confirm=True,
            format="xlsx",
        )
    
    def _trigger_data_update(
        self,
        source: str,
        days: Optional[int] = None,
        regions: Optional[list] = None,
        listing_types: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Ініціює оновлення даних у базі через pipeline raw → main → LLM (Phase 1 без LLM).
        source: olx, prozorro або all (обидва). regions/listing_types — точкове оновлення по областях та типах.
        """
        logger.info("🔍 [Data Update] Виклик trigger_data_update: source=%s, days=%s", source, days)
        source_lower = (source or "").strip().lower()
        days = days or 1
        sources = []
        if source_lower == "olx":
            sources = ["olx"]
        elif source_lower == "prozorro":
            sources = ["prozorro"]
        elif source_lower in ("all", "both", ""):
            sources = ["olx", "prozorro"]
        else:
            return {"success": False, "error": f"Невідомий source: {source}. Дозволено: olx, prozorro, all."}
        regions_list = regions if isinstance(regions, list) and regions else None
        listing_types_list = listing_types if isinstance(listing_types, list) and listing_types else None
        try:
            result = run_full_pipeline(
                settings=self.settings,
                sources=sources,
                days=days,
                regions=regions_list,
                listing_types=listing_types_list,
            )
            p1 = result.get("phase1", {})
            p2 = result.get("phase2", {})
            msg_parts = []
            if p1.get("olx"):
                msg_parts.append(
                    f"OLX: {p1['olx'].get('total_listings', 0)} оголошень, "
                    f"{len(p1['olx'].get('loaded_urls') or [])} завантажено в raw; LLM: {p2.get('olx_llm_processed', 0)}"
                )
            if p1.get("prozorro"):
                msg_parts.append(
                    f"ProZorro: {p1['prozorro'].get('count', 0)} аукціонів у raw; LLM: {p2.get('prozorro_llm_processed', 0)}"
                )
            logger.info("✓ [Data Update] %s", "; ".join(msg_parts) or "завершено")
            return {
                "success": True,
                "source": source_lower,
                "message": "Оновлення завершено (Phase 1 — сирі дані без LLM, Phase 2 — LLM для обраних). " + ("; ".join(msg_parts) if msg_parts else ""),
                "phase1": p1,
                "phase2": p2,
            }
        except Exception as e:
            logger.exception("✗ [Data Update] Помилка: %s", e)
            return {"success": False, "source": source_lower, "error": str(e)}
    
    def _get_standard_excel_format(self) -> Tuple[List[str], Dict[str, str]]:
        """
        Повертає стандартний формат Excel файлу (fieldnames та column_headers),
        який використовується для файлів за день/тиждень.
        
        Returns:
            Кортеж (fieldnames, column_headers)
        """
        fieldnames = [
            'date_updated',                      # Дата оновлення
            'address_region',                    # Область
            'address_city',                      # Населений пункт
            'address',                           # Адреса
            'property_type',                     # Тип нерухомості
            'cadastral_number',                  # Кадастровий номер
            'building_area_sqm',                 # Площа нерухомості (кв. м.)
            'land_area_ha',                      # Площа земельної ділянки (га)
            'base_price',                        # Стартова ціна
            'deposit_amount',                    # Розмір взносу
            'auction_start_date',                # Дата торгів
            'document_submission_deadline',      # Дата фінальної подачі документів
            'min_participants_count',            # Мінімальна кількість учасників
            'participants_count',                # Кількість зареєстрованих учасників
            'arrests_info',                      # Арешти
            'description',                       # Опис
            'auction_url',                       # Посилання
            'classification_code',               # Код класифікатора
            'is_repeat_auction',                 # Повторний аукціон
            'previous_auctions_links'            # Посилання на минулі аукціони
        ]
        
        column_headers = {
            'date_updated': 'Дата оновлення',
            'address_region': 'Область',
            'address_city': 'Населений пункт',
            'address': 'Адреса',
            'property_type': 'Тип нерухомості',
            'cadastral_number': 'Кадастровий номер',
            'building_area_sqm': 'Площа нерухомості (кв. м.)',
            'land_area_ha': 'Площа земельної ділянки (га)',
            'base_price': 'Стартова ціна',
            'deposit_amount': 'Розмір взносу',
            'auction_start_date': 'Дата торгів',
            'document_submission_deadline': 'Дата фінальної подачі документів',
            'min_participants_count': 'Мінімальна кількість учасників',
            'participants_count': 'Кількість зареєстрованих учасників',
            'arrests_info': 'Арешти',
            'description': 'Опис',
            'auction_url': 'Посилання',
            'classification_code': 'Код класифікатора',
            'is_repeat_auction': 'Повторний аукціон',
            'previous_auctions_links': 'Посилання на минулі аукціони'
        }
        
        return fieldnames, column_headers
    
    def _extract_field_value(self, row: Dict[str, Any], field_name: str) -> Any:
        """
        Витягує значення поля з рядка даних, враховуючи різні можливі структури.
        
        Args:
            row: Рядок даних з execute_aggregation
            field_name: Назва поля в стандартному форматі Excel
            
        Returns:
            Значення поля або порожній рядок
        """
        # Спочатку перевіряємо, чи поле є безпосередньо в рядку
        if field_name in row:
            return row[field_name]
        
        # Мапінг стандартних полів на можливі шляхи в даних
        field_mapping = {
            'date_updated': [
                'date_updated',
                'auction_data.dateModified',
                'auction_data.dateCreated'
            ],
            'address_region': [
                'address_region',
                'llm_cache_data.result.addresses.0.region',
                'llm_result.result.addresses.0.region',
                'addresses.0.region'
            ],
            'address_city': [
                'address_city',
                'llm_cache_data.result.addresses.0.settlement',
                'llm_result.result.addresses.0.settlement',
                'addresses.0.settlement'
            ],
            'address': [
                'address',
                'formatted_address'
            ],
            'property_type': [
                'property_type',
                'llm_cache_data.result.property_type',
                'llm_result.result.property_type'
            ],
            'cadastral_number': [
                'cadastral_number',
                'llm_cache_data.result.cadastral_number',
                'llm_result.result.cadastral_number'
            ],
            'building_area_sqm': [
                'building_area_sqm',
                'llm_cache_data.result.building_area_sqm',
                'llm_result.result.building_area_sqm'
            ],
            'land_area_ha': [
                'land_area_ha',
                'llm_cache_data.result.land_area_ha',
                'llm_result.result.land_area_ha'
            ],
            'base_price': [
                'base_price',
                'amount',
                'auction_data.value.amount',
                'value.amount'
            ],
            'deposit_amount': [
                'deposit_amount',
                'auction_data.guarantee.amount',
                'guarantee.amount'
            ],
            'auction_start_date': [
                'auction_start_date',
                'auction_data.auctionPeriod.startDate',
                'auctionPeriod.startDate'
            ],
            'document_submission_deadline': [
                'document_submission_deadline',
                'auction_data.enquiryPeriod.endDate',
                'enquiryPeriod.endDate'
            ],
            'min_participants_count': [
                'min_participants_count',
                'auction_data.minNumberOfQualifiedBids',
                'minNumberOfQualifiedBids'
            ],
            'participants_count': [
                'participants_count',
                'bids_count'
            ],
            'arrests_info': [
                'arrests_info',
                'auction_data.arrests',
                'arrests'
            ],
            'description': [
                'description',
                'auction_data.description.uk_UA',
                'auction_data.description.en_US'
            ],
            'auction_url': [
                'auction_url',
                'url'
            ],
            'classification_code': [
                'classification_code',
                'auction_data.items.0.classification.id',
                'items.0.classification.id'
            ],
            'is_repeat_auction': [
                'is_repeat_auction'
            ],
            'previous_auctions_links': [
                'previous_auctions_links'
            ]
        }
        
        # Шукаємо значення за мапінгом
        if field_name in field_mapping:
            for path in field_mapping[field_name]:
                value = self._get_nested_value(row, path)
                if value is not None and value != '':
                    # Додаткова обробка для специфічних полів
                    if field_name == 'auction_url' and value and not value.startswith('http'):
                        # Якщо це auction_id, формуємо URL
                        auction_id = str(value)
                        return f"https://prozorro.sale/auction/{auction_id}"
                    elif field_name == 'description' and isinstance(value, dict):
                        # Опис може бути об'єктом з uk_UA або en_US
                        return value.get('uk_UA', value.get('en_US', ''))
                    elif field_name in ['auction_start_date', 'document_submission_deadline', 'date_updated'] and value:
                        # Форматуємо дату в київському часі
                        if hasattr(value, 'year'):
                            return format_datetime_display(value, '%d.%m.%Y %H:%M')
                        return format_date_display(str(value), '%d.%m.%Y %H:%M')
                    return value
        
        return ''
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """
        Отримує значення з вкладеного словника за шляхом (наприклад, "a.b.c").
        
        Args:
            data: Словник з даними
            path: Шлях до значення (наприклад, "a.b.c" або "a.0.b" для масивів)
            
        Returns:
            Значення або None
        """
        parts = path.split('.')
        current = data
        
        for part in parts:
            if part.isdigit():
                # Індекс масиву
                idx = int(part)
                if isinstance(current, list) and 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                # Ключ словника
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    return None
                
                if current is None:
                    return None
        
        return current
    
    def _save_query_results_to_excel(
        self,
        results: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
        column_headers: Optional[Dict[str, str]] = None,
        filename: Optional[str] = None,
        use_standard_format: bool = True
    ) -> Dict[str, Any]:
        """
        Зберігає результати запиту у Excel файл.
        
        Args:
            results: Список словників з результатами запиту
            columns: Список колонок для включення (опціонально, якщо не вказано - використовуються стандартні або всі ключі з першого запису)
            column_headers: Словник з українськими назвами колонок (ключ -> назва, опціонально)
            filename: Назва файлу (опціонально)
            use_standard_format: Якщо True, використовує стандартний формат Excel (як для файлів за день/тиждень)
            
        Returns:
            Словник з результатом: success, file_base64, filename, error
        """
        logger.info("🔍 [Report MCP] Виклик save_query_results_to_excel")
        logger.info(f"Аргументи: results type={type(results)}, columns={columns}, column_headers={column_headers}, filename={filename}, use_standard_format={use_standard_format}")
        
        try:
            # Перевіряємо, чи results - це список
            if not isinstance(results, list):
                logger.error(f"results має бути списком, але отримано: {type(results)}")
                return {'success': False, 'error': f'results має бути списком, але отримано {type(results)}'}
            
            if not results:
                return {'success': False, 'error': 'Немає результатів для збереження'}
            
            logger.info(f"Кількість результатів: {len(results)}")
            
            # Визначаємо колонки, якщо не вказано
            if columns is None:
                if use_standard_format:
                    # Використовуємо стандартний формат
                    standard_fieldnames, _ = self._get_standard_excel_format()
                    # Перевіряємо, які з стандартних полів є в даних
                    available_keys = set(results[0].keys()) if results else set()
                    # Використовуємо стандартні поля, які є в даних, плюс додаємо інші поля з даних
                    columns = [col for col in standard_fieldnames if col in available_keys]
                    # Додаємо інші поля з даних, які не в стандартному форматі
                    for key in available_keys:
                        if key not in columns:
                            columns.append(key)
                    logger.info(f"Використовую стандартний формат. Колонки: {columns}")
                else:
                    # Використовуємо всі ключі з першого запису
                    columns = list(results[0].keys())
                    logger.info(f"Автоматично визначено колонки: {columns}")
            
            # Визначаємо заголовки колонок, якщо не вказано
            if column_headers is None:
                if use_standard_format:
                    # Використовуємо стандартні заголовки
                    _, standard_headers = self._get_standard_excel_format()
                    column_headers = {}
                    for col in columns:
                        if col in standard_headers:
                            column_headers[col] = standard_headers[col]
                        else:
                            # Для нестандартних колонок використовуємо ключ як заголовок
                            column_headers[col] = col
                    logger.info(f"Використовую стандартні заголовки")
                else:
                    # Використовуємо ключі як заголовки
                    column_headers = {col: col for col in columns}
            
            # Підготовка даних з мапінгом на стандартний формат
            formatted_data = []
            for row in results:
                formatted_row = {}
                for col in columns:
                    value = self._extract_field_value(row, col)
                    
                    # Обробляємо None та складні типи
                    if value is None:
                        formatted_row[col] = ''
                    elif isinstance(value, (dict, list)):
                        # Для складних типів конвертуємо в JSON рядок
                        formatted_row[col] = json.dumps(value, ensure_ascii=False, default=str)
                    else:
                        formatted_row[col] = value
                formatted_data.append(formatted_row)
            
            # Генеруємо Excel
            from utils.file_utils import generate_excel_in_memory
            import base64
            
            excel_bytes = generate_excel_in_memory(formatted_data, columns, column_headers)
            excel_bytes.seek(0)
            
            # Конвертуємо в base64
            file_base64 = base64.b64encode(excel_bytes.read()).decode('utf-8')
            
            # Генеруємо назву файлу, якщо не вказано
            if filename is None:
                from datetime import timezone
                timestamp = format_datetime_display(datetime.now(timezone.utc), '%Y%m%d_%H%M%S')
                filename = f'query_results_{timestamp}.xlsx'
            
            result = {
                'success': True,
                'file_base64': file_base64,
                'filename': filename,
                'rows_count': len(formatted_data),
                'columns_count': len(columns)
            }
            
            logger.info(f"✓ [Report MCP] Excel файл згенеровано: {filename}, рядків: {len(formatted_data)}, колонок: {len(columns)}")
            return result
            
        except Exception as e:
            logger.exception(f"✗ [Report MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _geocode_address(self, address_or_place: str, region: str = "ua") -> Dict[str, Any]:
        """Перетворює адресу або топонім на координати (через кеш або Google API)."""
        logger.info(f"🔍 [Geocoding MCP] Виклик geocode_address: {address_or_place!r}, region={region}")
        try:
            result = self.geocoding_service.geocode(
                query=address_or_place, region=region, caller="langchain_agent"
            )
            results_list = result.get("results", [])
            logger.info(f"✓ [Geocoding MCP] Результат: {len(results_list)} місць, from_cache={result.get('from_cache')}")
            result = dict(result)
            result["success"] = len(results_list) > 0
            return result
        except Exception as e:
            logger.exception(f"✗ [Geocoding MCP] Помилка: {e}")
            return {
                "query_hash": "",
                "query_text": address_or_place or "",
                "results": [],
                "from_cache": False,
                "error": str(e),
            }

    def _search_nearby_places(
        self,
        latitude: float,
        longitude: float,
        place_types: List[str],
        radius_meters: int = 500,
        max_results: int = 20,
    ) -> Dict[str, Any]:
        """Пошук місць поблизу координат (Google Places API)."""
        logger.info(
            "🔍 [Places] Виклик search_nearby_places: lat=%.4f, lng=%.4f, types=%s",
            latitude, longitude, place_types[:5] if place_types else [],
        )
        try:
            result = self.places_service.search_nearby(
                latitude=latitude,
                longitude=longitude,
                place_types=place_types if isinstance(place_types, list) else [str(place_types)],
                radius_meters=radius_meters,
                max_results=max_results,
            )
            logger.info("✓ [Places] Результат: %s місць", result.get("count", 0))
            return result
        except Exception as e:
            logger.exception("✗ [Places] Помилка: %s", e)
            return {"success": False, "places": [], "count": 0, "error": str(e)}

    def _get_listing_details(
        self,
        source: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Повертає повні дані оголошення (зведені + сирі) для аналізу локації та опису.
        Якщо source/source_id не передано — використовує _current_listing_context (контекст поточного оголошення).
        """
        logger.info("🔍 [ListingDetails] Виклик get_listing_details: source=%s", source or "(з контексту)")
        try:
            effective_source = source
            effective_source_id = source_id
            if not effective_source or not effective_source_id:
                lc = getattr(self, "_current_listing_context", None)
                if lc and isinstance(lc, dict):
                    effective_source = lc.get("detail_source") or ""
                    effective_source_id = lc.get("detail_id") or ""
                if not effective_source and not effective_source_id and lc and lc.get("page_url"):
                    page_url = (lc.get("page_url") or "").strip()
                    if "prozorro.sale" in page_url or "prozorro" in page_url:
                        effective_source = "prozorro"
                        effective_source_id = page_url.rstrip("/").split("/")[-1] or ""
                    elif "olx" in page_url:
                        effective_source = "olx"
                        effective_source_id = page_url
            if not effective_source or not effective_source_id:
                return {
                    "success": False,
                    "error": "Не вказано оголошення. Передай source (olx/prozorro) та source_id (url або auction_id) або викликай з контексту оголошення.",
                }
            from business.services.property_usage_analysis_service import PropertyUsageAnalysisService
            from data.repositories.unified_listings_repository import UnifiedListingsRepository
            usage_svc = PropertyUsageAnalysisService()
            unified_repo = UnifiedListingsRepository()
            raw_doc = usage_svc.get_listing_doc(effective_source, effective_source_id)
            unified_doc = unified_repo.find_by_source_id(effective_source, effective_source_id)
            out = {
                "success": True,
                "source": effective_source,
                "source_id": effective_source_id[:200] if effective_source_id else "",
                "summary": "",
                "title": "",
                "description": "",
                "location_raw": "",
                "llm_addresses": [],
                "region": "",
                "city": "",
                "addresses_unified": [],
            }
            if unified_doc:
                out["title"] = (unified_doc.get("title") or "")[:500]
                out["description"] = (unified_doc.get("description") or "")[:8000]
                out["region"] = (unified_doc.get("region") or "")[:200]
                out["city"] = (unified_doc.get("city") or "")[:200]
                addrs = unified_doc.get("addresses") or []
                out["addresses_unified"] = [
                    {"region": a.get("region"), "settlement": a.get("settlement"), "street": a.get("street"), "formatted": a.get("formatted_address")}
                    for a in addrs[:10] if isinstance(a, dict)
                ]
            if raw_doc:
                detail = raw_doc.get("detail") if isinstance(raw_doc.get("detail"), dict) else {}
                loc = detail.get("location") if isinstance(detail.get("location"), dict) else {}
                out["location_raw"] = (loc.get("raw") or "")[:1000]
                search_data = raw_doc.get("search_data") or {}
                if isinstance(search_data, dict) and search_data.get("location"):
                    loc_str = search_data["location"]
                    if isinstance(loc_str, str):
                        out["search_location"] = loc_str[:500]
                    elif isinstance(loc_str, dict):
                        out["search_location"] = (loc_str.get("city") or "") + ", " + (loc_str.get("region") or "")
                llm = detail.get("llm") or {}
                if isinstance(llm.get("addresses"), list):
                    out["llm_addresses"] = [
                        (a.get("full") or str(a))[:300] if isinstance(a, dict) else str(a)[:300]
                        for a in llm["addresses"][:15]
                    ]
                if not out["description"] and detail.get("description"):
                    out["description"] = (detail.get("description") or "")[:8000]
                if not out["title"] and (detail.get("title") or search_data.get("title")):
                    out["title"] = (detail.get("title") or search_data.get("title") or "")[:500]
            parts = []
            if out["title"]:
                parts.append("Назва: " + out["title"])
            if out["description"]:
                parts.append("Опис: " + out["description"][:3000] + ("..." if len(out["description"]) > 3000 else ""))
            if out["location_raw"]:
                parts.append("Локація (сирий текст): " + out["location_raw"])
            if out["llm_addresses"]:
                parts.append("Адреси з опису (LLM): " + "; ".join(out["llm_addresses"][:5]))
            if out["region"] or out["city"]:
                parts.append("Регіон/місто: " + (out["region"] or "") + ", " + (out["city"] or ""))
            out["summary"] = "\n\n".join(parts) if parts else "Немає даних для відображення."
            logger.info("✓ [ListingDetails] Отримано: title=%s, desc_len=%s", bool(out["title"]), len(out["description"]))
            return out
        except Exception as e:
            logger.exception("✗ [ListingDetails] Помилка: %s", e)
            return {"success": False, "error": str(e)}

    def run_tool(self, tool_name: str, tool_args: Optional[Dict[str, Any]] = None) -> Any:
        """
        Виконує один інструмент за назвою та аргументами. Для використання мультиагентною
        оркестрацією (планувальник/аналітик викликають інструменти через цей метод).
        Логує виклик з request_id (correlation ID), якщо встановлено _current_request_id.

        Args:
            tool_name: Назва інструмента (get_database_schema, execute_query, тощо)
            tool_args: Словник аргументів (kwargs для відповідного _методу)

        Returns:
            Результат виконання (зазвичай dict з success, data/error)
        """
        req_id = getattr(self, "_current_request_id", None) or ""
        log_ctx = f"[request_id={req_id}] " if req_id else ""
        logger.info("%srun_tool: %s", log_ctx, tool_name)
        args = tool_args or {}
        try:
            if tool_name == "get_database_schema":
                return self._get_database_schema()
            if tool_name == "get_collection_info":
                return self._get_collection_info(args.get("collection_name", ""))
            if tool_name == "get_data_dictionary":
                return self._get_data_dictionary()
            if tool_name == "get_allowed_collections":
                return self._get_allowed_collections()
            if tool_name == "get_distinct_values":
                return self._get_distinct_values(
                    collection_name=args.get("collection_name", ""),
                    field_path=args.get("field_path", ""),
                    limit=args.get("limit", 300),
                    unwrap_array=args.get("unwrap_array", False),
                )
            if tool_name == "execute_query":
                return self._execute_query(args.get("query", args))
            if tool_name == "execute_aggregation":
                return self._execute_aggregation(
                    args.get("collection_name", ""),
                    args.get("pipeline", []),
                    args.get("limit"),
                )
            if tool_name == "save_query_to_temp_collection":
                return self._save_query_to_temp_collection(args.get("query", args))
            if tool_name == "execute_analytics":
                q = args.get("query", args)
                return self._execute_analytics(q)
            if tool_name == "list_metrics":
                return self._list_metrics()
            if tool_name == "generate_report":
                return self._generate_report(
                    args.get("request", args),
                    args.get("return_base64", True),
                )
            if tool_name == "list_templates":
                return self._list_templates()
            if tool_name == "save_query_results_to_excel":
                # Підтримка обох форматів параметрів
                results = args.get("results") or args.get("data", [])
                return self._save_query_results_to_excel(
                    results=results,
                    columns=args.get("columns"),
                    column_headers=args.get("column_headers"),
                    filename=args.get("filename")
                )
            if tool_name == "export_listings_to_file":
                return self._export_listings_to_file(
                    ids=args.get("ids", []),
                    collection=args.get("collection", ""),
                    format=args.get("format", "xlsx"),
                    columns=args.get("columns"),
                    column_headers=args.get("column_headers"),
                    filename_prefix=args.get("filename_prefix"),
                )
            if tool_name == "export_from_temp_collection":
                return self._export_from_temp_collection(
                    temp_collection_id=args.get("temp_collection_id", ""),
                    format=args.get("format", "xlsx"),
                    filename_prefix=args.get("filename_prefix"),
                    skip_confirm=args.get("skip_confirm", False),
                )
            if tool_name == "generate_search_filter_string":
                return self._generate_search_filter_string(**args)
            if tool_name == "search_unified_listings":
                return self._search_unified_listings(
                    filter_string=args.get("filter_string"),
                    limit=args.get("limit", 50),
                    skip=args.get("skip", 0),
                    sort_field=args.get("sort_field", "source_updated_at"),
                    sort_order=args.get("sort_order", "desc"),
                    **{k: v for k, v in args.items() if k not in ("filter_string", "limit", "skip", "sort_field", "sort_order")},
                )
            if tool_name == "trigger_data_update":
                return self._trigger_data_update(
                    source=args.get("source", ""),
                    days=args.get("days"),
                    regions=args.get("regions"),
                    listing_types=args.get("listing_types"),
                )
            if tool_name == "geocode_address":
                return self._geocode_address(
                    address_or_place=args.get("address_or_place", args.get("address", "")),
                    region=args.get("region", "ua"),
                )
            if tool_name == "get_listing_details":
                return self._get_listing_details(
                    source=args.get("source"),
                    source_id=args.get("source_id"),
                )
            if tool_name == "search_nearby_places":
                pt = args.get("place_types", [])
                if isinstance(pt, str):
                    pt = [pt] if pt else []
                return self._search_nearby_places(
                    latitude=float(args.get("latitude", 0)),
                    longitude=float(args.get("longitude", 0)),
                    place_types=pt,
                    radius_meters=int(args.get("radius_meters", 500)),
                    max_results=int(args.get("max_results", 20)),
                )
            if tool_name == "analytics_extracts_aggregate":
                return self._analytics_extracts_aggregate(
                    metric=args.get("metric", ""),
                    aggregation=args.get("aggregation", "avg"),
                    group_by=args.get("group_by"),
                    filters=args.get("filters"),
                    limit=int(args.get("limit", 100)),
                )
            if tool_name == "analytics_extracts_search":
                return self._analytics_extracts_search(
                    filters=args.get("filters"),
                    fields=args.get("fields"),
                    sort=args.get("sort"),
                    limit=int(args.get("limit", 100)),
                    skip=int(args.get("skip", 0)),
                )
            if tool_name == "analytics_extracts_list_metrics":
                return self._analytics_extracts_list_metrics()
            if tool_name == "analytics_extracts_list_dimensions":
                return self._analytics_extracts_list_dimensions()
            if tool_name == "analytics_extracts_get_distinct":
                return self._analytics_extracts_get_distinct(
                    field=args.get("field", ""),
                    filters=args.get("filters"),
                )
            return {"success": False, "error": f"Невідомий інструмент: {tool_name}"}
        except Exception as e:
            logger.exception("%srun_tool %s: %s", log_ctx, tool_name, e)
            return {"success": False, "error": str(e)}

    def _user_asked_for_file(self, text: Optional[str]) -> bool:
        """Повертає True, якщо в тексті запиту є явна згадка файлу/Excel/експорту."""
        if not text or not isinstance(text, str):
            return False
        t = text.strip().lower()
        file_phrases = (
            "excel", "експорт", "файл", "file", "export", "xlsx", "надішли файл",
            "звіт у файл", "в excel", "в файл", "у файл", "таблицю", "таблиця",
            "збережи в", "завантаж", "скачай", "вигрузи", "вигрузка",
        )
        return any(p in t for p in file_phrases)

    def _validate_query(self, user_query: str) -> Tuple[bool, Optional[str]]:
        """
        Валідує запит користувача на рівні сервісу.
        
        Args:
            user_query: Запит користувача
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if not user_query or not user_query.strip():
            return False, "Запит не може бути порожнім"
        
        # Перевірка на спроби прямого доступу до БД
        forbidden_patterns = [
            'db.', 'collection.', 'mongodb://', 'mongo://',
            'pymongo', 'MongoClient', 'find_one', 'find(',
            'insert', 'update', 'delete', 'drop'
        ]
        
        query_lower = user_query.lower()
        for pattern in forbidden_patterns:
            if pattern in query_lower:
                logger.warning(f"Виявлено заборонений паттерн у запиті: {pattern}")
                return False, f"Запит містить заборонені операції. Використовуйте MCP tools для роботи з даними."
        
        return True, None
    
    def _extract_excel_files_from_history(self) -> List[Dict[str, Any]]:
        """
        Витягує Excel файли з історії conversation (з результатів tools).
        
        Returns:
            Список словників з інформацією про Excel файли: [{'file_base64': ..., 'filename': ..., 'rows_count': ...}, ...]
        """
        excel_files = []
        n_msgs = len(self.conversation_history)
        logger.info(
            "Перевіряю conversation_history для Excel файлів. Кількість повідомлень: %s%s",
            n_msgs,
            " (запит не оброблявся агентом — валідація або ранній вихід)" if n_msgs == 0 else "",
        )
        
        for i, msg in enumerate(self.conversation_history):
            if isinstance(msg, ToolMessage):
                try:
                    result = json.loads(msg.content)
                    logger.debug(f"ToolMessage {i}: success={result.get('success')}, filename={result.get('filename')}")
                    
                    if result.get('success') and result.get('file_base64') and result.get('filename'):
                        # Перевіряємо, чи це Excel файл
                        filename = result.get('filename', '')
                        if filename.endswith('.xlsx') or 'excel' in filename.lower():
                            logger.info(f"Знайдено Excel файл: {filename}")
                            excel_files.append({
                                'file_base64': result['file_base64'],
                                'filename': result['filename'],
                                'rows_count': result.get('rows_count', 0),
                                'columns_count': result.get('columns_count', 0)
                            })
                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"Помилка парсингу ToolMessage {i}: {e}")
                    continue
        
        logger.info(f"Знайдено Excel файлів: {len(excel_files)}")
        return excel_files

    def process_query(
        self,
        user_query: str,
        user_id: Optional[str] = None,
        chat_id: Optional[str] = None,
        listing_context: Optional[Dict[str, Any]] = None,
        stream_callback: Optional[Callable[[str], None]] = None,
        thinking_callback: Optional[Callable[[str], None]] = None,
        reply_to_text: Optional[str] = None,
        request_id: Optional[str] = None,
        route: Optional[str] = None,
    ) -> str:
        """
        Обробляє запит користувача з використанням LangChain агента та пам'яті (buffer, summary, vector retrieval).
        
        Використовує явний цикл агента:
        1. Plan: Агент аналізує запит та планує дії
        2. Act: Агент викликає tools
        3. Observe: Агент аналізує результати та приймає рішення
        
        Пам'ять: буфер останніх повідомлень, саммарі старішої частини, опційно релевантні фрагменти з векторного пошуку.
        Користувач може відповідати на конкретні повідомлення (reply_to_text) — агент отримує цей контекст.
        
        Args:
            user_query: Запит користувача
            user_id: Ідентифікатор користувача для per-user пам'яті (опційно)
            stream_callback: Функція для трансляції проміжних результатів
            thinking_callback: Функція для трансляції ходу думок (thinking) агента
            reply_to_text: Текст повідомлення, на яке користувач відповідає (контекст для відповіді на конкретне повідомлення)
            request_id: Ідентифікатор запиту для трасування в логах (опційно)
            route: Маршрут для підмножини tools (free_form, query_export, analytics, geo); None = free_form
            
        Returns:
            Відповідь агента (текст)
        """
        req_id = request_id or str(uuid.uuid4())
        self._current_request_id = req_id
        self._current_user_id = user_id
        self._current_chat_id = chat_id
        self._current_listing_context = listing_context if listing_context and isinstance(listing_context, dict) else None
        start_time = time.perf_counter()
        self._last_request_metrics = {}
        self._tool_failures_this_request = 0
        self._had_tool_failure_before_success = False
        log_ctx = f"[request_id={req_id}] "
        logger.info("="*80)
        logger.info("%sПОЧАТОК ОБРОБКИ ЗАПИТУ (LangChain Agent)", log_ctx)
        logger.info("%sЗапит користувача: %s", log_ctx, user_query)
        if reply_to_text:
            logger.info("%sКористувач відповідає на повідомлення: %s...", log_ctx, (reply_to_text[:80] if reply_to_text else ""))
        logger.info("="*80)
        try:
            return self._process_query_impl(
                user_query, user_id, chat_id, listing_context, stream_callback, thinking_callback,
                reply_to_text, req_id, log_ctx, start_time, route,
            )
        finally:
            self._current_request_id = None
            self._current_user_id = None
            self._current_chat_id = None
            self._current_listing_context = None
            duration_sec = time.perf_counter() - start_time
            if "duration_seconds" not in self._last_request_metrics:
                self._last_request_metrics["duration_seconds"] = duration_sec
            logger.info("%sЗАВЕРШЕНО ОБРОБКУ ЗАПИТУ | duration_sec=%.2f", log_ctx, duration_sec)
            logger.info("="*80)

    def _run_langgraph_loop(
        self,
        tools_for_request: List[Any],
        user_id: Optional[str],
        user_query: str,
        chat_id: Optional[str],
        req_id: str,
        log_ctx: str,
        start_time: float,
        stream_callback: Optional[Callable[[str], None]] = None,
        thinking_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Запускає агента через LangGraph state machine (agent -> tools -> agent)."""
        from business.services.langgraph_agent_runner import build_agent_graph
        max_iter = getattr(self.settings, 'llm_agent_max_iterations', self.MAX_ITERATIONS)
        graph = build_agent_graph(self, list(tools_for_request), max_iterations=max_iter)
        initial = {"messages": list(self.conversation_history), "iteration": 0}
        result = graph.invoke(initial)
        messages = result.get("messages", [])
        iteration = result.get("iteration", 0)
        # Витягуємо останню текстову відповідь
        for msg in reversed(messages):
            if isinstance(msg, AIMessage) and hasattr(msg, 'content') and msg.content:
                content = msg.content
                if isinstance(content, list):
                    parts = [p.get("text", "") if isinstance(p, dict) and p.get("type") == "text" else str(p) for p in content if isinstance(p, (str, dict))]
                    content = "\n".join(p for p in parts if p)
                if content and isinstance(content, str) and content.strip():
                    self._save_response_to_memory(user_id, user_query, content.strip(), chat_id)
                    self._last_request_metrics = self._build_request_metrics(iteration, start_time)
                    return content.strip()
        # Fallback: досягнуто max ітерацій без текстової відповіді
        fallback_msg = HumanMessage(
            content="[СИСТЕМА] Досягнуто ліміт ітерацій. На основі наявних даних дай фінальну відповідь текстом. НЕ викликай інструменти."
        )
        try:
            resp = self.llm.invoke(list(messages) + [fallback_msg])
            fc = getattr(resp, 'content', None)
            if fc:
                if isinstance(fc, list):
                    fc = "\n".join(str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in fc)
                if isinstance(fc, str) and fc.strip():
                    self._save_response_to_memory(user_id, user_query, fc.strip(), chat_id)
                    self._last_request_metrics = self._build_request_metrics(iteration, start_time)
                    return fc.strip()
        except Exception as e:
            logger.warning("%sLangGraph fallback LLM failed: %s", log_ctx, e)
        self._last_request_metrics = self._build_request_metrics(iteration, start_time)
        return "Не вдалося сформувати відповідь після максимальної кількості ітерацій."

    def _process_query_impl(
        self,
        user_query: str,
        user_id: Optional[str],
        chat_id: Optional[str],
        listing_context: Optional[Dict[str, Any]],
        stream_callback: Optional[Callable[[str], None]],
        thinking_callback: Optional[Callable[[str], None]],
        reply_to_text: Optional[str],
        req_id: str,
        log_ctx: str,
        start_time: float,
        route: Optional[str] = None,
    ) -> str:
        """Внутрішня реалізація process_query (для єдиного finally зверху)."""
        tools_for_request = self.get_tools_for_route(route or "free_form")
        time_budget_seconds = getattr(self.settings, "llm_agent_time_budget_seconds", None)
        # Валідація запиту
        is_valid, error_msg = self._validate_query(user_query)
        if not is_valid:
            logger.warning("%sЗапит не пройшов валідацію: %s", log_ctx, error_msg)
            return f"Помилка: {error_msg}"
        
        self.current_user_query = user_query
        self.excel_generated = False
        self.conversation_history = []
        
        # Пам'ять: при chat_id — з ChatSessionRepository (персистентна); інакше — in-memory (user_id)
        memory_key = f"{user_id}:{chat_id}" if (user_id and chat_id) else (user_id or "default")
        memory = self._get_or_create_memory(memory_key) if user_id else None
        if user_id and chat_id and memory:
            try:
                from data.repositories.chat_session_repository import ChatSessionRepository
                chat_repo = ChatSessionRepository()
                pairs = chat_repo.get_messages_for_context(str(user_id), chat_id, max_pairs=10)
                memory.buffer = [(p.get("user", ""), p.get("assistant", "")) for p in pairs]
            except Exception as e:
                logger.debug("chat session load: %s", e)
        
        # Системний промпт
        system_msg = SystemMessage(content=self.system_prompt)
        self.conversation_history.append(system_msg)
        
        # Контекст дати/часу
        from datetime import timezone
        now_utc = datetime.now(timezone.utc)
        context_info = f"""
## Контекст поточної дати та часу (Київ):
- Поточна дата та час: {format_datetime_display(now_utc, "%Y-%m-%d %H:%M:%S")}
- Поточна дата: {format_datetime_display(now_utc, "%Y-%m-%d")}
- Поточний час: {format_datetime_display(now_utc, "%H:%M:%S")}

## Корисні діапазони дат (UTC для запитів):
- За останню добу (24 години): від {(now_utc - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній тиждень (7 днів): від {(now_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній місяць (30 днів): від {(now_utc - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}

## ВАЖЛИВО — формат дат у MongoDB:
- **prozorro_auctions**: поля auction_data.dateModified та auction_data.dateCreated зберігаються як РЯДКИ ISO 8601. У $match використовуй порівняння рядків, наприклад: "auction_data.dateModified": {{"$gte": "2026-02-06T00:00:00.000Z", "$lte": "2026-02-07T23:59:59.999Z"}}. НЕ використовуй {{"$date": "..."}} — це не спрацює.
- **olx_listings**: поле updated_at зберігається як BSON Date; можна використовувати "$gte": new Date(...) або в pipeline передавати у форматі ISO рядка — залежить від драйвера.
"""
        self.conversation_history.append(SystemMessage(content=context_info))
        
        # Загальні знання про дані в колекціях (профілювання: статистика, топ значень)
        try:
            knowledge_service = CollectionKnowledgeService()
            data_knowledge = knowledge_service.get_knowledge_for_agent(max_length=3500)
            if data_knowledge:
                self.conversation_history.append(SystemMessage(content=data_knowledge))
        except Exception as e:
            logger.debug("Не вдалося завантажити знання про колекції: %s", e)
        
        # geo_assessment: профілі видів діяльності для оцінки придатності приміщення
        if route == "geo_assessment":
            try:
                import yaml
                from pathlib import Path
                profiles_path = Path(__file__).resolve().parents[2] / "config" / "business_profiles.yaml"
                if profiles_path.exists():
                    with open(profiles_path, "r", encoding="utf-8") as f:
                        profiles = yaml.safe_load(f) or {}
                    lines = ["## Профілі видів діяльності (для search_nearby_places):"]
                    for key, p in profiles.items():
                        if isinstance(p, dict):
                            name = p.get("name", key)
                            pois = p.get("poi_criteria", [])
                            types_str = ", ".join(str(x.get("type", "")) for x in pois if isinstance(x, dict))
                            lines.append(f"- {key}: {name} — типи POI: {types_str}")
                    if len(lines) > 1:
                        self.conversation_history.append(SystemMessage(content="\n".join(lines)))
            except Exception as e:
                logger.debug("Не вдалося завантажити business_profiles: %s", e)
        # listing_detail: підказка використовувати get_listing_details та geocode_address для визначення локації
        if route == "listing_detail":
            hint = SystemMessage(
                content="Для визначення місцезнаходження оголошення: 1) викликай get_listing_details (без аргументів — поточне оголошення), "
                "2) з поля summary/description та location_raw витягни топоніми (населені пункти, області, орієнтири), "
                "3) викликай geocode_address для кожного топоніма, 4) інтерпретуй результати та сформулюй відповідь. Якщо точних координат немає — опиши приблизну локацію за наявними даними."
            )
            self.conversation_history.append(hint)
        
        # Саммарі попередньої частини розмови (ConversationSummaryMemory)
        if memory and memory.summary:
            summary_msg = SystemMessage(
                content="## Підсумок попередньої частини розмови:\n" + memory.summary
            )
            self.conversation_history.append(summary_msg)
        
        # Релевантні фрагменти з історії (VectorStoreRetrieverMemory-стиль)
        if memory and self._embed_fn:
            relevant = memory.retrieve_relevant(user_query, self._embed_fn, k=VECTOR_RETRIEVE_TOP_K)
            if relevant:
                rel_text = "\n\n".join(f"- {r[:500]}" for r in relevant)
                self.conversation_history.append(
                    SystemMessage(content="## Релевантні фрагменти з історії розмови:\n" + rel_text)
                )
        
        # Буфер останніх повідомлень (ConversationBufferMemory)
        if memory:
            self.conversation_history.extend(memory.get_buffer_messages())
        
        # Підказка: користувач відповідає на конкретне повідомлення (наприклад на файл/аналітику)
        if reply_to_text:
            reply_hint = SystemMessage(
                content=f"Користувач відповідає на це повідомлення асистента:\n\"\"\"\n{reply_to_text[:2000]}\n\"\"\"\n"
                "Враховуй це як контекст: запит може стосуватися додаткової обробки, змін до запиту або уточнень щодо наданого результату."
            )
            self.conversation_history.append(reply_hint)

        # Предзапит: контекст оголошення — сильний фокус на конкретному об'єкті + попередній аналіз використання
        if listing_context and isinstance(listing_context, dict):
            page_url = listing_context.get("page_url") or ""
            summary = listing_context.get("summary") or ""
            usage_analysis = listing_context.get("_usage_analysis")
            if page_url or summary or usage_analysis:
                parts = [
                    "УВАГА: Розмова ведеться про КОНКРЕТНЕ оголошення. Усі відповіді, аналітика та рекомендації мають стосуватися САМЕ цього об'єкта.",
                    "Не змінюй фокус на інші оголошення. Якщо користувач питає «а яка ціна?», «що з оточенням?» — май на увазі це оголошення.",
                    "Фрази «оголошення», «в оголошенні», «місцезнаходження», «розташування», «адреса ділянки» стосуються САМЕ цього оголошення. Посилання вже надано — НЕ проси його. Для визначення локації використовуй інструмент get_listing_details (дані оголошення + опис), потім geocode_address для топонімів з опису.",
                ]
                if page_url:
                    parts.append(f"Посилання на оголошення: {page_url}")
                if summary:
                    parts.append(f"Короткий контекст: {summary}")
                if usage_analysis:
                    from business.services.property_usage_analysis_service import PropertyUsageAnalysisService
                    parts.append(PropertyUsageAnalysisService().format_analysis_for_llm(usage_analysis))
                listing_hint = SystemMessage(content="\n".join(parts))
                self.conversation_history.append(listing_hint)
        
        # Поточний запит
        user_msg = HumanMessage(content=user_query)
        self.conversation_history.append(user_msg)
        
        # LangGraph path: state machine з checkpoints (якщо use_langgraph=True)
        if getattr(self.settings, 'llm_agent_use_langgraph', False):
            try:
                from business.services.langgraph_agent_runner import build_agent_graph, LANGGRAPH_AVAILABLE
                if LANGGRAPH_AVAILABLE:
                    return self._run_langgraph_loop(
                        tools_for_request, user_id, user_query, chat_id,
                        req_id, log_ctx, start_time, stream_callback, thinking_callback,
                    )
            except Exception as e:
                logger.warning("%sLangGraph loop failed, falling back to while loop: %s", log_ctx, e)
        
        # Підказка при запиті на звіт/експорт за добу — щоб агент обов'язково викликав інструменти
        _q = (user_query or "").strip().lower()
        if any(phrase in _q for phrase in ("звіт за добу", "виведи звіт", "експорт за добу", "оголошення за добу", "звіт за день")):
            hint = SystemMessage(
                content="Запит виявлено як звіт або експорт за період. ОБОВ'ЯЗКОВО викликай інструменти: save_query_to_temp_collection (з filters за датою для prozorro_auctions та olx_listings), потім export_from_temp_collection для кожного temp_collection_id. Не відповідай лише текстом — користувач очікує файли Excel."
            )
            self.conversation_history.append(hint)
        
        # Явний цикл агента (plan → act → observe)
        iteration = 0
        max_iterations = getattr(self.settings, 'llm_agent_max_iterations', self.MAX_ITERATIONS)
        injected_final_hint = False
        injected_second_hint = False  # Повторна коротка підказка після другого порожнього відповіді

        while iteration < max_iterations:
            if time_budget_seconds and (time.perf_counter() - start_time) > time_budget_seconds:
                logger.warning("%sЧас вичерпано (time budget %s с). Зупинка.", log_ctx, time_budget_seconds)
                self._last_request_metrics = self._build_request_metrics(iteration, start_time, time_budget_exceeded=True)
                return "Час обробки запиту вичерпано. Спробуйте скоротити запит або повторити пізніше."
            iteration += 1
            logger.info("%s--- Ітерація %s/%s ---", log_ctx, iteration, max_iterations)

            try:
                # Bind tools до LLM асистента (thinking + grounding для Gemini)
                tools_to_bind = list(tools_for_request)
                # Google Search grounding несумісний з function tools у langchain-google:
                # https://github.com/langchain-ai/langchain-google/issues/1116
                # Додаємо лише коли немає інших tools (на практиці завжди є tools для агента)
                if (
                    getattr(self.settings, 'llm_agent_google_search_grounding', False)
                    and len(tools_for_request) == 0
                ):
                    tools_to_bind = tools_to_bind + [{"google_search": {}}]
                llm_with_tools = self.llm_assistant.bind_tools(tools_to_bind)
                
                # Отримуємо відповідь від LLM (з retry при тимчасових помилках)
                logger.info("%sВідправляю запит до LLM...", log_ctx)
                last_err = None
                response = None
                for attempt in range(AGENT_LLM_RETRY_ATTEMPTS + 1):
                    try:
                        response = llm_with_tools.invoke(self.conversation_history)
                        break
                    except Exception as e:
                        last_err = e
                        if attempt < AGENT_LLM_RETRY_ATTEMPTS and _is_transient_llm_error(e):
                            backoff = AGENT_LLM_RETRY_BACKOFF_SECONDS[min(attempt, len(AGENT_LLM_RETRY_BACKOFF_SECONDS) - 1)]
                            logger.warning("%sТимчасова помилка LLM (спроба %s): %s. Повтор через %.1f с.", log_ctx, attempt + 1, e, backoff)
                            time.sleep(backoff)
                        else:
                            raise
                else:
                    if last_err:
                        raise last_err
                if response is not None:
                    try:
                        usage = _extract_usage_from_aimessage(response)
                        meta = {"iteration": iteration, "request_id": req_id, "history_len": len(self.conversation_history)}
                        if usage:
                            meta["input_tokens"] = usage.get("input_tokens", 0)
                            meta["output_tokens"] = usage.get("output_tokens", 0)
                        self.logging_service.log_api_usage(
                            service="llm",
                            source="langchain_agent_main",
                            from_cache=False,
                            metadata=meta,
                        )
                        self.logging_service.log_llm_exchange(
                            request_text=_messages_to_request_text(self.conversation_history),
                            response_text=_aimessage_to_response_text(response),
                            input_tokens=meta.get("input_tokens", 0),
                            output_tokens=meta.get("output_tokens", 0),
                            source="langchain_agent_main",
                            request_id=req_id,
                            provider=(getattr(self.settings, "llm_assistant_provider", None) or "gemini"),
                        )
                    except Exception as e:
                        logger.warning("Не вдалося записати llm_exchange (main): %s", e)

                logger.info("%sОтримано відповідь від LLM. Тип: %s", log_ctx, type(response))
                
                # Витягуємо thinking (ход думок) для відображення
                if thinking_callback:
                    _extract_and_send_thinking(response, thinking_callback)
                
                # Додаємо відповідь до історії
                self.conversation_history.append(response)
                
                # Перевіряємо, чи є виклики tools
                # У LangChain tool_calls може бути списком або атрибутом об'єкта
                tool_calls = []
                if hasattr(response, 'tool_calls'):
                    tool_calls_raw = response.tool_calls
                    if tool_calls_raw:
                        tool_calls = tool_calls_raw if isinstance(tool_calls_raw, list) else [tool_calls_raw]
                
                logger.info(f"Знайдено викликів tools: {len(tool_calls)}")
                
                # Якщо немає викликів tools - перевіряємо, чи є текстова відповідь
                if not tool_calls:
                    # Перевіряємо, чи є текстова відповідь
                    response_content = None
                    if hasattr(response, 'content'):
                        response_content = response.content
                        if isinstance(response_content, list):
                            # Якщо content - список (наприклад блоки Gemini: {type:'text', text:'...'})
                            text_parts = []
                            for item in response_content:
                                if isinstance(item, str):
                                    text_parts.append(item)
                                elif isinstance(item, dict) and item.get("type") == "text" and "text" in item:
                                    text_parts.append(str(item["text"]))
                            response_content = "\n".join(text_parts) if text_parts else None
                        elif not isinstance(response_content, str):
                            response_content = str(response_content) if response_content else None
                    
                    if response_content and response_content.strip():
                        logger.info("%sЗнайдено фінальну текстову відповідь", log_ctx)
                        self._save_response_to_memory(user_id, user_query, response_content, getattr(self, "_current_chat_id", None))
                        self._last_request_metrics = self._build_request_metrics(iteration, start_time)
                        return response_content

                    # Порожня відповідь без викликів: підказка про фінальну відповідь (один раз, при повторі — коротша)
                    if not injected_final_hint:
                        injected_final_hint = True
                        hint_msg = HumanMessage(
                            content="Based on the tool results above, form a short final answer. The answer must be in Ukrainian, with numeric data (dates, values, units). Reply with text only, no tool calls."
                        )
                        self.conversation_history.append(hint_msg)
                        logger.info("%sДодано підказку про фінальну відповідь (порожня відповідь без tools)", log_ctx)
                        continue
                    # Повторна порожня відповідь після першої підказки — один раз додаємо коротшу підказку
                    logger.debug(
                        "%sПорожній content після підказки (iteration=%s), type=%s",
                        log_ctx, iteration, type(getattr(response, 'content', None)).__name__
                    )
                    if not injected_second_hint:
                        injected_second_hint = True
                        self.conversation_history.append(
                            HumanMessage(content="Give a short answer immediately, in Ukrainian, with numbers from the results above. Text only.")
                        )
                        logger.info("%sДодано повторну підказку (коротку)", log_ctx)
                    continue
                
                # Обмежуємо кількість викликів tools за ітерацію
                if len(tool_calls) > self.MAX_TOOL_CALLS_PER_ITERATION:
                    logger.warning(
                        f"Кількість викликів tools ({len(tool_calls)}) перевищує ліміт "
                        f"({self.MAX_TOOL_CALLS_PER_ITERATION}). Обмежую до {self.MAX_TOOL_CALLS_PER_ITERATION}."
                    )
                    tool_calls = tool_calls[:self.MAX_TOOL_CALLS_PER_ITERATION]
                
                # Виконуємо tools
                tool_messages = []
                for tool_call in tool_calls:
                    # Обробляємо різні формати tool_call
                    if isinstance(tool_call, dict):
                        tool_name = tool_call.get('name', '')
                        tool_args = tool_call.get('args', {})
                        tool_call_id = tool_call.get('id', '')
                    else:
                        # Якщо це об'єкт з атрибутами
                        tool_name = getattr(tool_call, 'name', '')
                        tool_args = getattr(tool_call, 'args', {})
                        tool_call_id = getattr(tool_call, 'id', '')
                    
                    logger.info(f"🔧 Виклик tool: {tool_name}")
                    logger.info(f"Аргументи: {json.dumps(tool_args, indent=2, ensure_ascii=False, default=str)[:500]}...")  # Обмежуємо довжину для читабельності
                    
                    # Знаходимо відповідний tool (серед дозволених для маршруту)
                    tool_func = None
                    for tool in tools_for_request:
                        if tool.name == tool_name:
                            tool_func = tool.func
                            break
                    
                    if not tool_func:
                        logger.error(f"Tool {tool_name} не знайдено")
                        tool_result = {'success': False, 'error': f'Tool {tool_name} не знайдено'}
                    else:
                        # Додаткова валідація та нормалізація для execute_query (query_builder очікує collection, filters, projection)
                        if tool_name == 'execute_query' and isinstance(tool_args, dict):
                            inner = tool_args.get('query', tool_args)
                            if not isinstance(inner, dict):
                                inner = {}
                            # Збираємо collection з різних місць (агент може передати collection_name або вкласти все в query)
                            coll = (
                                tool_args.get('collection')
                                or tool_args.get('collection_name')
                                or inner.get('collection')
                                or inner.get('collection_name')
                            )
                            # LLM часто передає "filter" замість "filters"
                            filters = inner.get('query') if 'query' in inner and isinstance(inner.get('query'), dict) else (inner.get('filters') or inner.get('filter') or {})
                            if not isinstance(filters, dict):
                                filters = {}
                            elif coll and 'collection' not in filters and 'collection_name' not in filters:
                                filters = {k: v for k, v in filters.items() if k not in ('project', 'projection', 'collection', 'collection_name', 'limit', 'filter')}
                            # limit не має потрапляти в filters ($match); передаємо окремо, за замовчуванням 100 для повноти вибірки/експорту
                            limit_val = inner.get('limit') or tool_args.get('limit')
                            if limit_val is not None and isinstance(limit_val, int) and limit_val > 0:
                                limit_val = min(limit_val, 100)
                            else:
                                limit_val = 100
                            proj_raw = tool_args.get('project') or tool_args.get('projection') or inner.get('project') or inner.get('projection')
                            if isinstance(proj_raw, dict):
                                proj_raw = [k for k, v in proj_raw.items() if v and k != '_id']
                            elif proj_raw is not None and not isinstance(proj_raw, list):
                                proj_raw = [proj_raw]
                            query_for_builder = {
                                'collection': coll or '',
                                'filters': filters if isinstance(filters, dict) else {},
                                'limit': limit_val,
                            }
                            if proj_raw is not None:
                                query_for_builder['projection'] = proj_raw if isinstance(proj_raw, list) else [proj_raw]
                            if not query_for_builder['collection']:
                                tool_result = {'success': False, 'error': "Поле 'collection' є обов'язковим. Передай collection або collection_name: prozorro_auctions, olx_listings, llm_cache."}
                            else:
                                query_str = json.dumps(query_for_builder, default=str)
                                if '$regex' in query_str.lower():
                                    logger.warning("Виявлено спробу використати $regex оператор (заборонено)")
                                    tool_result = {'success': False, 'error': '$regex заборонений. Використовуй execute_analytics для фільтрації за статусом.'}
                                else:
                                    try:
                                        tool_result = tool_func(query_for_builder)
                                    except Exception as e:
                                        logger.exception(f"Помилка виконання tool {tool_name}: {e}")
                                        tool_result = {'success': False, 'error': str(e)}
                        else:
                            # Викликаємо tool
                            try:
                                # Обробляємо аргументи для tools, що приймають словники
                                if tool_name in ['execute_analytics']:
                                    # Ці tools приймають словник як перший аргумент
                                    if isinstance(tool_args, dict):
                                        if 'query' in tool_args:
                                            tool_result = tool_func(tool_args['query'])
                                        else:
                                            tool_result = tool_func(tool_args)
                                    else:
                                        tool_result = tool_func(tool_args)
                                elif tool_name == 'execute_aggregation':
                                    # execute_aggregation приймає collection_name, pipeline, limit
                                    if isinstance(tool_args, dict):
                                        collection_name = tool_args.get('collection_name', '')
                                        pipeline = tool_args.get('pipeline', [])
                                        limit = tool_args.get('limit', None)
                                        tool_result = tool_func(collection_name, pipeline, limit)
                                    else:
                                        tool_result = {'success': False, 'error': 'Invalid arguments for execute_aggregation'}
                                elif tool_name == 'generate_report':
                                    # generate_report приймає request та return_base64
                                    if isinstance(tool_args, dict):
                                        request = tool_args.get('request', tool_args)
                                        return_base64 = tool_args.get('return_base64', True)
                                        tool_result = tool_func(request, return_base64)
                                    else:
                                        tool_result = tool_func(tool_args, True)
                                elif tool_name == 'save_query_results_to_excel':
                                    # save_query_results_to_excel приймає results, columns, column_headers, filename
                                    if isinstance(tool_args, dict):
                                        # Перевіряємо, чи results передано як список
                                        if 'results' in tool_args and not isinstance(tool_args['results'], list):
                                            logger.warning(f"results не є списком: {type(tool_args['results'])}")
                                            # Спробуємо конвертувати в список, якщо це можливо
                                            if isinstance(tool_args['results'], str):
                                                try:
                                                    tool_args['results'] = json.loads(tool_args['results'])
                                                except json.JSONDecodeError:
                                                    pass
                                        tool_result = tool_func(**tool_args)
                                    else:
                                        logger.error(f"Invalid arguments type for save_query_results_to_excel: {type(tool_args)}")
                                        tool_result = {'success': False, 'error': f'Invalid arguments for save_query_results_to_excel: expected dict, got {type(tool_args)}'}
                                elif tool_name == 'export_listings_to_file':
                                    if isinstance(tool_args, dict):
                                        ids = tool_args.get('ids', [])
                                        collection = tool_args.get('collection', '')
                                        tool_result = tool_func(
                                            ids=ids,
                                            collection=collection,
                                            format=tool_args.get('format', 'xlsx'),
                                            columns=tool_args.get('columns'),
                                            column_headers=tool_args.get('column_headers'),
                                            filename_prefix=tool_args.get('filename_prefix'),
                                        )
                                    else:
                                        tool_result = {'success': False, 'error': 'Invalid arguments for export_listings_to_file: expected dict'}
                                elif tool_name == 'save_query_to_temp_collection':
                                    if isinstance(tool_args, dict):
                                        query = tool_args.get('query', tool_args)
                                        tool_result = tool_func(query)
                                    else:
                                        tool_result = {'success': False, 'error': 'Invalid arguments: expected dict with query'}
                                elif tool_name == 'export_from_temp_collection':
                                    if isinstance(tool_args, dict):
                                        tool_result = tool_func(
                                            temp_collection_id=tool_args.get('temp_collection_id', ''),
                                            format=tool_args.get('format', 'xlsx'),
                                            filename_prefix=tool_args.get('filename_prefix'),
                                        )
                                    else:
                                        tool_result = {'success': False, 'error': 'Invalid arguments for export_from_temp_collection: expected dict'}
                                else:
                                    # Інші tools приймають keyword arguments
                                    if isinstance(tool_args, dict):
                                        tool_result = tool_func(**tool_args)
                                    else:
                                        tool_result = tool_func(tool_args)
                            except Exception as e:
                                logger.exception(f"Помилка виконання tool {tool_name}: {e}")
                                tool_result = {'success': False, 'error': str(e)}
                    
                    logger.info(f"Результат tool {tool_name}: success={tool_result.get('success', False)}")
                    # Метрики recovery: підрахунок помилок та відстеження спроби іншого підходу
                    if isinstance(tool_result, dict) and tool_result.get('success') is False:
                        self._tool_failures_this_request = getattr(self, '_tool_failures_this_request', 0) + 1
                    else:
                        if getattr(self, '_tool_failures_this_request', 0) > 0:
                            self._had_tool_failure_before_success = True
                    
                    # Логуємо, якщо отримано результати для Excel / експорту
                    if tool_name in ['execute_query', 'execute_aggregation'] and tool_result.get('success'):
                        data_count = 0
                        results_data = []
                        if 'data' in tool_result:
                            results_data = tool_result.get('data', [])
                            data_count = len(results_data)
                        elif 'results' in tool_result:
                            results_data = tool_result.get('results', [])
                            data_count = len(results_data)
                        
                        if data_count > 0:
                            user_wants_file = self._user_asked_for_file(getattr(self, "current_user_query", None))
                            if user_wants_file:
                                logger.warning(f"⚠️⚠️⚠️ ОТРИМАНО {data_count} РЕЗУЛЬТАТІВ З {tool_name}! Користувач просив файл — викликай export_listings_to_file.")
                            else:
                                logger.info(f"ℹ️ Отримано {data_count} результатів з {tool_name}. Користувач не просив файл — дай текстову відповідь.")
                            if isinstance(tool_result, dict):
                                tool_result = dict(tool_result)
                                coll = (tool_args.get("collection_name") or tool_args.get("collection") or (tool_args.get("query") or {}).get("collection", "")) if isinstance(tool_args, dict) else ""
                                id_field = "auction_id" if coll == "prozorro_auctions" else "url"
                                if user_wants_file:
                                    tool_result["_agent_hint"] = (
                                        f"Користувач просив файл Excel. ОБОВ'ЯЗКОВО викликай export_listings_to_file у НАСТУПНІЙ ітерації: "
                                        f"ids = список значень поля '{id_field}' з цих результатів (data/results), collection = '{coll or 'prozorro_auctions або olx_listings'}', "
                                        "НЕ передавай columns — буде стандартний формат. Файл відправиться користувачу автоматично."
                                    )
                                    logger.info("ℹ️ Додано підказку: викликати export_listings_to_file з ids з результатів.")
                                else:
                                    tool_result["_agent_hint"] = (
                                        "The user asked only to show/display data, not a file. Based on the results above give a final text answer in Ukrainian: "
                                        "short list or summary (titles, prices, links — whatever is in the data). Do not call export_listings_to_file."
                                    )
                        else:
                            logger.info(f"ℹ️ {tool_name} повернув 0 результатів. Можливо, потрібно перевірити фільтри або використати execute_analytics для перевірки наявності даних.")
                            user_wants_file = self._user_asked_for_file(getattr(self, "current_user_query", None))
                            coll = ""
                            if isinstance(tool_args, dict):
                                coll = (tool_args.get("collection_name") or tool_args.get("collection") or
                                        (tool_args.get("query") or {}).get("collection", "")) or ""
                            if isinstance(tool_result, dict):
                                tool_result = dict(tool_result)
                                hint_parts = []
                                if user_wants_file and coll in ("prozorro_auctions", "olx_listings"):
                                    hint_parts.append(
                                        f"Користувач просив файл Excel. Запит повернув 0 записів. "
                                        f"Викликай export_listings_to_file з ids=[] та collection={coll}, щоб надіслати порожній файл (файл відправиться автоматично)."
                                    )
                                # Retry при порожніх результатах: підказка про альтернативну колекцію (ProZorro↔OLX)
                                if not hint_parts or not user_wants_file:
                                    if coll == "prozorro_auctions":
                                        hint_parts.append(
                                            "По ProZorro результатів 0. Спробуй execute_query/execute_aggregation з collection: 'olx_listings' "
                                            "(ті ж фільтри, updated_at замість auction_data.dateModified) або execute_analytics з collection: 'olx_listings'."
                                        )
                                    elif coll == "olx_listings":
                                        hint_parts.append(
                                            "По OLX результатів 0. Спробуй execute_query/execute_aggregation з collection: 'prozorro_auctions' "
                                            "(auction_data.dateModified — рядки ISO 8601) або execute_analytics з collection: 'prozorro_auctions'."
                                        )
                                    elif coll == "unified_listings":
                                        hint_parts.append(
                                            "По unified_listings результатів 0. Спробуй окремо prozorro_auctions та olx_listings "
                                            "(execute_query/execute_aggregation або execute_analytics для кожної колекції)."
                                        )
                                if hint_parts:
                                    tool_result["_agent_hint"] = (tool_result.get("_agent_hint") or "") + " " + " ".join(hint_parts)
                    
                    # Логуємо результати execute_analytics та підказка агенту
                    if tool_name == 'execute_analytics' and tool_result.get('success'):
                        result_data = tool_result.get('results', tool_result.get('data', []))
                        total_count = tool_result.get('total_count', tool_result.get('count', 0))
                        logger.info(f"ℹ️ execute_analytics результат: total_count={total_count}, data records={len(result_data) if result_data else 0}")
                        if total_count > 0 or (result_data and len(result_data) > 0):
                            logger.info(
                                f"ℹ️ execute_analytics повернув {total_count if total_count > 0 else len(result_data)} записів. "
                                "Якщо користувач просив файл — викликай export; інакше підсумуй результати текстом з числами."
                            )
                            # Підказка: для запиту «аналітику» цих результатів достатньо — сформуй текстову відповідь
                            if isinstance(tool_result, dict) and '_agent_hint' not in tool_result:
                                tool_result = dict(tool_result)
                                tool_result['_agent_hint'] = (
                                    "ІНСТРУКЦІЯ: Це вже готова аналітика (згруповані дані). Підсумуй результати користувачу текстом: дата, значення, одиниця (наприклад грн/м²). "
                                    "Не викликай інші інструменти — наступним повідомленням дай лише фінальну текстову відповідь з числами."
                                )
                    
                    # Обмежуємо розмір результату в контексті (уникнення перевищення ліміту токенів LLM)
                    TRUNCATE_RESULTS_THRESHOLD = 20
                    if tool_name in ('execute_query', 'execute_aggregation') and tool_result.get('success'):
                        data_list = tool_result.get('results') or tool_result.get('data') or []
                        if len(data_list) > TRUNCATE_RESULTS_THRESHOLD:
                            id_field = "auction_id" if (tool_args.get("collection") or tool_args.get("collection_name") or "") == "prozorro_auctions" else "url"
                            ids_for_export = []
                            for r in data_list:
                                if isinstance(r, dict):
                                    vid = r.get(id_field) or r.get("_id") or r.get("url")
                                    if vid is not None:
                                        ids_for_export.append(str(vid))
                            hint = (tool_result.get("_agent_hint") or "") + " Використовуй ids=ids_for_export з цього результату."
                            truncated = {
                                "success": True,
                                "count": len(data_list),
                                "ids_for_export": ids_for_export,
                                "_agent_hint": hint.strip(),
                            }
                            logger.info(f"ℹ️ Результат обрізано: передано count={len(data_list)} та ids_for_export ({len(ids_for_export)} id) замість повних записів")
                            tool_result = truncated
                    
                    # Error recovery hint: при success=false додаємо інструкцію само-корекції (PALADIN, Reflexion)
                    if isinstance(tool_result, dict) and tool_result.get('success') is False:
                        error_hint = (
                            "[ПОМИЛКА ІНСТРУМЕНТУ] Результат невдалий. Проаналізуй причину "
                            "(невірні параметри, порожні дані, заборонені операції) і спробуй інший підхід: "
                            "інший інструмент, інші фільтри або зміна стратегії запиту.\n\n"
                        )
                        existing = tool_result.get('_agent_hint', '')
                        tool_result = dict(tool_result)
                        tool_result['_agent_hint'] = (error_hint + existing).strip() if existing else error_hint.strip()

                    # Створюємо ToolMessage для LangChain: підказку (_agent_hint) виводимо першою, щоб агент її врахував
                    if isinstance(tool_result, dict) and tool_result.get('_agent_hint'):
                        hint_text = tool_result.get('_agent_hint', '')
                        content_str = hint_text + '\n\n--- Результат ---\n' + json.dumps(tool_result, ensure_ascii=False, default=str)
                    else:
                        content_str = json.dumps(tool_result, ensure_ascii=False, default=str)
                    tool_message = ToolMessage(
                        content=content_str,
                        tool_call_id=tool_call_id
                    )
                    tool_messages.append(tool_message)
                
                # Додаємо результати tools до історії
                self.conversation_history.extend(tool_messages)
                
                # Продовжуємо цикл для отримання наступної відповіді
                continue
                
            except Exception as e:
                logger.exception("%sПомилка в ітерації %s: %s", log_ctx, iteration, e)
                self._last_request_metrics = self._build_request_metrics(iteration, start_time)
                error_msg = f"Помилка обробки запиту: {str(e)}"
                return error_msg
        
        # Якщо досягнуто максимум ітерацій
        logger.warning("%sДосягнуто максимум ітерацій (%s)", log_ctx, max_iterations)
        
        # Спробуємо отримати фінальну відповідь БЕЗ tools (щоб LLM не міг викликати інструменти, лише текст)
        try:
            fallback_messages = list(self.conversation_history)
            fallback_messages.append(HumanMessage(
                content="[СИСТЕМА] Досягнуто ліміт ітерацій. На основі наявних даних у цій бесіді дай фінальну відповідь користувачу текстом: перелік з посиланнями (URL), короткий опис кожного. НЕ викликай інструменти."
            ))
            try:
                last_err_fb = None
                final_response = None
                for attempt in range(AGENT_LLM_RETRY_ATTEMPTS + 1):
                    try:
                        final_response = self.llm.invoke(fallback_messages)
                        break
                    except Exception as e:
                        last_err_fb = e
                        if attempt < AGENT_LLM_RETRY_ATTEMPTS and _is_transient_llm_error(e):
                            backoff = AGENT_LLM_RETRY_BACKOFF_SECONDS[min(attempt, len(AGENT_LLM_RETRY_BACKOFF_SECONDS) - 1)]
                            logger.warning("%sТимчасова помилка LLM при фінальній відповіді (спроба %s): %s. Повтор через %.1f с.", log_ctx, attempt + 1, e, backoff)
                            time.sleep(backoff)
                        else:
                            raise
                else:
                    if last_err_fb:
                        raise last_err_fb
                if final_response is not None:
                    try:
                        usage = _extract_usage_from_aimessage(final_response)
                        meta = {"request_id": req_id, "iteration": iteration}
                        if usage:
                            meta["input_tokens"] = usage.get("input_tokens", 0)
                            meta["output_tokens"] = usage.get("output_tokens", 0)
                        self.logging_service.log_api_usage(
                            service="llm",
                            source="langchain_agent_fallback",
                            from_cache=False,
                            metadata=meta,
                        )
                        self.logging_service.log_llm_exchange(
                            request_text=_messages_to_request_text(fallback_messages),
                            response_text=_aimessage_to_response_text(final_response),
                            input_tokens=meta.get("input_tokens", 0),
                            output_tokens=meta.get("output_tokens", 0),
                            source="langchain_agent_fallback",
                            request_id=req_id,
                            provider=(getattr(self.settings, "llm_assistant_provider", None) or "gemini"),
                        )
                    except Exception as e:
                        logger.warning("Не вдалося записати llm_exchange (fallback): %s", e)
            except Exception as e:
                logger.debug("Fallback LLM invoke: %s", e)

            if final_response and hasattr(final_response, 'content') and final_response.content:
                fc = final_response.content
                if isinstance(fc, str):
                    final_str = fc
                elif isinstance(fc, list):
                    parts = []
                    for x in fc:
                        if isinstance(x, str):
                            parts.append(x)
                        elif isinstance(x, dict) and x.get("type") == "text" and "text" in x:
                            parts.append(str(x["text"]))
                    final_str = "\n".join(parts) if parts else ""
                else:
                    final_str = str(fc)
                if final_str.strip():
                    self._save_response_to_memory(user_id, user_query, final_str, chat_id)
                self._last_request_metrics = self._build_request_metrics(iteration, start_time)
                return final_str
            else:
                self._last_request_metrics = self._build_request_metrics(iteration, start_time)
                return "Не вдалося сформувати відповідь після максимальної кількості ітерацій."
        except Exception as e:
            logger.exception("%sПомилка формування фінальної відповіді: %s", log_ctx, e)
            self._last_request_metrics = self._build_request_metrics(iteration, start_time)
            return f"Помилка формування відповіді: {str(e)}"
