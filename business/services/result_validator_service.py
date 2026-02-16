# -*- coding: utf-8 -*-
"""
Сервіс валідації результатів виконання пайплайну.
Порівнює результати з очікуваннями користувача та виявляє невідповідності.
"""

import logging
from typing import Dict, Any, Optional, List
from business.services.llm_service import LLMService
from config.settings import Settings

logger = logging.getLogger(__name__)


class ResultValidatorService:
    """
    Сервіс для валідації результатів виконання пайплайну.
    Порівнює результати з очікуваннями користувача та виявляє невідповідності.
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm_service: Optional[LLMService] = None
    
    @property
    def llm_service(self) -> Optional[LLMService]:
        """Лінива ініціалізація LLM сервісу."""
        if self._llm_service is None:
            try:
                self._llm_service = LLMService(self.settings)
            except Exception as e:
                logger.warning("ResultValidatorService: LLM недоступний: %s", e)
        return self._llm_service
    
    def validate_results(
        self,
        results: List[Dict[str, Any]],
        query_structure: Dict[str, Any],
        user_query: str,
        pipeline_result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Валідує результати виконання пайплайну.
        
        Args:
            results: Результати виконання пайплайну
            query_structure: Структурний опис запиту
            user_query: Оригінальний запит користувача
            pipeline_result: Повний результат виконання пайплайну (опціонально)
        
        Returns:
            Словник з полями:
            - valid: чи результати валідні
            - issues: список проблем (якщо є)
            - should_retry: чи потрібно повторити виконання
            - retry_reason: причина повторного виконання (якщо є)
        """
        issues = []
        
        # Перевірка наявності результатів
        if not results:
            # Перевіряємо, чи очікувалися результати
            filter_metrics = query_structure.get("filter_metrics", {})
            if filter_metrics:
                issues.append({
                    "type": "no_results",
                    "message": "Результатів не знайдено, хоча вказані фільтри",
                    "severity": "warning"
                })

        # Логічна суперечність: geo-фільтр застосовано, документи є, але результатів 0
        if pipeline_result and not results:
            diag = pipeline_result.get("diagnostic_info") or {}
            total_docs = diag.get("total_documents_in_collection") or 0
            geo_applied = diag.get("geo_filter_applied") is True
            if total_docs > 0 and geo_applied:
                issues.append({
                    "type": "potential_filter_conflict",
                    "message": "Колекція містить документи, але гео-фільтр повернув 0 результатів. Можливий конфлікт фільтрів.",
                    "severity": "warning"
                })

        # Filter consistency: expected vs applied
        if pipeline_result:
            diag = pipeline_result.get("diagnostic_info") or {}
            applied_count = diag.get("applied_filters_count")
            filter_metrics = query_structure.get("filter_metrics", {})
            expected_count = len([k for k in filter_metrics if filter_metrics.get(k) is not None])
            if applied_count is not None and expected_count > applied_count:
                issues.append({
                    "type": "filter_loss",
                    "message": f"Втрата фільтрів: очікувалось {expected_count}, застосовано {applied_count}",
                    "severity": "error"
                })
        
        # Перевірка відповідності фільтрам
        filter_issues = self._check_filters_compliance(results, query_structure)
        issues.extend(filter_issues)
        
        # Перевірка наявності очікуваних метрик
        metric_issues = self._check_metrics_presence(results, query_structure)
        issues.extend(metric_issues)
        
        # Перевірка через LLM (якщо доступний)
        if self.llm_service and issues:
            llm_validation = self._validate_with_llm(results, user_query, query_structure)
            if llm_validation.get("issues"):
                issues.extend(llm_validation["issues"])
        
        # Визначаємо, чи потрібно повторити
        should_retry = False
        retry_reason = None

        critical_issues = [i for i in issues if i.get("severity") == "error"]
        potential_conflict = any(i.get("type") == "potential_filter_conflict" for i in issues)
        filter_loss = any(i.get("type") == "filter_loss" for i in issues)
        if critical_issues:
            should_retry = True
            retry_reason = critical_issues[0].get("message")
        elif potential_conflict:
            should_retry = True
            retry_reason = "Потенційний конфлікт гео-фільтрів — спроба fallback"
        elif filter_loss:
            should_retry = True
            retry_reason = "filter_loss"
        
        return {
            "valid": len(critical_issues) == 0,
            "issues": issues,
            "should_retry": should_retry,
            "retry_reason": retry_reason
        }
    
    def _check_filters_compliance(
        self,
        results: List[Dict[str, Any]],
        query_structure: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Перевіряє відповідність результатів фільтрам."""
        issues = []
        filter_metrics = query_structure.get("filter_metrics", {})
        
        if not filter_metrics or not results:
            return issues
        
        # Перевіряємо кожен фільтр
        for filter_key, filter_value in filter_metrics.items():
            if filter_key == "region":
                # Перевіряємо, чи немає заборонених регіонів
                if isinstance(filter_value, str) and filter_value.startswith("!"):
                    # Виключення регіону
                    excluded_region = filter_value[1:]
                    for result in results:
                        result_region = self._extract_region(result)
                        if result_region == excluded_region:
                            issues.append({
                                "type": "filter_violation",
                                "message": f"Знайдено заборонений регіон: {excluded_region}",
                                "severity": "error",
                                "filter": filter_key,
                                "value": excluded_region
                            })
                            break
        
        return issues
    
    def _check_metrics_presence(
        self,
        results: List[Dict[str, Any]],
        query_structure: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Перевіряє наявність очікуваних метрик у результатах."""
        issues = []
        response_metrics = query_structure.get("response_metrics", [])
        
        if not response_metrics or not results:
            return issues
        
        # Перевіряємо наявність метрик у першому результаті
        sample_result = results[0] if results else {}
        
        for metric in response_metrics:
            if metric not in sample_result:
                # Перевіряємо вкладені поля
                if not self._has_nested_field(sample_result, metric):
                    issues.append({
                        "type": "missing_metric",
                        "message": f"Метрика '{metric}' відсутня в результатах",
                        "severity": "warning",
                        "metric": metric
                    })
        
        return issues
    
    def _validate_with_llm(
        self,
        results: List[Dict[str, Any]],
        user_query: str,
        query_structure: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Валідує результати через LLM."""
        if not self.llm_service:
            return {"issues": []}
        
        try:
            prompt = self._build_validation_prompt(results, user_query, query_structure)
            
            response_text = self.llm_service.generate_text(
                prompt=prompt,
                system_prompt=None,
                temperature=0.2
            )
            
            # Парсимо відповідь — тільки перший JSON-об'єкт (LLM може повернути кілька або зайвий текст)
            import json
            start_idx = response_text.find("{")
            if start_idx == -1:
                return {"issues": []}
            decoder = json.JSONDecoder()
            validation_result, _ = decoder.raw_decode(response_text, start_idx)
            return validation_result
            
        except Exception as e:
            logger.exception("Помилка валідації через LLM: %s", e)
            return {"issues": []}
    
    def _build_validation_prompt(
        self,
        results: List[Dict[str, Any]],
        user_query: str,
        query_structure: Dict[str, Any]
    ) -> str:
        """Формує промпт для валідації через LLM."""
        import json
        from datetime import datetime, date
        
        def json_serializer(obj):
            """Custom JSON serializer для datetime та інших об'єктів."""
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        sample_results = results[:5]  # Беремо перші 5 результатів для аналізу
        
        prompt_parts = [
            "Проаналізуй результати виконання запиту та визнач, чи вони відповідають очікуванням користувача.",
            "",
            "## Запит користувача:",
            user_query,
            "",
            "## Структурний опис запиту:",
            json.dumps(query_structure, ensure_ascii=False, indent=2, default=json_serializer),
            "",
            "## Результати (перші 5):",
            json.dumps(sample_results, ensure_ascii=False, indent=2, default=json_serializer),
            "",
            "## Завдання:",
            "Визнач, чи результати відповідають запиту користувача.",
            "Перевір:",
            "- Чи є результати, які не повинні бути (наприклад, заборонені регіони)",
            "- Чи відсутні очікувані результати",
            "- Чи відповідають результати фільтрам",
            "",
            "Поверни JSON з полями:",
            "- valid: чи результати валідні (bool)",
            "- issues: список проблем (масив об'єктів з полями type, message, severity)",
            "- should_retry: чи потрібно повторити виконання (bool)",
            "- retry_reason: причина повторного виконання (string, опціонально)"
        ]
        
        return "\n".join(prompt_parts)
    
    def _extract_region(self, result: Dict[str, Any]) -> Optional[str]:
        """Витягує регіон з результату."""
        # Перевіряємо різні можливі шляхи
        if "region" in result:
            return result["region"]
        if "address_refs" in result:
            for ref in result.get("address_refs", []):
                if "region" in ref and "name" in ref["region"]:
                    return ref["region"]["name"]
        if "auction_data" in result and "address_refs" in result["auction_data"]:
            for ref in result["auction_data"].get("address_refs", []):
                if "region" in ref and "name" in ref["region"]:
                    return ref["region"]["name"]
        return None
    
    def _has_nested_field(self, obj: Dict[str, Any], field_path: str) -> bool:
        """Перевіряє наявність вкладеного поля."""
        parts = field_path.split(".")
        current = obj
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False
        
        return True
