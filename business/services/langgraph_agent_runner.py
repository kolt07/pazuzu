# -*- coding: utf-8 -*-
"""
LangGraph-оркестрація для AI-агента: state machine з checkpoints.

Використовується коли llm_agent_use_langgraph=True. Надає:
- Чітку state machine структуру (agent -> tools -> agent)
- Підтримку checkpoints для durable execution
- Можливість human-in-the-loop та time-travel debugging
"""

import json
import logging
from typing import Any, Dict, List, Literal, Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

logger = logging.getLogger(__name__)

LANGGRAPH_AVAILABLE = False
try:
    from langgraph.graph import StateGraph, START, END
    from langgraph.graph.message import add_messages
    from langgraph.checkpoint.memory import MemorySaver
    LANGGRAPH_AVAILABLE = True
except ImportError:
    StateGraph = None  # type: ignore
    START = None  # type: ignore
    END = None  # type: ignore
    add_messages = None  # type: ignore
    MemorySaver = None  # type: ignore


def build_agent_graph(service: Any, tools: List[Any], max_iterations: int = 10):
    """
    Будує LangGraph StateGraph для агента з tools.
    
    Args:
        service: LangChainAgentService — для run_tool та доступу до llm_assistant
        tools: список LangChain tools для bind
        max_iterations: макс. ітерацій циклу agent->tools->agent
        
    Returns:
        Скомпільований graph з опційним checkpointer
    """
    if not LANGGRAPH_AVAILABLE:
        raise ImportError("langgraph не встановлено. pip install langgraph")

    from typing import Annotated, TypedDict

    class AgentState(TypedDict):
        messages: Annotated[List, add_messages]
        iteration: int

    llm_with_tools = service.llm_assistant.bind_tools(tools)
    tools_by_name = {t.name: t for t in tools}

    def agent_node(state: AgentState) -> Dict[str, Any]:
        """LLM вирішує: tool_calls чи текстова відповідь."""
        iteration = state.get("iteration", 0) + 1
        messages = state["messages"]
        response = llm_with_tools.invoke(messages)
        return {"messages": [response], "iteration": iteration}

    def tool_node(state: AgentState) -> Dict[str, Any]:
        """Виконує tool_calls з останнього AIMessage."""
        last_msg = state["messages"][-1]
        if not hasattr(last_msg, "tool_calls") or not last_msg.tool_calls:
            return {"messages": []}
        tool_messages = []
        for tc in last_msg.tool_calls:
            name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
            args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
            tid = tc.get("id", "") if isinstance(tc, dict) else getattr(tc, "id", "")
            result = service.run_tool(name, args)
            # Метрики recovery
            if isinstance(result, dict) and result.get("success") is False:
                service._tool_failures_this_request = getattr(service, "_tool_failures_this_request", 0) + 1
            else:
                if getattr(service, "_tool_failures_this_request", 0) > 0:
                    service._had_tool_failure_before_success = True
            # Error recovery hint
            if isinstance(result, dict) and result.get("success") is False:
                err_hint = (
                    "[ПОМИЛКА ІНСТРУМЕНТУ] Результат невдалий. Проаналізуй причину "
                    "і спробуй інший підхід: інший інструмент, інші фільтри.\n\n"
                )
                result = dict(result)
                result["_agent_hint"] = (result.get("_agent_hint") or "") + err_hint
            if isinstance(result, dict) and result.get("_agent_hint"):
                content = result["_agent_hint"] + "\n\n--- Результат ---\n" + json.dumps(result, ensure_ascii=False, default=str)
            else:
                content = json.dumps(result, ensure_ascii=False, default=str) if isinstance(result, dict) else str(result)
            tool_messages.append(ToolMessage(content=content, tool_call_id=tid))
        return {"messages": tool_messages}

    def should_continue(state: AgentState) -> Literal["tools", "__end__"]:
        """Маршрутизація: tools чи кінець."""
        if state.get("iteration", 0) >= max_iterations:
            return "__end__"
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return "__end__"

    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "__end__": END})
    graph.add_edge("tools", "agent")
    checkpointer = MemorySaver() if MemorySaver else None
    return graph.compile(checkpointer=checkpointer)
