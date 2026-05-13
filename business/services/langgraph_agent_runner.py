# -*- coding: utf-8 -*-
"""
LangGraph-оркестрація для AI-агента: state machine з checkpoints.

Розширення cognitive-agent:
- explicit cognitive state у графі;
- вузол reflection (метакогніція);
- після tools — умовний перехід на reflect або назад до agent.
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
    Будує LangGraph StateGraph для агента з tools + cognitive state + reflection.

    Args:
        service: LangChainAgentService — run_tool, llm_assistant, налаштування когніції
        tools: список LangChain tools для bind
        max_iterations: макс. ітерацій циклу agent (LLM викликів)
    """
    if not LANGGRAPH_AVAILABLE:
        raise ImportError("langgraph не встановлено. pip install langgraph")

    from typing import Annotated, TypedDict

    class AgentState(TypedDict, total=False):
        messages: Annotated[List, add_messages]
        iteration: int
        cognitive: Dict[str, Any]
        tool_rounds: int
        last_chain_step_id: str

    settings = service.settings
    cognitive_on = bool(getattr(settings, "llm_agent_cognitive_enabled", True))
    reflect_on = bool(getattr(settings, "llm_agent_reflection_enabled", True))
    every_n = max(1, int(getattr(settings, "llm_agent_reflection_every_n", 3) or 3))
    reflect_fail = bool(getattr(settings, "llm_agent_reflect_on_tool_failure", True))

    llm_with_tools = service.llm_assistant.bind_tools(tools)

    def agent_node(state: AgentState) -> Dict[str, Any]:
        iteration = int(state.get("iteration") or 0) + 1
        messages = state["messages"]
        invoke_tail = list(messages)
        if cognitive_on:
            try:
                from business.services.cognitive_runtime_service import format_cognitive_for_prompt

                cog = state.get("cognitive") or {}
                block = format_cognitive_for_prompt(cog)
                if block:
                    invoke_tail = [SystemMessage(content=block)] + invoke_tail
            except Exception as e:
                logger.debug("cognitive prompt block: %s", e)
        response = llm_with_tools.invoke(invoke_tail)
        # Оновлення cognitive після «думки» LLM (без зміни фактів з tools)
        cog_out = dict(state.get("cognitive") or {})
        if cognitive_on:
            try:
                from business.services.cognitive_runtime_service import merge_after_agent_turn

                txt = ""
                if hasattr(response, "content"):
                    c = response.content
                    if isinstance(c, str):
                        txt = c
                    elif isinstance(c, list):
                        txt = " ".join(
                            str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in c if x
                        )
                has_tc = bool(getattr(response, "tool_calls", None))
                cog_out = merge_after_agent_turn(cog_out, assistant_text_excerpt=txt, has_tool_calls=has_tc)
            except Exception as e:
                logger.debug("merge_after_agent_turn: %s", e)
        return {"messages": [response], "iteration": iteration, "cognitive": cog_out}

    def tool_node(state: AgentState) -> Dict[str, Any]:
        last_msg = state["messages"][-1]
        if not hasattr(last_msg, "tool_calls") or not last_msg.tool_calls:
            return {"messages": []}
        tool_messages = []
        summaries = []
        cognitive_in = dict(state.get("cognitive") or {})
        for tc in last_msg.tool_calls:
            name = tc.get("name", "") if isinstance(tc, dict) else getattr(tc, "name", "")
            args = tc.get("args", {}) if isinstance(tc, dict) else getattr(tc, "args", {})
            tid = tc.get("id", "") if isinstance(tc, dict) else getattr(tc, "id", "")
            result = service.run_tool(name, args)
            if isinstance(result, dict) and result.get("success") is False:
                service._tool_failures_this_request = getattr(service, "_tool_failures_this_request", 0) + 1
            else:
                if getattr(service, "_tool_failures_this_request", 0) > 0:
                    service._had_tool_failure_before_success = True
            ok = not (isinstance(result, dict) and result.get("success") is False)
            if cognitive_on:
                try:
                    from business.services.cognitive_runtime_service import summarize_tool_result_for_chain

                    summaries.append((name, ok, summarize_tool_result_for_chain(result)))
                except Exception:
                    summaries.append((name, ok, str(result)[:400]))
            if isinstance(result, dict) and result.get("success") is False:
                err_hint = (
                    "[ПОМИЛКА ІНСТРУМЕНТУ] Результат невдалий. Проаналізуй причину "
                    "і спробуй інший підхід: інший інструмент, інші фільтри.\n\n"
                )
                result = dict(result)
                result["_agent_hint"] = (result.get("_agent_hint") or "") + err_hint
            if isinstance(result, dict) and result.get("_agent_hint"):
                content = result["_agent_hint"] + "\n\n--- Результат ---\n" + json.dumps(
                    result, ensure_ascii=False, default=str
                )
            else:
                content = (
                    json.dumps(result, ensure_ascii=False, default=str)
                    if isinstance(result, dict)
                    else str(result)
                )
            tool_messages.append(ToolMessage(content=content, tool_call_id=tid))
        cognitive_out = cognitive_in
        chain_sid: Optional[str] = None
        if cognitive_on and summaries:
            cognitive_out, chain_sid = service._cognitive_merge_after_tools_langgraph(cognitive_in, summaries, dict(state))
        tr = int(state.get("tool_rounds") or 0) + 1
        out: Dict[str, Any] = {
            "messages": tool_messages,
            "cognitive": cognitive_out,
            "tool_rounds": tr,
        }
        if chain_sid:
            out["last_chain_step_id"] = chain_sid
        return out

    def reflect_node(state: AgentState) -> Dict[str, Any]:
        if not reflect_on:
            return {"messages": []}
        text = service._cognitive_build_reflection_message(state.get("messages", []), state.get("cognitive") or {})
        if not text:
            return {"messages": []}
        return {"messages": [HumanMessage(content=text)]}

    def should_continue(state: AgentState) -> Literal["tools", "__end__"]:
        if state.get("iteration", 0) >= max_iterations:
            return "__end__"
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return "__end__"

    def route_after_tools(state: AgentState) -> Literal["reflect", "agent"]:
        if not reflect_on:
            return "agent"
        tr = int(state.get("tool_rounds") or 0)
        cog = state.get("cognitive") or {}
        failed = bool(cog.get("last_tool_failed"))
        if reflect_fail and failed:
            return "reflect"
        if tr > 0 and tr % every_n == 0:
            return "reflect"
        return "agent"

    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_node("reflect", reflect_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "__end__": END})
    graph.add_conditional_edges("tools", route_after_tools, {"reflect": "reflect", "agent": "agent"})
    graph.add_edge("reflect", "agent")
    checkpointer = MemorySaver() if MemorySaver else None
    return graph.compile(checkpointer=checkpointer)
