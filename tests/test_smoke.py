# -*- coding: utf-8 -*-
"""冒烟测试：核心链路（建表/种子/工具/多 Agent 编排/报告契约）不依赖真实 LLM（Mock 模式）。
用 asyncio.run 直接驱动异步逻辑，避免 pytest-asyncio 版本兼容问题。
"""
import asyncio
import json
from unittest.mock import AsyncMock, patch

from backend import db, seed, agent, llm


def _run(coro):
    return asyncio.run(coro)


def test_seed_and_profile():
    _run(db.init_db())
    _run(seed.seed())
    profile = _run(db.get_profile())
    assert profile and profile["username"] == "demo"
    food = _run(db.find_food("鸡胸"))
    assert food and food["food_name"] == "鸡胸肉"
    assert food["kcal"] == 133


def test_sum_nutrition():
    _run(db.init_db())
    foods = [{"name": "米饭", "amount_g": 150}, {"name": "鸡胸肉", "amount_g": 120}, {"name": "西兰花", "amount_g": 100}]
    items, totals = _run(agent._sum_nutrition(foods))
    assert len(items) == 3
    assert totals["total_kcal"] == 367.6
    assert totals["protein"] == 37.5


def test_health_score():
    foods = [{"name": "薯片", "amount_g": 100}, {"name": "可乐", "amount_g": 500}]
    nutrition = {"total_kcal": 800, "protein": 5, "fat": 40, "carbs": 80, "sodium": 600}
    checked = agent._calc_health_score(foods, nutrition, meal_kcal=600)
    assert checked["score"] < 60  # 热量超标 + 无蔬菜 + 垃圾食品，必须明显扣分
    assert any("热量" in r for r in checked["reasons"])


def test_full_graph_mock():
    _run(db.init_db())
    _run(seed.seed())
    state = {
        "username": "demo", "meal_type": "午餐", "image_path": "",
        "image_bytes": b"fake", "constraint": "重点看蛋白质够不够", "foods": [],
        "nutrition": {}, "health": {}, "profile": {}, "daily": {}, "report": {}, "agent_round": 0,
        "agent": "", "next_agents": [], "critique": {}, "review_count": 0, "handoff": {}, "events": [],
    }
    final = _run(agent.graph.ainvoke(state, {"recursion_limit": 30}))
    report = final["report"]
    # 报告契约 8 字段
    for key in ("title", "summary", "metrics", "charts", "insights", "actions", "evidence", "constraint_answers"):
        assert key in report, f"报告缺少 {key}"
    assert report["constraint_answers"][0]["question"] == state["constraint"]
    # 事件顺序完整
    events = [e["event"] for e in final["events"]]
    assert events[0] == "plan"
    assert "report" in events
    assert events[-1] == "done"
    assert final["critique"]["passed"] is True
    assert final["agent_round"] >= 3
    assert final["profile"] and final["daily"] and final["handoff"] == {}
    # 已落库
    meals = _run(db.list_meals())
    assert len(meals) >= 1


def test_agent_falls_back_to_missing_tool():
    state = {
        "agent": "health", "foods": [{"name": "鸡胸肉", "amount_g": 120}],
        "nutrition": {}, "health": {}, "profile": {"goal": "增肌"}, "daily": {"per_meal_kcal": 600},
        "constraint": "", "events": [],
    }
    with patch.object(llm.config, "LLM_MOCK_MODE", False), patch.object(
        llm, "chat_text", new=AsyncMock(return_value='{"action":"handoff","agent":"nutrition"}')
    ):
        result = _run(agent.run_agent(state))
    assert result["health"]["score"] >= 0
    assert not any(result["handoff"].values())
