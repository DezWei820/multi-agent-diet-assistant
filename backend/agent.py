# -*- coding: utf-8 -*-
"""Agent 层：LangGraph 自主多 Agent 编排。
流程：Supervisor 动态派发 → 专家自主执行工具 → Critic 质检返工 → 报告生成 → 记忆落库。
每个节点向 state.events 追加事件，main.py 通过 stream_mode="updates" 逐节点推送 SSE。
"""
import json
import operator
from datetime import datetime
from typing import Annotated, TypedDict
from langgraph.graph import StateGraph, END
from langgraph.types import Send

from . import db, llm
from .tools import get_food_nutrition, get_user_profile, calc_meal_target, calc_daily_target

# ---------- State ----------
def _merge_handoffs(old: dict, new: dict) -> dict:
    merged = {**(old or {}), **(new or {})}
    return {name: target for name, target in merged.items() if target}

class AgentState(TypedDict):
    username: str
    meal_type: str
    image_path: str
    image_bytes: bytes
    constraint: str            # 用户附加约束（如"只看蛋白质"）
    foods: list                # 识图结果 [{name, amount_g, note}]
    nutrition: dict            # 营养专家汇总
    health: dict               # 健康评估
    profile: dict              # 健康画像
    daily: dict                # 每日/每餐目标
    report: dict               # 最终报告
    agent_round: int           # Supervisor 调度轮次
    agent: str                 # 当前执行的专家
    next_agents: list          # 本轮派发的专家
    critique: dict             # Critic 质检结果
    review_count: int
    handoff: Annotated[dict, _merge_handoffs]  # 专家请求交接的 Agent
    events: Annotated[list, operator.add]

def _push(state: AgentState, event: str, data: dict) -> list:
    """构造一条事件，供节点返回追加"""
    return [{"event": event, "data": data}]

# ---------- 健康评分（确定性、可解释） ----------
def _calc_health_score(foods: list, nutrition: dict, meal_kcal: float) -> dict:
    """按规则给一餐打分并列出扣分点：热量/蛋白/钠/蔬菜/高油糖食物"""
    reasons = []
    score = 100
    kcal = nutrition.get("total_kcal", 0)
    # 热量偏离
    if meal_kcal > 0:
        ratio = kcal / meal_kcal
        if ratio > 1.3:
            score -= 20
            reasons.append(f"热量 {kcal:.0f} 千卡，超过本餐建议 {meal_kcal:.0f} 千卡的 30%")
        elif ratio < 0.5:
            score -= 10
            reasons.append(f"热量 {kcal:.0f} 千卡，不足本餐建议 {meal_kcal:.0f} 千卡的一半")
    # 蛋白质不足
    if nutrition.get("protein", 0) < 20:
        score -= 10
        reasons.append("蛋白质不足 20g，饱腹感与修复会打折扣")
    # 钠超标
    if nutrition.get("sodium", 0) > 800:
        score -= 10
        reasons.append(f"钠 {nutrition.get('sodium', 0):.0f}mg 偏高，注意控盐")
    # 无蔬菜
    veg_keywords = ("菜", "瓜", "番茄", "西红柿", "菌", "菇", "青椒", "胡萝卜", "菠菜", "生菜", "白菜",
                    "西兰花", "茄子", "芹", "葱", "蒜", "姜", "笋", "藕", "萝卜", "木耳", "海带",
                    "紫菜", "芦笋", "山药", "洋葱", "韭菜", "豆角")
    if not any(any(k in (f.get("name") or "") for k in veg_keywords) for f in foods):
        score -= 10
        reasons.append("这一餐没有蔬菜，建议搭配一份绿叶菜")
    # 高油高糖零食
    junk = ("薯片", "可乐", "奶茶", "蛋糕", "巧克力", "饼干", "啤酒", "白酒")
    junk_hits = [f.get("name") for f in foods if any(j in (f.get("name") or "") for j in junk)]
    if junk_hits:
        score -= min(20, 10 * len(junk_hits))
        reasons.append(f"含有 {', '.join(junk_hits)}，油/糖/酒精偏高")
    score = max(0, min(100, score))
    return {"score": score, "reasons": reasons}

def _required_agents(state: AgentState) -> list:
    """基础信息缺失时强制补齐，其余调度交给 Supervisor"""
    if not state.get("foods"):
        return ["vision"]
    return [name for name, key in (("nutrition", "nutrition"), ("health", "health")) if not state.get(key)]

# ---------- 节点 1：Supervisor 动态调度 ----------
async def supervisor_node(state: AgentState) -> dict:
    catalog = "\n".join(f"- {name}: {spec['desc']}；工具：{', '.join(spec['tools'])}" for name, spec in AGENT_SPECS.items())
    missing = _required_agents(state)
    ready = state.get("foods") and state.get("nutrition") and state.get("health")
    if missing or (ready and not state.get("critique")) or llm.config.LLM_MOCK_MODE or state.get("agent_round", 0) >= 8:
        agents = missing or ["synthesize"]
    else:
        prompt = (
            "你是饮食分析系统的 Supervisor，负责在多个专家 Agent 之间动态调度。\n"
            f"可选 Agent：\n{catalog}\n- synthesize: 汇总报告\n"
            f"当前结果：foods={bool(state.get('foods'))}, nutrition={bool(state.get('nutrition'))}, health={bool(state.get('health'))}\n"
            f"专家交接请求：{state.get('handoff') or '无'}\n"
            f"质检反馈：{json.dumps(state.get('critique') or {}, ensure_ascii=False)}\n"
            f"用户附加要求：{state.get('constraint') or '无'}\n"
            "基础信息缺失必须先补齐；有质检问题时派发对应 Agent 返工；信息完整且无问题时选择 synthesize。"
            '只输出 JSON：{"next":["Agent名"],"reason":"调度理由"}，本轮可选 1-3 个 Agent。'
        )
        plan = llm.extract_json(await llm.chat_text(prompt, system="你是严谨的多 Agent 调度器。"))
        allowed = set(AGENT_SPECS) | {"synthesize"}
        planned = plan.get("next") if isinstance(plan.get("next"), list) else []
        agents = [a for a in planned if a in allowed][:3]
        agents = missing or agents or ["synthesize"]
    requested = state.get("handoff") or {}
    source, target = next(((k, v) for k, v in reversed(requested.items()) if v), (None, None))
    if not missing and target in AGENT_SPECS:
        agents = [target]
    if "synthesize" in agents:
        agents = ["synthesize"]
    events = _push(state, "plan", {"intent": "analyze", "constraint": state.get("constraint", ""), "next": agents})
    if agents == ["synthesize"]:
        events += _push(state, "agent_status", {"stage": "gather", "msg": "基础分析完成，进入报告生成"})
    return {
        "next_agents": agents, "agent_round": state.get("agent_round", 0) + 1,
        "handoff": {source: ""} if source else {}, "events": events,
    }

# ---------- 节点 2：千问VL 识图 ----------
async def vision_node(state: AgentState) -> dict:
    constraint = state.get("constraint") or ""
    prompt = (
        "请识别这张饮食图片中的每一种食物。只输出 JSON："
        "{\"foods\": [{\"name\": \"食物名（常见叫法）\", \"amount_g\": 估算克数, \"note\": \"烹饪方式或备注\"}]}"
        "识别不到的模糊食物请用最接近的常见名称，不要编造不存在的食物。"
        f"用户附加要求：{constraint or '无'}。请重点核对与要求相关的可见信息，无法确认的在 note 中写明，不要臆测。"
    )
    result = await llm.vision_parse(state["image_bytes"], prompt)
    foods = result.get("foods", [])
    events = _push(state, "finding", {"stage": "vision", "foods": foods})
    return {"foods": foods, "events": events}

# ---------- 营养汇总（公共） ----------
async def _estimate_food(food_name: str, amount_g: float) -> dict:
    """食物库未收录时的兜底估算：真实模式让千问按同类食材推算，Mock 用默认值，保证营养不缺失"""
    if llm.config.LLM_MOCK_MODE:
        base = {"kcal": 60, "protein": 3.0, "fat": 2.0, "carbs": 8.0, "sodium": 100}
    else:
        prompt = (
            f"食物「{food_name}」不在标准成分表中。请按《中国食物成分表》常见同类食材推算其每100g营养成分，"
            '只输出 JSON：{"kcal": 千卡, "protein": 克, "fat": 克, "carbs": 克, "sodium": 毫克}，取常见范围，不要解释。'
        )
        raw = llm.extract_json(await llm.chat_text(prompt, system="你只输出合法 JSON。", temperature=0.1))
        base = {
            "kcal": float(raw.get("kcal", 60)),
            "protein": float(raw.get("protein", 3)),
            "fat": float(raw.get("fat", 2)),
            "carbs": float(raw.get("carbs", 8)),
            "sodium": float(raw.get("sodium", 100)),
        }
    ratio = amount_g / 100
    return {
        "food": food_name, "amount_g": amount_g,
        "kcal": round(base["kcal"] * ratio, 1),
        "protein": round(base["protein"] * ratio, 1),
        "fat": round(base["fat"] * ratio, 1),
        "carbs": round(base["carbs"] * ratio, 1),
        "sodium": round(base["sodium"] * ratio, 1),
        "estimated": True,   # 标记为估算，报告里可区分
    }

async def _sum_nutrition(foods: list) -> tuple[list, dict]:
    """逐食物查成分表并汇总三大营养素与钠（营养/健康专家共用，保证并行时各自独立）"""
    items = []
    totals = {"kcal": 0.0, "protein": 0.0, "fat": 0.0, "carbs": 0.0, "sodium": 0.0}
    for food in foods:
        raw = json.loads(await get_food_nutrition.ainvoke({"food_name": food.get("name", ""), "amount_g": float(food.get("amount_g", 100))}))
        if "error" in raw:
            # 库外食物：估算兜底，而不是让整餐营养缺失
            est = await _estimate_food(food.get("name", ""), float(food.get("amount_g", 100)))
            items.append(est)
            for k in totals:
                totals[k] += est.get(k, 0)
            continue
        items.append(raw)
        for k in totals:
            totals[k] += raw.get(k, 0)
    totals["total_kcal"] = round(totals.pop("kcal"), 1)
    for k in ("protein", "fat", "carbs", "sodium"):
        totals[k] = round(totals[k], 1)
    return items, totals

# ---------- 营养主路径：LLM 自主推算（失败回退查库） ----------
async def _llm_nutrition(foods: list, constraint: str = "") -> tuple[list, dict]:
    """让千问按《中国食物成分表》知识自主推算营养；Mock 或解析失败时回退 _sum_nutrition 查库"""
    if llm.config.LLM_MOCK_MODE:
        return await _sum_nutrition(foods)
    prompt = (
        "你是营养师。根据以下食物清单推算营养，只输出 JSON："
        '{"items": [{"food": 食物名, "amount_g": 克数, "kcal": 数值, "protein": 克, "fat": 克, "carbs": 克, "sodium": 毫克}], '
        '"totals": {"total_kcal": 数值, "protein": 克, "fat": 克, "carbs": 克, "sodium": 毫克}}\n'
        "要求：按《中国食物成分表》常见值推算每100g营养，再按实际克数换算，数值取合理范围，不要解释。\n"
        f"清单：{json.dumps(foods, ensure_ascii=False)}\n"
        f"用户补充信息：{constraint or '无'}（如有食材或数量，请一并纳入估算；无法确认时不要编造）"
    )
    raw = llm.extract_json(await llm.chat_text(prompt, system="你只输出合法 JSON。", temperature=0.1))
    items, totals = raw.get("items"), raw.get("totals")
    if not isinstance(items, list) or not isinstance(totals, dict) or "total_kcal" not in totals:
        return await _sum_nutrition(foods)   # 结构不对：回退查库
    clean_items = []
    for it in items:
        try:
            clean_items.append({
                "food": str(it.get("food", "")),
                "amount_g": float(it.get("amount_g", 0)),
                "kcal": round(float(it.get("kcal", 0)), 1),
                "protein": round(float(it.get("protein", 0)), 1),
                "fat": round(float(it.get("fat", 0)), 1),
                "carbs": round(float(it.get("carbs", 0)), 1),
                "sodium": round(float(it.get("sodium", 0)), 1),
            })
        except (TypeError, ValueError):
            continue
    if not clean_items:
        return await _sum_nutrition(foods)
    clean_totals = {
        "total_kcal": round(float(totals.get("total_kcal", 0)), 1),
        "protein": round(float(totals.get("protein", 0)), 1),
        "fat": round(float(totals.get("fat", 0)), 1),
        "carbs": round(float(totals.get("carbs", 0)), 1),
        "sodium": round(float(totals.get("sodium", 0)), 1),
    }
    return clean_items, clean_totals

# ---------- 健康主路径：LLM 自主评分（失败回退规则） ----------
async def _llm_health_score(foods: list, profile: dict, meal_kcal: float, constraint: str = "") -> dict:
    """让千问基于食物构成与用户画像自主打分（0-100）并给理由；Mock 或失败时回退 _calc_health_score"""
    if llm.config.LLM_MOCK_MODE:
        _items, totals = await _sum_nutrition(foods)
        return _calc_health_score(foods, totals, meal_kcal)
    prompt = (
        "你是营养师。评估这一餐是否健康、是否适合用户当前目标，只输出 JSON："
        '{"score": 0到100的整数, "reasons": ["理由1", "理由2"]}\n'
        "评分参考维度：热量是否匹配该餐次建议、蛋白质是否充足、有无蔬菜、高油高糖或酒精、钠是否偏高、整体搭配。\n"
        "reasons 写 2-4 条，先扣分点后亮点，简洁具体。"
        "reasons 用定性描述（如'热量偏低''蛋白质充足''缺少主食碳水'），不要出现具体热量或营养素数字（数值以营养专家的汇总为准）。\n"
        f"食物构成：{json.dumps(foods, ensure_ascii=False)}\n"
        f"用户画像：{json.dumps(profile, ensure_ascii=False)}\n"
        f"本餐建议热量：{meal_kcal:.0f} 千卡\n"
        f"用户附加要求：{constraint or '无'}"
    )
    raw = llm.extract_json(await llm.chat_text(prompt, system="你只输出合法 JSON。", temperature=0.2))
    try:
        score = int(raw.get("score", 0))
        reasons = [str(r) for r in raw.get("reasons", [])][:5]
    except (TypeError, ValueError, AttributeError):
        score, reasons = 0, []
    if not (0 <= score <= 100) or not reasons:
        _items, totals = await _sum_nutrition(foods)
        return _calc_health_score(foods, totals, meal_kcal)   # 结果非法：回退规则
    return {"score": score, "reasons": reasons}

# ---------- 节点 3：营养专家 ----------
async def nutrition_node(state: AgentState) -> dict:
    try:
        items, totals = await _llm_nutrition(state.get("foods", []), state.get("constraint", ""))
    except Exception:
        items, totals = await _sum_nutrition(state.get("foods", []))
    events = _push(state, "finding", {"stage": "nutrition", "items": items, "totals": totals})
    return {"nutrition": {"items": items, "totals": totals}, "events": events}

# ---------- 节点 4：健康评估 ----------
async def health_node(state: AgentState) -> dict:
    """读画像 → 算本餐目标热量 → 让千问基于食物构成自主评分（失败回退规则打分）"""
    profile = state.get("profile") or json.loads(await get_user_profile.ainvoke({}))
    daily = state.get("daily") or json.loads(await calc_meal_target.ainvoke({}))
    meal_kcal = daily.get("per_meal_kcal", 600)
    try:
        checked = await _llm_health_score(state.get("foods", []), profile, meal_kcal, state.get("constraint", ""))
    except Exception:
        _items, totals = await _sum_nutrition(state.get("foods", []))
        checked = _calc_health_score(state.get("foods", []), totals, meal_kcal)
    events = _push(state, "finding", {"stage": "health", "score": checked["score"], "reasons": checked["reasons"]})
    return {"health": {
        "profile": profile, "daily": daily, "meal_kcal": meal_kcal,
        "score": checked["score"], "reasons": checked["reasons"],
    }, "events": events}

def _result(observation: str, updates: dict = None, events: list = None) -> dict:
    return {"observation": observation, "updates": updates or {}, "events": events or []}

async def _tool_vision(state: AgentState) -> dict:
    data = await vision_node(state)
    return _result(f"识别到 {len(data['foods'])} 种食物", {"foods": data["foods"]}, data["events"])

async def _tool_estimate(state: AgentState) -> dict:
    data = await nutrition_node(state)
    return _result("已按食物成分数据估算营养", {"nutrition": data["nutrition"]}, data["events"])

async def _tool_lookup(state: AgentState) -> dict:
    items, totals = await _sum_nutrition(state.get("foods", []))
    events = _push(state, "finding", {"stage": "nutrition", "items": items, "totals": totals})
    return _result("已查询内置食物成分表", {"nutrition": {"items": items, "totals": totals}}, events)

async def _tool_profile(state: AgentState) -> dict:
    profile = json.loads(await get_user_profile.ainvoke({}))
    return _result("已读取用户健康画像", {"profile": profile})

async def _tool_target(state: AgentState) -> dict:
    daily = json.loads(await calc_meal_target.ainvoke({}))
    return _result("已计算本餐热量目标", {"daily": daily})

async def _tool_health(state: AgentState) -> dict:
    data = await health_node(state)
    return _result(f"健康评分 {data['health']['score']} 分", {"health": data["health"]}, data["events"])

TOOLS = {
    "identify_foods": {"desc": "识别图片中的食物与份量", "provides": "foods", "run": _tool_vision},
    "estimate_nutrition": {"desc": "按营养知识估算食物营养", "provides": "nutrition", "run": _tool_estimate},
    "lookup_nutrition": {"desc": "查询内置食物成分表", "provides": "nutrition", "run": _tool_lookup},
    "get_profile": {"desc": "读取用户健康画像", "provides": "profile", "run": _tool_profile},
    "get_target": {"desc": "计算每日与每餐热量目标", "provides": "daily", "run": _tool_target},
    "score_health": {"desc": "结合目标与附加要求评分", "provides": "health", "run": _tool_health},
}

AGENT_SPECS = {
    "vision": {"desc": "识别饮食图片", "tools": ("identify_foods",)},
    "nutrition": {"desc": "分析本餐营养", "tools": ("estimate_nutrition", "lookup_nutrition")},
    "health": {"desc": "评估健康度", "tools": ("get_profile", "get_target", "score_health")},
}

async def run_agent(state: AgentState) -> dict:
    """Agent 自主循环：选择工具 → 观察结果 → 继续、结束或交接"""
    name = state["agent"]
    spec = AGENT_SPECS[name]
    updates, observations, events, handoff, used = {}, [], [], "", []
    for _ in range(3):
        remaining = [t for t in spec["tools"] if t not in used]
        if llm.config.LLM_MOCK_MODE:
            action = ({"action": "tool", "tool": remaining[0]} if remaining and (not updates or name == "health")
                      else {"action": "done"})
        else:
            menu = "\n".join(f"- {t}: {TOOLS[t]['desc']}" for t in remaining)
            view = {k: state.get(k) for k in ("foods", "nutrition", "health", "profile", "daily", "constraint") if state.get(k)}
            prompt = (
                f"你是{spec['desc']} Agent。自主选择下一步，只输出 JSON：\n"
                '{"action":"tool","tool":"工具名"} | {"action":"handoff","agent":"vision|nutrition|health","reason":"原因"} | {"action":"done"}\n'
                f"可用工具：\n{menu}\n当前数据：{json.dumps({**view, **updates}, ensure_ascii=False)}\n"
                f"已执行：{used}；观察：{json.dumps(observations, ensure_ascii=False)}"
            )
            action = llm.extract_json(await llm.chat_text(prompt, system="你是自主工具型 Agent，只输出合法 JSON。"))
        if updates and action.get("action") == "handoff" and action.get("agent") in AGENT_SPECS and action["agent"] != name:
            handoff = action["agent"]
            break
        tool = action.get("tool")
        context = {**state, **updates}
        if action.get("action") != "tool" or tool not in remaining or context.get(TOOLS[tool]["provides"]):
            break
        used.append(tool)
        result = await TOOLS[tool]["run"](context)
        updates.update(result["updates"])
        events += result["events"]
        observations.append(result["observation"])
    if not updates and not handoff:
        context = {**state, **updates}
        tool = next((t for t in spec["tools"] if not context.get(TOOLS[t]["provides"])), None)
        if not tool:
            return {**updates, "handoff": {}, "events": events}
        result = await TOOLS[tool]["run"](context)
        updates.update(result["updates"])
        events += result["events"]
    return {**updates, "handoff": {name: handoff}, "events": events}

# ---------- 节点 5：报告生成 ----------
async def synthesize(state: AgentState) -> dict:
    foods = state.get("foods", [])
    totals = state["nutrition"]["totals"]
    health = state["health"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    constraint = state.get("constraint") or ""
    payload = {
        "foods": foods, "totals": totals, "score": health["score"],
        "reasons": health["reasons"], "daily": health["daily"],
        "meal_kcal": health["meal_kcal"], "constraint": constraint,
    }
    prompt = (
        "你是营养师。基于以下结构化数据生成一份饮食分析报告，只输出 JSON，字段严格如下：\n"
        "{\"title\": str, \"summary\": str, \"metrics\": [{\"key\": str, \"name\": str, \"value\": number, \"unit\": str, \"change_pct\": null}], "
        "\"charts\": [{\"type\": \"bar\", \"title\": str, \"x\": [str], \"series\": [{\"name\": str, \"data\": [number]}]}], "
        "\"insights\": [{\"title\": str, \"content\": str, \"evidence_ids\": [str]}], "
        "\"actions\": [{\"priority\": \"P1|P2\", \"action\": str, \"target\": str, \"expected_impact\": str, \"evidence_ids\": [str]}], "
        "\"evidence\": [{\"id\": str, \"source\": str, \"period\": str, \"tool\": str}], "
        "\"constraint_answers\": [{\"question\": str, \"answer\": str, \"evidence_ids\": [str]}]}\n"
        f"数据：{json.dumps(payload, ensure_ascii=False)}\n"
        "要求：summary 一句话给结论；metrics 至少包含 总热量/蛋白质/脂肪/碳水/钠 五项与健康评分；"
        "insights 基于 reasons；actions 给出 2-3 条具体可执行建议。"
        f"\n【附加要求】{constraint or '无'}。若有附加要求，constraint_answers 必须逐条覆盖其中每个问题或条件，"
        "question 提炼对应要求，answer 直接给出明确结论并引用相关数据；信息不足时说明无法确认，不得遗漏、答非所问或用通用建议代替。"
    )
    if llm.config.LLM_MOCK_MODE:
        report = _mock_report(payload, now)
    else:
        report = llm.extract_json(await llm.chat_text(prompt, system="你只输出合法 JSON，不输出任何其他内容。", temperature=0.2))
        if not report:
            report = _mock_report(payload, now)  # LLM 解析失败时兜底，不让整条链路失败
    for metric in report.get("metrics") or []:
        if isinstance(metric, dict) and metric.get("key") in ("health_score", "healthScore"):
            metric["key"] = "score"
    if constraint:
        answers = report.get("constraint_answers")
        if not isinstance(answers, list) or not any(isinstance(a, dict) and a.get("answer") for a in answers):
            report["constraint_answers"] = [{"question": constraint, "answer": report.get("summary", ""), "evidence_ids": []}]
    events = _push(state, "report", report)
    return {"report": report, "events": events}

# ---------- 节点 6：Critic 独立质检 ----------
def _critic_checks(state: AgentState) -> list:
    """先用确定性规则检查报告契约，再决定是否调用 LLM 做语义审核"""
    report = state.get("report") or {}
    metrics = report.get("metrics") or []
    issues = []
    if not report.get("summary") or not metrics:
        issues.append("报告摘要或指标不完整")
    if not any(m.get("key") in ("score", "health_score") for m in metrics if isinstance(m, dict)):
        issues.append("报告缺少健康评分")
    if state.get("constraint") and not any(
        isinstance(a, dict) and a.get("answer") for a in report.get("constraint_answers") or []
    ):
        issues.append("未逐项回应附加要求")
    return issues

async def critic_node(state: AgentState) -> dict:
    issues = _critic_checks(state)
    next_agents = []
    if not issues and not llm.config.LLM_MOCK_MODE:
        prompt = (
            "你是独立质检 Agent。审核饮食报告是否准确回答用户、数值是否与数据一致、结论是否自相矛盾。"
            "只输出 JSON，不重写报告："
            '{"passed": true|false, "issues": ["问题"], "next": ["vision"|"nutrition"|"health"|"synthesize"]}\n'
            f"附加要求：{state.get('constraint') or '无'}\n"
            f"数据：{json.dumps({'nutrition': state.get('nutrition'), 'health': state.get('health')}, ensure_ascii=False)}\n"
            f"报告：{json.dumps(state.get('report') or {}, ensure_ascii=False)}"
        )
        raw = llm.extract_json(await llm.chat_text(prompt, system="你是严格的报告质检员，只输出 JSON。", temperature=0.1))
        if raw and not raw.get("passed", True):
            issues = [str(x) for x in raw.get("issues", [])][:5] or ["报告未通过语义质检"]
            allowed = set(AGENT_SPECS) | {"synthesize"}
            next_agents = [a for a in raw.get("next", []) if a in allowed][:3]
    if issues and not next_agents:
        next_agents = ["synthesize"]
    passed = not issues
    events = _push(state, "agent_status", {
        "stage": "gather",
        "msg": "Critic 质检通过，报告已定稿" if passed else f"Critic 发现问题，触发返工：{'；'.join(issues)}",
    })
    return {
        "critique": {"passed": passed, "issues": issues, "next": next_agents},
        "review_count": state.get("review_count", 0) + 1,
        "events": events,
    }

def _mock_report(p: dict, now: str) -> dict:
    """Mock 报告：与真实模式同结构，便于前端与测试验证"""
    totals = p["totals"]
    return {
        "title": "饮食分析报告",
        "period": {"start": now, "end": now},
        "summary": (f"本餐约 {totals.get('total_kcal', 0):.0f} 千卡，健康评分 {p['score']} 分（满分100）。"
                    f"整体{'搭配均衡' if p['score'] >= 80 else '中等偏上' if p['score'] >= 60 else '需要调整'}。"),
        "metrics": [
            {"key": "score", "name": "健康评分", "value": p["score"], "unit": "分", "change_pct": None},
            {"key": "total_kcal", "name": "总热量", "value": totals.get("total_kcal", 0), "unit": "千卡", "change_pct": None},
            {"key": "protein", "name": "蛋白质", "value": totals.get("protein", 0), "unit": "g", "change_pct": None},
            {"key": "fat", "name": "脂肪", "value": totals.get("fat", 0), "unit": "g", "change_pct": None},
            {"key": "carbs", "name": "碳水", "value": totals.get("carbs", 0), "unit": "g", "change_pct": None},
            {"key": "sodium", "name": "钠", "value": totals.get("sodium", 0), "unit": "mg", "change_pct": None},
        ],
        "charts": [{
            "type": "bar", "title": "三大营养素摄入 vs 本餐建议",
            "x": ["蛋白质", "脂肪", "碳水"],
            "series": [
                {"name": "摄入", "data": [totals.get("protein", 0), totals.get("fat", 0), totals.get("carbs", 0)]},
                {"name": "建议", "data": [25, 20, 90]},
            ],
        }],
        "insights": [{"title": "关键发现", "content": "；".join(p["reasons"]) or "各项指标均在合理范围", "evidence_ids": ["e1"]}],
        "actions": [
            {"priority": "P2", "action": "保持当前搭配", "target": "整体饮食", "expected_impact": "营养均衡", "evidence_ids": ["e1"]},
            {"priority": "P2", "action": "根据扣分点逐项改善", "target": "重点关注项", "expected_impact": "提升健康评分", "evidence_ids": ["e1"]},
        ],
        "evidence": [{"id": "e1", "source": "food_standards / user_profile", "period": now, "tool": "get_food_nutrition"}],
        "constraint_answers": ([{
            "question": p["constraint"],
            "answer": f"针对「{p['constraint']}」：{'；'.join(p['reasons']) or '已结合本餐数据完成分析'}。",
            "evidence_ids": ["e1"],
        }] if p.get("constraint") else []),
    }

# ---------- 节点 7：记忆落库 ----------
async def persist_node(state: AgentState) -> dict:
    report = state["report"]
    metrics = {m["key"]: m["value"] for m in report.get("metrics", [])}
    meal_id = await db.save_meal(
        state["username"], state.get("meal_type", "正餐"), state["image_path"],
        state.get("foods", []), report, metrics.get("total_kcal", 0), metrics.get("score", 0))
    events = _push(state, "done", {"meal_id": meal_id})
    return {"events": events}

# ---------- 动态路由 ----------
def _dispatch(state: AgentState) -> list | str:
    """并行派发专家；报告生成走独立节点"""
    agents = state.get("next_agents") or ["synthesize"]
    if agents == ["synthesize"]:
        return "synthesize"
    return [Send("run_agent", {**state, "agent": name}) for name in agents]

def _after_critic(state: AgentState) -> str:
    critique = state.get("critique") or {}
    return "persist_node" if critique.get("passed") or state.get("review_count", 0) >= 2 else "supervisor"

# ---------- 构建图 ----------
def build_graph():
    g = StateGraph(AgentState)
    g.add_node("supervisor", supervisor_node)
    g.add_node("run_agent", run_agent)
    g.add_node("synthesize", synthesize)
    g.add_node("critic", critic_node)
    g.add_node("persist_node", persist_node)
    g.set_entry_point("supervisor")
    g.add_conditional_edges("supervisor", _dispatch, ["run_agent", "synthesize"])
    g.add_edge("run_agent", "supervisor")
    g.add_edge("synthesize", "critic")
    g.add_conditional_edges("critic", _after_critic, ["supervisor", "persist_node"])
    g.add_edge("persist_node", END)
    return g.compile()

graph = build_graph()
