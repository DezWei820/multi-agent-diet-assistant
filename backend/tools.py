# -*- coding: utf-8 -*-
"""工具层：营养查询、每日目标计算、健康对比。供 Agent 节点与测试复用。"""
import json
from langchain_core.tools import tool
from . import db
from .seed import DAILY_TARGETS

# ---------- 基础代谢（Mifflin-St Jeor 公式） ----------
def calc_bmr(profile: dict) -> float:
    """按画像计算基础代谢率（千卡/天）"""
    gender = profile.get("gender", "男")
    weight = float(profile.get("weight", 65))
    height = float(profile.get("height", 170))
    age = float(profile.get("age", 25))
    if gender == "女":
        return 10 * weight + 6.25 * height - 5 * age - 161
    return 10 * weight + 6.25 * height - 5 * age + 5

# ---------- 活动系数 ----------
_ACTIVITY_FACTOR = {
    "久坐": 1.2, "轻量运动": 1.375, "中等运动": 1.55, "高强度运动": 1.725,
}

def calc_daily_target(profile: dict) -> dict:
    """按画像计算每日热量目标（结合目标做增减）"""
    factor = _ACTIVITY_FACTOR.get(profile.get("activity_level", "轻量运动"), 1.375)
    target = calc_bmr(profile) * factor
    goal = profile.get("goal", "保持健康")
    if goal == "减脂":
        target -= 400          # 减脂：每日缺口约 400 千卡
    elif goal == "增肌":
        target += 300          # 增肌：每日盈余约 300 千卡
    return {"daily_kcal": round(target), "factor": factor}

# ---------- Agent 工具 ----------
@tool
async def get_food_nutrition(food_name: str, amount_g: float = 100) -> str:
    """
    查询某食物的营养成分（每份换算）。营养数据库覆盖常见主食、肉蛋、水产、蔬菜、水果与零食饮料。
    参数：
    - food_name: 食物名称（支持别名，如"鸡胸"、"西兰花"）
    - amount_g: 该食物的实际食用份量（克）
    返回：该食物热量、蛋白质、脂肪、碳水、钠的 JSON 字符串。
    """
    food = await db.find_food(food_name)
    if not food:
        candidates = await db.fuzzy_find_food(food_name)
        if not candidates:
            return json.dumps({"error": f"食物库中未找到 {food_name}，请换一个更常见的叫法"}, ensure_ascii=False)
        food = candidates[0]
    ratio = amount_g / 100
    return json.dumps({
        "food": food["food_name"],
        "amount_g": amount_g,
        "kcal": round(food["kcal"] * ratio, 1),
        "protein": round(food["protein"] * ratio, 1),
        "fat": round(food["fat"] * ratio, 1),
        "carbs": round(food["carbs"] * ratio, 1),
        "sodium": round(food["sodium"] * ratio, 1),
    }, ensure_ascii=False)

@tool
async def get_user_profile() -> str:
    """
    读取当前用户的健康画像（性别、年龄、身高、体重、目标、活动水平），用于个性化营养评估。
    返回：用户画像 JSON 字符串。
    """
    profile = await db.get_profile()
    return json.dumps(profile, ensure_ascii=False, default=str)

@tool
async def calc_meal_target() -> str:
    """
    按用户画像计算每日热量目标，并按三餐均分得到每餐建议热量。
    返回：每日目标与每餐建议的 JSON 字符串。
    """
    profile = await db.get_profile()
    daily = calc_daily_target(profile)
    return json.dumps({
        **daily,
        "per_meal_kcal": round(daily["daily_kcal"] / 3),
        "goal": profile.get("goal"),
    }, ensure_ascii=False)

@tool
async def search_nutrition_standard(keyword: str) -> str:
    """
    检索成人每日营养参考标准（蛋白质/脂肪/碳水/钠），用于判断某餐是否超标或不足。
    参数：
    - keyword: 检索关键词，如"蛋白质"、"盐"
    返回：对应营养素的每日建议摄入量 JSON 字符串。
    """
    name_map = {"蛋白质": "protein", "脂肪": "fat", "碳水": "carbs", "钠": "sodium", "盐": "sodium"}
    key = name_map.get(keyword)
    if not key:
        return json.dumps({"hint": "支持的关键词：蛋白质 / 脂肪 / 碳水 / 钠 / 盐"}, ensure_ascii=False)
    return json.dumps({"nutrient": keyword, "daily_recommend": DAILY_TARGETS[key]}, ensure_ascii=False)
