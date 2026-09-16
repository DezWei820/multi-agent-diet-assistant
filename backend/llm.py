# -*- coding: utf-8 -*-
"""LLM 层：千问文本/识图调用封装。Mock 模式返回固定数据，保证演示与测试不花钱。"""
import base64
import json
import re
from openai import AsyncOpenAI
from . import config

client = AsyncOpenAI(api_key=config.QWEN_API_KEY, base_url=config.QWEN_BASE_URL)

# ---------- 工具函数 ----------
def extract_json(text: str) -> dict:
    """从模型输出中提取 JSON 对象（容忍 ```json 围栏与前后杂质文字）"""
    text = text.strip()
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return {}
    try:
        return json.loads(match.group(0))
    except Exception:
        return {}

def _mock_foods():
    """Mock 识图结果：一份标准健康餐（米饭+鸡胸肉+西兰花）"""
    return [
        {"name": "米饭", "amount_g": 150, "note": "主食"},
        {"name": "鸡胸肉", "amount_g": 120, "note": "蛋白质来源"},
        {"name": "西兰花", "amount_g": 100, "note": "蔬菜"},
    ]

# ---------- 文本调用 ----------
async def chat_text(prompt: str, system: str = "", temperature: float = 0.3) -> str:
    """调用千问文本模型，返回纯文本回答"""
    if config.LLM_MOCK_MODE:
        # 按关键词简单模拟意图识别，保证编排流程可跑
        if "意图" in prompt:
            if "减脂" in prompt or "减肥" in prompt:
                return json.dumps({"intent": "analyze", "constraint": "重点关注热量和脂肪"}, ensure_ascii=False)
            return json.dumps({"intent": "analyze", "constraint": ""}, ensure_ascii=False)
        return "这是一份 mock 分析结果：整体搭配均衡，蛋白质充足，建议控制主食份量。"
    resp = await client.chat.completions.create(
        model=config.TEXT_MODEL,
        messages=[
            {"role": "system", "content": system or "你是一个专业的营养与健康饮食助手。"},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
    )
    return resp.choices[0].message.content or ""

# ---------- 识图调用 ----------
async def vision_parse(image_bytes: bytes, prompt: str) -> dict:
    """千问VL 识图：输入图片字节与指令，返回结构化 JSON（食物清单）"""
    if config.LLM_MOCK_MODE:
        return {"foods": _mock_foods()}
    b64 = base64.b64encode(image_bytes).decode()
    resp = await client.chat.completions.create(
        model=config.VISION_MODEL,
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
            ],
        }],
        temperature=0.1,
    )
    text = resp.choices[0].message.content or ""
    data = extract_json(text)
    foods = data.get("foods") if isinstance(data, dict) else None
    if not isinstance(foods, list):
        return {"foods": [], "raw": text}
    return {"foods": foods}
