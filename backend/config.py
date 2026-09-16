# -*- coding: utf-8 -*-
"""全局配置：环境变量、LLM。所有模块依赖的公共底层。"""
import os

# ---------- 配置 ----------
DB_PATH = os.getenv("DB_PATH", "data/diet.db")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "data/uploads")

# 千问 API（从环境变量读取，与宠物项目读取 DeepSeek 的方式一致）
QWEN_API_KEY = os.getenv("QWEN_API_KEY", "your-qwen-api-key")
QWEN_BASE_URL = os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
VISION_MODEL = os.getenv("VISION_MODEL", "qwen-vl-max")  # 识图：识别食物清单
TEXT_MODEL = os.getenv("TEXT_MODEL", "qwen-plus")         # 推理：意图识别 / 报告生成

# Mock 模式：true 时不调用真实模型（演示与测试免费跑通全流程）
LLM_MOCK_MODE = os.getenv("LLM_MOCK_MODE", "true").lower() == "true"
