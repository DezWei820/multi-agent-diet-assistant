# -*- coding: utf-8 -*-
"""入口模块：FastAPI 实例、生命周期、全部 API 路由（含 SSE 流式分析）与静态前端托管。
依赖模块：config（配置/LLM）、db（SQLite）、llm（千问）、tools（营养工具）、agent（LangGraph 编排）。
"""
import io
import json
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from . import config  # 先导入：设置环境变量默认值
from . import db, seed, agent

BASE_DIR = Path(__file__).resolve().parent.parent

# ---------- Pydantic 模型 ----------
class ProfileIn(BaseModel):
    gender: str = "男"
    age: float = 25
    height: float = 170
    weight: float = 65
    goal: str = "保持健康"          # 减脂 / 增肌 / 保持健康
    activity_level: str = "轻量运动"  # 久坐 / 轻量运动 / 中等运动 / 高强度运动

# ---------- 生命周期 ----------
@asynccontextmanager
async def lifespan(app):
    await db.init_db()   # 建表
    await seed.seed()    # 内置食物成分表 + 默认画像
    yield

# ---------- FastAPI 实例 ----------
app = FastAPI(title="多 agent 健康饮食助理 API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# 静态前端托管：根路径返回页面，/static 提供资源
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "frontend")), name="static")

# ---------- 基础 API ----------
@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "frontend" / "index.html")

@app.get("/api/health")
async def health():
    return {"status": "ok", "llm_mock_mode": config.LLM_MOCK_MODE, "vision_model": config.VISION_MODEL}

@app.get("/api/profile")
async def get_profile():
    profile = await db.get_profile()
    if not profile:
        raise HTTPException(status_code=404, detail="画像不存在")
    return profile

@app.put("/api/profile")
async def put_profile(profile: ProfileIn):
    await db.upsert_profile("demo", profile.model_dump())
    return {"message": "画像已保存"}

@app.get("/api/meals")
async def get_meals(limit: int = 20):
    return await db.list_meals(limit=min(limit, 50))

@app.get("/api/meals/{meal_id}")
async def get_meal(meal_id: int):
    meal = await db.get_meal(meal_id)
    if not meal:
        raise HTTPException(status_code=404, detail="记录不存在")
    return meal

@app.delete("/api/meals/{meal_id}")
async def delete_meal(meal_id: int):
    if not await db.delete_meal(meal_id):
        raise HTTPException(status_code=404, detail="记录不存在或无权删除")
    return {"message": "删除成功"}

# ---------- AI 饮食分析路由（SSE） ----------
MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10MB

@app.post("/api/analyze/stream")
async def analyze_stream(
    file: UploadFile = File(...),
    meal_type: str = Form("正餐"),
    constraint: str = Form(""),
):
    """上传饮食图片 → LangGraph 多 Agent 分析 → SSE 流式推送执行过程与报告"""
    image_bytes = await file.read()
    if not image_bytes:
        raise HTTPException(status_code=400, detail="图片内容为空")
    if len(image_bytes) > MAX_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail="图片不能超过 10MB")

    # 图片落盘（供历史记录回看）
    import os
    os.makedirs(config.UPLOAD_DIR, exist_ok=True)
    suffix = Path(file.filename or "image.jpg").suffix or ".jpg"
    saved_name = f"{uuid.uuid4().hex}{suffix}"
    saved_path = os.path.join(config.UPLOAD_DIR, saved_name)
    with open(saved_path, "wb") as f:
        f.write(image_bytes)

    initial = {
        "username": "demo",
        "meal_type": meal_type,
        "image_path": saved_path,
        "image_bytes": image_bytes,
        "constraint": constraint,
        "foods": [],
        "nutrition": {},
        "health": {},
        "profile": {},
        "daily": {},
        "report": {},
        "agent_round": 0,
        "agent": "",
        "next_agents": [],
        "critique": {},
        "review_count": 0,
        "handoff": {},
        "events": [],
    }
    graph_cfg = {"recursion_limit": 30}

    async def event_gen():
        ok = True
        try:
            yield {"event": "task_started", "data": json.dumps({"meal_type": meal_type}, ensure_ascii=False)}
            # stream_mode="updates"：按节点返回增量，逐个推送事件，实现执行过程实时展示
            async for update in agent.graph.astream(initial, graph_cfg, stream_mode="updates"):
                for _node, payload in update.items():
                    for ev in payload.get("events", []):
                        yield {"event": ev["event"], "data": json.dumps(ev["data"], ensure_ascii=False)}
        except Exception as e:
            ok = False
            yield {"event": "error", "data": json.dumps({"msg": str(e)}, ensure_ascii=False)}
        yield {"event": "done", "data": json.dumps({"ok": ok}, ensure_ascii=False)}

    return EventSourceResponse(event_gen())
