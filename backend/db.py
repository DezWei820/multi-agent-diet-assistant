# -*- coding: utf-8 -*-
"""数据库层：SQLite 连接、建表、用户画像与饮食记录查询。"""
import os
import aiosqlite
from .config import DB_PATH

# ---------- 建表 ----------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL DEFAULT 'demo',
    gender TEXT NOT NULL DEFAULT '男',
    age REAL NOT NULL DEFAULT 25,
    height REAL NOT NULL DEFAULT 170,        -- 身高 cm
    weight REAL NOT NULL DEFAULT 65,         -- 体重 kg
    goal TEXT NOT NULL DEFAULT '保持健康',    -- 减脂 / 增肌 / 保持健康
    activity_level TEXT NOT NULL DEFAULT '轻量运动',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL DEFAULT 'demo',
    meal_type TEXT NOT NULL DEFAULT '正餐',   -- 早餐 / 午餐 / 晚餐 / 加餐
    image_path TEXT DEFAULT '',
    foods_json TEXT DEFAULT '[]',             -- 识别出的食物清单
    report_json TEXT DEFAULT '{}',            -- 完整分析报告
    total_kcal REAL DEFAULT 0,
    score REAL DEFAULT 0,                     -- 健康评分 0-100
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_meal_user ON meals (username);

CREATE TABLE IF NOT EXISTS food_standards (   -- 《中国食物成分表》核心子集（每100g）
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    food_name TEXT UNIQUE NOT NULL,
    aliases TEXT DEFAULT '',                  -- 别名，逗号分隔，用于模糊匹配
    kcal REAL DEFAULT 0,
    protein REAL DEFAULT 0,
    fat REAL DEFAULT 0,
    carbs REAL DEFAULT 0,
    sodium REAL DEFAULT 0                     -- mg
);
"""

async def get_conn():
    """每次操作新建连接（SQLite 单进程内简单可靠，无需连接池）"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return await aiosqlite.connect(DB_PATH)

async def init_db():
    """lifespan 启动时建表（需要事件循环）"""
    conn = await get_conn()
    try:
        await conn.executescript(_SCHEMA)
        await conn.commit()
    finally:
        await conn.close()

# ---------- 用户画像 ----------
async def get_profile(username: str = "demo") -> dict | None:
    conn = await get_conn()
    try:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id, username, gender, age, height, weight, goal, activity_level, datetime(created_at, '+8 hours') AS created_at, datetime(updated_at, '+8 hours') AS updated_at FROM profiles WHERE username = ?", (username,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None
    finally:
        await conn.close()

async def upsert_profile(username: str, data: dict):
    """写入或更新用户画像（首次自动建默认画像）"""
    conn = await get_conn()
    try:
        await conn.execute("""
            INSERT INTO profiles (username, gender, age, height, weight, goal, activity_level)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                gender=excluded.gender, age=excluded.age, height=excluded.height,
                weight=excluded.weight, goal=excluded.goal, activity_level=excluded.activity_level,
                updated_at=CURRENT_TIMESTAMP
        """, (username, data.get("gender", "男"), data.get("age", 25),
              data.get("height", 170), data.get("weight", 65),
              data.get("goal", "保持健康"), data.get("activity_level", "轻量运动")))
        await conn.commit()
    finally:
        await conn.close()

# ---------- 饮食记录 ----------
async def save_meal(username: str, meal_type: str, image_path: str,
                    foods: list, report: dict, total_kcal: float, score: float) -> int:
    conn = await get_conn()
    try:
        import json
        cur = await conn.execute(
            "INSERT INTO meals (username, meal_type, image_path, foods_json, report_json, total_kcal, score) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (username, meal_type, image_path, json.dumps(foods, ensure_ascii=False),
             json.dumps(report, ensure_ascii=False), total_kcal, score))
        await conn.commit()
        return cur.lastrowid
    finally:
        await conn.close()

async def list_meals(username: str = "demo", limit: int = 20):
    conn = await get_conn()
    try:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT id, meal_type, total_kcal, score, datetime(created_at, '+8 hours') AS created_at FROM meals WHERE username = ? ORDER BY id DESC LIMIT ?",
            (username, limit)) as cur:
            rows = await cur.fetchall()
        meals = []
        for row in rows:
            m = dict(row)
            m["created_at"] = m["created_at"].replace("T", " ")[:19]
            meals.append(m)
        return meals
    finally:
        await conn.close()

async def get_meal(meal_id: int, username: str = "demo") -> dict | None:
    conn = await get_conn()
    try:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id, username, meal_type, image_path, foods_json, report_json, total_kcal, score, datetime(created_at, '+8 hours') AS created_at FROM meals WHERE id = ? AND username = ?", (meal_id, username)) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            m = dict(row)
            import json
            m["foods"] = json.loads(m.pop("foods_json") or "[]")
            m["report"] = json.loads(m.pop("report_json") or "{}")
            m["created_at"] = m["created_at"].replace("T", " ")[:19]
            return m
    finally:
        await conn.close()

async def delete_meal(meal_id: int, username: str = "demo") -> bool:
    conn = await get_conn()
    try:
        cur = await conn.execute("DELETE FROM meals WHERE id = ? AND username = ?", (meal_id, username))
        await conn.commit()
        return cur.rowcount > 0
    finally:
        await conn.close()

# ---------- 食物成分表 ----------
async def find_food(name: str) -> dict | None:
    """按标准名或别名精确匹配一个食物（返回每100g营养）"""
    conn = await get_conn()
    try:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM food_standards WHERE food_name = ?", (name,)) as cur:
            row = await cur.fetchone()
            if row:
                return dict(row)
        async with conn.execute("SELECT * FROM food_standards WHERE aliases LIKE ?", (f"%{name}%",)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None
    finally:
        await conn.close()

async def fuzzy_find_food(keyword: str) -> list:
    """按关键词模糊匹配多个食物（用于 LLM 拿不准名字时兜底）"""
    conn = await get_conn()
    try:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM food_standards WHERE food_name LIKE ? OR aliases LIKE ? LIMIT 3",
            (f"%{keyword}%", f"%{keyword}%")) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        await conn.close()
