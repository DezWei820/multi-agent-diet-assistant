# 多 agent 健康饮食助理 🥗

上传一张饮食图片，AI 通过 **多 Agent 编排** 完成全方位分析：食物识别 → 营养计算 → 对照健康画像评分 → 生成带证据的分析报告。

## 功能

- **图片上传分析**：支持拖拽上传，多 Agent 并行工作
- **自主多 Agent 编排（LangGraph）**：
  - Supervisor 动态调度 → 专家自主选工具/交接并可按需重复执行（Send 并行）→ Critic 质检返工 → 报告生成 → 记忆落库
- **营养分析**：内置《中国食物成分表》核心子集（约 60 种常见食物），逐项计算热量/蛋白/脂肪/碳水/钠
- **个性化健康评估**：按画像（身高/体重/目标/活动水平）算每日与每餐热量目标，规则化评分（可解释）
- **SSE 流式执行过程**：前端实时显示每个 Agent 的进度与结果
- **历史记录**：每次分析落库，可回看、可删除

## 快速启动

```powershell
# 1. 创建虚拟环境并安装依赖
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. 启动后端（默认 Mock 模式，不调用真实模型，可直接体验）
uvicorn backend:app --host 127.0.0.1 --port 8000

# 3. 浏览器打开
# http://127.0.0.1:8000
```

## 切换真实模型（千问）

环境变量已配置 `QWEN_API_KEY` 时，设置 `LLM_MOCK_MODE=false` 即可调用真实千问（识图 qwen-vl-max + 推理 qwen-plus）：

```powershell
$env:LLM_MOCK_MODE = "false"
uvicorn backend:app --host 127.0.0.1 --port 8000
```

## 项目结构

```
backend/
  __init__.py    # 暴露 app
  config.py      # 环境变量、LLM 配置
  db.py          # SQLite 建表与查询（画像/饮食记录/食物成分表）
  seed.py        # 内置食物成分表 + 默认画像
  llm.py         # 千问文本/识图封装（含 Mock 模式）
  tools.py       # Agent 工具：营养查询、目标计算、标准检索
  agent.py       # LangGraph 多 Agent 编排
  main.py        # FastAPI 入口、全部路由、SSE、静态托管
frontend/        # 纯静态页面（index.html + style.css + app.js，无需构建）
data/            # SQLite 数据库与上传图片（运行时生成）
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | /api/health | 健康检查（含 Mock 模式标识） |
| GET/PUT | /api/profile | 读取/保存用户画像 |
| POST | /api/analyze/stream | 上传图片 → SSE 流式分析 |
| GET | /api/meals | 历史记录 |
| GET/DELETE | /api/meals/{id} | 记录详情/删除 |

## 说明

- 营养数值为 AI 估算，仅供健康管理参考，不构成医疗建议。
- Mock 模式（默认）返回演示数据，流程与真实模式完全一致。
