# -*- coding: utf-8 -*-
"""backend 包：对外暴露 FastAPI 实例与常用接口。
启动命令保持 uvicorn backend:app 不变（import backend 即执行本文件）。"""
from .main import app
from .agent import graph
