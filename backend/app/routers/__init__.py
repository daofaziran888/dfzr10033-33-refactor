"""业务路由汇总。"""
from __future__ import annotations
from app.routers import cache_eviction, resource
ROUTERS = [cache_eviction, resource]
