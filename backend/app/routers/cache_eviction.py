"""立方缓存统一淘汰清单接口。

必须在资源路由之前注册：``/api/{module}`` 是动态路径，否则 ``cube-cache``
会被当成模块名抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services import cache_eviction
from app.services.aggregate_cache import AggregateCacheService
from app.services.olap_cube import OlapCubeService
from app.store import store

router = APIRouter(prefix="/api/cube-cache", tags=["立方缓存淘汰"])

cube_svc = OlapCubeService()
agg_svc = AggregateCacheService()


@router.post("/cubes/{cube_id}/refresh")
def refresh_cube(cube_id: int) -> dict[str, Any]:
    """正常刷新入口：开清单、重建、按清单释放旧版本缓存。"""
    result = cube_svc.refresh_cube(cube_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "立方刷新失败"))
    return result


@router.post("/cubes/{cube_id}/evict")
def evict_cube_cache(cube_id: int) -> dict[str, Any]:
    """手工淘汰入口：刷新进行中时并入同一清单，共用一次核验结果。"""
    if store.find("olap_cube", cube_id) is None:
        raise HTTPException(status_code=404, detail=f"立方 {cube_id} 不存在")
    return agg_svc.evict_stale_cache(cube_id)


@router.get("/evictions")
def list_eviction_manifests(cube_id: int | None = Query(default=None)) -> dict[str, Any]:
    manifests = cache_eviction.manifest_snapshot(cube_id=cube_id)
    if manifests is None:
        return {"ok": True, "manifest": None, "manifests": []}
    if isinstance(manifests, dict):
        return {"ok": True, "manifest": manifests}
    return {"ok": True, "manifests": manifests}


@router.get("/evictions/{manifest_id}")
def get_eviction_manifest(manifest_id: str) -> dict[str, Any]:
    manifest = cache_eviction.manifest_snapshot(manifest_id=manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"淘汰清单 {manifest_id} 不存在")
    return {"ok": True, "manifest": manifest}
