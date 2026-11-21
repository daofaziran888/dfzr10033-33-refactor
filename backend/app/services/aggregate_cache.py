"""聚合缓存领域服务：缓存预热、命中统计、淘汰策略与跨模块缓存一致性管理。"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.services import cache_eviction
from app.services.base import BaseModuleService
from app.store import store


class AggregateCacheService(BaseModuleService):
    MODULE = "aggregate_cache"
    ENTITY = "缓存配置"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["配置编号", "目标立方", "缓存粒度", "TTL秒数", "预热策略", "命中阈值", "淘汰算法", "缓存大小"]
    REQUIRED_FIELDS = ["配置编号", "目标立方"]
    STATUS_ORDER = ["草稿", "生效中", "淘汰中", "已停用"]
    ACTION_RULES = {"启效缓存": "生效中", "强制淘汰": "淘汰中", "停用缓存": "已停用"}
    NEGATIVE_ACTIONS = ["强制淘汰", "停用缓存"]

    def check_cache_consistency(self, cube_id: int) -> dict[str, Any]:
        """检查缓存与源 olap_cube 数据的一致性，过期则淘汰。"""
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        cache_rows = store.rows(self.MODULE)
        stale_count = 0

        for cache_entry in cache_rows:
            if cache_entry.get("status") != "生效中":
                continue
            if not cache_eviction.entry_matches_cube(self.MODULE, cache_entry, cube_id):
                continue

            # 检查缓存是否过期
            ttl = int(cache_entry.get("TTL秒数", "3600"))
            cube_rows = cube.get("数据行数", "0")
            cache_size = cache_entry.get("缓存大小", "0")

            # 如果立方行数变化超过20%，标记为过期
            try:
                cube_total = int(cube_rows) if cube_rows.isdigit() else 0
                cache_total = int(cache_size) if cache_size.isdigit() else 0
            except (ValueError, TypeError):
                cube_total = 0
                cache_total = 0

            if cube_total > 0 and abs(cube_total - cache_total) / cube_total > 0.2:
                cache_entry["status"] = "淘汰中"
                cache_entry["pending"] = True
                stale_count += 1

        return {
            "ok": True,
            "cube_id": cube_id,
            "stale_entries": stale_count,
            "cube_row_count": cube.get("数据行数"),
        }

    def prewarm_cube_cache(self, cube_id: int) -> dict[str, Any]:
        """预热立方缓存：读取 olap_cube 和 semantic_model，写入 aggregate_cache 和 result_cache。"""
        cube = store.find("olap_cube", cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        dims = cube.get("维度列表", "id").split(",")
        dims = [d.strip() for d in dims if d.strip()]
        meas = cube.get("度量列表", "value").split(",")
        meas = [m.strip() for m in meas if m.strip()]

        # 生成预热数据
        import random
        prewarmed = []
        for i in range(random.randint(20, 100)):
            row = {}
            for d in dims:
                row[d] = f"{d}_prewarm_{random.randint(1, 10)}"
            for m in meas:
                row[m] = random.randint(100, 50000)
            prewarmed.append(row)

        # 写入 aggregate_cache
        rows = store.rows(self.MODULE)
        cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        data_str = str(prewarmed)
        cache = {
            "id": cache_id,
            "配置编号": f"AGGR-PW{cube_id:04d}",
            "目标立方": f"olap_cube:{cube_id}",
            "依赖立方": str(cube_id),
            "依赖版本": cache_eviction.current_cube_version(cube),
            "缓存粒度": f"{len(dims)}维 x {len(meas)}度",
            "TTL秒数": "7200",
            "预热策略": "EAGER",
            "命中阈值": "0.85",
            "淘汰算法": "LRU",
            "缓存大小": str(len(data_str)),
            "status": "生效中",
            "pending": True,
            "abnormal": False,
            "cached_data": prewarmed,
        }
        rows.append(cache)

        # 也写入 result_cache
        result_rows = store.rows("result_cache")
        result_id = max((int(r.get("id", 0)) for r in result_rows), default=0) + 1
        result = {
            "id": result_id,
            "条目编号": f"RESU-PW{cube_id:04d}",
            "查询摘要": f"prewarm_cube_{cube_id}",
            "依赖立方": str(cube_id),
            "依赖版本": cache_eviction.current_cube_version(cube),
            "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
            "缓存命中": "0",
            "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 7200)),
            "存储大小": str(len(data_str)),
            "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "有效",
            "pending": True,
            "abnormal": False,
            "result_data": prewarmed,
        }
        result_rows.append(result)

        return {
            "ok": True,
            "cube_id": cube_id,
            "prewarmed_rows": len(prewarmed),
            "cache_entry_id": cache_id,
            "result_cache_id": result_id,
        }

    def evict_stale_cache(self, cube_id: int) -> dict[str, Any]:
        """手工淘汰：并入立方刷新清单，或按当前版本独立开清单。"""
        receipt = cache_eviction.evict_for_manual(cube_id)
        total = receipt["evicted_count"]

        # 检查数据新鲜度并通知
        _check_freshness_after_eviction(cube_id, total)

        # 写入审计
        _audit_cache_eviction(cube_id, total)

        return {
            "ok": True,
            "cube_id": cube_id,
            "cube_version": receipt["cube_version"],
            "evicted_count": total,
            "entries": receipt["entries"][:10],
            "manifest_id": receipt["manifest_id"],
            "receipt_id": receipt["receipt_id"],
            "deferred_count": receipt["deferred_count"],
            "deferred": receipt["deferred"][:10],
            "skipped_count": receipt["skipped_count"],
            "released_by_others": receipt["released_by_others"],
        }


def _check_freshness_after_eviction(cube_id: int, evicted_count: int) -> None:
    fresh_rows = store.rows("freshness_mon")
    for fr in fresh_rows:
        cube_ref = fr.get("目标数据集", "")
        if str(cube_id) in str(cube_ref) or f"cube_{cube_id}" in str(cube_ref):
            if evicted_count > 0:
                fr["status"] = "已告警"
                fr["触发次数"] = str(int(fr.get("触发次数", "0")) + 1)
                fr["最后检查"] = time.strftime("%Y-%m-%d %H:%M:%S")


def _audit_cache_eviction(cube_id: int, count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"CACHE_EVICT cube_id={cube_id}",
        "执行耗时": "100ms",
        "扫描行数": str(count),
        "返回行数": str(count),
        "数据来源": "aggregate_cache",
        "操作用户": "cache_evictor",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)