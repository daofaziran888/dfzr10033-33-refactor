"""多维立方领域服务：立方构建、聚合计算、缓存预热与分区策略。"""
from __future__ import annotations

import hashlib
import random
import time
import uuid
from typing import Any

from app.services import cache_eviction
from app.services.base import BaseModuleService
from app.store import store


class OlapCubeService(BaseModuleService):
    MODULE = "olap_cube"
    ENTITY = "OLAP立方"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["立方编号", "立方名称", "维度列表", "度量列表", "聚合类型", "分区策略", "刷新周期", "数据行数"]
    REQUIRED_FIELDS = ["立方编号", "立方名称", "维度列表"]
    STATUS_ORDER = ["草稿", "构建中", "可用", "构建失败"]
    ACTION_RULES = {"启动构建": "构建中", "标记可用": "可用", "标记失败": "构建失败"}
    NEGATIVE_ACTIONS = []

    def build_cube(self, cube_id: int, data_sources: list[str] | None = None) -> dict[str, Any]:
        """构建多维立方、聚合计算并写入 aggregate_cache、result_cache、query_audit。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        dims = cube.get("维度列表", "").split(",")
        dims = [d.strip() for d in dims if d.strip()]
        meas = cube.get("度量列表", "").split(",")
        meas = [m.strip() for m in meas if m.strip()]

        if not dims or not meas:
            return {"ok": False, "message": "维度和度量定义不能为空"}

        # 从语义模型获取字段映射
        models = _get_models_for_cube(dims)
        field_map = {}
        for m in models:
            aliases = m.get("字段别名", "").split(",")
            for a in aliases:
                a = a.strip()
                if a:
                    field_map[a] = a

        # 从数据源获取原始数据
        sources = data_sources or _get_registered_sources()
        raw_data = _fetch_source_data(sources, dims)

        # 执行聚合计算
        t0 = time.time()
        aggregated = _compute_aggregates(raw_data, dims, meas, cube.get("聚合类型", "SUM"))
        elapsed_ms = (time.time() - t0) * 1000

        # 写入多维立方
        cube["数据行数"] = str(len(aggregated))
        cube["status"] = "可用"
        cube["pending"] = False

        # 写入聚合缓存
        _write_aggregate_cache(cube_id, dims, meas, aggregated)

        # 写入结果缓存
        _write_result_cache(cube_id, aggregated)

        # 写入审计
        _write_cube_audit(cube_id, elapsed_ms, len(aggregated))

        # 更新相关指标工厂
        _update_metrics(cube_id, meas, aggregated)

        return {
            "ok": True,
            "cube_id": cube_id,
            "dims": len(dims),
            "measures": len(meas),
            "aggregated_rows": len(aggregated),
            "elapsed_ms": elapsed_ms,
            "sample": aggregated[:5],
        }

    def refresh_cube(self, cube_id: int) -> dict[str, Any]:
        """刷新立方：按统一淘汰清单归档旧版本缓存、重建、刷新依赖集市。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        # 开清单：按当前立方版本核验依赖条目与影响范围
        manifest = cache_eviction.begin_refresh(cube_id)
        history = manifest.finished_at is not None

        # 构建新版本；版本号只在这里推进，归档范围始终由清单版本固定
        original_version = cube.get("立方版本")
        cube["立方版本"] = cache_eviction.next_cube_version(manifest.cube_version)
        try:
            result = self.build_cube(cube_id)

            # 同一清单可被手工淘汰并入；释放回执写回刷新清单。
            # 已结束的清单只复用核验结果：重复提交不再产生第二次淘汰。
            receipt = (
                _history_receipt(manifest)
                if history
                else cache_eviction.evict_for_refresh(manifest)
            )
        except Exception:
            cube["立方版本"] = original_version
            raise
        finally:
            cache_eviction.finish_refresh(manifest)

        # 更新数据集市（如果存在依赖）
        _refresh_dependent_marts(cube_id)

        result["cube_version"] = cube["立方版本"]
        result["eviction"] = {
            "manifest_id": manifest.manifest_id,
            "evicted_count": receipt["evicted_count"],
            "deferred_count": receipt["deferred_count"],
            "skipped_count": receipt["skipped_count"],
            "entries": receipt["entries"][:10],
            "receipt_id": receipt["receipt_id"],
            "released_by_others": receipt["released_by_others"],
        }
        return result

    def estimate_partition_cost(
        self, cube_id: int, partition_count: int, partition_rows: int
    ) -> dict[str, Any]:
        """估算分区构建代价，写入 cost_govern。"""
        cube = store.find(self.MODULE, cube_id)
        if cube is None:
            return {"ok": False, "message": "立方不存在"}

        total_rows = partition_count * partition_rows
        estimated_memory = total_rows * 0.5  # MB
        estimated_time = total_rows / 10000  # seconds

        # 写入成本治理
        cost_rows = store.rows("cost_govern")
        cost_id = max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1
        cost = {
            "id": cost_id,
            "策略编号": f"COST-C{cube_id:04d}",
            "适用角色": "olap_cube_builder",
            "查询上限": str(total_rows),
            "并发上限": str(partition_count),
            "月度配额": str(total_rows * 8),
            "告警阈值": str(int(estimated_memory * 1.5)),
            "限流动作": "monitor",
            "生效时段": time.strftime("%Y-%m-%d"),
            "status": "已生效" if estimated_memory < 1000 else "已告警",
            "pending": estimated_memory >= 1000,
            "abnormal": estimated_memory >= 2000,
        }
        cost_rows.append(cost)

        return {
            "ok": True,
            "estimated_rows": total_rows,
            "estimated_memory_mb": estimated_memory,
            "estimated_time_s": estimated_time,
            "cost_entry_id": cost_id,
        }


# ---------- 跨模块辅助函数 ----------

def _history_receipt(manifest: cache_eviction.EvictionManifest) -> dict[str, Any]:
    """已结束清单的回执：只报此前由刷新入口释放的条目，不重新淘汰。"""
    released = [
        {
            "type": item.module,
            "id": item.entry_id,
            ("配置编号" if item.module == "aggregate_cache" else "条目编号"): item.entry_no,
        }
        for item in manifest.entries.values()
        if item.released_by == cache_eviction.OWNER_REFRESH
    ]
    return {
        "receipt_id": None,
        "owner": cache_eviction.OWNER_REFRESH,
        "manifest_id": manifest.manifest_id,
        "evicted_count": len(released),
        "entries": released,
        "deferred_count": 0,
        "deferred": [],
        "skipped_count": 0,
        "skipped": [],
        "released_by_others": cache_eviction.released_by_others(manifest),
        "complete": True,
        "replayed": True,
    }


def _get_models_for_cube(dims: list[str]) -> list[dict[str, Any]]:
    models = store.rows("semantic_model")
    return [m for m in models if m.get("status") == "已投产"]


def _get_registered_sources() -> list[str]:
    sources = store.rows("source_registry")
    return [s.get("源编号", f"src_{i}") for i, s in enumerate(sources) if s.get("status") == "已注册"]


def _fetch_source_data(sources: list[str], dims: list[str]) -> list[dict[str, Any]]:
    """模拟从数据源读取原始数据。"""
    data = []
    for i in range(random.randint(20, 200)):
        row = {"_source": sources[i % len(sources)] if sources else "local"}
        for dim in dims:
            row[dim] = f"{dim}_val_{random.randint(1, 20)}"
        row["_value"] = random.randint(1, 10000)
        data.append(row)
    return data


def _compute_aggregates(
    raw: list[dict[str, Any]],
    dims: list[str],
    measures: list[str],
    agg_type: str,
) -> list[dict[str, Any]]:
    """按维度分组执行聚合计算。"""
    groups: dict[tuple, list[int]] = {}
    for row in raw:
        key = tuple(row.get(d, "?") for d in dims)
        groups.setdefault(key, []).append(row.get("_value", 0))

    result = []
    for key, values in groups.items():
        agg_row = {}
        for i, d in enumerate(dims):
            agg_row[d] = key[i]
        if agg_type == "SUM":
            agg_row["aggregated_value"] = sum(values)
        elif agg_type == "AVG":
            agg_row["aggregated_value"] = sum(values) / len(values)
        elif agg_type == "COUNT":
            agg_row["aggregated_value"] = len(values)
        elif agg_type == "MAX":
            agg_row["aggregated_value"] = max(values)
        elif agg_type == "MIN":
            agg_row["aggregated_value"] = min(values)
        else:
            agg_row["aggregated_value"] = sum(values)
        agg_row["group_count"] = len(values)
        result.append(agg_row)
    return result


def _write_aggregate_cache(
    cube_id: int, dims: list[str], measures: list[str], data: list[dict[str, Any]]
) -> None:
    rows = store.rows("aggregate_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(data)
    cache = {
        "id": cache_id,
        "配置编号": f"AGGR-C{cube_id:04d}",
        "目标立方": f"olap_cube:{cube_id}",
        "依赖立方": str(cube_id),
        "依赖版本": cache_eviction.current_cube_version(store.find("olap_cube", cube_id)),
        "缓存粒度": f"{len(dims)}维 x {len(measures)}度",
        "TTL秒数": "3600",
        "预热策略": "LAZY",
        "命中阈值": "0.8",
        "淘汰算法": "LRU",
        "缓存大小": str(len(data_str)),
        "status": "生效中",
        "pending": True,
        "abnormal": False,
        "cached_data": data,
    }
    rows.append(cache)


def _write_result_cache(cube_id: int, data: list[dict[str, Any]]) -> None:
    rows = store.rows("result_cache")
    cache_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    data_str = str(data)
    cache = {
        "id": cache_id,
        "条目编号": f"RESU-C{cube_id:04d}",
        "查询摘要": f"cube_build_{cube_id}",
        "依赖立方": str(cube_id),
        "依赖版本": cache_eviction.current_cube_version(store.find("olap_cube", cube_id)),
        "结果哈希": hashlib.md5(data_str.encode()).hexdigest()[:12],
        "缓存命中": str(len(data)),
        "过期时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 7200)),
        "存储大小": str(len(data_str)),
        "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "最后命中": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "有效",
        "pending": True,
        "abnormal": False,
    }
    rows.append(cache)


def _write_cube_audit(cube_id: int, elapsed_ms: float, row_count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    status = "正常" if elapsed_ms < 10000 else "慢查询"
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"CUBE_BUILD cube_id={cube_id}",
        "执行耗时": f"{elapsed_ms:.0f}ms",
        "扫描行数": str(row_count),
        "返回行数": str(row_count),
        "数据来源": "olap_cube",
        "操作用户": "cube_builder",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "pending": False,
        "abnormal": elapsed_ms > 30000,
    }
    rows.append(audit)


def _update_metrics(cube_id: int, measures: list[str], data: list[dict[str, Any]]) -> None:
    """更新指标工厂的计算指标。"""
    metrics = store.rows("metric_factory")
    for m in metrics:
        if m.get("依赖立方", "").find(str(cube_id)) >= 0:
            m["最后产出"] = time.strftime("%Y-%m-%d %H:%M:%S")
            m["status"] = "已产出"


def _refresh_dependent_marts(cube_id: int) -> None:
    """刷新依赖此立方的结果集市。"""
    marts = store.rows("result_mart")
    for mart in marts:
        if str(cube_id) in str(mart.get("数据来源", "")):
            mart["status"] = "刷新中"
            mart["pending"] = True