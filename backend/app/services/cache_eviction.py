"""立方缓存淘汰协调器：统一依赖识别、版本归档、淘汰清单与回执。

立方刷新（``olap_cube.refresh_cube``）和手工淘汰（``aggregate_cache.evict_stale_cache``）
原先各自实现了一遍“聚合条目 + 结果条目”的归档判断。这里把依赖匹配、版本范围、
淘汰清单和释放回执收口到一处：

* 清单按 ``(立方, 版本)`` 生成，一次核验产出候选条目与影响范围；
* 每个条目最多被一个入口释放，释放结果同时写回清单和该入口回执；
* 正在被读取的依赖只登记为延后，不更新状态、不计入已释放；
* 重复提交复用同一清单，不会产生第二次淘汰。
"""
from __future__ import annotations

import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from app.store import store

AGGREGATE = "aggregate_cache"
RESULT = "result_cache"
CUBE = "olap_cube"

OWNER_REFRESH = "olap_cube.refresh"
OWNER_MANUAL = "aggregate_cache.evict"

ARCHIVE_MANIFEST_FIELD = "归档清单"
RELEASE_RECEIPT_FIELD = "释放回执"
RELEASE_OWNER_FIELD = "释放入口"

# 两个入口各自的状态范围；None 表示不限状态（沿用原有刷新行为）。
RELEASE_RULES: dict[str, dict[str, dict[str, Any]]] = {
    OWNER_REFRESH: {
        AGGREGATE: {"new_status": "淘汰中", "allowed": None, "abnormal": None},
        RESULT: {"new_status": "已淘汰", "allowed": None, "abnormal": None},
    },
    OWNER_MANUAL: {
        AGGREGATE: {
            "new_status": "淘汰中",
            "allowed": {"生效中", "草稿"},
            "abnormal": True,
        },
        RESULT: {"new_status": "已过期", "allowed": {"有效"}, "abnormal": None},
    },
}

_CUBE_VERSION_FIELDS = ("立方版本", "版本序号", "版本号", "cube_version")
_READER_FLAGS = ("读取中", "正在读取", "访问中", "读取锁定")
_READER_COUNTS = ("活跃读取", "读取计数", "reader_count", "readers")

_LOCK = threading.RLock()
_MANIFESTS: dict[str, "EvictionManifest"] = {}
_MANIFEST_INDEX: dict[tuple[int, str | None], str] = {}
_ACTIVE_REFRESH: dict[int, str] = {}


# ---------------------------------------------------------------------------
# 版本与依赖识别
# ---------------------------------------------------------------------------

def current_cube_version(cube: dict[str, Any] | None) -> str:
    """读取立方版本，缺省视为第 1 版。"""
    if cube:
        for name in _CUBE_VERSION_FIELDS:
            raw = str(cube.get(name) or "").strip()
            if raw:
                return raw
    return "1"


def version_number(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group()) if match else None


def next_cube_version(version: Any) -> str:
    number = version_number(version)
    if number is None:
        text = str(version or "").strip() or "1"
        return f"{text}.1"
    return str(number + 1)


def version_allows(entry_version: str | None, manifest_version: str | None) -> bool:
    """条目依赖版本是否落在本次清单的归档范围内。

    无版本的历史条目按兼容范围纳入；清单没有版本时不做版本限制。
    """
    if entry_version is None or manifest_version is None:
        return True
    entry_number = version_number(entry_version)
    manifest_number = version_number(manifest_version)
    if entry_number is None or manifest_number is None:
        return str(entry_version) == str(manifest_version)
    return entry_number <= manifest_number


def _parse_cube_id(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    match = re.search(r"(?:olap_cube\s*[:#/-]?\s*|cube\s*[:#/-]\s*)(\d+)\b", text, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _explicit_dependency(row: dict[str, Any]) -> tuple[bool, Any]:
    """返回 (是否声明依赖字段, 字段原值)。空值视为未声明。"""
    for name in ("依赖立方", "依赖立方编号"):
        if name in row:
            raw = row.get(name)
            if raw is not None and str(raw).strip():
                return True, raw
    return False, None


def resolve_dependency(module: str, row: dict[str, Any]) -> dict[str, Any]:
    """解析条目的立方依赖和依赖版本。

    ``explicit`` 为 True 表示条目带有依赖数据；若依赖数据无法解析，则
    ``confirmed`` 为 False，该条目不会被纳入淘汰清单，更不会被算作已释放。
    """
    declared, raw_cube = _explicit_dependency(row)
    if declared:
        cube_id = _parse_cube_id(raw_cube)
        return {
            "explicit": True,
            "confirmed": cube_id is not None,
            "cube_id": cube_id,
            "version": str(row.get("依赖版本") or "").strip() or None,
        }
    legacy_cube: int | None = None
    if module == AGGREGATE:
        legacy_cube = _parse_cube_id(row.get("目标立方"))
    return {
        "explicit": False,
        "confirmed": True,
        "cube_id": legacy_cube,
        "version": str(row.get("依赖版本") or "").strip() or None,
    }


def entry_owners(module: str, row: dict[str, Any], cube_id: int) -> set[str]:
    """条目属于哪些入口的匹配范围。

    有依赖数据的条目由两个入口共同认领；历史条目沿用各入口原来的字符串
    包含规则，因此刷新和手工淘汰的既有范围保持不变。
    """
    declared, raw_cube = _explicit_dependency(row)
    if declared:
        if _parse_cube_id(raw_cube) != cube_id:
            return set()
        return {OWNER_REFRESH, OWNER_MANUAL}

    if module == AGGREGATE:
        if str(cube_id) in str(row.get("目标立方", "")):
            return {OWNER_REFRESH, OWNER_MANUAL}
        return set()

    summary = str(row.get("查询摘要", ""))
    owners: set[str] = set()
    if str(cube_id) in summary:  # 刷新原有范围
        owners.add(OWNER_REFRESH)
    if f"cube_{cube_id}" in summary:  # 手工淘汰原有范围
        owners.add(OWNER_MANUAL)
    return owners


def entry_matches_cube(module: str, row: dict[str, Any], cube_id: int) -> bool:
    """依赖匹配的公共入口，供一致性检查等只读场景复用。"""
    if not entry_owners(module, row, cube_id):
        return False
    dependency = resolve_dependency(module, row)
    return not dependency["explicit"] or dependency["confirmed"]


def _entry_no(module: str, row: dict[str, Any]) -> str:
    if module == AGGREGATE:
        return str(row.get("配置编号") or "")
    return str(row.get("条目编号") or "")


def _receipt_item(module: str, row: dict[str, Any]) -> dict[str, Any]:
    if module == AGGREGATE:
        return {
            "type": AGGREGATE,
            "id": row.get("id"),
            "配置编号": row.get("配置编号"),
        }
    return {
        "type": RESULT,
        "id": row.get("id"),
        "条目编号": row.get("条目编号"),
    }


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() not in ("", "0", "false", "no", "none", "null")


def active_readers(row: dict[str, Any]) -> int:
    """正在读取该缓存条目的依赖数；大于 0 时不能立即回收。"""
    for name in _READER_FLAGS:
        if _truthy(row.get(name)):
            return 1
    for name in _READER_COUNTS:
        raw = row.get(name)
        if raw is None:
            continue
        try:
            count = int(str(raw).strip())
        except (TypeError, ValueError):
            count = 1 if _truthy(raw) else 0
        if count > 0:
            return count
    return 0


# ---------------------------------------------------------------------------
# 清单
# ---------------------------------------------------------------------------

@dataclass
class ManifestEntry:
    module: str
    entry_id: int
    entry_no: str
    owners: set[str] = field(default_factory=set)
    version: str | None = None
    state: str = "pending"  # pending / released / deferred / skipped / lost / elsewhere
    released_by: str | None = None
    receipt_id: str | None = None
    reason: str | None = None
    readers: int = 0

    @property
    def key(self) -> str:
        return f"{self.module}:{self.entry_id}"


@dataclass
class EvictionManifest:
    manifest_id: str
    cube_id: int
    cube_version: str | None
    created_at: str
    entries: dict[str, ManifestEntry] = field(default_factory=dict)
    receipts: list[dict[str, Any]] = field(default_factory=list)
    requested_by: list[str] = field(default_factory=list)
    impact: dict[str, Any] = field(default_factory=dict)
    finished_at: str | None = None

    def entry_snapshot(self) -> list[dict[str, Any]]:
        rows = []
        for item in self.entries.values():
            rows.append({
                "module": item.module,
                "entry_id": item.entry_id,
                "entry_no": item.entry_no,
                "owners": sorted(item.owners),
                "version": item.version,
                "state": item.state,
                "released_by": item.released_by,
                "receipt_id": item.receipt_id,
                "reason": item.reason,
                "readers": item.readers,
            })
        return rows

    def snapshot(self) -> dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "cube_id": self.cube_id,
            "cube_version": self.cube_version,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "requested_by": list(self.requested_by),
            "impact": dict(self.impact),
            "entries": self.entry_snapshot(),
            "receipts": [dict(receipt) for receipt in self.receipts],
        }


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _verify(manifest: EvictionManifest, request_owner: str) -> None:
    """一次核验：补齐清单候选并登记影响范围。调用方必须持有锁。"""
    if request_owner not in manifest.requested_by:
        manifest.requested_by.append(request_owner)

    unconfirmed: set[str] = set()
    for previous in manifest.impact.get("unconfirmed", []):
        unconfirmed.add(str(previous.get("key")))

    modules = (AGGREGATE, RESULT)
    for module in modules:
        for row in list(store.rows(module)):
            try:
                entry_id = int(row.get("id"))
            except (TypeError, ValueError):
                continue
            key = f"{module}:{entry_id}"
            dependency = resolve_dependency(module, row)
            if dependency["explicit"] and not dependency["confirmed"]:
                unconfirmed.add(key)
                continue
            owners = entry_owners(module, row, manifest.cube_id)
            if not owners:
                continue
            if not version_allows(dependency["version"], manifest.cube_version):
                continue
            if row.get(ARCHIVE_MANIFEST_FIELD):
                # 已被其它清单释放：重复提交不再产生第二次淘汰。
                continue
            existing = manifest.entries.get(key)
            if existing is not None:
                existing.owners |= owners
                continue
            manifest.entries[key] = ManifestEntry(
                module=module,
                entry_id=entry_id,
                entry_no=_entry_no(module, row),
                owners=set(owners),
                version=dependency["version"],
            )

    by_module: dict[str, int] = {}
    by_version: dict[str, int] = {}
    for item in manifest.entries.values():
        by_module[item.module] = by_module.get(item.module, 0) + 1
        version_key = item.version or "unknown"
        by_version[version_key] = by_version.get(version_key, 0) + 1
    manifest.impact = {
        "cube_id": manifest.cube_id,
        "cube_version": manifest.cube_version,
        "verified_at": _now(),
        "requested_by": list(manifest.requested_by),
        "scope_count": len(manifest.entries),
        "by_module": by_module,
        "by_version": by_version,
        "unconfirmed": [
            {"key": key, "reason": "依赖数据无法确认"}
            for key in sorted(unconfirmed)
        ],
        "unconfirmed_count": len(unconfirmed),
    }
    _refresh_progress(manifest)


def _refresh_progress(manifest: EvictionManifest) -> None:
    """按清单当前状态登记已释放/延后数量，不改动任何条目或回执。"""
    manifest.impact["released_count"] = sum(
        1 for item in manifest.entries.values() if item.state == "released"
    )
    manifest.impact["deferred_count"] = sum(
        1 for item in manifest.entries.values() if item.state == "deferred"
    )


def _get_or_create(cube_id: int, version: str | None, owner: str) -> EvictionManifest:
    with _LOCK:
        key = (cube_id, version)
        manifest_id = _MANIFEST_INDEX.get(key)
        manifest = _MANIFESTS.get(manifest_id) if manifest_id else None
        if manifest is None:
            manifest = EvictionManifest(
                manifest_id=f"EVCT-{uuid.uuid4().hex[:10]}",
                cube_id=cube_id,
                cube_version=version,
                created_at=_now(),
            )
            _MANIFESTS[manifest.manifest_id] = manifest
            _MANIFEST_INDEX[key] = manifest.manifest_id
        _verify(manifest, owner)
        return manifest


def _manifest_version(cube_id: int) -> str | None:
    cube = store.find(CUBE, cube_id)
    return current_cube_version(cube) if cube is not None else None


def begin_refresh(cube_id: int, version: str | None = None) -> EvictionManifest:
    """刷新入口开清单：清单版本是即将被替换的旧版本。"""
    with _LOCK:
        target = version if version is not None else _manifest_version(cube_id)
        manifest = _get_or_create(cube_id, target, OWNER_REFRESH)
        _ACTIVE_REFRESH[cube_id] = manifest.manifest_id
        return manifest


def finish_refresh(manifest: EvictionManifest) -> None:
    with _LOCK:
        # 没有产生过释放的刷新（例如构建失败）不封单，重试仍按同一份核验处理。
        if manifest.receipts:
            manifest.finished_at = _now()
        active = _ACTIVE_REFRESH.get(manifest.cube_id)
        if active == manifest.manifest_id:
            _ACTIVE_REFRESH.pop(manifest.cube_id, None)


def _terminal_for(manifest: EvictionManifest, owner: str) -> bool:
    return all(
        item.state in ("released", "skipped", "lost", "elsewhere")
        for item in manifest.entries.values()
        if owner in item.owners
    )


def _released_by_others(manifest: EvictionManifest, owner: str) -> list[dict[str, Any]]:
    rows = []
    for item in manifest.entries.values():
        if item.state != "released" or not item.released_by or item.released_by == owner:
            continue
        detail = {"type": item.module, "id": item.entry_id}
        detail["配置编号" if item.module == AGGREGATE else "条目编号"] = item.entry_no
        detail["released_by"] = item.released_by
        detail["receipt_id"] = item.receipt_id
        rows.append(detail)
    return rows


def released_by_others(manifest: EvictionManifest, owner: str | None = None) -> list[dict[str, Any]]:
    """清单里由其它入口释放的条目。调用方必须持有锁。"""
    if owner is None:
        owner = manifest.requested_by[-1] if manifest.requested_by else ""
    return _released_by_others(manifest, owner)


def _release(manifest: EvictionManifest, owner: str) -> dict[str, Any]:
    """按清单核验结果释放条目；同一清单内每个条目只会被一个入口释放。"""
    with _LOCK:
        if owner not in manifest.requested_by:
            _verify(manifest, owner)

        receipt_id = f"EVRT-{uuid.uuid4().hex[:10]}"
        released: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for item in manifest.entries.values():
            if owner not in item.owners or item.state == "released":
                continue
            row = store.find(item.module, item.entry_id)
            if row is None:
                item.state = "lost"
                item.reason = "条目已不存在"
                continue

            archived_by = str(row.get(ARCHIVE_MANIFEST_FIELD) or "").strip()
            if archived_by and archived_by != manifest.manifest_id:
                # 另一份清单已经释放并登记，本入口不能再声称释放同一入口。
                item.state = "elsewhere"
                item.released_by = str(row.get(RELEASE_OWNER_FIELD) or "") or None
                item.receipt_id = row.get(RELEASE_RECEIPT_FIELD)
                item.reason = f"已由清单 {archived_by} 释放"
                skipped.append({
                    "module": item.module,
                    "id": item.entry_id,
                    "entry_no": item.entry_no,
                    "status": row.get("status"),
                    "archived_by": archived_by,
                })
                continue

            readers = active_readers(row)
            if readers:
                # 已核实的部分可以保留在清单里，但依赖正在读取时不能立即回收。
                item.state = "deferred"
                item.reason = "依赖正在读取"
                item.readers = readers
                deferred.append({
                    "module": item.module,
                    "id": item.entry_id,
                    "entry_no": item.entry_no,
                    "readers": readers,
                })
                continue

            rule = RELEASE_RULES[owner][item.module]
            status = row.get("status")
            if rule["allowed"] is not None and status not in rule["allowed"]:
                item.state = "skipped"
                item.reason = f"状态 {status} 不在{owner}范围"
                skipped.append({
                    "module": item.module,
                    "id": item.entry_id,
                    "entry_no": item.entry_no,
                    "status": status,
                })
                continue

            # 单条目认领：状态更新与回执登记来自同一次核验结果。
            row["status"] = rule["new_status"]
            if rule["abnormal"] is not None:
                row["abnormal"] = rule["abnormal"]
            row[ARCHIVE_MANIFEST_FIELD] = manifest.manifest_id
            row[RELEASE_RECEIPT_FIELD] = receipt_id
            row[RELEASE_OWNER_FIELD] = owner

            item.state = "released"
            item.released_by = owner
            item.receipt_id = receipt_id
            item.reason = None
            released.append(_receipt_item(item.module, row))

        complete = _terminal_for(manifest, owner)
        receipt = {
            "receipt_id": receipt_id,
            "owner": owner,
            "manifest_id": manifest.manifest_id,
            "cube_id": manifest.cube_id,
            "cube_version": manifest.cube_version,
            "evicted_count": len(released),
            "entries": released,
            "deferred_count": len(deferred),
            "deferred": deferred,
            "skipped_count": len(skipped),
            "skipped": skipped,
            "released_by_others": _released_by_others(manifest, owner),
            "complete": complete,
            "unconfirmed_count": manifest.impact.get("unconfirmed_count", 0),
        }
        manifest.receipts.append({
            "receipt_id": receipt_id,
            "owner": owner,
            "at": _now(),
            "evicted_count": len(released),
            "deferred_count": len(deferred),
            "skipped_count": len(skipped),
        })
        _refresh_progress(manifest)
        return receipt


def evict_for_refresh(manifest: EvictionManifest) -> dict[str, Any]:
    return _release(manifest, OWNER_REFRESH)


def evict_for_manual(cube_id: int) -> dict[str, Any]:
    """手工淘汰入口：刷新进行中时并入同一份清单，否则按当前版本开清单。"""
    with _LOCK:
        active_id = _ACTIVE_REFRESH.get(cube_id)
        manifest = _MANIFESTS.get(active_id) if active_id else None
        if manifest is None:
            manifest = _get_or_create(cube_id, _manifest_version(cube_id), OWNER_MANUAL)
        if manifest.finished_at is not None:
            # 已结束清单只复用核验结果，重复提交不产生第二次淘汰。
            return _replayed_receipt(manifest, OWNER_MANUAL)
        return _release(manifest, OWNER_MANUAL)


def _replayed_receipt(manifest: EvictionManifest, owner: str) -> dict[str, Any]:
    released = [
        _receipt_item(item.module, {
            "id": item.entry_id,
            "配置编号": item.entry_no,
            "条目编号": item.entry_no,
        })
        for item in manifest.entries.values()
        if item.released_by == owner
    ]
    return {
        "receipt_id": None,
        "owner": owner,
        "manifest_id": manifest.manifest_id,
        "cube_id": manifest.cube_id,
        "cube_version": manifest.cube_version,
        "evicted_count": 0,
        "entries": [],
        "deferred_count": 0,
        "deferred": [],
        "skipped_count": 0,
        "skipped": [],
        "released_by_others": _released_by_others(manifest, owner),
        "complete": True,
        "unconfirmed_count": manifest.impact.get("unconfirmed_count", 0),
        "replayed": True,
        "previously_released_count": len(released),
        "previously_released": released,
    }


# ---------------------------------------------------------------------------
# 只读快照
# ---------------------------------------------------------------------------

def manifest_snapshot(
    manifest_id: str | None = None,
    cube_id: int | None = None,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    with _LOCK:
        if manifest_id is not None:
            manifest = _MANIFESTS.get(manifest_id)
            return manifest.snapshot() if manifest else None
        if cube_id is not None:
            for manifest in reversed(list(_MANIFESTS.values())):
                if manifest.cube_id == cube_id:
                    return manifest.snapshot()
            return None
        return [manifest.snapshot() for manifest in _MANIFESTS.values()]


def reset_tracking() -> None:
    """清空运行时清单（测试隔离用）。"""
    with _LOCK:
        _MANIFESTS.clear()
        _MANIFEST_INDEX.clear()
        _ACTIVE_REFRESH.clear()
