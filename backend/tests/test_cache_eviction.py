import pytest

from app.services import cache_eviction
from app.services.aggregate_cache import AggregateCacheService
from app.services.olap_cube import OlapCubeService
from app.store import store


def _cube(svc: OlapCubeService, no: str, version: str | None = None) -> dict:
    entry, missing = svc.create_entry({
        "立方编号": no, "立方名称": no, "维度列表": "region", "度量列表": "value",
        "聚合类型": "SUM", "分区策略": "dt", "刷新周期": "3600", "数据行数": "0"})
    assert entry is not None, missing
    if version is not None:
        entry["立方版本"] = version
    return entry


def _legacy_agg(no: str, cube_no: str, status: str = "生效中", **extra) -> dict:
    rows = store.rows("aggregate_cache")
    row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
           "配置编号": no, "目标立方": cube_no, "缓存粒度": "1维x1度",
           "TTL秒数": "3600", "预热策略": "LAZY", "命中阈值": "0.8",
           "淘汰算法": "LRU", "缓存大小": "10", "status": status,
           "pending": True, "abnormal": False}
    row.update(extra)
    rows.append(row)
    return row


def _legacy_result(no: str, summary: str, status: str = "有效", **extra) -> dict:
    rows = store.rows("result_cache")
    row = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
           "条目编号": no, "查询摘要": summary, "结果哈希": "h", "缓存命中": "0",
           "过期时间": "2027-01-01 00:00:00", "存储大小": "10",
           "创建时间": "2026-11-01 00:00:00", "最后命中": "2026-11-01 00:00:00",
           "status": status, "pending": True, "abnormal": False}
    row.update(extra)
    rows.append(row)
    return row


@pytest.fixture(autouse=True)
def clean():
    cache_eviction.reset_tracking()
    yield
    cache_eviction.reset_tracking()


class TestLegacyEntryPoints:
    def test_refresh_keeps_legacy_scope_and_status(self):
        svc = OlapCubeService()
        cube = _cube(svc, "TEST-LEG-R")
        other = _cube(svc, "TEST-LEG-R2")
        cid, oid = cube["id"], other["id"]

        old_agg = _legacy_agg("AGGR-LEG-R1", f"olap_cube:{cid}", status="已停用")
        old_res = _legacy_result("RESU-LEG-R1", f"cube_build_{cid}")
        foreign_res = _legacy_result("RESU-LEG-R2", f"cube_build_{cid}_v2")
        other_res = _legacy_result("RESU-LEG-R3", f"cube_build_{oid}")
        other_agg = _legacy_agg("AGGR-LEG-R2", f"olap_cube:{oid}")

        before_other = (other_res["status"], other_agg["status"])
        result = svc.refresh_cube(cid)

        assert result["ok"]
        assert old_agg["status"] == "淘汰中"      # 刷新不限起始状态
        assert old_res["status"] == "已淘汰"      # 刷新沿用“已淘汰”
        assert foreign_res["status"] == "已淘汰"  # 字符串包含匹配保持不变
        assert (other_res["status"], other_agg["status"]) == before_other
        assert result["cube_version"] == "2"
        assert result["eviction"]["evicted_count"] == 3
        assert result["eviction"]["manifest_id"]

    def test_manual_keeps_legacy_scope_and_status(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-LEG-M", version="1")
        cid = cube["id"]
        other = _cube(svc, "TEST-LEG-M2")
        oid = other["id"]

        live_agg = _legacy_agg("AGGR-LEG-M1", f"olap_cube:{cid}")
        stopped_agg = _legacy_agg("AGGR-LEG-M2", f"olap_cube:{cid}", status="已停用")
        draft_agg = _legacy_agg("AGGR-LEG-M3", f"olap_cube:{cid}", status="草稿")
        live_res = _legacy_result("RESU-LEG-M1", f"prewarm_cube_{cid}")
        untouched_res = _legacy_result("RESU-LEG-M2", f"cube_build_{cid}")       # 只在刷新范围
        other_res = _legacy_result("RESU-LEG-M3", f"prewarm_cube_{oid}")

        resp = agg_svc.evict_stale_cache(cid)

        assert resp["ok"] and resp["evicted_count"] == 3
        assert live_agg["status"] == "淘汰中" and live_agg["abnormal"] is True
        assert draft_agg["status"] == "淘汰中" and draft_agg["abnormal"] is True
        assert stopped_agg["status"] == "已停用" and stopped_agg["abnormal"] is False
        assert live_res["status"] == "已过期"
        assert untouched_res["status"] == "有效"   # 手工入口原本不碰它
        assert other_res["status"] == "有效"
        # 原有回执形状仍在
        assert {e["type"] for e in resp["entries"]} <= {"aggregate_cache", "result_cache"}

    def test_repeat_submit_does_not_evict_twice(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-REPEAT", version="1")
        cid = cube["id"]
        agg = _legacy_agg("AGGR-REP-1", f"olap_cube:{cid}")
        res = _legacy_result("RESU-REP-1", f"prewarm_cube_{cid}")

        first = agg_svc.evict_stale_cache(cid)
        second = agg_svc.evict_stale_cache(cid)

        assert first["evicted_count"] == 2
        assert second["evicted_count"] == 0
        assert first["manifest_id"] == second["manifest_id"]
        assert first["receipt_id"] != second["receipt_id"]
        assert agg["status"] == "淘汰中" and res["status"] == "已过期"
        assert agg["归档清单"] == first["manifest_id"]

        third = agg_svc.evict_stale_cache(cid)
        assert third["evicted_count"] == 0
        # 重复提交复用同一清单，不另登记释放，也不算作已释放。
        assert third["manifest_id"] == first["manifest_id"]
        assert first["entries"] and {e["id"] for e in first["entries"]} == {agg["id"], res["id"]}

    def test_each_entry_released_once_in_shared_manifest(self):
        svc = OlapCubeService()
        manifest = cache_eviction.begin_refresh(1)
        refresh_receipt = cache_eviction.evict_for_refresh(manifest)
        manual_receipt = cache_eviction.evict_for_manual(1)
        cache_eviction.finish_refresh(manifest)

        released_by_refresh = {e["id"] for e in refresh_receipt["entries"]}
        released_by_manual = {e["id"] for e in manual_receipt["entries"]}
        assert released_by_refresh and not released_by_manual
        for row in store.rows("aggregate_cache") + store.rows("result_cache"):
            if row["id"] in released_by_refresh and row.get("归档清单") == manifest.manifest_id:
                assert row["释放入口"] == cache_eviction.OWNER_REFRESH
                assert row["释放回执"] == refresh_receipt["receipt_id"]

    def test_concurrent_refresh_and_manual_release_only_what_receipts_prove(self):
        import threading

        svc = OlapCubeService()
        cube = _cube(svc, "TEST-RACE", version="1")
        cid = cube["id"]
        entries = [
            _legacy_agg("AGGR-RACE-1", f"olap_cube:{cid}"),
            _legacy_agg("AGGR-RACE-2", f"olap_cube:{cid}"),
            _legacy_result("RESU-RACE-1", f"prewarm_cube_{cid}"),
            _legacy_result("RESU-RACE-2", f"cube_build_{cid}"),
        ]
        before = {(row.get("配置编号") or row.get("条目编号")): row["status"] for row in entries}

        gate = threading.Barrier(2)
        receipts: dict[str, dict] = {}

        def refresh_path():
            manifest = cache_eviction.begin_refresh(cid)
            gate.wait()
            receipts["refresh"] = cache_eviction.evict_for_refresh(manifest)
            cache_eviction.finish_refresh(manifest)

        def manual_path():
            gate.wait()
            receipts["manual"] = cache_eviction.evict_for_manual(cid)

        threads = [threading.Thread(target=refresh_path), threading.Thread(target=manual_path)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        def keys(receipt, field="entries"):
            return {(e["type"], e["id"]) for e in receipt[field]}

        refresh_ids = keys(receipts["refresh"])
        manual_ids = keys(receipts["manual"])
        assert refresh_ids.isdisjoint(manual_ids)
        assert receipts["refresh"]["manifest_id"] == receipts["manual"]["manifest_id"]
        assert len(refresh_ids) + len(manual_ids) == len(entries)

        changed = {
            (("aggregate_cache" if "配置编号" in row else "result_cache"), row["id"])
            for row in entries
            if row["status"] != before[row.get("配置编号") or row.get("条目编号")]
        }
        receipt_proven = refresh_ids | manual_ids | keys(
            receipts["manual"], "released_by_others") | keys(receipts["refresh"], "released_by_others")
        assert changed == receipt_proven
        for row in entries:
            key = ("aggregate_cache" if "配置编号" in row else "result_cache", row["id"])
            if key in changed:
                assert row["归档清单"] == receipts["refresh"]["manifest_id"]
                assert row["释放回执"] in {
                    receipts["refresh"]["receipt_id"], receipts["manual"]["receipt_id"]}

        manifest = cache_eviction.manifest_snapshot(cube_id=cid)
        assert len(manifest["receipts"]) == 2
        assert sum(r["evicted_count"] for r in manifest["receipts"]) == len(entries)
        assert manifest["impact"]["released_count"] == len(entries)
        assert manifest["impact"]["deferred_count"] == 0


class TestConflictAndConcurrency:
    def test_manual_joins_refresh_manifest_without_double_release(self):
        svc = OlapCubeService()
        manifest = cache_eviction.begin_refresh(1)
        snapshot_before = [e["id"] for e in store.rows("aggregate_cache")
                           if str(1) in str(e.get("目标立方", ""))]

        refresh_receipt = cache_eviction.evict_for_refresh(manifest)
        manual_receipt = cache_eviction.evict_for_manual(1)

        released_ids = [e["id"] for e in refresh_receipt["entries"]]
        manual_ids = [e["id"] for e in manual_receipt["entries"]]
        assert set(released_ids).isdisjoint(manual_ids)
        assert manual_receipt["manifest_id"] == manifest.manifest_id
        assert manual_receipt["evicted_count"] == 0
        assert {e["id"] for e in manual_receipt["released_by_others"]} == set(released_ids)
        assert set(snapshot_before).issuperset(released_ids)
        cache_eviction.finish_refresh(manifest)

    def test_deferred_when_dependency_is_being_read(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-READ", version="1")
        cid = cube["id"]
        busy = _legacy_agg("AGGR-READ-1", f"olap_cube:{cid}", 读取中=True)
        free = _legacy_agg("AGGR-READ-2", f"olap_cube:{cid}")

        resp = agg_svc.evict_stale_cache(cid)

        assert busy["status"] == "生效中"
        assert busy.get("归档清单") is None
        assert free["status"] == "淘汰中"
        assert resp["deferred_count"] == 1
        assert [d["id"] for d in resp["deferred"]] == [busy["id"]]
        assert resp["evicted_count"] == 1

        manifest = cache_eviction.manifest_snapshot(cube_id=cid)
        states = {e["entry_id"]: e["state"] for e in manifest["entries"]}
        assert states[busy["id"]] == "deferred"
        assert states[free["id"]] == "released"

    def test_deferred_entry_is_reclaimed_after_readers_finish(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-READ-2", version="1")
        cid = cube["id"]
        busy = _legacy_agg("AGGR-READ-3", f"olap_cube:{cid}", 读取中=True)

        first = agg_svc.evict_stale_cache(cid)
        assert first["evicted_count"] == 0 and first["deferred_count"] == 1
        assert busy["status"] == "生效中"

        busy["读取中"] = False
        second = agg_svc.evict_stale_cache(cid)

        assert second["evicted_count"] == 1
        assert busy["status"] == "淘汰中" and busy["abnormal"] is True
        assert busy["归档清单"] == second["manifest_id"]
        assert busy["释放回执"] == second["receipt_id"]
        assert first["manifest_id"] == second["manifest_id"]

    def test_unconfirmed_scope_is_not_released(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-BAD", version="1")
        cid = cube["id"]
        broken = _legacy_agg("AGGR-BAD-1", "olap_cube:not-a-number", 依赖立方="cube?")
        good = _legacy_agg("AGGR-BAD-2", f"olap_cube:{cid}")

        resp = agg_svc.evict_stale_cache(cid)

        assert broken["status"] == "生效中" and broken.get("归档清单") is None
        assert good["status"] == "淘汰中"
        assert resp["evicted_count"] == 1

        manifest = cache_eviction.manifest_snapshot(cube_id=cid)
        assert manifest["impact"]["unconfirmed_count"] == 1
        assert manifest["impact"]["unconfirmed"][0]["key"] == f"aggregate_cache:{broken['id']}"

    def test_versioned_entries_follow_cube_version(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-VER", version="3")
        cid = cube["id"]
        v2 = _legacy_agg("AGGR-VER-1", f"olap_cube:{cid}", 依赖立方=str(cid), 依赖版本="2")
        v4 = _legacy_agg("AGGR-VER-2", f"olap_cube:{cid}", 依赖立方=str(cid), 依赖版本="4")
        legacy = _legacy_agg("AGGR-VER-3", f"olap_cube:{cid}")

        refresh = svc.refresh_cube(cid)

        assert v2["status"] == "淘汰中"
        assert v4["status"] == "生效中"
        assert legacy["status"] == "淘汰中"          # 无版本历史条目仍在兼容范围
        assert refresh["eviction"]["evicted_count"] == 2
        assert refresh["cube_version"] == "4"

        # 刷新清单固定在旧版本，重建出的新版本条目不在其归档范围
        manifest = cache_eviction.manifest_snapshot(manifest_id=refresh["eviction"]["manifest_id"])
        manifest_ids = {e["entry_id"] for e in manifest["entries"]}
        assert {v2["id"], legacy["id"]} <= manifest_ids
        assert v4["id"] not in manifest_ids
        assert all(e["version"] != "4" for e in manifest["entries"])

        # 同一版本重复提交复用同一清单，不再产生淘汰
        replay = cache_eviction.begin_refresh(cid, version="3")
        assert replay.manifest_id == refresh["eviction"]["manifest_id"]
        assert cache_eviction.evict_for_refresh(replay)["evicted_count"] == 0
        cache_eviction.finish_refresh(replay)

    def test_archived_entries_skipped_by_refresh(self):
        svc = OlapCubeService()
        agg_svc = AggregateCacheService()
        cube = _cube(svc, "TEST-SKIP", version="1")
        cid = cube["id"]
        stopped = _legacy_agg("AGGR-SKIP-1", f"olap_cube:{cid}", status="已停用")
        first = agg_svc.evict_stale_cache(cid)
        assert first["evicted_count"] == 0
        assert first["skipped_count"] == 1

        result = svc.refresh_cube(cid)
        # 手工入口跳过的已停用条目仍由刷新入口按原范围处理
        assert result["eviction"]["evicted_count"] >= 1
        assert stopped["status"] == "淘汰中"
        assert stopped["释放入口"] == cache_eviction.OWNER_REFRESH

    def test_repeated_refresh_reuses_verification_without_evicting_again(self):
        svc = OlapCubeService()
        cube = _cube(svc, "TEST-RFR-REP", version="1")
        cid = cube["id"]
        old = _legacy_agg("AGGR-RFR-1", f"olap_cube:{cid}")
        provided = _legacy_agg(
            "AGGR-RFR-2", f"olap_cube:{cid}", 依赖立方=str(cid), 依赖版本="1")
        new_version = _legacy_agg(
            "AGGR-RFR-3", f"olap_cube:{cid}", 依赖立方=str(cid), 依赖版本="9")

        first = svc.refresh_cube(cid)
        assert first["eviction"]["evicted_count"] == 2
        assert first["cube_version"] == "2"
        assert new_version["status"] == "生效中"

        second = svc.refresh_cube(cid)
        # 第二次刷新是新版本：只归档第二次构建前的条目，不重复记第一次的释放
        assert second["cube_version"] == "3"
        assert old["释放回执"] == first["eviction"]["receipt_id"]
        assert provided["释放回执"] == first["eviction"]["receipt_id"]
        assert second["eviction"]["manifest_id"] != first["eviction"]["manifest_id"]

        # 旧清单已结束：同一版本重复提交只复述回执，不产生第二次淘汰
        replay = cache_eviction.begin_refresh(cid, version="1")
        replay_receipt = cache_eviction.evict_for_refresh(replay)
        assert replay_receipt["evicted_count"] == 0
