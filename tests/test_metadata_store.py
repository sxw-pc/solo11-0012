"""SQLite 任务、分片指针、历史、索引检索与回滚测试。"""
from __future__ import annotations

from pathlib import Path

from time_archiver.metadata_store import MetadataStore, utc_now
from time_archiver.schema import IndexEntry, ShardRecord


def shard(shard_id="shard-1", task_id="task-1", path="archives/a.tar.gz",
          digest="a" * 64, tier="archive") -> ShardRecord:
    return ShardRecord(shard_id, task_id, tier, "source", "2024-01",
                       path, "tar.gz", 10, 60, 2, 3, digest, utc_now())


def entry(shard_id="shard-1", package_path="archives/a.tar.gz") -> IndexEntry:
    return IndexEntry(shard_id, package_path, "data.parquet", 512, 1024, 10,
                      "2024-01-01T00:00:00Z", "2024-01-31T00:00:00Z",
                      {"sensor_id": ["s-a"], "metric_group": ["environment"]},
                      "archive", "source")


def test_task_lifecycle(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    created = store.start_task("task-1", "archive", "sensor", "policy-hash",
                               "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z")
    assert created["status"] == "running"
    assert store.start_task("task-1", "archive", "sensor", "other")["task_id"] == "task-1"
    store.update_task("task-1", stage="tier:hot", unknown="ignored")
    store.update_task("task-1", nonsense="ignored")
    store.finish_task("task-1", "completed", {"shards": 2})
    task = store.get_task("task-1")
    assert task["stage"] == "finished"
    assert task["result"] == {"shards": 2}
    assert task["finished_at"]
    assert store.get_task("missing") is None
    assert store.list_tasks(status="completed")[0]["task_id"] == "task-1"
    assert store.list_tasks(kind="archive")[0]["task_id"] == "task-1"
    assert store.list_tasks(kind="other") == []
    assert store.latest_task("archive")["task_id"] == "task-1"
    assert store.latest_task("other") is None


def test_save_shard_idempotent_and_history(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    first = shard()
    assert store.save_shard(first) is True
    assert store.save_shard(first) is False
    assert store.counts()["shards"] == 1
    changed = shard(task_id="task-2", path="archives/b.tar.gz", digest="b" * 64)
    assert store.save_shard(changed) is True
    assert store.get_shard("shard-1")["path"] == "archives/b.tar.gz"
    assert [s["task_id"] for s in store.shards_for_task("task-2")] == ["task-2"]
    restored = store.rollback_task("task-2")
    assert restored[0]["current_path"] == "archives/b.tar.gz"
    assert store.get_shard("shard-1")["path"] == "archives/a.tar.gz"
    assert store.get_shard("shard-1")["task_id"] == "task-1"


def test_rollback_new_shard_deactivates(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    store.start_task("task-only", "archive", "sensor", "hash")
    store.save_shard(shard(task_id="task-only"))
    store.replace_index([entry()])
    assert store.search(tag_key="sensor_id", tag_value="s-a")
    changed = store.rollback_task("task-only")
    assert len(changed) == 1
    assert store.get_shard("shard-1")["active"] == 0
    assert store.search() == []
    assert store.list_shards(active_only=True) == []
    assert store.index_summary() == {"index_shards": 0, "label_entries": 0}


def test_index_search_by_time_and_tags(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    store.save_shard(shard("s1", "t1", "archives/s1.tar.gz"))
    store.save_shard(shard("s2", "t1", "archives/s2.tar.gz", digest="b" * 64))
    first = entry(shard_id="s1", package_path="archives/s1.tar.gz")
    second = IndexEntry("s2", "archives/s2.tar.gz", "data.parquet", 1024, 2048, 20,
                        "2024-02-01T00:00:00Z", "2024-02-29T00:00:00Z",
                        {"sensor_id": ["s-b"]}, "archive", "source")
    store.replace_index([first, second])
    assert len(store.search()) == 2
    assert store.search(start="2024-02-10")[0]["shard_id"] == "s2"
    assert store.search(end="2024-01-15")[0]["shard_id"] == "s1"
    assert store.search(tag_key="sensor_id", tag_value="s-b")[0]["shard_id"] == "s2"
    assert store.search(tag_key="sensor_id", tag_value="none") == []
    assert store.search(limit=1)[0]["shard_id"] == "s1"
    assert store.index_entries_for_shard("s1")[0]["tags"]["sensor_id"] == ["s-a"]
    assert store.index_summary() == {"index_shards": 2, "label_entries": 3}
    store.replace_index([first, second])
    assert len(store.search()) == 2
    assert store.index_summary() == {"index_shards": 2, "label_entries": 3}


def test_list_shards_and_counts(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    store.save_shard(shard("s1", "t1", tier="hot", path="tiers/hot/a.parquet"))
    store.save_shard(shard("s2", "t1", tier="cold", path="tiers/cold/a.parquet",
                           digest="c" * 64))
    assert len(store.list_shards()) == 2
    assert [s["tier"] for s in store.list_shards(tier="hot")] == ["hot"]
    assert store.counts() == {"tasks": 0, "shards": 2, "archives": 0}
    assert store.shards_for_task("none") == []


def test_legacy_schema_migrates_additively(workdir: Path):
    import sqlite3
    db_path = workdir / "state" / "legacy.db"
    # 先按新结构写入数据，再手动回退成旧版表结构
    store = MetadataStore(db_path)
    store.save_shard(shard())
    store.replace_index([entry()])
    connection = sqlite3.connect(db_path)
    connection.executescript(
        "CREATE TABLE old_idx AS SELECT id,shard_id,package_path,member_name,offset,"
        "size,row_count,min_ts,max_ts,tags_json,tier,source FROM archive_index; "
        "DROP TABLE archive_index; ALTER TABLE old_idx RENAME TO archive_index; "
        "DROP TABLE restore_tasks; DROP TABLE restore_units;")
    connection.commit()
    connection.close()
    # 重新打开应自动补列补表，且历史行不丢
    migrated = MetadataStore(db_path)
    rows = migrated.search()
    assert len(rows) == 1
    assert rows[0]["member_sha256"] == ""
    spec = {"start": "", "end": "", "labels": []}
    migrated.save_restore_task("restore-x", spec, "restored", 2, 3)
    migrated.save_restore_unit("restore-x", 0, "pending", "archives/a.tar.gz", 1,
                               {"data.parquet": "d"}, ["a.parquet"],
                               str(workdir / "staging"))
    units = migrated.list_restore_units("restore-x")
    assert units[0]["member_sha256"] == {"data.parquet": "d"}
    assert migrated.get_restore_task("restore-x")["total_units"] == 2
    migrated.update_restore_task("restore-x", "completed")
    assert migrated.get_restore_task("restore-x")["status"] == "completed"


def test_search_members_multi_label_and(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    store.save_shard(shard("s1", "t1", "archives/s1.tar.gz"))
    store.save_shard(shard("s2", "t1", "archives/s2.tar.gz", digest="b" * 64))
    first = entry(shard_id="s1", package_path="archives/s1.tar.gz")
    second = IndexEntry("s2", "archives/s2.tar.gz", "data.parquet", 1024, 2048, 20,
                        "2024-02-01T00:00:00Z", "2024-02-29T00:00:00Z",
                        {"sensor_id": ["s-b"], "group": ["g2"]}, "archive", "source")
    store.replace_index([first, second])
    assert len(store.search_members(labels=[("sensor_id", "s-a")])) == 1
    assert store.search_members(labels=[("sensor_id", "s-b"),
                                        ("group", "g2")])[0]["shard_id"] == "s2"
    assert store.search_members(labels=[("sensor_id", "s-b"),
                                        ("group", "missing")]) == []
    assert store.search_members(start="2024-02-01")[0]["package_sha256"] == "b" * 64


def test_search_members_without_limit_never_truncates(workdir: Path):
    store = MetadataStore(workdir / "state" / "store.db")
    total = 25
    for i in range(total):
        sid = f"s-{i:02d}"
        store.save_shard(shard(sid, "t1", f"archives/{sid}.tar.gz",
                               digest=("a" * 63) + format(i, "x")))
        store.replace_index([IndexEntry(
            sid, f"archives/{sid}.tar.gz", "data.parquet", 0, 10, 1,
            "2024-01-01T00:00:00Z", "2024-01-31T00:00:00Z",
            {"sensor_id": [f"v{i}"]}, "archive", "source")])
    # 默认交互查询有上限；批量恢复路径用 limit=None 必须取全量
    assert len(store.search_members(limit=10)) == 10
    assert len(store.search_members(limit=None)) == total
