"""分层归档全链路、幂等性、断点重试、恢复与回滚测试。"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from time_archiver.archiver import Archiver
from time_archiver.audit import AuditLog
from time_archiver.config_loader import load_policy
from time_archiver.fixture_loader import demo_sources
from time_archiver.schema import ArchiveError, DataSourceError, SourceSpec


def output_hashes(root: Path):
    records = []
    ignored = {"state", "audit", "staging", "policy.yaml"}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part in ignored for part in relative.parts):
            continue
        records.append((relative.as_posix(), hashlib.sha256(path.read_bytes()).hexdigest()))
    return records


def test_full_pipeline_tiers_index_and_audit(archiver, long_source):
    result = archiver.run_source(long_source, actor="WEB")
    assert result["status"] == "completed"
    assert result["cached"] is False
    assert set(result["tiers"]) == {"hot", "warm", "cold", "archive"}
    assert all(result["tiers"][name]["rows"] > 0 for name in result["tiers"])
    assert result["shards"] == sum(t["shards"] for t in result["tiers"].values())
    assert result["output_hashes"] == sorted(result["output_hashes"])
    shards = archiver.store.list_shards()
    assert len(shards) == result["shards"]
    for record in shards:
        path = archiver.workdir / record["path"]
        assert path.exists()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"]
    assert not any(archiver.staging.iterdir())
    entries = archiver.store.search(tag_key="sensor_id", tag_value="sensor-long")
    assert entries
    assert all(entry["member_name"].startswith("data.") for entry in entries)
    assert all(entry["offset"] > 0 for entry in entries)
    assert archiver.audit.verify()["valid"]
    assert archiver.audit.read(actor="WEB")[0]["operation"] == "archive"
    assert archiver.store.get_task(result["task_id"])["result"]["shards"] == result["shards"]


def test_repeat_three_times_is_byte_identical(archiver, long_source):
    first = archiver.run_source(long_source)
    snapshot = output_hashes(archiver.workdir)
    counts = archiver.store.counts()
    for _ in range(2):
        repeated = archiver.run_source(long_source)
        assert repeated["cached"] is True
        assert repeated["output_hashes"] == first["output_hashes"]
        assert output_hashes(archiver.workdir) == snapshot
        assert archiver.store.counts() == counts
    assert len(archiver.store.list_tasks()) == 1
    assert [e["operation"] for e in archiver.audit.read()].count("archive") == 1
    assert [e["operation"] for e in archiver.audit.read()].count("archive_cached") == 2


def test_restore_archive_entry(archiver, long_source):
    archiver.run_source(long_source)
    entry = archiver.store.search(tag_key="sensor_id", tag_value="sensor-long")[0]
    restored = archiver.restore_entry(entry, archiver.workdir / "restored")
    assert restored.exists()
    frame = pd.read_parquet(restored)
    assert len(frame) == entry["row_count"]
    assert archiver.audit.read(operation="restore")[0]["output_hashes"]
    forged = dict(entry)
    forged["package_path"] = "../../outside.tar.gz"
    with pytest.raises(ArchiveError, match="越出"):
        archiver.restore_entry(forged, archiver.workdir / "restored")


def test_rollback_deactivates_but_retains_packages(archiver, long_source):
    result = archiver.run_source(long_source)
    package = [p for p in (archiver.workdir / "archives").iterdir() if p.is_file()]
    assert package
    outcome = archiver.rollback(result["task_id"], actor="WEB")
    assert outcome["restored_shards"] == result["shards"]
    assert outcome["packages_retained"] is True
    assert archiver.store.search() == []
    assert archiver.store.list_shards(active_only=True) == []
    assert all(path.exists() for path in package)
    assert archiver.audit.read(operation="rollback")[0]["actor"] == "WEB"
    rerun = archiver.run_source(long_source)
    assert rerun["cached"] is False
    assert archiver.store.search(tag_key="sensor_id", tag_value="sensor-long")
    with pytest.raises(ArchiveError, match="未找到"):
        archiver.rollback("missing-task")
    second = archiver.rollback()
    assert second["task_id"] == result["task_id"]


def test_missing_output_triggers_safe_rewrite(archiver, long_source):
    first = archiver.run_source(long_source)
    shard = archiver.store.list_shards()[0]
    missing = archiver.workdir / shard["path"]
    missing.unlink()
    second = archiver.run_source(long_source)
    assert second["cached"] is False
    assert missing.exists()
    assert hashlib.sha256(missing.read_bytes()).hexdigest() == shard["sha256"]
    assert second["output_hashes"] == first["output_hashes"]


def test_failed_source_is_audited(tmp_path, archiver, sensor_frame_factory):
    empty = tmp_path / "empty.parquet"
    pd.DataFrame(columns=["timestamp", "sensor_id", "temperature"]).to_parquet(empty, index=False)
    spec = SourceSpec(empty, metric_columns=("temperature",), source_id="empty")
    with pytest.raises(DataSourceError):
        archiver.run_source(spec)
    failures = archiver.audit.read(result="failure")
    assert failures and failures[0]["operation"] == "archive"
    assert archiver.store.list_tasks() == []


def test_cross_workdir_outputs_are_identical(tmp_path, long_source):
    work_a, work_b = tmp_path / "a", tmp_path / "b"
    from time_archiver.config_loader import init_workspace
    policy_a = load_policy(init_workspace(work_a), work_a)
    policy_b = load_policy(init_workspace(work_b), work_b)
    first = Archiver(work_a, policy_a).run_source(long_source)
    second = Archiver(work_b, policy_b).run_source(long_source)
    assert first["output_hashes"] == second["output_hashes"]
    assert output_hashes(work_a) == output_hashes(work_b)
    assert first["input_hash"] == second["input_hash"]


def test_from_workdir_and_source_ids(workdir, long_source):
    archiver = Archiver.from_workdir(workdir)
    assert archiver.policy.name
    assert archiver.task_id_for(long_source).startswith("task-")
    with pytest.raises(DataSourceError):
        archiver.task_id_for(SourceSpec(workdir / "none.parquet"))


def test_single_tier_policy_keeps_all_rows(workdir, policy, source_file):
    from dataclasses import replace
    from time_archiver.schema import PolicyTier
    only = PolicyTier("only", "1h", 30, "archives", format="parquet")
    archiver = Archiver(workdir, replace(policy, tiers=(only,)))
    result = archiver.run_source(source_file(name="single"))
    assert set(result["tiers"]) == {"only"}
    assert result["tiers"]["only"]["rows"] > 0
    assert archiver.store.search(tag_key="sensor_id")


def test_empty_tier_layer_is_reported(workdir, policy, tmp_path, sensor_frame_factory):
    from dataclasses import replace
    from time_archiver.schema import PolicyTier
    stamps = pd.to_datetime(["2024-01-01", "2024-01-26", "2024-02-05"])
    frame = sensor_frame_factory(periods=3)
    frame["timestamp"] = stamps
    path = tmp_path / "sparse.parquet"
    frame.to_parquet(path, index=False)
    tiers = (
        PolicyTier("hot", "1h", 10, "tiers/hot"),
        PolicyTier("warm", "1d", 20, "tiers/warm"),
        PolicyTier("cold", "1d", 30, "tiers/cold"),
        PolicyTier("archive", "1d", 40, "archives"),
    )
    archiver = Archiver(workdir, replace(policy, tiers=tiers))
    spec = SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                      source_id="sparse")
    result = archiver.run_source(spec)
    assert result["tiers"]["warm"] == {"rows": 0, "raw_rows": 0, "shards": 0,
                                       "missing": 0, "anomalies": 0}
    assert result["tiers"]["hot"]["rows"] > 0
    assert result["tiers"]["archive"]["rows"] > 0


def test_empty_downsample_result_is_reported(archiver, source_file, monkeypatch):
    spec = source_file(name="empty-aggregate")
    monkeypatch.setattr("time_archiver.archiver.downsample_frame",
                        lambda *args, **kwargs: pd.DataFrame())
    result = archiver.run_source(spec)
    assert result["tiers"]["hot"]["rows"] == 0
    assert result["tiers"]["hot"]["raw_rows"] > 0
    assert result["shards"] == 0


def test_rollback_rejects_unfinished_task(archiver):
    archiver.store.start_task("pending-task", "archive", "source", "hash", "", "")
    with pytest.raises(ArchiveError, match="已完成"):
        archiver.rollback("pending-task")


def test_bounds_without_timestamps():
    from time_archiver.archiver import _bounds
    assert _bounds(pd.DataFrame({"timestamp": pd.Series([], dtype="datetime64[ns]")})) == ("", "")
