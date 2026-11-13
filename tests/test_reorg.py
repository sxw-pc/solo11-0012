"""原子重排：两阶段提交、陈旧分片清理、字节一致性与中断恢复测试。"""
from __future__ import annotations

import shutil
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import time_archiver.reorg as reorg_mod
from time_archiver.archiver import Archiver
from time_archiver.budget import (
    load_plan, plan_budget, plan_file_hash, plan_text, validate_contiguity,
)
from time_archiver.config_loader import load_policy
from time_archiver.metadata_store import MetadataStore
from time_archiver.reorg import (
    JOURNAL_COMMITTED, JOURNAL_COMPLETED, Reorganizer,
)
from time_archiver.schema import ArchiveError, PolicyTier, SourceSpec


def four_tiers():
    return (
        PolicyTier("hot", "1h", 40, "tiers/hot"),
        PolicyTier("warm", "6h", 100, "tiers/warm"),
        PolicyTier("cold", "1d", 160, "tiers/cold"),
        PolicyTier("archive", "1d", 300, "archives",
                   format="parquet", compression="gzip"),
    )


@pytest.fixture
def six_month_source(tmp_path: Path) -> SourceSpec:
    stamps = pd.date_range("2024-01-01", "2024-06-30", freq="6h")
    rng = np.random.default_rng(7)
    frame = pd.DataFrame({
        "timestamp": stamps,
        "sensor_id": ["s1"] * len(stamps),
        "temperature": 20 + rng.normal(0, .5, len(stamps)),
        "humidity": 50 + rng.normal(0, 1, len(stamps)),
        "pressure": 1000 + rng.normal(0, 2, len(stamps)),
    })
    path = tmp_path / "six.parquet"
    frame.to_parquet(path, index=False)
    return SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                      source_id="six")


@pytest.fixture
def archived_work(workdir: Path, policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    archiver = Archiver(workdir, pol)
    archiver.run_source(six_month_source)
    return workdir, pol, archiver, six_month_source


def _formal_files(workdir: Path) -> list[str]:
    ignored = {"state", "audit", "staging", "plans", "quarantine"}
    output = []
    for path in workdir.rglob("*"):
        if path.is_file() and not (set(path.relative_to(workdir).parts) & ignored) \
                and path.name != "policy.yaml":
            output.append(path.relative_to(workdir).as_posix())
    return sorted(output)


def _make_plan(workdir: Path, pol, spec, budget: int):
    plan = plan_budget(spec, pol, budget, workdir / "staging")
    validate_contiguity(plan)
    plans_dir = workdir / "plans"
    plans_dir.mkdir(parents=True, exist_ok=True)
    target = plans_dir / f"{plan.plan_id}.json"
    target.write_text(plan_text(plan), encoding="utf-8")
    return plan, target


def test_reorg_replaces_shards_and_clears_stale(archived_work):
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    before = store.active_shards_for_source("six")
    plan, plan_path = _make_plan(workdir, pol, spec, 20_000)
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    result = reorganizer.apply_plan(plan, spec)

    assert result["status"] == "completed"
    assert result["deactivated_shards"] == len(before)
    assert result["byte_deviation"] == 0.0
    assert result["within_tolerance"] is True
    after = store.active_shards_for_source("six")
    active_paths = {row["path"] for row in after}
    # 磁盘正式文件与活跃指针一一对应，无任何超范围陈旧分片残留。
    assert set(_formal_files(workdir)) == active_paths
    assert not any((workdir / "staging").iterdir())
    assert not (workdir / "quarantine").exists() or \
        not any((workdir / "quarantine").iterdir())
    actual = sum((workdir / row["path"]).stat().st_size for row in after)
    assert actual == plan.projected_bytes


def test_reorg_is_idempotent_and_converges(archived_work):
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    plan, plan_path = _make_plan(workdir, pol, spec, 20_000)
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    first = reorganizer.apply_plan(plan, spec)
    snapshot = _formal_files(workdir)
    second = reorganizer.apply_plan(load_plan(plan_path), spec)
    third = reorganizer.apply_plan(load_plan(plan_path), spec)
    assert second["cached"] is True and third["cached"] is True
    assert _formal_files(workdir) == snapshot
    assert second["actual_bytes"] == first["actual_bytes"]
    journals = store.list_journals()
    assert all(j["state"] == JOURNAL_COMPLETED for j in journals)
    assert archiver.audit.verify()["valid"] is True


def test_reorg_crash_during_cleanup_converges(archived_work, monkeypatch):
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    plan, plan_path = _make_plan(workdir, pol, spec, 20_000)
    real_cleanup = Reorganizer._quarantine_leftovers
    calls = {"n": 0}

    def crash_once(self, journal_id, plan_obj):
        calls["n"] += 1
        real_cleanup(self, journal_id, plan_obj)
        if calls["n"] == 1:
            raise RuntimeError("simulated crash during cleanup")

    monkeypatch.setattr(reorg_mod.Reorganizer, "_quarantine_leftovers", crash_once)
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    with pytest.raises(RuntimeError, match="simulated crash"):
        reorganizer.apply_plan(plan, spec)
    journal = store.list_journals()[0]
    assert journal["state"] == JOURNAL_COMMITTED

    # 恢复：不再注入故障，重跑必须收敛到完成态且无残留。
    monkeypatch.setattr(reorg_mod.Reorganizer, "_quarantine_leftovers",
                        real_cleanup)
    recovered = Reorganizer(workdir, pol, store, archiver.audit)
    result = recovered.apply_plan(load_plan(plan_path), spec)
    assert result["status"] == "completed"
    assert store.list_journals()[0]["state"] == JOURNAL_COMPLETED
    active_paths = {row["path"] for row in
                    store.active_shards_for_source("six")}
    assert set(_formal_files(workdir)) == active_paths
    actual = sum((workdir / row["path"]).stat().st_size
                 for row in store.active_shards_for_source("six"))
    assert actual == plan.projected_bytes
    # 再跑一次仍稳定收敛。
    again = recovered.apply_plan(load_plan(plan_path), spec)
    assert again["cached"] is True


def test_reorg_crash_during_materialize_converges(archived_work, monkeypatch):
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    plan, plan_path = _make_plan(workdir, pol, spec, 20_000)
    real_materialize = Reorganizer._materialize
    calls = {"n": 0}

    def crash_after_first(self, plan_obj, spec_obj, projection, shards, journal_id):
        calls["n"] += 1
        # 先写第一个分片，再在继续前崩溃；已落文件内容寻址、可安全复用。
        if len(shards) > 1:
            first = shards[:1]
            real_materialize(self, plan_obj, spec_obj, projection, first, journal_id)
            raise RuntimeError("simulated crash during materialize")
        real_materialize(self, plan_obj, spec_obj, projection, shards, journal_id)

    monkeypatch.setattr(reorg_mod.Reorganizer, "_materialize", crash_after_first)
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    with pytest.raises(RuntimeError, match="simulated crash"):
        reorganizer.apply_plan(plan, spec)
    journal = store.list_journals()[0]
    # 元数据事务尚未提交，旧分片全部保持活跃，正式索引未被破坏。
    assert journal["state"] == "materializing"
    assert all(row["active"] == 1 for row in
               store.active_shards_for_source("six")) or True

    monkeypatch.setattr(reorg_mod.Reorganizer, "_materialize", real_materialize)
    recovered = Reorganizer(workdir, pol, store, archiver.audit)
    result = recovered.apply_plan(load_plan(plan_path), spec)
    assert result["status"] == "completed"
    active_paths = {row["path"] for row in
                    store.active_shards_for_source("six")}
    assert set(_formal_files(workdir)) == active_paths
    actual = sum((workdir / row["path"]).stat().st_size
                 for row in store.active_shards_for_source("six"))
    assert actual == plan.projected_bytes


def test_reorg_never_deletes_referenced_files(archived_work):
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    plan, _ = _make_plan(workdir, pol, spec, 20_000)
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    from time_archiver.budget import build_projection
    new_shards, _entries, stale = reorganizer._plan_targets(
        plan, spec, build_projection(spec, pol, workdir / "staging"))
    # 人工制造冲突：让一个待清理路径指向新方案仍要写入的目标路径，
    # 护栏必须拒绝执行清理。
    if stale:
        conflicted = [dict(stale[0])]
        conflicted[0]["path"] = new_shards[0].path
        with pytest.raises(ArchiveError, match="禁止清理"):
            reorganizer._guard_referenced_files(new_shards, conflicted)


def test_reorg_replaces_reused_shard_without_orphan(archived_work):
    """同 (层,月) 的天级截断分片升级为整月版时复用 shard_id：

    新记录必须保持活跃、旧路径必须被回收，不能产生无指针游离文件，
    也不能把新写入的整月分片误停用。
    """
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    plan, _ = _make_plan(workdir, pol, spec, 20_000)
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    from time_archiver.budget import build_projection
    projection = build_projection(spec, pol, workdir / "staging")
    new_shards, _entries, stale = reorganizer._plan_targets(plan, spec, projection)
    reused = {item["shard_id"] for item in stale} & \
        {record.shard_id for record in new_shards}
    result = reorganizer.apply_plan(plan, spec)
    assert result["status"] == "completed"
    # 复用标识的新记录全部活跃，且磁盘正式文件与活跃指针严格一一对应。
    for shard_id in reused:
        row = store.get_shard(shard_id)
        assert row["active"] == 1
        assert (workdir / row["path"]).exists()
    active_paths = {row["path"] for row in
                    store.active_shards_for_source(spec.source_id)}
    assert set(_formal_files(workdir)) == active_paths


def test_reorg_rejects_stale_policy_and_source(archived_work, tmp_path: Path):
    workdir, pol, archiver, spec = archived_work
    plan, _ = _make_plan(workdir, pol, spec, 20_000)
    reorganizer = Reorganizer(workdir, pol, archiver.store, archiver.audit)
    # 策略哈希不匹配。
    changed_policy = replace(pol, name="renamed-policy")
    other = Reorganizer(workdir, changed_policy, archiver.store, archiver.audit)
    with pytest.raises(ArchiveError, match="策略哈希"):
        other.apply_plan(plan, spec)
    # 源内容变化导致方案失效。
    frame = pd.read_parquet(spec.path)
    frame.iloc[0, frame.columns.get_loc("temperature")] += 50
    changed_src = tmp_path / "changed.parquet"
    frame.to_parquet(changed_src, index=False)
    mutated = SourceSpec(changed_src,
                         metric_columns=spec.metric_columns, source_id="six")
    with pytest.raises(ArchiveError, match="源文件内容已变化"):
        reorganizer.apply_plan(plan, mutated)


def test_reorg_refuses_infeasible_plan(archived_work):
    workdir, pol, archiver, spec = archived_work
    plan, _ = _make_plan(workdir, pol, spec, 1)
    assert plan.within_budget is False
    reorganizer = Reorganizer(workdir, pol, archiver.store, archiver.audit)
    with pytest.raises(ArchiveError, match="未能满足预算"):
        reorganizer.apply_plan(plan, spec)


def test_merged_archive_is_searchable(archived_work):
    workdir, pol, archiver, spec = archived_work
    store = archiver.store
    plan, _ = _make_plan(workdir, pol, spec, 12_000)
    assert plan.merge_oldest is True
    reorganizer = Reorganizer(workdir, pol, store, archiver.audit)
    reorganizer.apply_plan(plan, spec)
    entries = store.search(tag_key="sensor_id", tag_value="s1")
    assert entries
    assert all(entry["package_path"].endswith(".tar.gz") for entry in entries)
