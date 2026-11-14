"""准入与资源预算：预估确定性、预算计划、批量编排、审计校准与故障释放。"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from time_archiver import cli as cli_module
from time_archiver.admission import (
    BudgetPool, build_estimate, peak_rss_bytes, plan_admission,
)
from time_archiver.archiver import Archiver
from time_archiver.config_loader import load_budget, load_policy
from time_archiver.fixture_loader import load_source_manifest
from time_archiver.schema import (
    ArchiveError, Budget, ConfigError, Policy, PolicyTier, QualityReport,
    SourceSpec, canonical_json,
)
from time_archiver.web import create_app


def make_source(tmp_path: Path, factory, name: str, periods: int,
                freq: str = "1h", start: str = "2024-01-01") -> SourceSpec:
    frame = factory(start=start, periods=periods, freq=freq, labels=(name,))
    path = tmp_path / f"{name}.parquet"
    frame.to_parquet(path, index=False)
    return SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                      source_id=name)


def output_hashes(root: Path):
    records = []
    ignored = {"state", "audit", "staging", "policy.yaml", "budget.yaml"}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part in ignored for part in relative.parts):
            continue
        records.append((relative.as_posix(), hashlib.sha256(path.read_bytes()).hexdigest()))
    return records


def admission_records(archiver: Archiver, result: str = ""):
    records = archiver.audit.read(operation="admission")
    if result:
        records = [record for record in records if record["result"] == result]
    return records


# ---------------------------------------------------------------------------
# 预估模型：确定性、边界与安全系数
# ---------------------------------------------------------------------------

def test_estimate_matches_manual_model(sensor_frame_factory):
    frame = sensor_frame_factory(start="2024-01-01", periods=100, freq="1min",
                                 labels=("sensor-1",))
    tier = PolicyTier(name="hot", granularity="10m", retention_days=7,
                      path="tiers/hot", gap_strategy="skip")
    policy = Policy(name="p", tiers=(tier,))
    report = QualityReport(source_id="s", valid_rows=100,
                           interval_seconds=60.0, source_hash="h")
    spec = SourceSpec(Path("x.parquet"), label_columns=("sensor_id",),
                      metric_columns=("temperature",), source_id="s")
    estimate = build_estimate(frame, report, policy, spec, safety_factor=1.0)
    tier_est = estimate["tiers"]["hot"]
    # 99 分钟跨度 / 10 分钟粒度 = 9 个整桶 + 1
    assert tier_est["buckets_per_combo"] == 10
    assert tier_est["label_combinations"] == 1
    assert tier_est["selected_rows"] == 100
    assert tier_est["estimated_rows"] == 10  # min(1×10, 100)
    assert tier_est["estimated_partitions"] == 1
    assert estimate["totals"]["estimated_output_rows"] == 10
    assert estimate["totals"]["estimated_shards"] == 1
    assert estimate["totals"]["estimated_peak_bytes"] > 0
    doubled = build_estimate(frame, report, policy, spec, safety_factor=2.0)
    assert doubled["tiers"]["hot"]["estimated_rows"] == 20


def test_estimate_gap_strategy_changes_row_bound(sensor_frame_factory):
    frame = sensor_frame_factory(start="2024-01-01", periods=10, freq="1h",
                                 labels=("sensor-1",))
    report = QualityReport(source_id="s", valid_rows=10,
                           interval_seconds=3600.0, source_hash="h")
    spec = SourceSpec(Path("x.parquet"), label_columns=("sensor_id",),
                      metric_columns=("temperature",), source_id="s")
    skip_tier = PolicyTier(name="hot", granularity="10m", retention_days=7,
                           path="tiers/hot", gap_strategy="skip")
    null_tier = PolicyTier(name="hot", granularity="10m", retention_days=7,
                           path="tiers/hot", gap_strategy="null")
    skip_est = build_estimate(frame, report, Policy(name="p", tiers=(skip_tier,)),
                              spec, safety_factor=1.0)["tiers"]["hot"]
    null_est = build_estimate(frame, report, Policy(name="p", tiers=(null_tier,)),
                              spec, safety_factor=1.0)["tiers"]["hot"]
    assert skip_est["buckets_per_combo"] == 55  # 9 小时 / 10 分钟 + 1
    assert skip_est["estimated_rows"] == 10     # min(55, 10 行原始)
    assert null_est["estimated_rows"] == 55     # 补桶策略输出全网格


def test_estimate_single_tier_and_empty_window(sensor_frame_factory):
    frame = sensor_frame_factory(start="2024-01-01", periods=72, freq="1h",
                                 labels=("sensor-1",))
    report = QualityReport(source_id="s", valid_rows=72,
                           interval_seconds=3600.0, source_hash="h")
    spec = SourceSpec(Path("x.parquet"), label_columns=("sensor_id",),
                      metric_columns=("temperature",), source_id="s")
    single = PolicyTier(name="only", granularity="1h", retention_days=1,
                        path="tiers/only", gap_strategy="skip")
    estimate = build_estimate(frame, report, Policy(name="p", tiers=(single,)),
                              spec, safety_factor=1.0)
    # 单层策略不做年龄裁剪，72 行全部进入该层
    assert estimate["tiers"]["only"]["selected_rows"] == 72

    hot = PolicyTier(name="hot", granularity="10m", retention_days=1,
                     path="tiers/hot", gap_strategy="skip")
    archive = PolicyTier(name="archive", granularity="1d", retention_days=30,
                         path="archives", gap_strategy="skip")
    short_frame = sensor_frame_factory(start="2024-01-01", periods=12, freq="1h",
                                       labels=("sensor-1",))
    estimate = build_estimate(short_frame, report,
                              Policy(name="p", tiers=(hot, archive)),
                              spec, safety_factor=1.0)
    assert estimate["tiers"]["archive"]["selected_rows"] == 0
    assert estimate["tiers"]["archive"]["estimated_rows"] == 0
    assert estimate["tiers"]["archive"]["estimated_partitions"] == 0
    assert estimate["tiers"]["archive"]["estimated_bytes"] == 0


def test_estimate_without_labels_and_single_row(sensor_frame_factory):
    frame = sensor_frame_factory(start="2024-01-01", periods=1, freq="1h")
    report = QualityReport(source_id="s", valid_rows=1,
                           interval_seconds=0.0, source_hash="h")
    spec = SourceSpec(Path("x.parquet"), label_columns=(),
                      metric_columns=("temperature",), source_id="s")
    tier = PolicyTier(name="hot", granularity="10m", retention_days=7,
                      path="tiers/hot", gap_strategy="skip")
    estimate = build_estimate(frame, report, Policy(name="p", tiers=(tier,)),
                              spec, safety_factor=1.5)
    tier_est = estimate["tiers"]["hot"]
    assert tier_est["label_combinations"] == 1
    assert tier_est["buckets_per_combo"] == 1
    assert tier_est["estimated_rows"] == 2  # ceil(1 × 1.5)


def test_estimate_is_byte_identical_across_runs(workdir, archiver, source_file):
    spec = source_file(name="determinism")
    first = archiver.estimate_source(spec)
    second = archiver.estimate_source(spec)
    assert canonical_json(first) == canonical_json(second)
    # 预估记录不包含任何时钟字段
    assert "timestamp" not in canonical_json(first).lower()


def test_plan_admission_orders_queues_and_rejects():
    admitted, queued, rejected = plan_admission(
        [("c", 30), ("a", 10), ("b", 20)], total_bytes=30)
    assert admitted == ["a", "b"]       # 成本升序，累计 30 恰好容纳
    assert queued == ["c"]              # 单源未超预算但当前余量不足
    assert rejected == []
    admitted, queued, rejected = plan_admission(
        [("a", 10), ("b", 20), ("c", 30)], total_bytes=25)
    assert admitted == ["a"]
    assert queued == ["b"]
    assert rejected == ["c"]            # 超过总预算，直接拒绝
    admitted, _, _ = plan_admission([("b", 10), ("a", 10)], total_bytes=20)
    assert admitted == ["a", "b"]       # 成本相同按键排序，保证确定性


def test_budget_pool_acquire_release_and_guard():
    pool = BudgetPool(100)
    pool.acquire("x", 40)
    assert pool.free_bytes == 60
    pool.acquire("y", 60)
    assert pool.free_bytes == 0
    with pytest.raises(ArchiveError, match="预算不足"):
        pool.acquire("z", 1)
    assert pool.release("x") == 40
    assert pool.release("x") == 0       # 重复释放是安全的 no-op
    assert pool.free_bytes == 40        # y 仍持有 60
    assert pool.release("y") == 60
    assert pool.free_bytes == 100
    snapshot = pool.snapshot()
    assert snapshot["held_bytes"] == 0 and snapshot["held_count"] == 0


# ---------------------------------------------------------------------------
# 预算配置加载
# ---------------------------------------------------------------------------

def test_load_budget_missing_valid_and_invalid(workdir):
    assert load_budget(workdir) is None
    path = workdir / "budget.yaml"
    path.write_text(yaml.safe_dump({
        "version": 1, "max_inflight_bytes": 1024,
        "safety_factor": 2.0, "history_reference": False}), encoding="utf-8")
    budget = load_budget(workdir)
    assert budget == Budget(max_inflight_bytes=1024, safety_factor=2.0,
                            history_reference=False)
    assert Archiver.from_workdir(workdir).budget == budget

    for payload in (
        {"max_inflight_bytes": 0},
        {"max_inflight_bytes": -1},
        {"max_inflight_bytes": "256MB"},
        {"max_inflight_bytes": 1024, "safety_factor": 0.5},
        {"max_inflight_bytes": 1024, "safety_factor": 11},
        {"max_inflight_bytes": 1024, "history_reference": "maybe"},
        {"max_inflight_bytes": 1024, "unknown_key": 1},
        {"version": 2, "max_inflight_bytes": 1024},
    ):
        path.write_text(yaml.safe_dump(payload), encoding="utf-8")
        with pytest.raises(ConfigError):
            load_budget(workdir)
    path.unlink()


# ---------------------------------------------------------------------------
# 验收一：偏紧预算下大源被拒绝、小源正常出分片、审计对上预估
# ---------------------------------------------------------------------------

def test_tight_budget_rejects_big_source_and_audits_estimates(
        tmp_path, workdir, policy, sensor_frame_factory):
    small = make_source(tmp_path, sensor_frame_factory, "small", 600)
    mid = make_source(tmp_path, sensor_frame_factory, "mid", 6000)
    big = make_source(tmp_path, sensor_frame_factory, "big", 12000)
    probe = Archiver(workdir, policy, budget=Budget(max_inflight_bytes=1))
    estimates = {spec.source_id: probe.estimate_source(spec)
                 for spec in (small, mid, big)}
    peak = {sid: est["totals"]["estimated_peak_bytes"]
            for sid, est in estimates.items()}
    assert peak["small"] < peak["mid"] < peak["big"]
    assert peak["big"] > peak["small"] + peak["mid"]

    budget = Budget(max_inflight_bytes=peak["small"] + peak["mid"])
    archiver = Archiver(workdir, policy, budget=budget)
    batch = archiver.run_batch([big, small, mid])  # 乱序提交，验证按成本调度
    decisions = batch["admission"]["decisions"]
    assert decisions == {"small": "completed", "mid": "completed",
                         "big": "rejected"}
    assert batch["admission"]["execution_order"] == ["small", "mid"]

    # 小源正常出分片；被拒绝的大源没有任务记录、没有任何分片
    assert archiver.store.list_shards()
    assert all(shard["source"] != "big" for shard in archiver.store.list_shards())
    assert archiver.store.get_task(archiver.task_id_for(big)) is None
    assert not list(archiver.staging.iterdir())
    assert archiver.store.list_tasks(status="running") == []

    # 每条准入或拒绝记录都能对上预估数字
    admitted = admission_records(archiver, "admitted")
    rejected = admission_records(archiver, "rejected")
    released = admission_records(archiver, "released")
    summary = admission_records(archiver, "summary")
    assert len(admitted) == 2 and len(rejected) == 1 and len(released) == 2
    assert len(summary) == 1
    for record in admitted:
        source = record["details"]["source"]
        assert record["details"]["estimate"] == estimates[source]
        assert (record["details"]["estimate"]["totals"]["estimated_peak_bytes"]
                <= budget.max_inflight_bytes)
    record = rejected[0]
    assert record["details"]["source"] == "big"
    assert record["details"]["estimate"] == estimates["big"]
    assert "超过准入总预算" in record["details"]["reason"]
    assert (record["details"]["estimate"]["totals"]["estimated_peak_bytes"]
            > budget.max_inflight_bytes)

    # 释放记录同时携带预估与实际落盘数字，且预估是实际的上界
    for record in released:
        estimate = record["details"]["estimate"]
        actual = record["details"]["actual"]
        assert record["details"]["outcome"] == "completed"
        assert actual["output_rows"] <= estimate["estimated_output_rows"]
        assert actual["shards"] <= estimate["estimated_shards"]
        shards = archiver.store.shards_for_task(record["task_id"])
        assert actual["shards"] == len(shards)
        assert actual["output_rows"] == sum(s["row_count"] for s in shards)
        assert actual["output_bytes"] > 0
        if peak_rss_bytes() is not None:
            assert actual["peak_rss_bytes"] > 0
        assert record["details"]["budget"]["free_bytes"] == budget.max_inflight_bytes

    assert summary[0]["details"]["rejected"] == 1
    assert summary[0]["details"]["completed"] == 2
    assert archiver.audit.verify()["valid"]


def test_queued_source_promoted_as_budget_releases(
        tmp_path, workdir, policy, sensor_frame_factory):
    frame = sensor_frame_factory(start="2024-01-01", periods=3000, freq="1h",
                                 labels=("sensor-q",))
    specs = []
    for name in ("queue-c", "queue-a", "queue-b"):
        path = tmp_path / f"{name}.parquet"
        frame.to_parquet(path, index=False)
        specs.append(SourceSpec(path, metric_columns=(
            "temperature", "humidity", "pressure"), source_id=name))
    probe = Archiver(workdir, policy, budget=Budget(max_inflight_bytes=1))
    each = probe.estimate_source(specs[0])["totals"]["estimated_peak_bytes"]
    budget = Budget(max_inflight_bytes=2 * each)
    archiver = Archiver(workdir, policy, budget=budget)
    batch = archiver.run_batch(specs)
    assert all(result["status"] == "completed" for result in batch["results"])
    # 预算只够两个并发占用：第三个先排队，释放后再准入
    assert batch["admission"]["execution_order"] == ["queue-c", "queue-a", "queue-b"]
    admitted = admission_records(archiver, "admitted")
    released = admission_records(archiver, "released")
    assert [record["details"]["queued"] for record in admitted] == [False, False, True]
    first_release_seq = released[0]["sequence"]
    promoted = [record for record in admitted if record["details"]["queued"]][0]
    assert promoted["sequence"] > first_release_seq
    assert promoted["details"]["source"] == "queue-b"
    assert admission_records(archiver, "summary")[0]["details"]["queued_then_admitted"] == 1


# ---------------------------------------------------------------------------
# 验收二：放开预算后全部完成，已完成源命中缓存不重算
# ---------------------------------------------------------------------------

def test_relaxed_budget_rerun_completes_and_hits_cache(
        tmp_path, workdir, policy, sensor_frame_factory):
    small = make_source(tmp_path, sensor_frame_factory, "small", 600)
    mid = make_source(tmp_path, sensor_frame_factory, "mid", 6000)
    big = make_source(tmp_path, sensor_frame_factory, "big", 12000)
    probe = Archiver(workdir, policy, budget=Budget(max_inflight_bytes=1))
    peak = {spec.source_id: probe.estimate_source(spec)["totals"]["estimated_peak_bytes"]
            for spec in (small, mid, big)}

    tight = Budget(max_inflight_bytes=peak["small"] + peak["mid"])
    archiver = Archiver(workdir, policy, budget=tight)
    first = archiver.run_batch([small, mid, big])
    assert first["admission"]["decisions"]["big"] == "rejected"
    snapshot = output_hashes(workdir)
    marker = len(archiver.audit.read())

    relaxed = Budget(max_inflight_bytes=sum(peak.values()) + 1)
    archiver.budget = relaxed
    second = archiver.run_batch([small, mid, big])
    decisions = second["admission"]["decisions"]
    assert decisions["big"] == "completed"
    assert decisions["small"] == "completed"
    assert decisions["mid"] == "completed"
    results = {result["source"]: result for result in second["results"]}
    # 已完成源全部命中缓存，不重新进入准入与分层链路
    assert results["small"]["cached"] is True
    assert results["mid"]["cached"] is True
    assert results["big"]["cached"] is False
    second_run = [r for r in archiver.audit.read() if r["sequence"] > marker]
    cached_ops = [r for r in second_run if r["operation"] == "archive_cached"]
    assert {r["details"]["source"] for r in cached_ops} == {"small", "mid"}
    admitted = [r for r in second_run
                if r["operation"] == "admission" and r["result"] == "admitted"]
    assert [r["details"]["source"] for r in admitted] == ["big"]
    # 小源与温源的既有输出逐字节未变
    for relative, digest in snapshot:
        path = workdir / relative
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert archiver.audit.verify()["valid"]


# ---------------------------------------------------------------------------
# 验收三：准入后第一层中途失败，预算释放且重跑收敛
# ---------------------------------------------------------------------------

def test_failure_releases_budget_and_rerun_converges(
        tmp_path, workdir, policy, sensor_frame_factory, monkeypatch):
    spec = make_source(tmp_path, sensor_frame_factory, "fragile", 6000)
    probe = Archiver(workdir, policy, budget=Budget(max_inflight_bytes=1))
    peak = probe.estimate_source(spec)["totals"]["estimated_peak_bytes"]
    budget = Budget(max_inflight_bytes=2 * peak)
    archiver = Archiver(workdir, policy, budget=budget)

    def boom(*args, **kwargs):
        raise ArchiveError("注入故障：第一层序列化失败")

    monkeypatch.setattr("time_archiver.archiver.serialize_frame", boom)
    batch = archiver.run_batch([spec])
    assert batch["results"][0]["status"] == "failed"
    assert "注入故障" in batch["results"][0]["error"]

    task_id = archiver.task_id_for(spec)
    task = archiver.store.get_task(task_id)
    assert task["status"] == "failed"           # 不留悬空 running 任务
    assert archiver.store.list_tasks(status="running") == []
    assert not list(archiver.staging.iterdir())  # 不留半截 staging

    released = admission_records(archiver, "released")
    assert len(released) == 1
    assert released[0]["details"]["outcome"] == "failed"
    # 预算已归还：释放后快照余量等于总预算
    assert (released[0]["details"]["budget"]["free_bytes"]
            == budget.max_inflight_bytes)

    monkeypatch.undo()
    second = archiver.run_batch([spec])
    assert second["results"][0]["status"] == "completed"
    assert archiver.store.get_task(task_id)["status"] == "completed"
    # 重跑准入时预算池只占用当前这一份，证明上次占用没有泄漏
    admitted = [record for record in admission_records(archiver, "admitted")
                if record["sequence"] > released[0]["sequence"]][0]
    assert admitted["details"]["budget"]["held_bytes"] == peak
    assert (admitted["details"]["budget"]["free_bytes"]
            == budget.max_inflight_bytes - peak)
    shards = archiver.store.shards_for_task(task_id)
    assert shards and all((workdir / s["path"]).is_file() for s in shards)
    assert archiver.audit.verify()["valid"]


# ---------------------------------------------------------------------------
# 兼容性与历史参照
# ---------------------------------------------------------------------------

def test_no_budget_batch_matches_legacy_loop(
        tmp_path, workdir, policy, sensor_frame_factory):
    first = make_source(tmp_path, sensor_frame_factory, "legacy-a", 600)
    second = make_source(tmp_path, sensor_frame_factory, "legacy-b", 2000)
    archiver = Archiver(workdir, policy)  # 未配置预算
    batch = archiver.run_batch([first, second])
    assert batch["admission"] is None
    assert all(result["status"] == "completed" for result in batch["results"])
    ops_batch = [r["operation"] for r in archiver.audit.read()]

    legacy_workdir = tmp_path / "legacy"
    from time_archiver.config_loader import init_workspace
    init_workspace(legacy_workdir)
    legacy = Archiver(legacy_workdir, load_policy(legacy_workdir / "policy.yaml",
                                                  legacy_workdir))
    for spec in (first, second):
        legacy.run_source(spec)
    ops_legacy = [r["operation"] for r in legacy.audit.read()]
    assert ops_batch == ops_legacy
    assert output_hashes(workdir) == output_hashes(legacy_workdir)


def test_history_reference_floors_estimate_and_stays_stable(
        tmp_path, workdir, policy, sensor_frame_factory):
    spec = make_source(tmp_path, sensor_frame_factory, "historied", 6000)
    budget = Budget(max_inflight_bytes=10 ** 12, history_reference=True)
    archiver = Archiver(workdir, policy, budget=budget)
    before = archiver.estimate_source(spec)
    # 已启用参照但尚无历史分片：非空层记录 0，空窗口层记录 None
    assert all(t["history_rows"] in (None, 0) for t in before["tiers"].values())
    assert any(t["history_rows"] == 0 for t in before["tiers"].values()
               if t["selected_rows"] > 0)

    archiver.run_source(spec)
    after = archiver.estimate_source(spec)
    for name, tier_est in after["tiers"].items():
        actual = archiver.store.tier_actual_rows(spec.source_id, name)
        # 空窗口层不查历史（None），非空层记录实际行数合计
        assert (tier_est["history_rows"] or 0) == actual
        if tier_est["selected_rows"] > 0:
            assert tier_est["history_rows"] == actual
        # 解析预估本来就是实际上界，历史参照不会让预估值漂移
        assert (tier_est["estimated_rows"]
                == before["tiers"][name]["estimated_rows"])
    assert (after["totals"]["estimated_peak_bytes"]
            == before["totals"]["estimated_peak_bytes"])

    blind = Archiver(workdir, policy,
                     budget=Budget(max_inflight_bytes=10 ** 12,
                                   history_reference=False))
    estimate = blind.estimate_source(spec)
    assert estimate["history_reference"] is False
    assert all(t["history_rows"] is None for t in estimate["tiers"].values())


def test_estimation_read_failure_uses_legacy_failure_semantics(
        tmp_path, workdir, policy):
    missing = SourceSpec(tmp_path / "absent.parquet", source_id="absent")
    archiver = Archiver(workdir, policy, budget=Budget(max_inflight_bytes=10 ** 9))
    batch = archiver.run_batch([missing])
    assert batch["results"][0]["status"] == "failed"
    assert "无法读取输入文件" in batch["results"][0]["error"]
    # 读取失败不进入准入计划，只留批次汇总记录
    assert admission_records(archiver, "admitted") == []
    assert admission_records(archiver, "rejected") == []
    assert admission_records(archiver, "released") == []
    assert admission_records(archiver, "summary")[0]["details"]["failed"] == 1

    empty = tmp_path / "empty.csv"
    empty.write_text("timestamp,sensor_id,value\n", encoding="utf-8")
    bad = SourceSpec(empty, fmt="csv", metric_columns=("value",), source_id="empty")
    batch = archiver.run_batch([bad])
    assert batch["results"][0]["status"] == "failed"
    failures = [r for r in archiver.audit.read(operation="archive")
                    if r["result"] == "failure"]
    assert len(failures) == 1  # 与原链路一致：质量失败写失败审计


def test_source_manifest_loading(tmp_path):
    manifest = tmp_path / "sources.yaml"
    manifest.write_text(yaml.safe_dump({
        "sources": [
            {"path": "/data/a.parquet", "format": "parquet",
             "label_columns": ["sensor_id"], "metric_columns": ["temperature"],
             "source_id": "edge-a"},
            {"path": "/data/b.csv", "format": "csv"},
        ]}), encoding="utf-8")
    specs = load_source_manifest(manifest)
    assert [s.source_id for s in specs] == ["edge-a", "b"]
    assert specs[1].label_columns == ("sensor_id",)
    with pytest.raises(ConfigError):
        load_source_manifest(tmp_path / "missing.yaml")
    manifest.write_text(yaml.safe_dump({"sources": [{"format": "csv"}]}),
                        encoding="utf-8")
    with pytest.raises(ConfigError):
        load_source_manifest(manifest)


# ---------------------------------------------------------------------------
# CLI 与 Web 入口
# ---------------------------------------------------------------------------

def test_cli_run_batch_requires_budget_and_supports_override(
        tmp_path, long_source, monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(cli_module, "demo_sources", lambda: [long_source])
    workdir = tmp_path / "cli"

    result = runner.invoke(cli_module.cli,
                           ["--workdir", str(workdir), "run-batch"])
    assert result.exit_code != 0
    assert "未配置准入预算" in result.output

    result = runner.invoke(cli_module.cli, [
        "--workdir", str(workdir), "run-batch", "--budget-bytes", str(10 ** 10)])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["results"][0]["status"] == "completed"
    assert payload["admission"]["budget"]["max_inflight_bytes"] == 10 ** 10

    other = tmp_path / "cli-tight"
    result = runner.invoke(cli_module.cli, [
        "--workdir", str(other), "run-batch", "--budget-bytes", "1024"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["results"][0]["status"] == "rejected"
    assert "超过准入总预算" in payload["results"][0]["reason"]


def test_cli_run_batch_with_manifest(tmp_path, long_source):
    workdir = tmp_path / "cli-manifest"
    manifest = tmp_path / "sources.yaml"
    manifest.write_text(yaml.safe_dump({"sources": [{
        "path": str(long_source.path), "format": "parquet",
        "label_columns": ["sensor_id"],
        "metric_columns": ["temperature", "humidity", "pressure"],
        "source_id": "manifest-source"}]}), encoding="utf-8")
    result = CliRunner().invoke(cli_module.cli, [
        "--workdir", str(workdir), "run-batch", "--manifest", str(manifest),
        "--budget-bytes", str(10 ** 10)])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["results"][0]["source"] == "manifest-source"
    assert payload["results"][0]["status"] == "completed"


def test_cli_archive_respects_configured_budget(tmp_path, long_source):
    workdir = tmp_path / "cli-archive"
    (workdir / "budget.yaml").parent.mkdir(parents=True, exist_ok=True)
    (workdir / "budget.yaml").write_text(yaml.safe_dump(
        {"max_inflight_bytes": 1024}), encoding="utf-8")
    args = ["--workdir", str(workdir), "archive", "--source",
            str(long_source.path), "--format", "parquet",
            "--metric-columns", "temperature,humidity,pressure",
            "--source-id", "budgeted"]
    result = CliRunner().invoke(cli_module.cli, args)
    assert result.exit_code != 0
    assert "超过准入总预算" in result.output

    (workdir / "budget.yaml").write_text(yaml.safe_dump(
        {"max_inflight_bytes": 10 ** 10}), encoding="utf-8")
    result = CliRunner().invoke(cli_module.cli, args)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["status"] == "completed"


def test_web_run_with_budget_rejects_and_records(tmp_path, long_source):
    workdir = tmp_path / "web"
    workdir.mkdir()
    (workdir / "budget.yaml").write_text(yaml.safe_dump(
        {"max_inflight_bytes": 1024}), encoding="utf-8")
    app = create_app(workdir, testing=True)
    client = app.test_client()
    response = client.post("/tasks/run", data={
        "mode": "custom", "source_path": str(long_source.path),
        "format": "parquet", "timestamp_column": "timestamp",
        "label_columns": "sensor_id",
        "metric_columns": "temperature,humidity,pressure",
        "source_id": "web-budgeted",
    })
    assert response.status_code == 200
    text = response.get_data(as_text=True)
    assert "准入拒绝" in text
    archiver = app.extensions["time_archiver"]["archiver"]
    assert archiver.store.counts()["tasks"] == 0  # 拒绝不产生任务与分片
    rejected = [r for r in archiver.audit.read(operation="admission")
                if r["result"] == "rejected"]
    assert len(rejected) == 1
    assert rejected[0]["actor"] == "WEB"
