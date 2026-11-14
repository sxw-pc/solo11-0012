"""标签组合基数/稀疏度预检、局部聚合、覆盖率表与兼容性测试。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from time_archiver.archiver import Archiver
from time_archiver.cli import cli
from time_archiver.config_loader import load_policy
from time_archiver.data_reader import TIMESTAMP, read_source
from time_archiver.downsample import combo_interval, downsample_frame
from time_archiver.index_builder import (
    deserialize_frame, extract_member, read_package_coverage, read_package_index,
)
from time_archiver.metadata_store import MetadataStore
from time_archiver.precheck import (
    combo_coverage, persist_report, precheck_frame, report_bytes, run_precheck,
    select_tier_rows,
)
from time_archiver.schema import (
    ConfigError, PolicyTier, PrecheckConfig, SourceSpec, canonical_hash,
)

PRECHECK_POLICY = """
version: 1
name: precheck_test_policy
reference: data_max
quality:
  value_ranges:
    temperature: {min: -50, max: 150}
precheck:
  enabled: true
  max_combinations: 3
  min_avg_bucket_coverage: 2.0
  auto_apply: true
tiers:
  - name: hot
    granularity: 1h
    retention_days: 10
    path: tiers/hot
    format: parquet
    compression: zstd
    aggregations: {default: mean}
    gap_strategy: skip
  - name: archive
    granularity: 1d
    retention_days: 120
    path: archives
    format: parquet
    compression: gzip
    aggregations: {default: mean}
    gap_strategy: skip
"""

NO_APPLY_POLICY = PRECHECK_POLICY.replace("auto_apply: true", "auto_apply: false")

# 与 PRECHECK_POLICY 层级完全一致、但不含 precheck 段的对照策略。
PLAIN_POLICY = PRECHECK_POLICY.replace("precheck:\n  enabled: true\n"
                                       "  max_combinations: 3\n"
                                       "  min_avg_bucket_coverage: 2.0\n"
                                       "  auto_apply: true\n", "")

OLD_SCHEMA = """
CREATE TABLE tasks (
  task_id TEXT PRIMARY KEY, kind TEXT NOT NULL, source TEXT NOT NULL,
  status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
  policy_hash TEXT, range_start TEXT, range_end TEXT, stage TEXT,
  result_json TEXT, error TEXT
);
CREATE TABLE shards (
  shard_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tier TEXT NOT NULL,
  source TEXT NOT NULL, partition TEXT NOT NULL, path TEXT NOT NULL,
  format TEXT NOT NULL, row_count INTEGER NOT NULL, raw_count INTEGER NOT NULL,
  missing_count INTEGER NOT NULL, anomaly_count INTEGER NOT NULL,
  sha256 TEXT NOT NULL, created_at TEXT NOT NULL, active INTEGER DEFAULT 1
);
CREATE TABLE shard_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT, shard_id TEXT NOT NULL,
  task_id TEXT NOT NULL, prev_task_id TEXT, prev_path TEXT NOT NULL,
  prev_format TEXT, prev_sha256 TEXT, prev_row_count INTEGER,
  changed_at TEXT NOT NULL
);
CREATE TABLE archive_index (
  id INTEGER PRIMARY KEY AUTOINCREMENT, shard_id TEXT NOT NULL,
  package_path TEXT NOT NULL, member_name TEXT NOT NULL, offset INTEGER NOT NULL,
  size INTEGER NOT NULL, row_count INTEGER NOT NULL, min_ts TEXT NOT NULL,
  max_ts TEXT NOT NULL, tags_json TEXT NOT NULL, tier TEXT NOT NULL,
  source TEXT NOT NULL
);
CREATE TABLE label_index (
  shard_id TEXT NOT NULL, tag_key TEXT NOT NULL, tag_value TEXT NOT NULL,
  PRIMARY KEY (shard_id, tag_key, tag_value)
);
"""


def write_policy(workdir: Path, text: str = PRECHECK_POLICY):
    path = workdir / "policy.yaml"
    path.write_text(text, encoding="utf-8")
    policy = load_policy(path, workdir)
    return Archiver(workdir, policy), policy


def dense_frame() -> pd.DataFrame:
    """2 个组合、1 小时节奏、45 天：基数与稀疏度均不超限。"""
    stamps = pd.date_range("2024-01-01", periods=45 * 24, freq="1h")
    frames = []
    for offset, sensor in enumerate(("sensor-a", "sensor-b")):
        frames.append(pd.DataFrame({
            "timestamp": stamps,
            "sensor_id": sensor,
            "metric_group": "env",
            "temperature": 20.0 + offset,
            "humidity": 50.0,
            "pressure": 1000.0,
        }))
    return pd.concat(frames, ignore_index=True)


def sparse_frame() -> pd.DataFrame:
    """12 个组合、每个组合 45 天内仅 3 个点：组合数与稀疏度均超限。"""
    base = pd.Timestamp("2024-01-01T06:00:00")
    rows = []
    for index in range(12):
        for step in range(3):
            rows.append({
                "timestamp": base + pd.Timedelta(days=15 * step + index),
                "sensor_id": f"dev-{index:02d}", "metric_group": "env",
                "temperature": 20.0 + index, "humidity": 50.0,
                "pressure": 1000.0,
            })
    return pd.DataFrame(rows)


def long_sparse_frame() -> pd.DataFrame:
    """12 个组合、每月 1 个点、跨 14 个月：让默认策略四层都有数据。"""
    base = pd.Timestamp("2023-01-05T06:00:00")
    rows = []
    for index in range(12):
        for month in range(14):
            rows.append({
                "timestamp": base + pd.Timedelta(days=30 * month + index % 3),
                "sensor_id": f"dev-{index:02d}", "metric_group": "env",
                "temperature": 20.0 + index, "humidity": 50.0,
                "pressure": 1000.0,
            })
    return pd.DataFrame(rows)


def write_source(tmp_path: Path, frame: pd.DataFrame, name: str,
                 labels=("sensor_id", "metric_group")) -> SourceSpec:
    path = tmp_path / f"{name}.parquet"
    frame.to_parquet(path, index=False)
    return SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                      label_columns=tuple(labels), source_id=name)


def shard_dataframe(archiver: Archiver, shard) -> pd.DataFrame:
    path = archiver.workdir / shard["path"]
    if shard["path"].endswith(".tar.gz"):
        target = extract_member(path, "data.parquet", archiver.workdir / "restored",
                                shard_id=shard["shard_id"])
        return pd.read_parquet(target)
    return pd.read_parquet(path)


def recompute_coverage(spec: SourceSpec, policy, tier_name: str, partition: str):
    """独立回算：从原始数据推算指定层、指定月份每个组合的覆盖桶数。"""
    frame, _ = read_source(spec, policy.quality)
    tiers = {tier.name: tier for tier in policy.tiers}
    tier = tiers[tier_name]
    position = list(tiers).index(tier_name)
    selected = select_tier_rows(frame, policy.tiers, position)
    stamps = pd.to_datetime(selected[TIMESTAMP])
    selected = selected[stamps.dt.strftime("%Y-%m") == partition]
    bucket = (pd.to_datetime(selected[TIMESTAMP])
              .dt.floor(pd.Timedelta(seconds=tier.granularity_seconds)))
    work = selected.assign(__bucket=bucket)
    expected = {}
    for keys, part in work.groupby(list(spec.label_columns), sort=True):
        key_tuple = keys if isinstance(keys, tuple) else (keys,)
        labels = {label: str(value)
                  for label, value in zip(spec.label_columns, key_tuple)}
        valid = part[list(spec.metric_columns)].notna().any(axis=1).astype("int64")
        expected[json.dumps(labels, sort_keys=True)] = {
            "labels": labels,
            "covered_buckets": int(part["__bucket"].nunique()),
            "valid_points": int(valid.sum()),
        }
    return expected


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


def test_precheck_report_is_deterministic_and_task_free(workdir, tmp_path):
    archiver, policy = write_policy(workdir)
    spec = write_source(tmp_path, sparse_frame(), "sparse")
    frame, quality = read_source(spec, policy.quality)
    first, digest_first, path_first = run_precheck(workdir, spec, policy, frame,
                                                   quality.source_hash)
    second, digest_second, path_second = run_precheck(workdir, spec, policy, frame,
                                                      quality.source_hash)
    assert digest_first == digest_second
    assert path_first == path_second
    assert report_bytes(first) == report_bytes(second)
    assert path_first.read_bytes() == path_second.read_bytes()
    assert first["decisions"] == {"hot": "local", "archive": "local"}
    hot = next(t for t in first["tiers"] if t["tier"] == "hot")
    assert hot["exceeded"] is True
    reasons = {reason for month in hot["months"] for reason in month["reasons"]}
    assert "combinations" in reasons
    # 预检不写入任务表，不占用归档任务序号
    assert archiver.store.list_tasks() == []
    assert archiver.store.counts()["tasks"] == 0


def test_precheck_cli_writes_report_and_audit_only(workdir, tmp_path):
    write_policy(workdir)
    spec = write_source(tmp_path, sparse_frame(), "sparse-cli")
    runner = CliRunner()
    result = runner.invoke(cli, ["--workdir", str(workdir), "precheck",
                                 "--source", str(spec.path), "--source-id", "sparse-cli",
                                 "--label-columns", "sensor_id,metric_group",
                                 "--metric-columns", "temperature,humidity,pressure"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["decisions"]["archive"] == "local"
    report_path = Path(payload["report_path"])
    assert report_path.exists()
    assert payload["report_hash"] == hashlib.sha256(report_path.read_bytes()).hexdigest()
    again = runner.invoke(cli, ["--workdir", str(workdir), "precheck",
                                "--source", str(spec.path), "--source-id", "sparse-cli",
                                "--label-columns", "sensor_id,metric_group",
                                "--metric-columns", "temperature,humidity,pressure"])
    assert json.loads(again.output)["report_hash"] == payload["report_hash"]
    store = MetadataStore(workdir / "state" / "archive.db")
    assert store.list_tasks() == []
    from time_archiver.audit import AuditLog
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    entries = audit.read(operation="precheck")
    assert len(entries) == 2
    assert entries[0]["task_id"].startswith("precheck-")
    assert entries[0]["details"]["exceeded"]
    assert audit.verify()["valid"]


def test_archive_applies_local_mode_and_records_coverage(workdir, tmp_path):
    archiver, policy = write_policy(workdir)
    spec = write_source(tmp_path, sparse_frame(), "sparse")
    result = archiver.run_source(spec)
    assert result["status"] == "completed"
    assert result["tiers"]["archive"]["mode"] == "local"
    assert result["tiers"]["hot"]["mode"] == "local"
    shards = archiver.store.list_shards()
    assert shards and all(s["aggregation_mode"] == "local" for s in shards)
    for shard in shards:
        coverage = archiver.store.coverage_for_shard(shard["shard_id"])
        assert coverage, "局部聚合分片必须带组合覆盖率表"
        expected = recompute_coverage(spec, policy, shard["tier"], shard["partition"])
        got = {json.dumps(entry["labels"], sort_keys=True): entry for entry in coverage}
        assert got == expected, "覆盖率表必须与原始数据回算一致"
        frame = shard_dataframe(archiver, shard)
        assert sum(e["valid_points"] for e in coverage) == int(frame["valid_count"].sum())
        assert sum(e["covered_buckets"] for e in coverage) == len(frame)
    archive_shards = [s for s in shards if s["tier"] == "archive"]
    assert archive_shards
    for shard in archive_shards:
        package = archiver.workdir / shard["path"]
        packaged = read_package_coverage(package)
        assert packaged == archiver.store.coverage_for_shard(shard["shard_id"])
        manifest = read_package_index(package)
        assert manifest["aggregation_mode"] == "local"
        assert manifest["coverage_member"] == "coverage.json"
        entries = archiver.store.index_entries_for_shard(shard["shard_id"])
        assert entries[0]["coverage_member"] == "coverage.json"
    audits = archiver.audit.read(operation="precheck")
    assert len(audits) == 1
    assert audits[0]["details"]["decisions"]["archive"] == "local"
    archive_audit = archiver.audit.read(operation="archive")[0]
    assert archive_audit["details"]["aggregation"]["archive"] == "local"
    assert archiver.audit.verify()["valid"]


def test_exceeded_precheck_keeps_quality_field_semantics(workdir, tmp_path):
    frame = sparse_frame()
    frame.loc[0, "temperature"] = 999.0  # 超阈值 -> 异常行
    archiver, policy = write_policy(workdir)
    spec = write_source(tmp_path, frame, "sparse-anomaly")
    result = archiver.run_source(spec)
    assert result["status"] == "completed"
    raw, _ = read_source(spec, policy.quality)
    for shard in archiver.store.list_shards():
        data = shard_dataframe(archiver, shard)
        for column in ("raw_count", "valid_count", "missing_count", "anomaly_count"):
            assert column in data.columns
            assert pd.api.types.is_integer_dtype(data[column])
        tier = {t.name: t for t in policy.tiers}[shard["tier"]]
        position = [t.name for t in policy.tiers].index(shard["tier"])
        selected = select_tier_rows(raw, policy.tiers, position)
        stamps = pd.to_datetime(selected[TIMESTAMP])
        window = selected[stamps.dt.strftime("%Y-%m") == shard["partition"]]
        assert int(data["raw_count"].sum()) == len(window)
        assert int(data["anomaly_count"].sum()) == int(window["__anomaly"].sum())
        assert (data["valid_count"] <= data["raw_count"]).all()
        assert (data["missing_count"] >= 0).all()


def test_local_mode_matches_global_for_single_combo(workdir, tmp_path):
    frame = dense_frame()
    frame = frame[frame["sensor_id"] == "sensor-a"].reset_index(drop=True)
    spec = write_source(tmp_path, frame, "single")
    local_policy_text = PRECHECK_POLICY.replace(
        "min_avg_bucket_coverage: 2.0", "min_avg_bucket_coverage: 999999")
    local_archiver, _ = write_policy(workdir, local_policy_text)
    local_result = local_archiver.run_source(spec)
    assert local_result["tiers"]["archive"]["mode"] == "local"

    plain_workdir = tmp_path / "plain-work"
    from time_archiver.config_loader import init_workspace
    init_workspace(plain_workdir)
    (plain_workdir / "policy.yaml").write_text(PLAIN_POLICY, encoding="utf-8")
    plain_policy = load_policy(plain_workdir / "policy.yaml", plain_workdir)
    plain = Archiver(plain_workdir, plain_policy)
    plain.run_source(spec)

    local_shards = {(s["tier"], s["partition"]): s
                    for s in local_archiver.store.list_shards()}
    plain_shards = {(s["tier"], s["partition"]): s
                    for s in plain.store.list_shards()}
    assert set(local_shards) == set(plain_shards)
    for key, plain_shard in plain_shards.items():
        local_shard = local_shards[key]
        local_data = shard_dataframe(local_archiver, local_shard)
        plain_data = shard_dataframe(plain, plain_shard)
        pd.testing.assert_frame_equal(local_data, plain_data)
        if not local_shard["path"].endswith(".tar.gz"):
            assert local_shard["sha256"] == plain_shard["sha256"]


def test_dense_source_stays_global_and_byte_identical(workdir, tmp_path):
    archiver, _ = write_policy(workdir)
    spec = write_source(tmp_path, dense_frame(), "dense")
    result = archiver.run_source(spec)
    assert all(t["mode"] == "global" for t in result["tiers"].values())
    assert all(not archiver.store.coverage_for_shard(s["shard_id"])
               for s in archiver.store.list_shards())

    plain_workdir = tmp_path / "plain-work"
    from time_archiver.config_loader import init_workspace
    init_workspace(plain_workdir)
    (plain_workdir / "policy.yaml").write_text(PLAIN_POLICY, encoding="utf-8")
    plain_policy = load_policy(plain_workdir / "policy.yaml", plain_workdir)
    plain_result = Archiver(plain_workdir, plain_policy).run_source(spec)
    assert result["output_hashes"] == plain_result["output_hashes"]
    assert output_hashes(workdir) == output_hashes(plain_workdir)


def test_repeat_archive_with_precheck_is_byte_identical(workdir, tmp_path):
    archiver, _ = write_policy(workdir)
    spec = write_source(tmp_path, sparse_frame(), "sparse")
    first = archiver.run_source(spec)
    snapshot = output_hashes(workdir)
    counts = archiver.store.counts()
    report_files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in (workdir / "state" / "precheck").glob("*.json")}
    for _ in range(2):
        repeated = archiver.run_source(spec)
        assert repeated["cached"] is True
        assert repeated["output_hashes"] == first["output_hashes"]
        assert output_hashes(workdir) == snapshot
        assert archiver.store.counts() == counts
    assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (workdir / "state" / "precheck").glob("*.json")} == report_files
    assert len(archiver.store.list_tasks()) == 1
    assert len(archiver.audit.read(operation="precheck")) == 1


def test_auto_apply_disabled_keeps_global_aggregation(workdir, tmp_path):
    archiver, _ = write_policy(workdir, NO_APPLY_POLICY)
    spec = write_source(tmp_path, sparse_frame(), "sparse")
    result = archiver.run_source(spec)
    assert all("mode" not in tier for tier in result["tiers"].values())
    assert all(s["aggregation_mode"] == "global"
               for s in archiver.store.list_shards())
    assert all(not archiver.store.coverage_for_shard(s["shard_id"])
               for s in archiver.store.list_shards())
    assert len(archiver.audit.read(operation="precheck")) == 1
    assert (workdir / "state" / "precheck").exists()


def test_old_workdir_schema_migrates_and_stays_readable(tmp_path):
    db_path = tmp_path / "state" / "archive.db"
    db_path.parent.mkdir(parents=True)
    connection = sqlite3.connect(db_path)
    with connection:
        connection.executescript(OLD_SCHEMA)
        connection.execute(
            "INSERT INTO shards(shard_id,task_id,tier,source,partition,path,format,"
            "row_count,raw_count,missing_count,anomaly_count,sha256,created_at,active) "
            "VALUES('shard-old','task-old','archive','src','2024-01',"
            "'archives/old.tar.gz','tar.gz',5,5,0,0,'abc','2024-02-01T00:00:00Z',1)")
        connection.execute(
            "INSERT INTO archive_index(shard_id,package_path,member_name,offset,size,"
            "row_count,min_ts,max_ts,tags_json,tier,source) "
            "VALUES('shard-old','archives/old.tar.gz','data.parquet',512,100,5,"
            "'2024-01-01T00:00:00Z','2024-01-31T00:00:00Z','{}','archive','src')")
        connection.execute(
            "INSERT INTO label_index(shard_id,tag_key,tag_value) "
            "VALUES('shard-old','sensor_id','legacy')")
    connection.close()
    store = MetadataStore(db_path)
    shard = store.get_shard("shard-old")
    assert shard["aggregation_mode"] == "global"
    entries = store.search(tag_key="sensor_id", tag_value="legacy")
    assert len(entries) == 1
    assert entries[0]["coverage_member"] == ""
    assert store.coverage_for_shard("shard-old") == []
    store.replace_coverage("shard-old", [
        {"labels": {"sensor_id": "legacy"}, "covered_buckets": 3, "valid_points": 3}])
    assert store.coverage_for_shard("shard-old")[0]["covered_buckets"] == 3
    changed = store.rollback_task("task-old")
    assert changed and store.get_shard("shard-old")["active"] == 0
    assert store.coverage_for_shard("shard-old") == []


def test_default_policy_outputs_have_no_coverage_and_restore(workdir, tmp_path):
    spec = write_source(tmp_path, long_sparse_frame(), "legacy")
    policy = load_policy(workdir / "policy.yaml", workdir)
    archiver = Archiver(workdir, policy)
    result = archiver.run_source(spec)
    assert result["status"] == "completed"
    assert all("mode" not in tier for tier in result["tiers"].values())
    assert result["tiers"]["archive"]["rows"] > 0
    entries = archiver.store.search(tag_key="sensor_id", tag_value="dev-00")
    assert entries
    assert all(entry["coverage_member"] == "" for entry in entries)
    for shard in archiver.store.list_shards():
        assert shard["aggregation_mode"] == "global"
        assert archiver.store.coverage_for_shard(shard["shard_id"]) == []
        if shard["path"].endswith(".tar.gz"):
            assert read_package_coverage(archiver.workdir / shard["path"]) is None
    restored = archiver.restore_entry(entries[0], workdir / "restored")
    assert restored.exists()
    assert len(pd.read_parquet(restored)) == entries[0]["row_count"]


def test_rollback_rebuilds_coverage_from_package(workdir, tmp_path):
    archiver, _ = write_policy(workdir)
    frame = sparse_frame()
    spec_v1 = write_source(tmp_path, frame, "sparse")
    first = archiver.run_source(spec_v1)
    archive_shards = [s for s in archiver.store.list_shards() if s["tier"] == "archive"]
    assert archive_shards
    original_coverage = {s["shard_id"]: archiver.store.coverage_for_shard(s["shard_id"])
                         for s in archive_shards}
    assert all(original_coverage.values())

    extra = sparse_frame()
    extra["timestamp"] = extra["timestamp"] + pd.Timedelta(days=90)
    grown = pd.concat([frame, extra], ignore_index=True)
    spec_v2 = write_source(tmp_path, grown, "sparse")
    second = archiver.run_source(spec_v2)
    assert second["task_id"] != first["task_id"]

    outcome = archiver.rollback(second["task_id"])
    assert outcome["restored_shards"] > 0
    for shard in archive_shards:
        assert (archiver.store.coverage_for_shard(shard["shard_id"])
                == original_coverage[shard["shard_id"]])
    rerun = archiver.run_source(spec_v2)
    assert rerun["cached"] is False
    for shard in archiver.store.list_shards():
        if shard["aggregation_mode"] == "local":
            assert archiver.store.coverage_for_shard(shard["shard_id"])


def test_downsample_local_uses_combo_own_interval():
    tier = PolicyTier("t", "1m", 30, "tiers/t", aggregations={"default": "mean"})
    dense = pd.date_range("2024-01-01", periods=6, freq="10s")
    sparse = pd.date_range("2024-01-01", periods=2, freq="1h")
    frame = pd.DataFrame({
        "timestamp": list(dense) + list(sparse),
        "sensor_id": ["a"] * 6 + ["b"] * 2,
        "temperature": [1.0] * 6 + [10.0, 20.0],
    })
    global_out = downsample_frame(frame, tier, ["sensor_id"], ["temperature"],
                                  source_interval=10)
    local_out = downsample_frame(frame, tier, ["sensor_id"], ["temperature"],
                                 source_interval=10, mode="local")
    dense_global = global_out[global_out["sensor_id"] == "a"].iloc[0]
    sparse_global = global_out[global_out["sensor_id"] == "b"]
    sparse_local = local_out[local_out["sensor_id"] == "b"]
    assert dense_global["missing_count"] == 0
    assert sparse_global["missing_count"].tolist() == [5, 5]  # 全局 10s 节奏稀释稀疏组合
    assert sparse_local["missing_count"].tolist() == [0, 0]   # 局部按组合自身 1h 节奏
    assert (sparse_local["temperature"].tolist()
            == sparse_global["temperature"].tolist() == [10.0, 20.0])
    assert sparse_local["raw_count"].tolist() == sparse_global["raw_count"].tolist()
    for column in ("raw_count", "valid_count", "missing_count", "anomaly_count"):
        assert pd.api.types.is_integer_dtype(local_out[column])
    with pytest.raises(ConfigError, match="未知聚合模式"):
        downsample_frame(frame, tier, ["sensor_id"], ["temperature"], mode="shard")


def test_combo_interval_helper():
    assert combo_interval(pd.Series([], dtype="datetime64[ns]")) == 0.0
    assert combo_interval(pd.Series([pd.Timestamp("2024-01-01")])) == 0.0
    regular = pd.Series(pd.date_range("2024-01-01", periods=5, freq="30s"))
    assert combo_interval(regular) == 30.0
    irregular = pd.Series(pd.to_datetime(
        ["2024-01-01T00:00:00", "2024-01-01T00:00:10", "2024-01-01T00:00:20",
         "2024-01-01T00:00:50"]))
    assert combo_interval(irregular) == 10.0


def test_precheck_config_validation_and_policy_hash(workdir, tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "version: 1\nname: x\nprecheck:\n  enabled: yes_please\n  unknown: 1\n"
        "  max_combinations: -3\ntiers: []\n", encoding="utf-8")
    with pytest.raises(ConfigError) as captured:
        load_policy(bad, workdir)
    messages = "；".join(captured.value.errors)
    assert "precheck.enabled" in messages
    assert "未知字段" in messages
    assert "max_combinations" in messages

    not_mapping = tmp_path / "bad2.yaml"
    not_mapping.write_text("version: 1\nname: x\nprecheck: 5\ntiers: []\n",
                           encoding="utf-8")
    with pytest.raises(ConfigError, match="precheck"):
        load_policy(not_mapping, workdir)

    plain = tmp_path / "plain.yaml"
    plain.write_text(PRECHECK_POLICY.replace("precheck:\n  enabled: true\n"
                                             "  max_combinations: 3\n"
                                             "  min_avg_bucket_coverage: 2.0\n"
                                             "  auto_apply: true\n", ""),
                     encoding="utf-8")
    with_precheck = load_policy(workdir / "policy.yaml", workdir) \
        if (workdir / "policy.yaml").exists() else None
    write_policy(workdir)
    configured = load_policy(workdir / "policy.yaml", workdir)
    without = load_policy(plain, workdir)
    assert configured.precheck.enabled is True
    assert without.precheck == PrecheckConfig()
    assert configured.hash != without.hash
    assert "precheck" in configured.to_dict()
    assert "precheck" not in without.to_dict()


def test_precheck_cli_with_default_policy_uses_defaults(workdir, tmp_path):
    spec = write_source(tmp_path, dense_frame(), "dense")
    runner = CliRunner()
    result = runner.invoke(cli, ["--workdir", str(workdir), "precheck",
                                 "--source", str(spec.path), "--source-id", "dense",
                                 "--label-columns", "sensor_id,metric_group",
                                 "--metric-columns", "temperature,humidity,pressure"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["enabled"] is False
    assert payload["thresholds"]["max_combinations"] == 5000
    assert all(mode == "global" for mode in payload["decisions"].values())
    store = MetadataStore(workdir / "state" / "archive.db")
    assert store.list_tasks() == []


def test_combo_coverage_empty_and_no_labels():
    tier = PolicyTier("t", "1h", 30, "tiers/t")
    assert combo_coverage(pd.DataFrame(), tier, ["sensor_id"], ["temperature"]) == []
    frame = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=3, freq="1h"),
        "temperature": [1.0, None, 3.0],
    })
    coverage = combo_coverage(frame, tier, [], ["temperature"])
    assert coverage == [{"labels": {}, "covered_buckets": 3, "valid_points": 2}]
