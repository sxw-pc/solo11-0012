"""并发批量恢复：规划、确定性、断点续跑、staging 复用、篡改隔离与清理。"""
from __future__ import annotations

import gzip
import io
import json
import tarfile
from pathlib import Path

import pandas as pd
import pytest

from time_archiver import restore as restore_module
from time_archiver.metadata_store import MetadataStore
from time_archiver.audit import AuditLog
from time_archiver.restore import (
    MANIFEST_NAME, RestorePlanner, build_spec, cleanup_staging,
    output_filenames, parse_labels, plan_members, split_units, task_id_for_spec,
)
from time_archiver.schema import ArchiveError, IndexEntry, ShardRecord, bytes_sha256
from time_archiver.metadata_store import utc_now


@pytest.fixture
def many_archives(workdir, sensor_frame_factory):
    """构造跨三个月、多数据源的归档集，产生多个 tar.gz 成员。"""
    from time_archiver.config_loader import load_policy
    from time_archiver.archiver import Archiver
    from time_archiver.schema import SourceSpec
    from dataclasses import replace

    policy = replace(load_policy(workdir / "policy.yaml", workdir),
                     tiers=(load_policy(workdir / "policy.yaml", workdir).tiers[-1],))
    archiver = Archiver(workdir, policy)
    for index in range(6):
        frame = sensor_frame_factory(
            start="2024-01-01", periods=90 * 24, freq="1h",
            labels=(f"sensor-{index}",))
        path = workdir.parent / f"src{index}.parquet"
        frame.to_parquet(path, index=False)
        spec = SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                          source_id=f"src-{index}")
        archiver.run_source(spec)
    return archiver


@pytest.fixture
def small_units(monkeypatch):
    monkeypatch.setattr(restore_module, "MAX_MEMBERS_PER_UNIT", 4)
    monkeypatch.setattr(restore_module, "MAX_UNIT_BYTES", 4)


@pytest.fixture
def planner(workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    return RestorePlanner(workdir, store, audit)


def test_split_units_never_splits_a_package_group():
    from time_archiver.restore import RestoreMember
    def member(package, name):
        return RestoreMember(
            shard_id=f"s-{package}", package_path=package, member_name=name,
            offset=0, size=10, row_count=1, min_ts="2024-01-01T00:00:00Z",
            max_ts="2024-01-01T00:00:00Z", member_sha256="", package_sha256="",
            tier="archive", source="x")
    # big 包含 50 个成员，远超单元上限 32；它必须整体落在同一个单元
    members = [member("archives/big.tar.gz", f"data-{i:02d}.parquet")
               for i in range(50)]
    members += [member(f"archives/small-{i}.tar.gz", "data.parquet")
                for i in range(10)]
    units = split_units(members)
    big_units = [unit for unit in units
                 if any(m.package_path == "archives/big.tar.gz" for m in unit)]
    assert len(big_units) == 1
    assert len(big_units[0]) == 50  # 整组未被切开
    # 不变量：每个归档包的成员只出现在一个单元里
    package_units: dict = {}
    for index, unit in enumerate(units):
        for m in unit:
            package_units.setdefault(m.package_path, set()).add(index)
    assert all(len(seqs) == 1 for seqs in package_units.values())
    # 确定性
    assert split_units(list(reversed(members))) == units


def test_parse_labels_and_spec_determinism():
    labels = parse_labels(["sensor_id=a ", " group=x"])
    assert labels == [("sensor_id", "a"), ("group", "x")]
    with pytest.raises(ArchiveError):
        parse_labels(["missing-equals"])
    with pytest.raises(ArchiveError):
        parse_labels(["=value"])
    spec_a = build_spec("2024-01-01", "2024-03-31", [("b", "2"), ("a", "1")])
    spec_b = build_spec("2024-01-01", "2024-03-31", [("a", "1"), ("b", "2")])
    assert spec_a == spec_b
    assert task_id_for_spec(spec_a) == task_id_for_spec(spec_b)


def test_split_units_groups_packages_and_is_deterministic(many_archives, workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    members = plan_members(workdir, store, "", "", [])
    assert len(members) >= 18  # 6 源 x 3 个月
    units = split_units(members)
    packages = [{m.package_path for m in unit} for unit in units]
    # 同一包的成员不会跨单元
    assert len(set().union(*packages)) == sum(len(p) for p in packages)
    assert split_units(list(reversed(members))) == units
    names = output_filenames(units)
    assert sum(len(n) for n in names) == len(members)
    assert len({name for group in names for name in group}) == len(members)


def test_plan_members_validates_path_and_missing_package(many_archives, workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    members = plan_members(workdir, store, "", "", [])
    package = workdir / members[0].package_path
    package.unlink()
    with pytest.raises(ArchiveError, match="不存在"):
        plan_members(workdir, store, "", "", [])


def test_jobs_1_and_4_outputs_byte_identical(many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    spec = build_spec("2024-01-01", "2024-12-31", [])
    out1 = workdir / "out1"
    out4 = workdir / "out4"
    first = RestorePlanner(workdir, store, audit).run(spec, out1, jobs=1)
    second = RestorePlanner(workdir, store, audit).run(spec, out4, jobs=4)
    assert first["total_units"] == second["total_units"] > 1
    names1 = sorted(p.name for p in out1.iterdir() if p.is_file())
    names4 = sorted(p.name for p in out4.iterdir() if p.is_file())
    assert names1 == names4
    for name in names1:
        assert (out1 / name).read_bytes() == (out4 / name).read_bytes()
    # 清单按 min_ts、成员名主序，且不含并行度/时间戳造成的抖动
    manifest = json.loads((out1 / MANIFEST_NAME).read_text(encoding="utf-8"))
    keys = [(m["min_ts"], m["member_name"], m["package_path"]) for m in manifest["members"]]
    assert keys == sorted(keys)
    assert audit.verify()["valid"]


def test_rerun_skips_completed_units_without_package_reads(
        many_archives, workdir, small_units, monkeypatch):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    planner = RestorePlanner(workdir, store, audit)
    spec = build_spec("", "", [])
    first = planner.run(spec, workdir / "rerun", jobs=4)
    assert first["status"] == "completed"
    reads = []
    monkeypatch.setattr(restore_module, "read_member_at_offset",
                        lambda *a, **k: reads.append(a) or b"")
    second = planner.run(spec, workdir / "rerun", jobs=4)
    assert second["skipped_units"] == first["total_units"]
    assert second["executed_units"] == 0
    assert reads == []


def test_interrupted_run_resumes_only_pending_units(many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    planner = RestorePlanner(workdir, store, audit)
    spec = build_spec("2024-02-01", "2024-12-31", [])
    task_id, units, rows = planner.prepare(spec, workdir / "partial")
    done = 2
    for seq in range(done):
        planner.publish_unit(task_id, seq, units[seq], rows[seq], workdir / "partial")
    done_packages = {(workdir / m.package_path).resolve()
                     for unit in units[:done] for m in unit}
    touched = []
    original = restore_module.read_member_at_offset

    def spy(package, offset, size):
        touched.append(Path(package).resolve())
        return original(package, offset, size)

    restore_module.read_member_at_offset = spy
    try:
        result = planner.run(spec, workdir / "partial", jobs=3)
    finally:
        restore_module.read_member_at_offset = original
    assert result["skipped_units"] == done
    assert result["failed_units"] == 0
    assert not any(path in done_packages for path in touched)
    assert store.get_restore_task(task_id)["status"] == "completed"


def test_staging_residue_is_reused_after_hash_recheck(
        many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    planner = RestorePlanner(workdir, store, audit)
    spec = build_spec("2024-03-01", "2024-12-31", [])
    task_id, units, rows = planner.prepare(spec, workdir / "reuse")
    planner.run_unit(task_id, 0, units[0], rows[0])  # 落盘前“被杀”
    assert list(Path(rows[0]["staging_dir"]).iterdir())
    reads = []
    original = restore_module.read_member_at_offset
    restore_module.read_member_at_offset = (
        lambda *a, **k: reads.append(a) or original(*a, **k))
    try:
        result = planner.run(spec, workdir / "reuse", jobs=2)
    finally:
        restore_module.read_member_at_offset = original
    expected = sum(len(unit) for unit in units[1:])
    assert len(reads) == expected  # 单元 0 走残留哈希复核，不读包
    assert result["failed_units"] == 0


def test_tampered_member_fails_unit_but_others_continue(
        many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    members = plan_members(workdir, store, "", "", [])
    victim = workdir / members[0].package_path
    with gzip.open(victim, "rb") as handle:
        raw = handle.read()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        payloads = {m.name: archive.extractfile(m).read()
                    for m in archive.getmembers()}
    data_name = next(name for name in payloads if name.startswith("data."))
    broken = bytearray(payloads[data_name])
    broken[10] ^= 0xFF
    payloads[data_name] = bytes(broken)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name in sorted(payloads):
            info = tarfile.TarInfo(name)
            info.size = len(payloads[name]); info.mtime = 0; info.mode = 0o644
            info.uid = info.gid = 0; info.uname = info.gname = ""
            archive.addfile(info, io.BytesIO(payloads[name]))
    victim.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))

    planner = RestorePlanner(workdir, store, audit)
    result = planner.run(build_spec("", "", []), workdir / "tampered", jobs=4)
    assert result["status"] == "failed"
    assert result["failed_units"] == 1
    assert result["executed_units"] == result["total_units"] - 1
    assert "sha256" in result["failures"][0]["error"]
    failed = audit.read(operation="restore_unit_failed")
    assert failed and failed[0]["result"] == "failure"
    assert audit.verify()["valid"]
    rerun = planner.run(build_spec("", "", []), workdir / "tampered", jobs=4)
    assert rerun["failed_units"] == 1
    assert rerun["skipped_units"] == result["total_units"] - 1


def test_cleanup_protects_running_residue_and_force_reclaims(
        many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    planner = RestorePlanner(workdir, store, audit)
    task_id, units, rows = planner.prepare(
        build_spec("2024-01-05", "", []), workdir / "protect")
    planner.run_unit(task_id, 0, units[0], rows[0])
    staging = Path(rows[0]["staging_dir"])
    assert staging.exists()
    assert cleanup_staging(workdir, store)["removed"] == []
    assert staging.exists()
    outcome = cleanup_staging(workdir, store, remove_all=True)
    assert staging.name in {Path(name).name for name in outcome["removed"]} or \
        not staging.exists()
    assert not staging.exists()


def test_legacy_package_without_member_hash_uses_package_hash(workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    payload = b"PAR1" + b"x" * 128
    manifest = json.dumps(
        {"row_count": 1, "tags": {"sensor_id": ["legacy"]}, "tier": "archive",
         "source": "legacy", "min_ts": "2023-01-01T00:00:00Z",
         "max_ts": "2023-01-31T00:00:00Z"}, sort_keys=True).encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as tf:
        for name, data in sorted([("data.parquet", payload), ("index.json", manifest)]):
            info = tarfile.TarInfo(name)
            info.size = len(data); info.mtime = 0; info.mode = 0o644
            info.uid = info.gid = 0; info.uname = info.gname = ""
            tf.addfile(info, io.BytesIO(data))
    package_bytes = gzip.compress(buffer.getvalue(), mtime=0)
    package = workdir / "archives" / "legacy.tar.gz"
    package.parent.mkdir(parents=True, exist_ok=True)
    package.write_bytes(package_bytes)
    raw = gzip.decompress(package_bytes)
    with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
        member = tf.getmember("data.parquet")
        offset, size = member.offset_data, member.size
    shard_id = "legacy-shard"
    store.save_shard(ShardRecord(
        shard_id, "task-legacy", "archive", "legacy", "2023-01",
        "archives/legacy.tar.gz", "tar.gz", 1, 1, 0, 0,
        bytes_sha256(package_bytes), utc_now()))
    store.replace_index([IndexEntry(
        shard_id, "archives/legacy.tar.gz", "data.parquet", offset, size, 1,
        "2023-01-01T00:00:00Z", "2023-01-31T00:00:00Z",
        {"sensor_id": ["legacy"]}, "archive", "legacy", member_sha256="")])
    planner = RestorePlanner(workdir, store, audit)
    spec = build_spec("2023-01-01", "2023-02-01", [("sensor_id", "legacy")])
    assert planner.run(spec, workdir / "legacy-out")["status"] == "completed"
    package.write_bytes(package_bytes[:-4] + b"ZZZZ")
    failed = planner.run(spec, workdir / "legacy-out-2")
    assert failed["failed_units"] == 1


def test_keyboard_interrupt_marks_interrupted_and_resumes(
        many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    planner = RestorePlanner(workdir, store, audit)
    spec = build_spec("2024-01-10", "", [])
    original = planner.run_unit
    calls = {"n": 0}

    def interrupting(task_id, seq, unit, row):
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt()
        return original(task_id, seq, unit, row)

    planner.run_unit = interrupting
    with pytest.raises(KeyboardInterrupt):
        planner.run(spec, workdir / "kbd", jobs=1)
    task_id = task_id_for_spec(spec)
    assert store.get_restore_task(task_id)["status"] == "interrupted"
    assert cleanup_staging(workdir, store)["removed"] == []
    planner.run_unit = original
    result = RestorePlanner(workdir, store, audit).run(spec, workdir / "kbd", jobs=2)
    assert result["status"] == "completed"
    assert result["skipped_units"] >= 1


def test_no_matching_members_raises(many_archives, workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    with pytest.raises(ArchiveError, match="没有命中"):
        RestorePlanner(workdir, store, audit).run(
            build_spec("2099-01-01", "2099-02-01", []), workdir / "empty")


def test_jobs_exceeding_unit_count_succeeds(many_archives, workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    spec = build_spec("2024-01-01", "2024-01-31", [])
    planner = RestorePlanner(workdir, store, audit)
    first = planner.run(spec, workdir / "oversub", jobs=32)
    assert first["status"] == "completed"
    assert first["failed_units"] == 0
    again = planner.run(spec, workdir / "oversub", jobs=1)
    assert again["skipped_units"] == first["total_units"]


def test_different_specs_get_isolated_task_tracks(many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    planner = RestorePlanner(workdir, store, audit)
    spec_a = build_spec("2024-01-01", "2024-01-31", [])
    spec_b = build_spec("2024-02-01", "2024-03-31", [])
    assert task_id_for_spec(spec_a) != task_id_for_spec(spec_b)
    planner.run(spec_a, workdir / "a", jobs=2)
    planner.run(spec_b, workdir / "b", jobs=2)
    task_a = store.get_restore_task(task_id_for_spec(spec_a))
    task_b = store.get_restore_task(task_id_for_spec(spec_b))
    assert task_a["total_members"] != task_b["total_members"]
    # 两套 staging 目录按 task_id 隔离
    roots = {p.name for p in (workdir / "staging" / "restore").iterdir() if p.is_dir()}
    from time_archiver.schema import safe_name
    assert {safe_name(task_a["task_id"]), safe_name(task_b["task_id"])} <= roots


def test_restore_manifest_is_byte_identical_across_repeats(
        many_archives, workdir, small_units):
    store = MetadataStore(workdir / "state" / "archive.db")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    spec = build_spec("2024-01-01", "2024-03-31", [])
    planner = RestorePlanner(workdir, store, audit)
    planner.run(spec, workdir / "a", jobs=1)
    planner.run(spec, workdir / "a", jobs=4)  # 同目录重复恢复
    first = (workdir / "a" / MANIFEST_NAME).read_bytes()
    planner.run(spec, workdir / "b", jobs=2)
    second = (workdir / "b" / MANIFEST_NAME).read_bytes()
    assert first == second
    batches = audit.read(operation="restore_batch")
    assert len(batches) == 3  # 每次恢复只追加，历史链不变
    assert audit.verify()["valid"]
