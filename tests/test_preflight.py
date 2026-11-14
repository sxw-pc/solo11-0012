"""部署自检：依赖解析、配置定位、工作目录、指纹、残留与审计幂等。"""
from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from time_archiver.audit import AuditLog
from time_archiver.cli import cli
from time_archiver.config_loader import init_workspace
from time_archiver.metadata_store import MetadataStore
from time_archiver.preflight import (
    PREFLIGHT_OPERATION, PreflightError, _abi_tag_compatible, _marker_allows,
    _parse_spec_set, _parse_wheel_filename, _platform_tag_compatible,
    _python_tag_compatible, _satisfies,
    build_report, check_configs, check_metadata, check_residue,
    check_shard_fingerprints, check_workdir, cleanup_residue,
    resolve_wheelhouse, restore_cleanup, run_preflight,
)


def make_wheel(directory: Path, name: str, version: str, *,
               python_tag: str = "py3", abi_tag: str = "none",
               platform_tag: str = "any",
               requirements=()) -> Path:
    """生成最小合法 wheel：包含 WHEEL 与 METADATA 即可被自检解析。"""
    directory.mkdir(parents=True, exist_ok=True)
    normalized = name.replace("-", "_")
    filename = f"{normalized}-{version}-{python_tag}-{abi_tag}-{platform_tag}.whl"
    path = directory / filename
    dist_info = f"{normalized}-{version}.dist-info"
    metadata = [f"Metadata-Version: 2.1", f"Name: {name}", f"Version: {version}"]
    metadata += [f"Requires-Dist: {item}" for item in requirements]
    wheel = ("Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\n"
             f"Tag: {python_tag}-{abi_tag}-{platform_tag}\n")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{dist_info}/METADATA", "\n".join(metadata) + "\n")
        archive.writestr(f"{dist_info}/WHEEL", wheel)
    return path


def write_requirements(path: Path, lines) -> Path:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def snapshot(root: Path, ignored=()) -> dict:
    records = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if any(relative.startswith(item) for item in ignored):
            continue
        records[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return records


# ---------------------------------------------------------------------------
# 依赖与 wheel 标签
# ---------------------------------------------------------------------------

def test_wheel_filename_and_tags():
    info = _parse_wheel_filename(
        Path("pandas-2.3.3-cp311-cp311-win_amd64.whl"))
    assert info["name"] == "pandas" and info["version"] == "2.3.3"
    assert info["python"] == "cp311" and info["platform"] == "win_amd64"
    build_tag = _parse_wheel_filename(
        Path("demo-1.2-2-cp39-cp39-manylinux1_x86_64.whl"))
    assert build_tag["name"] == "demo" and build_tag["version"] == "1.2"
    assert _parse_wheel_filename(Path("not-a-wheel.txt")) is None
    assert _parse_wheel_filename(Path("broken.whl")) is None


def test_python_and_abi_tag_rules():
    assert _python_tag_compatible("py3")
    assert _python_tag_compatible(f"cp3{sys_major_minor()}")
    assert not _python_tag_compatible("cp39") or sys_major_minor() == "9"
    assert _abi_tag_compatible("none", "py3")
    assert _abi_tag_compatible("abi3", "cp38")
    assert not _abi_tag_compatible("cp38", "cp38")


def sys_major_minor() -> int:
    import sys
    return sys.version_info.minor


def test_platform_tag_rules():
    assert _platform_tag_compatible("any")
    system = __import__("platform").system().lower()
    if system == "linux":
        assert not _platform_tag_compatible("win_amd64")
        assert _platform_tag_compatible("manylinux_2_17_" + _machine_tag())
    if system == "windows":
        assert _platform_tag_compatible("win_amd64") == (
            _machine_tag() in {"amd64", "x86_64"})
        assert not _platform_tag_compatible("manylinux1_x86_64")


def _machine_tag() -> str:
    import platform
    return platform.machine().lower()


def test_resolver_reaches_fixed_point_with_transitive_constraints(tmp_path):
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "alpha", "2.0", requirements=["shared>=1"])
    make_wheel(wheelhouse, "shared", "1.0")
    make_wheel(wheelhouse, "shared", "2.0")
    requirements = write_requirements(tmp_path / "req.txt",
                                      ["alpha>=2", "shared<2"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert result["failures"] == []
    assert result["resolved"]["shared"]["version"] == "1.0"
    assert result["resolved"]["shared"]["specifier"] == "<2,>=1"


def test_resolver_reports_direct_constraint_conflict(tmp_path):
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "alpha", "1.0")
    make_wheel(wheelhouse, "alpha", "2.5")
    requirements = write_requirements(tmp_path / "req.txt", ["alpha>=2", "alpha<2"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert result["unresolved"] == ["alpha"]
    assert len(result["failures"]) == 1


def test_requirements_comments_and_extras(tmp_path):
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "alpha", "1.0")
    requirements = write_requirements(
        tmp_path / "req.txt",
        ["# 注释行", "alpha[extra]>=1  # 行尾注释", "--index-url https://example.invalid"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert result["failures"] == []
    assert "alpha" in result["resolved"]


def test_cli_cleanup_error_returns_click_error(tmp_path, workdir, monkeypatch):
    def boom(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr("time_archiver.preflight.cleanup_residue", boom)
    result = _cli(workdir, "preflight", "--cleanup")
    assert result.exit_code == 1
    assert "disk full" in result.output
    records = AuditLog(workdir / "audit" / "audit.jsonl").read()
    assert records[-1]["result"] == "failure"


def test_cli_restore_without_manifest_fails(tmp_path, workdir):
    result = _cli(workdir, "preflight", "--restore-cleanup", "latest")
    assert result.exit_code == 1
    assert "没有可恢复" in result.output


def test_specifier_parsing_and_markers():
    assert _parse_spec_set(">=6.0,<7") == [(">=", "6.0"), ("<", "7")]
    assert _satisfies("6.0.3", [(">=", "6.0"), ("<", "7")])
    assert not _satisfies("7.0", [(">=", "6.0"), ("<", "7")])
    assert _satisfies("2.9.0.post0", [(">=", "2.8"), ("<", "3")])
    assert _satisfies("1.4.2", [("~=", "1.4")])
    assert _satisfies("1.5.0", [("~=", "1.4")])       # ~=1.4 等价于 >=1.4,==1.*
    assert not _satisfies("2.0.0", [("~=", "1.4")])
    assert _satisfies("1.4.9", [("~=", "1.4.2")])     # ~=1.4.2 等价于 >=1.4.2,==1.4.*
    assert not _satisfies("1.5.0", [("~=", "1.4.2")])
    assert _satisfies("1.2.3", [("==", "1.2.*")])
    assert _satisfies("5.0", [("!=", "4.0")])
    assert _satisfies("2.0", [("===", "2.0")])
    assert not _satisfies("2.1", [("===", "2.0")])
    assert _marker_allows("python_version >= '3.8'")
    assert _marker_allows("")
    assert not _marker_allows("extra == 'test'")
    assert _marker_allows("sys_platform == 'definitely-not-this' or python_version >= '3'")
    assert _marker_allows("(python_version >= '3') and sys_platform != 'nope'")
    assert _marker_allows("this is not a valid marker")


def test_resolve_wheelhouse_complete_and_missing(tmp_path):
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "alpha", "2.0.0", requirements=["beta>=1.0"])
    make_wheel(wheelhouse, "beta", "1.5.0")
    requirements = write_requirements(tmp_path / "req.txt", ["alpha>=2,<3"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert result["failures"] == []
    assert set(result["resolved"]) == {"alpha", "beta"}
    assert result["resolved"]["beta"]["required"] == "beta>=1.0"

    write_requirements(requirements, ["alpha>=3"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert [item["name"] for item in result["failures"]] == ["alpha"]
    assert "不满足约束" in result["failures"][0]["reason"]
    write_requirements(requirements, ["gamma>=1"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert "缺少依赖 gamma" in result["failures"][0]["reason"]


def test_resolve_wheelhouse_abi_incompatible(tmp_path):
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "native", "1.0", python_tag="cp99",
               abi_tag="cp99", platform_tag="win_amd64")
    requirements = write_requirements(tmp_path / "req.txt", ["native>=1"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert result["failures"]
    assert "没有与当前解释器兼容的轮子" in result["failures"][0]["reason"]
    assert result["incompatible_wheels"][0]["wheel"].startswith("native-1.0-cp99")
    assert result["resolved"] == {}


def test_resolve_wheelhouse_ignores_extra_and_platform_markers(tmp_path):
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "alpha", "1.0", requirements=[
        "only-extra ; extra == 'dev'",
        "never-here ; sys_platform == 'nope'",
    ])
    requirements = write_requirements(tmp_path / "req.txt", ["alpha>=1"])
    result = resolve_wheelhouse(wheelhouse, requirements)
    assert result["failures"] == []
    assert set(result["resolved"]) == {"alpha"}


def test_resolve_wheelhouse_missing_inputs(tmp_path):
    result = resolve_wheelhouse(tmp_path / "absent", tmp_path / "req.txt")
    assert result["failures"] == []
    assert "requirements_error" in result
    requirements = write_requirements(tmp_path / "req.txt", ["alpha"])
    result = resolve_wheelhouse(tmp_path / "absent", requirements)
    assert result["failures"][0]["name"] == "alpha"


# ---------------------------------------------------------------------------
# 配置与工作目录
# ---------------------------------------------------------------------------

def test_check_configs_points_at_broken_file(tmp_path, workdir):
    config_root = tmp_path / "configs"
    example = config_root / "example_policies"
    example.mkdir(parents=True)
    (config_root / "default_policy.yaml").write_text(
        (Path(__file__).resolve().parents[1] / "config" / "default_policy.yaml")
        .read_text(encoding="utf-8"), encoding="utf-8")
    broken = example / "broken.yaml"
    broken.write_text("version: 1\nname: broken\ntiers: []\n", encoding="utf-8")
    check = check_configs(workdir, config_root)
    assert check["status"] == "fail"
    assert "broken.yaml" in check["summary"]
    failures = [item for item in check["details"]["files"] if item["status"] == "fail"]
    assert failures and failures[0]["path"].endswith("broken.yaml")
    assert any("tiers" in error for error in failures[0]["errors"])


def test_check_configs_workdir_policy_included_and_uninitialized_is_warn(tmp_path):
    config_root = Path(__file__).resolve().parents[1] / "config"
    report = check_configs(tmp_path / "uninitialized", config_root)
    assert report["status"] == "warn"
    assert report["details"]["failed"] == 0
    init_workspace(tmp_path / "ready")
    report = check_configs(tmp_path / "ready", config_root)
    assert report["status"] == "pass"


def test_check_workdir_writable_and_missing(tmp_path):
    ready = tmp_path / "ready"
    init_workspace(ready)
    assert check_workdir(ready)["status"] == "pass"
    missing = check_workdir(tmp_path / "new" / "nested")
    assert missing["status"] == "warn"
    assert missing["details"]["exists"] is False


def test_check_workdir_unwritable_ancestor(tmp_path, monkeypatch):
    ready = tmp_path / "ready"
    init_workspace(ready)
    monkeypatch.setattr("time_archiver.preflight._probe_writable",
                        lambda directory: "permission denied")
    result = check_workdir(ready)
    assert result["status"] == "fail"
    assert result["details"]["problems"]


def test_check_metadata_missing_valid_and_broken(tmp_path, workdir):
    assert check_metadata(workdir)["status"] == "warn"
    store = MetadataStore(workdir / "state" / "archive.db")
    store.start_task("task-1", "archive", "source", "hash")
    result = check_metadata(workdir)
    assert result["status"] == "pass"
    assert result["details"]["task_counts"]["running"] == 1
    db = workdir / "state" / "archive.db"
    db.write_bytes(b"not a database")
    assert check_metadata(workdir)["status"] == "fail"


def test_check_metadata_missing_table(workdir):
    import sqlite3
    db = workdir / "state" / "archive.db"
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE unrelated(x)")
    connection.commit()
    connection.close()
    result = check_metadata(workdir)
    assert result["status"] == "fail"
    assert "tasks" in result["details"]["missing_tables"]


# ---------------------------------------------------------------------------
# 分片指纹
# ---------------------------------------------------------------------------

def _seed_shard(workdir: Path, name: str, payload: bytes) -> Path:
    tier = workdir / "tiers" / "hot"
    tier.mkdir(parents=True, exist_ok=True)
    path = tier / name
    path.write_bytes(payload)
    return path


def test_fingerprint_matches_and_detects_tamper(workdir):
    payload = b"deterministic-shard"
    digest = hashlib.sha256(payload).hexdigest()
    name = f"source__hot__2024-01__{digest[:10]}.parquet"
    path = _seed_shard(workdir, name, payload)
    assert check_shard_fingerprints(workdir)["status"] == "pass"
    path.write_bytes(b"tampered")
    result = check_shard_fingerprints(workdir)
    assert result["status"] == "fail"
    assert result["details"]["problems"][0]["path"] == (
        path.relative_to(workdir).as_posix())
    assert result["details"]["problems"][0]["kind"] == "fingerprint_mismatch"
    assert digest[:10] in result["summary"]


def test_fingerprint_uses_metadata_when_name_has_no_fingerprint(workdir):
    payload = b"metadata-only"
    digest = hashlib.sha256(payload).hexdigest()
    tier = workdir / "tiers" / "hot"
    path = tier / "legacy.parquet"
    path.write_bytes(payload)
    store = MetadataStore(workdir / "state" / "archive.db")
    from time_archiver.schema import ShardRecord
    store.save_shard(ShardRecord("s1", "t1", "hot", "src", "2024-01",
                                 "tiers/hot/legacy.parquet", "parquet", 1, 1, 0, 0,
                                 digest))
    assert check_shard_fingerprints(workdir)["status"] == "pass"
    path.write_bytes(b"changed")
    result = check_shard_fingerprints(workdir)
    assert result["status"] == "fail"
    assert result["details"]["problems"][0]["kind"] == "metadata_hash_mismatch"


def test_fingerprint_missing_metadata_file(workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    from time_archiver.schema import ShardRecord
    store.save_shard(ShardRecord("s1", "t1", "hot", "src", "2024-01",
                                 "tiers/hot/gone__hot__2024-01__aaaaaaaaaa.parquet",
                                 "parquet", 1, 1, 0, 0, "a" * 64))
    result = check_shard_fingerprints(workdir)
    assert result["status"] == "fail"
    assert result["details"]["problems"][0]["kind"] == "missing_file"


def test_fingerprint_no_shards_is_pass(workdir):
    result = check_shard_fingerprints(workdir)
    assert result["status"] == "pass"
    assert result["details"]["checked"] == 0


def test_fingerprint_ignores_non_content_names_without_metadata(workdir):
    tier = workdir / "tiers" / "hot"
    tier.mkdir(parents=True, exist_ok=True)
    (tier / "notes.txt").write_text("not a shard", encoding="utf-8")
    assert check_shard_fingerprints(workdir)["status"] == "pass"


def test_fingerprint_unreadable_file_reports_error(workdir, monkeypatch):
    payload = b"payload"
    digest = hashlib.sha256(payload).hexdigest()
    path = _seed_shard(workdir, f"a__hot__2024-01__{digest[:10]}.parquet", payload)

    def boom(*args, **kwargs):
        raise OSError("permission denied")
    monkeypatch.setattr("time_archiver.preflight.file_sha256", boom)
    result = check_shard_fingerprints(workdir)
    assert result["status"] == "fail"
    assert result["details"]["problems"][0]["kind"] == "unreadable"
    assert "permission denied" in result["details"]["problems"][0]["error"]


def test_fingerprint_inactive_shard_still_checked(workdir):
    from time_archiver.schema import ShardRecord
    payload = b"old-shard"
    digest = hashlib.sha256(payload).hexdigest()
    path = _seed_shard(workdir, f"a__hot__2024-01__{digest[:10]}.parquet", payload)
    store = MetadataStore(workdir / "state" / "archive.db")
    store.save_shard(ShardRecord("s1", "t1", "hot", "src", "2024-01",
                                 path.relative_to(workdir).as_posix(), "parquet",
                                 1, 1, 0, 0, digest))
    store.rollback_task("t1")
    assert check_shard_fingerprints(workdir)["status"] == "pass"


def test_archive_shard_tamper_detected(tmp_path, archiver, long_source):
    archiver.run_source(long_source)
    target = sorted((archiver.workdir / "archives").glob("*.tar.gz"))[0]
    payload = bytearray(target.read_bytes())
    payload[-1] ^= 0xFF
    target.write_bytes(bytes(payload))
    result = check_shard_fingerprints(archiver.workdir)
    assert result["status"] == "fail"
    assert result["details"]["problems"][0]["path"] == (
        target.relative_to(archiver.workdir).as_posix())


# ---------------------------------------------------------------------------
# 残留与清理
# ---------------------------------------------------------------------------

def test_residue_is_warning_and_cleanup_is_reversible(workdir):
    staging = workdir / "staging"
    residue = staging / "leftover.tmp"
    residue.write_text("partial", encoding="utf-8")
    atomic = workdir / "tiers" / "hot" / ".stage_1"
    atomic.parent.mkdir(parents=True, exist_ok=True)
    atomic.write_text("partial", encoding="utf-8")
    check = check_residue(workdir)
    assert check["status"] == "warn"
    assert check["details"]["pending_cleanup"] is True
    assert {item["path"] for item in check["details"]["files"]} == {
        "staging/leftover.tmp", "tiers/hot/.stage_1"}

    cleanup = cleanup_residue(workdir, check["details"]["files"])
    assert cleanup["moved"] == 2
    assert not residue.exists() and not atomic.exists()
    manifest = Path(cleanup["manifest"])
    assert manifest.is_file()
    assert json.loads(manifest.read_text(encoding="utf-8"))["entries"]

    restored = restore_cleanup(workdir, cleanup["manifest"])
    assert restored["restored"] == 2
    assert residue.read_text(encoding="utf-8") == "partial"
    assert atomic.exists()

    second = restore_cleanup(workdir, "latest")
    assert second["restored"] == 0
    assert second["skipped"]


def test_residue_reports_failed_tasks(workdir):
    store = MetadataStore(workdir / "state" / "archive.db")
    store.start_task("task-1", "archive", "source", "hash")
    store.update_task("task-1", status="failed", error="boom")
    check = check_residue(workdir)
    assert check["status"] == "warn"
    assert check["details"]["tasks"][0]["task_id"] == "task-1"
    assert "失败" in check["summary"]


def test_restore_rejects_outside_manifest_and_empty(workdir):
    with pytest.raises(PreflightError):
        restore_cleanup(workdir, "latest")
    outside = workdir.parent / "manifest.json"
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(PreflightError):
        restore_cleanup(workdir, str(outside))


def test_restore_rejects_manifest_with_traversal_entries(workdir):
    target = workdir / "staging" / "value.tmp"
    target.write_text("x", encoding="utf-8")
    cleanup = cleanup_residue(
        workdir, [{"path": "staging/value.tmp", "kind": "staging_temp", "size": 1}])
    manifest = Path(cleanup["manifest"])
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["entries"][0]["original"] = "../escape.tmp"
    manifest.write_text(json.dumps(data), encoding="utf-8")
    result = restore_cleanup(workdir, str(manifest))
    assert result["restored"] == 0
    assert "越界" in result["skipped"][0]["reason"]
    assert not (workdir.parent / "escape.tmp").exists()


def test_cleanup_no_files_is_noop(workdir):
    result = cleanup_residue(workdir, [])
    assert result["moved"] == 0 and result["manifest"] == ""


def test_cleanup_restore_mismatch_and_existing_target(workdir):
    staging = workdir / "staging"
    target = staging / "value.tmp"
    target.write_text("original", encoding="utf-8")
    files = [{"path": "staging/value.tmp", "kind": "staging_temp", "size": 8}]
    cleanup = cleanup_residue(workdir, files)
    manifest = Path(cleanup["manifest"])
    data = json.loads(manifest.read_text(encoding="utf-8"))
    data["entries"][0]["sha256"] = "0" * 64
    manifest.write_text(json.dumps(data), encoding="utf-8")
    result = restore_cleanup(workdir, str(manifest))
    assert result["restored"] == 0
    assert "指纹不匹配" in result["skipped"][0]["reason"]

    clean = cleanup_residue(workdir, files)
    target.write_text("blocker", encoding="utf-8")
    result = restore_cleanup(workdir, clean["manifest"])
    assert result["restored"] == 0
    assert "拒绝覆盖" in result["skipped"][0]["reason"]
    assert target.read_text(encoding="utf-8") == "blocker"


# ---------------------------------------------------------------------------
# 报告、审计与幂等
# ---------------------------------------------------------------------------

def _good_wheelhouse_and_requirements(tmp_path: Path) -> tuple:
    wheelhouse = tmp_path / "wh"
    make_wheel(wheelhouse, "alpha", "2.0.0")
    requirements = write_requirements(tmp_path / "req.txt", ["alpha>=2"])
    return wheelhouse, requirements


def test_build_report_strict_and_lenient(tmp_path, workdir):
    wheelhouse = tmp_path / "wh"
    requirements = write_requirements(tmp_path / "req.txt", ["missing>=1"])
    strict = build_report(workdir, strict=True, wheelhouse=wheelhouse,
                          requirements=requirements)
    assert strict["status"] == "fail"
    assert strict["allowed"] is False
    lenient = build_report(workdir, strict=False, wheelhouse=wheelhouse,
                           requirements=requirements)
    assert lenient["allowed"] is True
    assert lenient["status"] == "fail"
    names = [check["name"] for check in lenient["checks"]]
    assert names == ["dependencies", "config", "workdir", "metadata",
                     "shard_fingerprints", "residue"]
    assert lenient["report_id"].startswith("preflight-")


def test_run_preflight_writes_single_preflight_audit_entry(tmp_path, workdir):
    wheelhouse, requirements = _good_wheelhouse_and_requirements(tmp_path)
    report = run_preflight(workdir, wheelhouse=wheelhouse,
                           requirements=requirements)
    assert report["audit"]["sequence"] == 1
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    records = audit.read()
    assert len(records) == 1
    assert records[0]["operation"] == PREFLIGHT_OPERATION
    assert records[0]["task_id"] == ""
    assert records[0]["result"] == "success"
    assert records[0]["details"]["checks"]
    assert audit.verify()["valid"]
    assert MetadataStore(workdir / "state" / "archive.db").counts()["tasks"] == 0


def test_preflight_twice_is_byte_idempotent_outside_audit(tmp_path, workdir):
    wheelhouse, requirements = _good_wheelhouse_and_requirements(tmp_path)
    before = snapshot(workdir, ignored=("audit/",))
    first = run_preflight(workdir, wheelhouse=wheelhouse,
                          requirements=requirements)
    middle = snapshot(workdir, ignored=("audit/",))
    second = run_preflight(workdir, wheelhouse=wheelhouse,
                           requirements=requirements)
    after = snapshot(workdir, ignored=("audit/",))
    assert before == middle == after
    assert first["report_id"] == second["report_id"]
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    records = audit.read(operation=PREFLIGHT_OPERATION)
    assert [item["sequence"] for item in records] == [1, 2]
    assert audit.verify()["valid"]


def test_preflight_does_not_create_workdir_or_policy(tmp_path):
    workdir = tmp_path / "never-created"
    report = run_preflight(workdir)
    assert report["checks"]
    # 审计文件是唯一被创建的内容；策略、元数据库和分片目录都不应出现。
    assert (workdir / "audit" / "audit.jsonl").is_file()
    assert not (workdir / "policy.yaml").exists()
    assert not (workdir / "state").exists()
    assert not (workdir / "tiers").exists()
    assert not (workdir / "archives").exists()
    assert not (workdir / "staging").exists()


def test_preflight_failure_audit_result_and_error(tmp_path, workdir):
    wheelhouse = tmp_path / "wh"
    requirements = write_requirements(tmp_path / "req.txt", ["absent>=1"])
    report = run_preflight(workdir, wheelhouse=wheelhouse, requirements=requirements)
    assert report["status"] == "fail"
    records = AuditLog(workdir / "audit" / "audit.jsonl").read()
    assert records[0]["result"] == "failure"
    assert "absent" in records[0]["error"]
    assert records[0]["details"]["failures"]


def test_run_preflight_cleanup_and_restore_actions(tmp_path, workdir):
    wheelhouse, requirements = _good_wheelhouse_and_requirements(tmp_path)
    residue = workdir / "staging" / "leftover.tmp"
    residue.write_text("partial", encoding="utf-8")
    report = run_preflight(workdir, wheelhouse=wheelhouse,
                           requirements=requirements, cleanup=True)
    assert report["cleanup"]["moved"] == 1
    assert not residue.exists()
    assert report["warnings"]  # residue 已清理但仍记录动作
    manifest = report["cleanup"]["manifest"]
    restored = run_preflight(workdir, wheelhouse=wheelhouse,
                             requirements=requirements, restore=manifest)
    assert restored["cleanup_restore"]["restored"] == 1
    assert residue.exists()
    records = AuditLog(workdir / "audit" / "audit.jsonl").read()
    actions = [action["action"] for record in records
               for action in record["details"]["actions"]]
    assert actions == ["cleanup_residue", "restore_cleanup"]


def test_run_preflight_cleanup_error_is_audited(tmp_path, workdir, monkeypatch):
    def boom(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr("time_archiver.preflight.cleanup_residue", boom)
    with pytest.raises(PreflightError):
        run_preflight(workdir, cleanup=True)
    records = AuditLog(workdir / "audit" / "audit.jsonl").read()
    assert records[-1]["result"] == "failure"
    assert "disk full" in records[-1]["error"]


# ---------------------------------------------------------------------------
# CLI 验收场景
# ---------------------------------------------------------------------------

def _cli(workdir: Path, *args):
    return CliRunner().invoke(cli, ["--workdir", str(workdir), *args])


def test_cli_strict_missing_wheel_exits_nonzero_and_audits(tmp_path):
    workdir = tmp_path / "cli"
    runner_args = ["--wheelhouse", str(tmp_path / "empty-wh"),
                   "--requirements", str(write_requirements(tmp_path / "req.txt",
                                                            ["absent>=1"]))]
    strict = _cli(workdir, "preflight", "--strict", *runner_args)
    assert strict.exit_code == 1
    payload = json.loads(strict.output)
    assert payload["allowed"] is False
    records = AuditLog(workdir / "audit" / "audit.jsonl").read()
    assert records[-1]["operation"] == "preflight"
    assert records[-1]["result"] == "failure"
    assert "absent" in records[-1]["error"]

    lenient = _cli(workdir, "preflight", *runner_args)
    assert lenient.exit_code == 0
    assert json.loads(lenient.output)["allowed"] is True


def test_cli_strict_blocks_archive_and_lenient_continues(tmp_path, long_source):
    workdir = tmp_path / "cli"
    bad_args = ["--wheelhouse", str(tmp_path / "empty"),
                "--requirements", str(write_requirements(tmp_path / "req.txt",
                                                         ["absent>=1"]))]
    blocked = _cli(workdir, "archive", "--source", str(long_source.path),
                   "--metric-columns", "temperature,humidity,pressure",
                   "--preflight", "--strict", *bad_args)
    assert blocked.exit_code == 1
    assert json.loads(blocked.output)["status"] == "blocked"
    assert MetadataStore(workdir / "state" / "archive.db").counts()["tasks"] == 0

    continued = _cli(workdir, "archive", "--source", str(long_source.path),
                     "--metric-columns", "temperature,humidity,pressure",
                     "--preflight", *bad_args)
    assert continued.exit_code == 0, continued.output
    assert json.loads(continued.output)["status"] == "completed"


def test_cli_config_failure_names_policy_file(tmp_path):
    workdir = tmp_path / "cli"
    config_root = tmp_path / "configs"
    example = config_root / "example_policies"
    example.mkdir(parents=True)
    broken = example / "server_metric.yaml"
    broken.write_text("version: 1\nname: bad\ntiers: []\n", encoding="utf-8")
    result = _cli(workdir, "preflight", "--config-root", str(config_root))
    assert result.exit_code == 0
    payload = json.loads(result.output)
    config = next(item for item in payload["checks"] if item["name"] == "config")
    assert config["status"] == "fail"
    failing = [item for item in config["details"]["files"] if item["status"] == "fail"]
    assert failing[0]["path"].endswith("server_metric.yaml")
    assert any("tiers" in error for error in failing[0]["errors"])


def test_cli_tampered_shard_reports_specific_file(tmp_path, archiver, long_source):
    """CLI 体检能定位到被篡改的具体分片文件。"""
    archiver.run_source(long_source)
    workdir = archiver.workdir
    target = sorted((workdir / "tiers" / "hot").glob("*"))[0]
    payload = bytearray(target.read_bytes())
    payload[100] ^= 0xFF
    target.write_bytes(bytes(payload))
    result = _cli(workdir, "preflight", "--wheelhouse", str(tmp_path / "empty"),
                  "--requirements", str(write_requirements(tmp_path / "req.txt",
                                                           ["absent>=1"])))
    assert result.exit_code == 0
    payload = json.loads(result.output)
    shards = next(item for item in payload["checks"]
                  if item["name"] == "shard_fingerprints")
    assert shards["status"] == "fail"
    assert target.relative_to(workdir).as_posix() in shards["summary"]
    assert shards["details"]["problems"][0]["path"] == target.relative_to(workdir).as_posix()


def test_cli_preflight_twice_creates_only_audit(tmp_path, workdir):
    wheelhouse, requirements = _good_wheelhouse_and_requirements(tmp_path)
    args = ["preflight", "--wheelhouse", str(wheelhouse),
            "--requirements", str(requirements)]
    before = snapshot(workdir, ignored=("audit/",))
    assert _cli(workdir, *args).exit_code == 0
    first = snapshot(workdir, ignored=("audit/",))
    assert _cli(workdir, *args).exit_code == 0
    second = snapshot(workdir, ignored=("audit/",))
    assert before == first == second
    records = AuditLog(workdir / "audit" / "audit.jsonl").read(
        operation=PREFLIGHT_OPERATION)
    assert len(records) == 2
    assert records[0]["details"]["report_id"] == records[1]["details"]["report_id"]
    assert AuditLog(workdir / "audit" / "audit.jsonl").verify()["valid"]
