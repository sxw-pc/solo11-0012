"""CLI 初始化、演示、归档、调度、检索、恢复、回滚与导出测试。"""
from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from time_archiver import cli as cli_module
from time_archiver.cli import cli
from time_archiver.fixture_loader import fixture_summary
from time_archiver.metadata_store import MetadataStore


def run(cli_runner, workdir: Path, *args):
    return cli_runner.invoke(cli, ["--workdir", str(workdir), *args])


def test_init_load_demo_and_help(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    result = run(runner, workdir, "load-demo")
    assert result.exit_code == 0
    assert "sensor_high_freq.parquet" in result.output
    assert (workdir / "policy.yaml").exists()
    result = run(runner, workdir, "init")
    assert json.loads(result.output)["status"] == "initialized"
    assert cli_module.cli.name == "cli"


def test_validate_config_success_and_failure(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    result = run(runner, workdir, "validate-config")
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["valid"] is True
    bad = tmp_path / "bad.yaml"
    bad.write_text("version: 1\nname: broken\ntiers: []\n", encoding="utf-8")
    failure = run(runner, workdir, "validate-config", "--config", str(bad))
    assert failure.exit_code == 1
    assert "配置校验失败" in failure.output


def test_archive_tasks_search_restore_and_export(tmp_path, long_source):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    archived = run(runner, workdir, "archive", "--source", str(long_source.path),
                   "--source-id", "cli-long", "--metric-columns",
                   "temperature,humidity,pressure")
    assert archived.exit_code == 0, archived.output
    payload = json.loads(archived.output)
    assert payload["status"] == "completed"

    tasks = run(runner, workdir, "tasks", "--limit", "5")
    assert tasks.exit_code == 0
    assert len(json.loads(tasks.output)) == 1

    searched = run(runner, workdir, "search", "--label-key", "sensor_id",
                   "--label-value", "sensor-long")
    entries = json.loads(searched.output)["entries"]
    assert entries
    entry = entries[0]
    restored = run(runner, workdir, "restore", "--shard-id", entry["shard_id"],
                   "--out", str(tmp_path / "restore"))
    assert restored.exit_code == 0
    assert Path(json.loads(restored.output)["restored"]).exists()

    package = workdir / entry["package_path"]
    direct = run(runner, workdir, "restore", "--package", str(package),
                 "--member", entry["member_name"], "--out", str(tmp_path / "direct"))
    assert direct.exit_code == 0
    export_path = tmp_path / "audit.json"
    exported = run(runner, workdir, "audit-export", "--output", str(export_path))
    assert exported.exit_code == 0
    assert json.loads(export_path.read_text(encoding="utf-8"))

    rollback = run(runner, workdir, "rollback")
    assert rollback.exit_code == 0
    assert json.loads(rollback.output)["packages_retained"] is True


def test_archive_errors_and_restore_validation(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    missing = run(runner, workdir, "archive", "--source", str(tmp_path / "none.parquet"))
    assert missing.exit_code != 0
    no_arg = run(runner, workdir, "restore")
    assert no_arg.exit_code == 1
    no_shard = run(runner, workdir, "restore", "--shard-id", "unknown")
    assert no_shard.exit_code == 1


def test_audit_export_markdown(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    run(runner, workdir, "init")
    target = tmp_path / "audit.md"
    result = run(runner, workdir, "audit-export", "--format", "markdown",
                 "--output", str(target))
    assert result.exit_code == 0
    assert target.exists()
    assert "时档匠审计导出" in target.read_text(encoding="utf-8")


def test_demo_schedule_and_web_wrappers(tmp_path, long_source, monkeypatch):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    monkeypatch.setattr(cli_module, "demo_sources", lambda: [long_source])
    demo = run(runner, workdir, "demo")
    assert demo.exit_code == 0, demo.output
    assert json.loads(demo.output)["sources"][0]["source"] == "sensor-long"

    schedule_work = tmp_path / "schedule"
    monkeypatch.setattr(cli_module, "fixture_summary", lambda: {
        "files": [], "description": "", "span_start": "", "span_end": ""})
    scheduled = run(runner, schedule_work, "schedule", "--interval", "1",
                    "--iterations", "1")
    assert scheduled.exit_code == 0, scheduled.output
    state = MetadataStore(schedule_work / "state" / "archive.db")
    assert state.counts()["tasks"] == 1

    recorded = {}

    class DummyApp:
        def run(self, **kwargs):
            recorded.update(kwargs)

    class DummyWeb:
        @staticmethod
        def create_app(workdir):
            recorded["workdir"] = workdir
            return DummyApp()

    monkeypatch.setattr("time_archiver.web.create_app", DummyWeb.create_app)
    web = run(runner, workdir, "web", "--port", "8765")
    assert web.exit_code == 0
    assert recorded["host"] == "127.0.0.1"
    assert recorded["use_reloader"] is False


def test_test_command_wrapper(tmp_path, monkeypatch):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    calls = {}
    monkeypatch.setattr(cli_module.subprocess, "call",
                        lambda command, cwd: calls.update(command=command, cwd=cwd) or 0)
    result = run(runner, workdir, "test", "--no-cov")
    assert result.exit_code == 0
    assert "pytest" in calls["command"]
    assert Path(calls["cwd"]).name == "solo11-0012" or Path(calls["cwd"]).exists()


def test_restore_batch_and_cleanup_cli(tmp_path, long_source, monkeypatch):
    import time_archiver.restore as restore_module
    monkeypatch.setattr(restore_module, "MAX_MEMBERS_PER_UNIT", 1)
    runner = CliRunner()
    workdir = tmp_path / "cli"
    archived = run(runner, workdir, "archive", "--source", str(long_source.path),
                   "--source-id", "batch-long",
                   "--metric-columns", "temperature,humidity,pressure")
    assert archived.exit_code == 0, archived.output
    out_a = tmp_path / "batch-a"
    first = run(runner, workdir, "restore-batch", "--jobs", "1",
                "--label", "sensor_id=sensor-long", "--out", str(out_a))
    assert first.exit_code == 0, first.output
    payload = json.loads(first.output)
    assert payload["status"] == "completed"
    assert payload["total_units"] >= 1
    assert (out_a / "restore_manifest.json").exists()
    out_b = tmp_path / "batch-b"
    second = run(runner, workdir, "restore-batch", "--jobs", "4",
                 "--label", "sensor_id=sensor-long", "--out", str(out_b))
    assert second.exit_code == 0
    names = sorted(p.name for p in out_a.iterdir())
    assert names == sorted(p.name for p in out_b.iterdir())
    for name in names:
        assert (out_a / name).read_bytes() == (out_b / name).read_bytes()
    # 重跑同目录：全部单元跳过
    again = run(runner, workdir, "restore-batch", "--jobs", "4",
                "--label", "sensor_id=sensor-long", "--out", str(out_a))
    assert json.loads(again.output)["skipped_units"] == payload["total_units"]
    cleanup = run(runner, workdir, "restore-cleanup")
    assert cleanup.exit_code == 0
    bad_label = run(runner, workdir, "restore-batch", "--label", "no-equals")
    assert bad_label.exit_code == 1
    no_match = run(runner, workdir, "restore-batch",
                   "--start", "2099-01-01", "--end", "2099-02-01")
    assert no_match.exit_code == 1
    assert "没有命中" in no_match.output


def test_legacy_restore_defaults_unchanged(tmp_path, long_source, monkeypatch):
    runner = CliRunner()
    workdir = tmp_path / "legacy-cli"
    run(runner, workdir, "archive", "--source", str(long_source.path),
        "--source-id", "legacy-long",
        "--metric-columns", "temperature,humidity,pressure")
    entry = json.loads(run(runner, workdir, "search",
                           "--label-value", "sensor-long").output)["entries"][0]
    # 默认 --out restored 是相对当前工作目录的；在临时目录内验证该行为不变
    monkeypatch.chdir(tmp_path)
    restored = run(runner, workdir, "restore", "--shard-id", entry["shard_id"])
    assert restored.exit_code == 0
    target = Path(json.loads(restored.output)["restored"])
    assert target == Path("restored") / target.name
    assert (tmp_path / "restored" / target.name).exists()
