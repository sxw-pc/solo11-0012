"""预算推演与重排 CLI 入口测试。"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from click.testing import CliRunner

from time_archiver.cli import cli
from time_archiver.metadata_store import MetadataStore


def run(cli_runner, workdir: Path, *args):
    return cli_runner.invoke(cli, ["--workdir", str(workdir), *args])


def _source(tmp_path: Path) -> Path:
    stamps = pd.date_range("2024-01-01", "2024-06-30", freq="6h")
    rng = np.random.default_rng(7)
    frame = pd.DataFrame({
        "timestamp": stamps, "sensor_id": ["s1"] * len(stamps),
        "temperature": 20 + rng.normal(0, .5, len(stamps)),
        "humidity": 50 + rng.normal(0, 1, len(stamps)),
        "pressure": 1000 + rng.normal(0, 2, len(stamps)),
    })
    path = tmp_path / "six.parquet"
    frame.to_parquet(path, index=False)
    return path


def test_plan_budget_persists_and_reorg_applies(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    source = _source(tmp_path)
    planned = run(runner, workdir, "plan-budget", "--source", str(source),
                  "--metric-columns", "temperature,humidity,pressure",
                  "--source-id", "six", "--budget-bytes", "20000")
    assert planned.exit_code == 0, planned.output
    payload = json.loads(planned.output)
    plan = payload["plan"]
    plan_path = Path(payload["plan_path"])
    assert plan_path.exists()
    assert payload["plan_hash"]
    # 方案文件本身逐字节稳定：再次推演落盘内容一致。
    again = run(runner, workdir, "plan-budget", "--source", str(source),
                "--metric-columns", "temperature,humidity,pressure",
                "--source-id", "six", "--budget-bytes", "20000")
    assert json.loads(again.output)["plan_hash"] == payload["plan_hash"]

    # plans 列表命令可读。
    listing = run(runner, workdir, "plans")
    assert listing.exit_code == 0
    assert any(item["plan_id"] == plan["plan_id"]
               for item in json.loads(listing.output))

    # 先正常归档，再按方案重排。
    archived = run(runner, workdir, "archive", "--source", str(source),
                   "--metric-columns", "temperature,humidity,pressure",
                   "--source-id", "six")
    assert archived.exit_code == 0, archived.output
    applied = run(runner, workdir, "reorg-apply", "--source", str(source),
                  "--metric-columns", "temperature,humidity,pressure",
                  "--source-id", "six", "--plan", str(plan_path))
    assert applied.exit_code == 0, applied.output
    result = json.loads(applied.output)
    assert result["status"] == "completed"
    assert result["byte_deviation"] == 0.0
    store = MetadataStore(workdir / "state" / "archive.db")
    assert store.list_journals()[0]["state"] == "completed"

    # 重跑命中幂等。
    rerun = run(runner, workdir, "reorg-apply", "--source", str(source),
                "--metric-columns", "temperature,humidity,pressure",
                "--source-id", "six", "--plan", str(plan_path))
    assert json.loads(rerun.output)["cached"] is True


def test_plan_budget_infeasible_is_reported(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    source = _source(tmp_path)
    planned = run(runner, workdir, "plan-budget", "--source", str(source),
                  "--metric-columns", "temperature,humidity,pressure",
                  "--source-id", "six", "--budget-bytes", "1")
    # 推演本身成功，但结果标记为不可行；方案仍持久化供复核。
    assert planned.exit_code == 0
    payload = json.loads(planned.output)
    assert payload["plan"]["within_budget"] is False


def test_reorg_apply_missing_plan_fails(tmp_path):
    runner = CliRunner()
    workdir = tmp_path / "cli"
    source = _source(tmp_path)
    missing = run(runner, workdir, "reorg-apply", "--source", str(source),
                  "--plan", str(tmp_path / "nope.json"))
    assert missing.exit_code != 0
