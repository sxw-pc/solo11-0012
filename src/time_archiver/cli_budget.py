"""预算推演与原子重排的 CLI 入口（额外入口，不改动既有命令与策略结构）。"""
from __future__ import annotations

from pathlib import Path

import click

from .budget import (
    load_plan, plan_budget, plan_file_hash, plan_text, validate_contiguity,
)
from .cli_helpers import emit, runtime
from .config_loader import init_workspace
from .metadata_store import MetadataStore
from .reorg import Reorganizer
from .schema import (
    ArchiveError, ConfigError, DataSourceError, SourceSpec, file_sha256,
)


def _build_spec(source_path: Path, fmt: str, timestamp_column: str,
                label_columns: str, metric_columns: str, table: str,
                source_id: str) -> SourceSpec:
    return SourceSpec(
        source_path, fmt, timestamp_column,
        tuple(filter(None, label_columns.split(","))),
        tuple(filter(None, metric_columns.split(","))),
        table or None, source_id)


def _source_options(function):
    options = [
        click.option("--source", "source_path", required=True,
                     type=click.Path(exists=True, path_type=Path)),
        click.option("--format", "fmt", type=click.Choice(["parquet", "csv", "sqlite"]),
                     default="parquet"),
        click.option("--timestamp-column", default="timestamp"),
        click.option("--table", default=None),
        click.option("--label-columns", default="sensor_id"),
        click.option("--metric-columns", default="value"),
        click.option("--source-id", default=""),
    ]
    for option in reversed(options):
        function = option(function)
    return function


@click.command("plan-budget")
@_source_options
@click.option("--budget-bytes", type=click.IntRange(min=1), required=True,
              help="正式分层输出的总字节预算（不含原始输入与元数据）")
@click.option("--tolerance", type=click.FloatRange(min=0.0, max=1.0), default=0.05,
              show_default=True, help="推演与实际字节偏差的可接受比例")
@click.option("--plan-output", type=click.Path(path_type=Path), default=None,
              help="方案 JSON 输出路径；默认写入工作目录 plans/")
@click.pass_context
def plan_budget_cmd(ctx, source_path, fmt, timestamp_column, label_columns,
                    metric_columns, table, source_id, budget_bytes, tolerance,
                    plan_output):
    """在正式归档前推演各层占用，并在预算内确定性地收缩保留上界。"""
    workdir = ctx.obj["workdir"]
    policy, _archiver, store, audit = runtime(workdir)
    spec = _build_spec(source_path, fmt, timestamp_column, label_columns,
                       metric_columns, table, source_id)
    try:
        plan = plan_budget(spec, policy, budget_bytes,
                           workdir / "staging", tolerance=tolerance)
        validate_contiguity(plan)
        plans_dir = workdir / "plans"
        plans_dir.mkdir(parents=True, exist_ok=True)
        target = plan_output or (plans_dir / f"{plan.plan_id}.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        text = plan_text(plan)
        target.write_text(text, encoding="utf-8")
        if plan_file_hash(plan) != file_sha256(target):
            raise ArchiveError("方案落盘哈希不一致")
        store.save_plan(
            plan.plan_id, spec.source_id, plan.source_hash, plan.policy_hash,
            plan.budget_bytes, plan.merge_oldest, plan.within_budget,
            plan.projected_bytes, plan.baseline_bytes,
            str(target.relative_to(workdir)), plan_file_hash(plan))
        audit.log("plan_budget", "CLI", task_id=plan.plan_id,
                  input_hashes=[plan.source_hash],
                  result="success" if plan.within_budget else "failure",
                  details={"budget_bytes": budget_bytes,
                           "projected_bytes": plan.projected_bytes,
                           "baseline_bytes": plan.baseline_bytes,
                           "merge_oldest": plan.merge_oldest,
                           "plan_path": str(target), "moves": list(plan.moves)})
        emit({"plan": plan.to_dict(), "plan_path": str(target),
              "plan_hash": plan_file_hash(plan)})
    except (DataSourceError, ArchiveError, ConfigError) as exc:
        audit.log("plan_budget", "CLI", result="failure", error=str(exc)[:1000])
        raise click.ClickException(str(exc))


@click.command("reorg-apply")
@_source_options
@click.option("--plan", "plan_path", required=True,
              type=click.Path(exists=True, path_type=Path),
              help="plan-budget 持久化的方案 JSON")
@click.pass_context
def reorg_apply_cmd(ctx, source_path, fmt, timestamp_column, label_columns,
                    metric_columns, table, source_id, plan_path):
    """按已持久化方案原子执行分层重排与陈旧分片清理（可中断重跑）。"""
    workdir = ctx.obj["workdir"]
    policy, _archiver, store, audit = runtime(workdir)
    spec = _build_spec(source_path, fmt, timestamp_column, label_columns,
                       metric_columns, table, source_id)
    try:
        plan = load_plan(plan_path)
        on_disk_hash = plan_file_hash(plan)
        row = store.get_plan_row(plan.plan_id)
        if row and row["plan_hash"] != on_disk_hash:
            raise ArchiveError("方案文件与登记哈希不一致，可能被篡改，拒绝执行")
        reorganizer = Reorganizer(workdir, policy, store, audit)
        result = reorganizer.apply_plan(plan, spec)
        emit(result)
    except (DataSourceError, ArchiveError, ConfigError) as exc:
        audit.log("reorg", "CLI", result="failure", error=str(exc)[:1000],
                  details={"plan": str(plan_path)})
        raise click.ClickException(str(exc))


@click.command("plans")
@click.option("--limit", type=click.IntRange(min=1, max=500), default=20)
@click.pass_context
def plans_cmd(ctx, limit):
    """列出工作目录中已持久化的预算推演方案。"""
    workdir = ctx.obj["workdir"]
    init_workspace(workdir)
    store = MetadataStore(workdir / "state" / "archive.db")
    emit(store.list_plans(limit=limit))
