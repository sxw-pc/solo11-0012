"""数据操作类子命令：归档、恢复和审计导出。"""
from __future__ import annotations

from pathlib import Path

import click

from .audit import AuditLog
from .cli_helpers import emit, runtime
from .config_loader import init_workspace
from .index_builder import extract_member
from .preflight import (
    DEFAULT_REQUIREMENTS, DEFAULT_WHEELHOUSE, PreflightError, run_preflight,
)
from .schema import ArchiveError, ConfigError, DataSourceError, SourceSpec, file_sha256


def preflight_options(function):
    """为命令挂载部署自检参数（严格模式与可回退清理）。"""
    options = [
        click.option("--strict/--lenient", "strict", default=False,
                     help="严格模式：任一检查不通过则拒绝后续归档并以非零退出"),
        click.option("--wheelhouse", "wheelhouse_path",
                     type=click.Path(path_type=Path), default=DEFAULT_WHEELHOUSE,
                     show_default=True, help="离线轮子目录"),
        click.option("--requirements", "requirements_path",
                     type=click.Path(path_type=Path), default=DEFAULT_REQUIREMENTS,
                     show_default=True, help="依赖清单"),
        click.option("--config-root", "config_root_path",
                     type=click.Path(path_type=Path), default=None,
                     help="默认策略与示例策略目录"),
        click.option("--cleanup/--no-cleanup", default=False,
                     help="把 staging 残留移入工作目录内的隔离区（可回退）"),
        click.option("--restore-cleanup", "restore_cleanup_path", default=None,
                     help="按隔离清单恢复上次清理，latest 表示最近一份"),
    ]
    for option in reversed(options):
        function = option(function)
    return function


def _run_preflight(workdir, strict, wheelhouse_path, requirements_path,
                   config_root_path, cleanup, restore_cleanup_path):
    try:
        return run_preflight(
            workdir, strict=strict, wheelhouse=wheelhouse_path,
            requirements=requirements_path, config_root=config_root_path,
            cleanup=cleanup, restore=restore_cleanup_path)
    except PreflightError as exc:
        raise click.ClickException(str(exc))


@click.command("preflight")
@preflight_options
@click.pass_context
def preflight_cmd(ctx, strict, wheelhouse_path, requirements_path,
                  config_root_path, cleanup, restore_cleanup_path):
    """只读部署自检：离线依赖、策略、工作目录、分片指纹与残留。"""
    report = _run_preflight(ctx.obj["workdir"], strict, wheelhouse_path,
                            requirements_path, config_root_path, cleanup,
                            restore_cleanup_path)
    emit(report)
    if strict and not report["allowed"]:
        raise SystemExit(1)


def source_options(function):
    """为归档命令挂载输入格式、列映射和源标识参数。"""
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


@click.command("archive")
@source_options
@click.option("--preflight/--no-preflight", "preflight_", default=False,
              help="归档前执行部署自检")
@click.option("--strict/--lenient", "strict", default=False,
              help="配合 --preflight：自检不通过时拒绝归档")
@click.option("--wheelhouse", "wheelhouse_path",
              type=click.Path(path_type=Path), default=DEFAULT_WHEELHOUSE,
              help="配合 --preflight：离线轮子目录")
@click.option("--requirements", "requirements_path",
              type=click.Path(path_type=Path), default=DEFAULT_REQUIREMENTS,
              help="配合 --preflight：依赖清单")
@click.pass_context
def archive_cmd(ctx, source_path, fmt, timestamp_column, label_columns,
                metric_columns, table, source_id, preflight_, strict,
                wheelhouse_path, requirements_path):
    """对单个本地数据源执行一次分层归档。"""
    workdir = ctx.obj["workdir"]
    if preflight_:
        report = _run_preflight(workdir, strict, wheelhouse_path,
                                requirements_path, None, False, None)
        if strict and not report["allowed"]:
            emit({"status": "blocked", "preflight": report})
            raise SystemExit(1)
    _, archiver, _, _ = runtime(workdir)
    spec = SourceSpec(source_path, fmt, timestamp_column,
                      tuple(filter(None, label_columns.split(","))),
                      tuple(filter(None, metric_columns.split(","))), table, source_id)
    try:
        emit(archiver.run_source(spec))
    except (DataSourceError, ArchiveError, ConfigError) as exc:
        raise click.ClickException(str(exc))


@click.command("restore")
@click.option("--shard-id", default="")
@click.option("--package", type=click.Path(path_type=Path), default=None)
@click.option("--member", default="")
@click.option("--out", "output_dir", type=click.Path(path_type=Path),
              default=Path("restored"))
@click.pass_context
def restore_cmd(ctx, shard_id, package, member, output_dir):
    """按索引记录恢复归档数据，或从指定包定点提取成员。"""
    workdir = ctx.obj["workdir"]
    _, archiver, store, audit = runtime(workdir)
    if shard_id:
        entries = store.index_entries_for_shard(shard_id)
        if entries:
            emit({"restored": str(archiver.restore_entry(entries[0], output_dir))})
            return
        shard = store.get_shard(shard_id)
        if not shard:
            raise click.ClickException("未找到分片")
        target = Path(output_dir) / Path(shard["path"]).name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((workdir / shard["path"]).read_bytes())
        audit.log("restore", "CLI", task_id=shard_id, result="success",
                  output_hashes=[file_sha256(target)])
        emit({"restored": str(target)})
        return
    if not package or not member:
        raise click.ClickException("必须提供 --shard-id，或同时提供 --package 与 --member")
    target = extract_member(Path(package), member, output_dir, shard_id=member)
    audit.log("restore", "CLI", result="success", output_hashes=[file_sha256(target)],
              details={"package": str(package), "member": member})
    emit({"restored": str(target)})


@click.command("audit-export")
@click.option("--format", "fmt", type=click.Choice(["json", "markdown"]), default="json")
@click.option("--output", type=click.Path(path_type=Path), default=None)
@click.option("--task-id", default="")
@click.pass_context
def audit_export_cmd(ctx, fmt, output, task_id):
    """按任务筛选并导出审计日志。"""
    workdir = ctx.obj["workdir"]
    init_workspace(workdir)
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    suffix = "md" if fmt == "markdown" else "json"
    target = output or workdir / "audit" / f"audit_export.{suffix}"
    filters = {"task_id": task_id} if task_id else {}
    if fmt == "markdown":
        text = audit.export_markdown(target, **filters)
    else:
        text = audit.export_json(target, **filters)
    audit.log("audit_export", "CLI", task_id=task_id, result="success",
              output_hashes=[file_sha256(target)], details={"format": fmt})
    click.echo(text if not output else f"已导出: {target}")
