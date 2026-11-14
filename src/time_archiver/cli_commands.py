"""数据操作类子命令：归档、预检、恢复和审计导出。"""
from __future__ import annotations

from pathlib import Path

import click

from .audit import AuditLog
from .cli_helpers import emit, runtime
from .config_loader import init_workspace
from .data_reader import read_source
from .index_builder import extract_member
from .precheck import exceeded_months, run_precheck
from .schema import ArchiveError, ConfigError, DataSourceError, SourceSpec, file_sha256


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
@click.pass_context
def archive_cmd(ctx, source_path, fmt, timestamp_column, label_columns,
                metric_columns, table, source_id):
    """对单个本地数据源执行一次分层归档。"""
    _, archiver, _, _ = runtime(ctx.obj["workdir"])
    spec = SourceSpec(source_path, fmt, timestamp_column,
                      tuple(filter(None, label_columns.split(","))),
                      tuple(filter(None, metric_columns.split(","))), table, source_id)
    try:
        emit(archiver.run_source(spec))
    except (DataSourceError, ArchiveError, ConfigError) as exc:
        raise click.ClickException(str(exc))


@click.command("precheck")
@source_options
@click.pass_context
def precheck_cmd(ctx, source_path, fmt, timestamp_column, label_columns,
                 metric_columns, table, source_id):
    """归档前预检：推算每层每月的标签组合基数与稀疏度并给出聚合决策。

    只读取数据源与策略，不写任务表；报告确定性落盘到
    state/precheck/ 并写一条 precheck 审计记录。
    """
    workdir = ctx.obj["workdir"]
    policy, _, _, audit = runtime(workdir)
    spec = SourceSpec(source_path, fmt, timestamp_column,
                      tuple(filter(None, label_columns.split(","))),
                      tuple(filter(None, metric_columns.split(","))), table, source_id)
    try:
        frame, quality = read_source(spec, policy.quality)
        if not quality.ok:
            raise DataSourceError("；".join(quality.issues) or "数据质量校验未通过")
        report, digest, path = run_precheck(workdir, spec, policy, frame,
                                            quality.source_hash)
    except (DataSourceError, ConfigError) as exc:
        raise click.ClickException(str(exc))
    audit.log("precheck", "CLI", task_id=report["precheck_id"],
              range_start=report["generated_from"]["range_start"],
              range_end=report["generated_from"]["range_end"],
              input_hashes=[quality.source_hash], output_hashes=[digest],
              result="success",
              details={"source": spec.source_id, "policy": policy.name,
                       "decisions": report["decisions"],
                       "exceeded": exceeded_months(report),
                       "report": path.relative_to(workdir).as_posix(),
                       "auto_apply": policy.precheck.auto_apply,
                       "enabled": policy.precheck.enabled})
    emit({**report, "report_path": str(path), "report_hash": digest,
          "enabled": policy.precheck.enabled,
          "auto_apply": policy.precheck.auto_apply,
          "quality": quality.to_dict()})


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
