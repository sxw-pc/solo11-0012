"""时档匠 Click CLI：初始化、演示、调度、检索、回滚与本地 Web。"""
from __future__ import annotations

import sched
import subprocess
import sys
import time
from pathlib import Path

import click

from .audit import AuditLog
from .cli_commands import archive_cmd, audit_export_cmd, restore_cmd
from .cli_helpers import emit, runtime
from .config_loader import init_workspace, load_policy
from .fixture_loader import demo_sources, fixture_summary, load_source_manifest
from .schema import ArchiveError, Budget, ConfigError

DEFAULT_WORKDIR = Path("work")
CONTEXT = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT)
@click.option("--workdir", type=click.Path(path_type=Path), default=DEFAULT_WORKDIR,
              show_default=True, help="独立工作目录，所有输出均写入此处")
@click.pass_context
def cli(ctx, workdir):
    """时档匠：纯本地离线的时序数据分层降质归档工具。"""
    ctx.ensure_object(dict)
    ctx.obj["workdir"] = Path(workdir).resolve()


@cli.command("init")
@click.pass_context
def init_cmd(ctx):
    """初始化工作目录与默认策略。"""
    workdir = ctx.obj["workdir"]
    policy_path = init_workspace(workdir)
    emit({"workdir": str(workdir), "policy": str(policy_path), "status": "initialized"})


@cli.command("load-demo")
@click.pass_context
def load_demo_cmd(ctx):
    """校验并加载三组内置演示数据（原始 fixture 只读）。"""
    init_workspace(ctx.obj["workdir"])
    summary = fixture_summary()
    summary["sources"] = [source.to_dict() for source in demo_sources()]
    emit(summary)


@cli.command("validate-config")
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=None,
              help="待校验 YAML；默认为工作目录 policy.yaml")
@click.pass_context
def validate_config_cmd(ctx, config_path):
    """校验策略字段、粒度、保留周期和路径可写性。"""
    workdir = ctx.obj["workdir"]
    init_workspace(workdir)
    path = config_path or (workdir / "policy.yaml")
    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    try:
        policy = load_policy(path, workdir)
    except ConfigError as exc:
        audit.log("config_validate", "CLI", result="failure", error=str(exc),
                  details={"config": str(path)})
        emit({"valid": False, "errors": exc.errors})
        raise click.ClickException("配置校验失败")
    audit.log("config_validate", "CLI", result="success",
              details={"config": str(path), "policy_hash": policy.hash})
    emit({"valid": True, "policy": policy.to_dict(), "policy_hash": policy.hash})


@cli.command("demo")
@click.pass_context
def demo_cmd(ctx):
    """一键加载 mock 数据、全量归档、构建索引、检索并恢复样本。"""
    workdir = ctx.obj["workdir"]
    _, archiver, store, audit = runtime(workdir)
    results = archiver.run_batch(demo_sources(), actor="CLI")["results"]
    entries = store.search(limit=5)
    restored = archiver.restore_entry(entries[0], workdir / "restored") if entries else None
    audit.log("demo", "CLI", result="success",
              details={"sources": len(results), "archives": len(entries)})
    emit({"sources": [{"source": result["source"], "shards": result["shards"],
                       "tiers": list(result["tiers"]), "cached": result.get("cached", False)}
                      for result in results],
          "index": store.index_summary(), "search_samples": len(entries),
          "restored_sample": str(restored) if restored else "",
          "audit_chain": audit.verify(), "fixtures": fixture_summary()})


@cli.command("run-batch")
@click.option("--manifest", "manifest_path",
              type=click.Path(exists=True, path_type=Path), default=None,
              help="批量数据源 YAML 清单；缺省使用内置演示数据")
@click.option("--budget-bytes", type=click.IntRange(min=1), default=None,
              help="覆盖 budget.yaml 的准入总预算（字节）")
@click.option("--safety-factor", type=click.FloatRange(min=1.0, max=10.0),
              default=None, help="覆盖预估安全系数")
@click.pass_context
def run_batch_cmd(ctx, manifest_path, budget_bytes, safety_factor):
    """按准入预算批量归档一批数据源，排队源随预算释放逐步推进。"""
    _, archiver, _, _ = runtime(ctx.obj["workdir"])
    budget = archiver.budget
    if budget_bytes is not None or safety_factor is not None:
        budget = Budget(
            max_inflight_bytes=budget_bytes or (budget.max_inflight_bytes
                                                if budget else 0),
            safety_factor=(safety_factor if safety_factor is not None
                           else (budget.safety_factor if budget else 1.5)),
            history_reference=budget.history_reference if budget else True)
    if budget is None or budget.max_inflight_bytes <= 0:
        raise click.ClickException(
            "未配置准入预算：请在工作目录 budget.yaml 中设置 max_inflight_bytes，"
            "或使用 --budget-bytes 指定")
    specs = (load_source_manifest(manifest_path)
             if manifest_path else demo_sources())
    emit(archiver.run_batch(specs, actor="CLI", budget=budget))


@cli.command("schedule")
@click.option("--interval", type=click.IntRange(min=1), default=3600, show_default=True)
@click.option("--iterations", type=click.IntRange(min=0), default=0,
              help="运行次数，0 表示持续运行")
@click.pass_context
def schedule_cmd(ctx, interval, iterations):
    """启动本地定时调度器，周期归档全部演示数据源。"""
    _, archiver, _, audit = runtime(ctx.obj["workdir"])
    runner = sched.scheduler(time.time, time.sleep)
    state = {"count": 0}

    def tick():
        state["count"] += 1
        results = archiver.run_batch(demo_sources())["results"]
        audit.log("schedule", "CLI", result="success",
                  details={"iteration": state["count"],
                           "shards": sum(result["shards"] for result in results)})
        emit({"iteration": state["count"], "sources": len(results)})
        if iterations == 0 or state["count"] < iterations:
            runner.enter(interval, 1, tick)

    runner.enter(0, 1, tick)
    try:
        runner.run()
    except KeyboardInterrupt:
        audit.log("schedule_stop", "CLI", result="success",
                  details={"iterations": state["count"]})


@cli.command("tasks")
@click.option("--limit", type=click.IntRange(min=1, max=500), default=20)
@click.option("--status", default=None)
@click.pass_context
def tasks_cmd(ctx, limit, status):
    """查询持久化任务历史与断点状态。"""
    _, _, store, _ = runtime(ctx.obj["workdir"])
    emit(store.list_tasks(limit=limit, status=status))


@cli.command("search")
@click.option("--start", default="")
@click.option("--end", default="")
@click.option("--label-key", default="sensor_id")
@click.option("--label-value", default="")
@click.option("--limit", type=click.IntRange(min=1, max=1000), default=50)
@click.pass_context
def search_cmd(ctx, start, end, label_key, label_value, limit):
    """按时间范围与标签组合检索归档索引（不解压全量包）。"""
    _, _, store, _ = runtime(ctx.obj["workdir"])
    emit({"entries": store.search(start, end, label_key, label_value, limit),
          "summary": store.index_summary()})


@cli.command("rollback")
@click.option("--task-id", default="")
@click.pass_context
def rollback_cmd(ctx, task_id):
    """回滚最近一次（或指定）归档任务，归档包保留。"""
    _, archiver, _, _ = runtime(ctx.obj["workdir"])
    try:
        emit(archiver.rollback(task_id))
    except ArchiveError as exc:
        raise click.ClickException(str(exc))


@cli.command("web")
@click.option("--port", type=click.IntRange(min=1024, max=65535), default=8765,
              show_default=True)
@click.pass_context
def web_cmd(ctx, port):
    """启动仅绑定 127.0.0.1 的本地 Web 服务。"""
    from .web import create_app
    app = create_app(ctx.obj["workdir"])
    app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)


@cli.command("test")
@click.option("--cov/--no-cov", default=True, help="是否生成覆盖率报告")
def test_cmd(cov):
    """运行内置 pytest 全量测试。"""
    command = [sys.executable, "-m", "pytest", "-q"]
    if cov:
        command += ["--cov=src/time_archiver", "--cov-report=term-missing"]
    raise SystemExit(subprocess.call(command, cwd=Path(__file__).resolve().parents[2]))


cli.add_command(archive_cmd)
cli.add_command(restore_cmd)
cli.add_command(audit_export_cmd)


if __name__ == "__main__":
    cli()
