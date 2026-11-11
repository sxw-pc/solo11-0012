"""本地 Web 页面与 JSON API；无登录、无外部资源。"""
from __future__ import annotations

from datetime import timezone
from pathlib import Path

from dateutil import parser as date_parser
from flask import (
    Blueprint, Response, current_app, jsonify, redirect, render_template,
    request, send_file, url_for,
)

from ..config_loader import save_policy_text
from ..fixture_loader import demo_sources
from ..schema import ConfigError, DataSourceError, SourceSpec
from . import refresh_policy, store_for

bp = Blueprint("main", __name__)


def _archiver():
    return current_app.extensions["time_archiver"]["archiver"]


def _policy_file() -> Path:
    return Path(current_app.extensions["time_archiver"]["policy_file"])


@bp.get("/")
def index():
    return redirect(url_for("main.tasks_page"))


@bp.route("/policy", methods=["GET", "POST"])
def policy_page():
    errors, message = [], ""
    text = _policy_file().read_text(encoding="utf-8")
    if request.method == "POST":
        submitted = request.form.get("yaml_text", "")
        try:
            if not submitted.strip():
                raise ConfigError("YAML 内容不能为空")
            policy = save_policy_text(_policy_file(), submitted,
                                      Path(current_app.config["WORKDIR"]))
            refresh_policy(current_app)
            _archiver().audit.log("config_update", "WEB", result="success",
                                  details={"policy": policy.name,
                                           "policy_hash": policy.hash})
            text, message = submitted, "策略已校验并保存"
        except ConfigError as exc:
            errors = exc.errors
            text = submitted
            _archiver().audit.log("config_update", "WEB", result="failure",
                                  error=str(exc))
    return render_template("policy.html", policy=_archiver().policy,
                           yaml_text=text, errors=errors, message=message)


@bp.get("/tasks")
def tasks_page():
    store = store_for(current_app)
    return render_template("task.html", tasks=store.list_tasks(limit=100),
                           counts=store.counts(), index=store.index_summary(),
                           audit=_archiver().audit.verify(),
                           message=request.args.get("message", ""),
                           errors=request.args.get("errors", "").split("||") if
                           request.args.get("errors") else [])


@bp.post("/tasks/run")
def run_task():
    mode = request.form.get("mode", "demo")
    archiver = _archiver()
    results, errors = [], []
    try:
        if mode == "demo":
            specs = demo_sources()
        else:
            columns = lambda name: tuple(filter(None, request.form.get(name, "").split(",")))
            source_path = request.form.get("source_path", "").strip()
            if not source_path:
                raise DataSourceError("自定义数据源路径不能为空")
            specs = [SourceSpec(
                path=Path(source_path), fmt=request.form.get("format", "parquet"),
                table=request.form.get("table") or None,
                timestamp_column=request.form.get("timestamp_column", "timestamp"),
                label_columns=columns("label_columns") or ("sensor_id",),
                metric_columns=columns("metric_columns") or ("value",),
                source_id=request.form.get("source_id", "").strip())]
        for spec in specs:
            try:
                results.append(archiver.run_source(spec, actor="WEB"))
            except Exception as exc:  # 单个源失败不阻断其余源
                errors.append(f"{spec.source_id}: {exc}")
    except Exception as exc:
        errors.append(str(exc))
    store = store_for(current_app)
    return render_template(
        "task.html", tasks=store.list_tasks(limit=100), counts=store.counts(),
        index=store.index_summary(), audit=archiver.audit.verify(),
        message=f"已触发 {len(results)} 个数据源", errors=errors,
        results=[{"source": r["source"], "shards": r["shards"],
                  "cached": r.get("cached", False)} for r in results])


@bp.get("/api/tasks")
def api_tasks():
    return jsonify({"tasks": store_for(current_app).list_tasks(limit=50),
                    "counts": store_for(current_app).counts()})


@bp.get("/api/tasks/<task_id>")
def api_task(task_id):
    task = store_for(current_app).get_task(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify(task)


@bp.get("/archive")
def archive_page():
    store = store_for(current_app)
    errors = []
    try:
        start = _normalize_time(request.args.get("start", ""), end=False)
        end = _normalize_time(request.args.get("end", ""), end=True)
    except ValueError as exc:
        start, end, errors = "", "", [f"时间格式非法: {exc}"]
    key = request.args.get("label_key", "sensor_id").strip()
    value = request.args.get("label_value", "").strip()
    try:
        limit = max(1, min(1000, int(request.args.get("limit", 100))))
    except ValueError:
        limit, errors = 100, [*errors, "limit 必须是整数"]
    entries = store.search(start, end, key, value, limit=limit)
    return render_template("archive.html", entries=entries, start=start, end=end,
                           label_key=key, label_value=value, errors=errors,
                           index=store.index_summary())


@bp.post("/archive/restore")
def restore_archive():
    store = store_for(current_app)
    shard_id = request.form.get("shard_id", "").strip()
    entries = store.index_entries_for_shard(shard_id)
    if not entries:
        return render_template("archive.html", entries=[], start="", end="",
                               label_key="sensor_id", label_value="", index=store.index_summary(),
                               errors=["该分片没有可恢复的归档索引"]), 404
    target = _archiver().restore_entry(entries[0], Path(current_app.config["WORKDIR"]) / "restored",
                                       actor="WEB")
    return send_file(target, as_attachment=True, download_name=target.name)


@bp.get("/audit")
def audit_page():
    audit = _archiver().audit
    filters = {
        "operation": request.args.get("operation", ""),
        "task_id": request.args.get("task_id", ""),
        "result": request.args.get("result", ""),
        "actor": request.args.get("actor", ""),
    }
    records = audit.read(limit=int(request.args.get("limit", 300)), **filters)
    return render_template("audit.html", records=records, filters=filters,
                           chain=audit.verify(), summary=audit.summary())


@bp.get("/audit/export")
def export_audit():
    fmt = request.args.get("format", "json")
    if fmt not in ("json", "markdown"):
        return jsonify({"error": "仅支持 json 或 markdown"}), 400
    audit = _archiver().audit
    filters = {key: request.args.get(key, "") for key in
               ("operation", "task_id", "result", "actor")}
    filters = {key: value for key, value in filters.items() if value}
    audit.log("audit_export", "WEB", task_id=filters.get("task_id", ""),
              result="success", details={"format": fmt})
    if fmt == "markdown":
        return Response(audit.export_markdown(**filters), mimetype="text/markdown",
                        headers={"Content-Disposition": "attachment; filename=audit_export.md"})
    return Response(audit.export_json(**filters), mimetype="application/json",
                    headers={"Content-Disposition": "attachment; filename=audit_export.json"})


def _normalize_time(value: str, end: bool) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    parsed = date_parser.isoparse(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if len(value) <= 10:
        parsed = parsed.replace(hour=23 if end else 0,
                                minute=59 if end else 0,
                                second=59 if end else 0)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
