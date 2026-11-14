"""分层归档编排：任务幂等、两阶段提交、断点状态、准入预算与回滚。"""
from __future__ import annotations

import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd

from .admission import (
    DEFAULT_SAFETY_FACTOR, BudgetPool, build_estimate, estimate_summary,
    peak_rss_bytes, plan_admission,
)
from .audit import AuditLog
from .config_loader import load_budget, load_policy
from .data_reader import TIMESTAMP, read_source
from .downsample import downsample_frame
from .index_builder import (
    atomic_write, build_archive_package, entries_for_existing_package,
    extension_for, restore_by_entry, serialize_frame, verify_package,
    verify_serialized,
)
from .metadata_store import MetadataStore, utc_now
from .schema import (
    ArchiveError, Budget, DataSourceError, IndexEntry, Policy, PolicyTier,
    ShardRecord, SourceSpec, bytes_sha256, canonical_hash, file_sha256,
    safe_name,
)


class Archiver:
    """面向单个数据源执行全量分层归档，可安全重复调用。"""

    def __init__(self, workdir: Path, policy: Policy,
                 budget: Optional[Budget] = None):
        self.workdir = Path(workdir).resolve()
        self.policy = policy
        self.budget = budget
        self.store = MetadataStore(self.workdir / "state" / "archive.db")
        self.audit = AuditLog(self.workdir / "audit" / "audit.jsonl")
        self.staging = self.workdir / "staging"
        self.staging.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_workdir(cls, workdir: Path) -> "Archiver":
        workdir = Path(workdir)
        policy = load_policy(workdir / "policy.yaml", workdir)
        return cls(workdir, policy, budget=load_budget(workdir))

    def task_id_for(self, spec: SourceSpec) -> str:
        try:
            digest = file_sha256(spec.path)
        except OSError as exc:
            raise DataSourceError(f"无法读取输入文件: {exc}") from exc
        return self._task_id_from_hash(spec, digest)

    def _task_id_from_hash(self, spec: SourceSpec, source_hash: str) -> str:
        return "task-" + canonical_hash({
            "policy": self.policy.hash, "source_hash": source_hash,
            "spec": spec.to_dict(),
        })[:24]

    def _cached_result(self, task_id: str, spec: SourceSpec,
                       actor: str) -> Optional[Dict[str, Any]]:
        """已完成且输出完整的任务直接复用结果，并写缓存命中审计。"""
        existing = self.store.get_task(task_id)
        if not (existing and existing["status"] == "completed"
                and self._intact(task_id, existing.get("result") or {})):
            return None
        result = dict(existing.get("result") or {})
        result["cached"] = True
        self.audit.log("archive_cached", actor, task_id=task_id,
                       result="success", details={"source": spec.source_id})
        return result

    def run_source(self, spec: SourceSpec, actor: str = "CLI") -> Dict[str, Any]:
        task_id = self.task_id_for(spec)
        cached = self._cached_result(task_id, spec, actor)
        if cached is not None:
            return cached
        try:
            frame, report = read_source(spec, self.policy.quality)
            if not report.ok:
                raise DataSourceError("；".join(report.issues) or "数据质量校验未通过")
            start, end = _bounds(frame)
            self.store.start_task(task_id, "archive", spec.source_id,
                                  self.policy.hash, start, end)
            self.store.update_task(task_id, status="running", stage="source_loaded",
                                   range_start=start, range_end=end)
            result = self._archive_frame(task_id, spec, frame, report, start, end)
            self.store.finish_task(task_id, "completed", result)
            self.audit.log(
                "archive", actor, task_id=task_id, range_start=start, range_end=end,
                input_hashes=[report.source_hash],
                output_hashes=result["output_hashes"], result="success",
                details={"source": spec.source_id, "policy": self.policy.name,
                         "tiers": {k: v["rows"] for k, v in result["tiers"].items()},
                         "quality": report.to_dict()})
            return result
        except Exception as exc:
            self.store.update_task(task_id, status="failed", stage="failed",
                                   finished_at=utc_now(), error=str(exc)[:1000])
            self.audit.log("archive", actor, task_id=task_id, input_hashes=[],
                           result="failure", error=str(exc)[:1000],
                           details={"source": spec.source_id})
            raise

    def estimate_source(self, spec: SourceSpec,
                        budget: Optional[Budget] = None) -> Dict[str, Any]:
        """读取源数据并生成逐层成本预估；结果对同一输入逐字节确定。"""
        budget = budget if budget is not None else self.budget
        frame, report = read_source(spec, self.policy.quality)
        if not report.ok:
            raise DataSourceError("；".join(report.issues) or "数据质量校验未通过")
        factor = budget.safety_factor if budget else DEFAULT_SAFETY_FACTOR
        history = None
        if budget and budget.history_reference:
            def history(tier_name: str) -> int:
                return self.store.tier_actual_rows(spec.source_id, tier_name)
        return build_estimate(frame, report, self.policy, spec, factor, history)

    def run_batch(self, specs: List[SourceSpec], actor: str = "CLI",
                  budget: Optional[Budget] = None) -> Dict[str, Any]:
        """准入控制的批量归档；未配置预算时退化为逐源串行执行。

        流程：已完成任务先命中缓存（不重算）；其余源逐一读取并做确定性
        成本预估；按预估成本升序做准入计划，超预算的源拒绝并写审计；
        准入源占用预算进入原分层链路，完成或失败都释放预算，并按释放
        后的余量提拔排队源。预估与实际落盘行数、分片数一并写入审计。
        """
        specs = list(specs)
        budget = budget if budget is not None else self.budget
        if budget is None or not specs:
            results = [self._run_guarded(spec, actor) for spec in specs]
            return {"results": results, "admission": None}

        results: Dict[int, Dict[str, Any]] = {}
        estimates: Dict[int, Dict[str, Any]] = {}
        pending: List[int] = []
        for index, spec in enumerate(specs):
            try:
                task_id = self.task_id_for(spec)
            except DataSourceError:
                task_id = ""
            cached = (self._cached_result(task_id, spec, actor)
                      if task_id else None)
            if cached is not None:
                cached["queued"] = False
                results[index] = cached
            else:
                pending.append(index)
        estimable: List[int] = []
        for index in pending:
            try:
                estimates[index] = self.estimate_source(specs[index], budget)
                estimable.append(index)
            except DataSourceError:
                # 读取或质量失败不属于预算拒绝，回到原链路复现失败语义。
                results[index] = self._run_guarded(specs[index], actor)

        items = [(index, estimates[index]["totals"]["estimated_peak_bytes"])
                 for index in estimable]
        admitted, queued, rejected = plan_admission(
            items, budget.max_inflight_bytes)
        for index in rejected:
            spec = specs[index]
            estimate = estimates[index]
            estimated_bytes = estimate["totals"]["estimated_peak_bytes"]
            reason = (f"预估峰值内存 {estimated_bytes} 字节超过准入总预算 "
                      f"{budget.max_inflight_bytes} 字节，本窗口无法容纳；"
                      "请调大 max_inflight_bytes、检查 safety_factor 或拆分数据源")
            self.audit.log(
                "admission", actor,
                task_id=self._task_id_from_hash(spec, estimate["source_hash"]),
                result="rejected", error=reason,
                details={"source": spec.source_id, "reason": reason,
                         "estimate": estimate, "budget": budget.to_dict()})
            results[index] = {
                "task_id": "", "source": spec.source_id, "status": "rejected",
                "reason": reason, "estimated_peak_bytes": estimated_bytes,
                "shards": 0, "tiers": {}, "cached": False, "queued": False,
            }

        pool = BudgetPool(budget.max_inflight_bytes)
        queue = list(queued)
        queued_set = set(queued)
        ready = list(admitted)
        execution_order: List[str] = []
        while ready:
            index = ready.pop(0)
            spec = specs[index]
            estimate = estimates[index]
            estimated_bytes = estimate["totals"]["estimated_peak_bytes"]
            was_queued = index in queued_set
            task_id = self._task_id_from_hash(spec, estimate["source_hash"])
            pool.acquire(index, estimated_bytes)
            self.audit.log(
                "admission", actor, task_id=task_id, result="admitted",
                details={"source": spec.source_id, "estimate": estimate,
                         "queued": was_queued, "budget": pool.snapshot()})
            result: Optional[Dict[str, Any]] = None
            error = ""
            try:
                result = self.run_source(spec, actor=actor)
                result["queued"] = was_queued
                result["estimated_peak_bytes"] = estimated_bytes
                outcome = "cached" if result.get("cached") else "completed"
                results[index] = result
            except Exception as exc:  # 单源失败不阻断批次，预算必须归还
                outcome = "failed"
                error = str(exc)[:1000]
                results[index] = {
                    "task_id": "", "source": spec.source_id, "status": "failed",
                    "error": error, "estimated_peak_bytes": estimated_bytes,
                    "shards": 0, "tiers": {}, "cached": False,
                    "queued": was_queued,
                }
            finally:
                pool.release(index)
            self.audit.log(
                "admission", actor,
                task_id=(result or {}).get("task_id") or task_id,
                result="released", error=error,
                details={"source": spec.source_id, "outcome": outcome,
                         "estimate": estimate_summary(estimate),
                         "actual": self._actuals_for(result, error),
                         "budget": pool.snapshot()})
            execution_order.append(spec.source_id)
            still_queued: List[int] = []
            for waiting in queue:
                if (estimates[waiting]["totals"]["estimated_peak_bytes"]
                        <= pool.free_bytes):
                    ready.append(waiting)
                else:
                    still_queued.append(waiting)
            queue = still_queued
        if queue:  # pragma: no cover - 排队源预估均不超过总预算，防御性分支
            raise ArchiveError("准入调度内部不一致：预算已空转但仍有排队源")

        summary = {
            "budget": budget.to_dict(), "sources": len(specs),
            "completed": sum(1 for r in results.values()
                             if r["status"] == "completed" and not r.get("cached")),
            "cached": sum(1 for r in results.values() if r.get("cached")),
            "failed": sum(1 for r in results.values()
                          if r["status"] == "failed"),
            "rejected": sum(1 for r in results.values()
                            if r["status"] == "rejected"),
            "queued_then_admitted": sum(1 for r in results.values()
                                        if r.get("queued")),
            "execution_order": execution_order,
        }
        self.audit.log("admission", actor, result="summary", details=summary)
        admission = {
            "budget": budget.to_dict(),
            "decisions": {specs[i].source_id: results[i]["status"]
                          for i in range(len(specs))},
            "rejected": {specs[i].source_id: results[i]["reason"]
                         for i in range(len(specs))
                         if results[i]["status"] == "rejected"},
            "estimated_peak_bytes": {
                specs[i].source_id: estimates[i]["totals"]["estimated_peak_bytes"]
                for i in estimates},
            "execution_order": execution_order,
        }
        return {"results": [results[i] for i in range(len(specs))],
                "admission": admission}

    def _run_guarded(self, spec: SourceSpec, actor: str) -> Dict[str, Any]:
        """执行单个源并把异常收敛为失败处置，保证批次内其余源继续。"""
        try:
            result = self.run_source(spec, actor=actor)
            result["queued"] = False
            return result
        except Exception as exc:
            return {
                "task_id": "", "source": spec.source_id, "status": "failed",
                "error": str(exc)[:1000], "shards": 0, "tiers": {},
                "cached": False, "queued": False,
            }

    def _actuals_for(self, result: Optional[Dict[str, Any]],
                     error: str) -> Dict[str, Any]:
        """落盘后的实际行数、分片数与字节数，供校准预估安全系数。"""
        if result is None:
            return {"error": error} if error else {}
        tiers = result.get("tiers") or {}
        output_bytes = 0
        for shard in self.store.shards_for_task(result.get("task_id", "")):
            path = self.workdir / shard["path"]
            if path.is_file():
                output_bytes += path.stat().st_size
        actual: Dict[str, Any] = {
            "tiers": {name: {"rows": tier["rows"], "shards": tier["shards"]}
                      for name, tier in tiers.items()},
            "output_rows": sum(tier["rows"] for tier in tiers.values()),
            "shards": int(result.get("shards", 0)),
            "output_bytes": output_bytes,
        }
        peak = peak_rss_bytes()
        if peak is not None:
            actual["peak_rss_bytes"] = peak
        return actual

    def _archive_frame(self, task_id: str, spec: SourceSpec, frame: pd.DataFrame,
                       report: Any, start: str, end: str) -> Dict[str, Any]:
        anchor = pd.to_datetime(frame[TIMESTAMP], errors="coerce").max()
        age = anchor - pd.to_datetime(frame[TIMESTAMP])
        tiers_result: Dict[str, Any] = {}
        output_hashes: list[str] = []
        previous_days = 0
        for position, tier in enumerate(self.policy.tiers):
            self.store.update_task(task_id, stage=f"tier:{tier.name}")
            if len(self.policy.tiers) == 1:
                selected = frame
            elif position == 0:
                selected = frame[age <= timedelta(days=tier.retention_days)]
            elif position == len(self.policy.tiers) - 1:
                selected = frame[age > timedelta(days=previous_days)]
            else:
                selected = frame[(age > timedelta(days=previous_days)) &
                                 (age <= timedelta(days=tier.retention_days))]
            previous_days = tier.retention_days
            if selected.empty:
                tiers_result[tier.name] = {"rows": 0, "raw_rows": 0, "shards": 0,
                                           "missing": 0, "anomalies": 0}
                continue
            aggregated = downsample_frame(
                selected, tier, spec.label_columns, spec.metric_columns,
                report.interval_seconds)
            if aggregated.empty:
                tiers_result[tier.name] = {"rows": 0, "raw_rows": len(selected),
                                           "shards": 0, "missing": 0, "anomalies": 0}
                continue
            aggregated["__partition"] = pd.to_datetime(aggregated[TIMESTAMP]).dt.strftime("%Y-%m")
            rows = 0
            shards = 0
            for partition, part in aggregated.groupby("__partition", sort=True):
                record, entries = self._persist(task_id, spec, tier, str(partition),
                                                part.drop(columns="__partition"))
                output_hashes.append(record.sha256)
                rows += record.row_count
                shards += 1
                if entries:
                    entries = [replace(entry, shard_id=record.shard_id,
                                       package_path=record.path) for entry in entries]
                    self.store.replace_index(entries)
            tiers_result[tier.name] = {
                "rows": rows, "raw_rows": int(len(selected)), "shards": shards,
                "missing": int(aggregated["missing_count"].sum()),
                "anomalies": int(aggregated["anomaly_count"].sum()),
            }
        return {
            "task_id": task_id, "source": spec.source_id, "status": "completed",
            "range_start": start, "range_end": end,
            "input_hash": report.source_hash, "quality": report.to_dict(),
            "tiers": tiers_result, "output_hashes": sorted(output_hashes),
            "shards": sum(item["shards"] for item in tiers_result.values()),
            "cached": False,
        }

    def _persist(self, task_id: str, spec: SourceSpec, tier: PolicyTier,
                 partition: str, part: pd.DataFrame):
        shard_id = canonical_hash([tier.name, spec.source_id, partition])[:24]
        staging = self.staging / f"{shard_id}-{uuid4().hex}.tmp"
        is_archive = tier is self.policy.archive_tier
        entries: list[IndexEntry] = []
        try:
            if is_archive:
                digest, _size, entries = build_archive_package(
                    part, staging, tier, spec.label_columns, spec.metric_columns,
                    spec.source_id)
                if not verify_package(staging):
                    raise ArchiveError(f"归档包完整性校验失败: {partition}")
                extension = "tar.gz"
            else:
                data = serialize_frame(part, tier.format, tier.compression)
                if not verify_serialized(data, tier.format, len(part)):
                    raise ArchiveError(f"输出完整性校验失败: {partition}")
                digest = bytes_sha256(data)
                staging.write_bytes(data)
                extension = extension_for(tier.format, tier.compression)
            filename = (f"{safe_name(spec.source_id)}__{safe_name(tier.name)}__"
                        f"{partition}__{digest[:10]}.{extension}")
            target = self.workdir / tier.path / filename
            target.parent.mkdir(parents=True, exist_ok=True)
            if not is_archive:
                if not (target.exists() and file_sha256(target) == digest):
                    atomic_write(target, staging.read_bytes())
                staging.unlink(missing_ok=True)
            elif target.exists() and file_sha256(target) == digest:
                staging.unlink(missing_ok=True)
            else:
                os.replace(staging, target)
        finally:
            staging.unlink(missing_ok=True)
        relative = target.relative_to(self.workdir).as_posix()
        record = ShardRecord(
            shard_id=shard_id, task_id=task_id, tier=tier.name,
            source=spec.source_id, partition=partition, path=relative,
            fmt="tar.gz" if is_archive else extension_for(tier.format, tier.compression),
            row_count=len(part),
            raw_count=int(part["raw_count"].sum()),
            missing_count=int(part["missing_count"].sum()),
            anomaly_count=int(part["anomaly_count"].sum()),
            sha256=digest, created_at=utc_now())
        self.store.save_shard(record)
        if entries:
            self.store.replace_index([replace(entry, shard_id=shard_id,
                                              package_path=relative) for entry in entries])
        return record, entries

    def _intact(self, task_id: str, result: Dict[str, Any]) -> bool:
        shards = self.store.shards_for_task(task_id)
        if len(shards) != int(result.get("shards", 0)):
            return False
        for shard in shards:
            path = self.workdir / shard["path"]
            if not path.is_file() or file_sha256(path) != shard["sha256"]:
                return False
        return True

    def restore_entry(self, entry: Dict[str, Any], output_dir: Path,
                      actor: str = "CLI") -> Path:
        package = (self.workdir / entry["package_path"]).resolve()
        if self.workdir not in package.parents:
            raise ArchiveError("归档包路径越出工作目录")
        target = restore_by_entry(self.workdir, entry, Path(output_dir))
        self.audit.log("restore", actor, task_id=entry.get("shard_id", ""),
                       range_start=entry.get("min_ts", ""),
                       range_end=entry.get("max_ts", ""),
                       input_hashes=[file_sha256(package)],
                       output_hashes=[file_sha256(target)], result="success",
                       details={"member": entry.get("member_name", ""),
                                "output": str(target)})
        return target

    def rollback(self, task_id: str = "", actor: str = "CLI") -> Dict[str, Any]:
        task = self.store.get_task(task_id) if task_id else self.store.latest_task("archive")
        if not task:
            raise ArchiveError("未找到可回滚的归档任务")
        if task["status"] != "completed":
            raise ArchiveError("仅可回滚已完成的归档任务")
        changed = self.store.rollback_task(task["task_id"])
        for item in changed:
            shard = self.store.get_shard(item["shard_id"])
            if shard and shard["active"] and shard["path"].endswith(".tar.gz"):
                package = self.workdir / shard["path"]
                entries = entries_for_existing_package(package, shard["path"],
                                                       shard["shard_id"])
                self.store.replace_index(entries)
        self.audit.log("rollback", actor, task_id=task["task_id"],
                       range_start=task.get("range_start", ""),
                       range_end=task.get("range_end", ""),
                       result="success",
                       details={"restored_shards": len(changed),
                                "packages_retained": True})
        return {"task_id": task["task_id"], "restored_shards": len(changed),
                "packages_retained": True}


def _bounds(frame: pd.DataFrame) -> tuple[str, str]:
    values = pd.to_datetime(frame[TIMESTAMP]).dropna()
    if values.empty:
        return "", ""
    def iso(value):
        stamp = pd.Timestamp(value)
        return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    return iso(values.min()), iso(values.max())
