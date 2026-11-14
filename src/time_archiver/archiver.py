"""分层归档编排：任务幂等、两阶段提交、断点状态与回滚。"""
from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

import pandas as pd

from .audit import AuditLog
from .config_loader import load_policy
from .data_reader import TIMESTAMP, read_source
from .downsample import downsample_frame
from .index_builder import (
    atomic_write, build_archive_package, entries_for_existing_package,
    extension_for, read_package_coverage, restore_by_entry, serialize_frame,
    verify_package, verify_serialized,
)
from .metadata_store import MetadataStore, utc_now
from .precheck import (
    combo_coverage, exceeded_months, run_precheck, select_tier_rows,
)
from .schema import (
    ArchiveError, DataSourceError, IndexEntry, Policy, PolicyTier, ShardRecord,
    SourceSpec, bytes_sha256, canonical_hash, file_sha256, safe_name,
)


class Archiver:
    """面向单个数据源执行全量分层归档，可安全重复调用。"""

    def __init__(self, workdir: Path, policy: Policy):
        self.workdir = Path(workdir).resolve()
        self.policy = policy
        self.store = MetadataStore(self.workdir / "state" / "archive.db")
        self.audit = AuditLog(self.workdir / "audit" / "audit.jsonl")
        self.staging = self.workdir / "staging"
        self.staging.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_workdir(cls, workdir: Path) -> "Archiver":
        policy = load_policy(Path(workdir) / "policy.yaml", Path(workdir))
        return cls(workdir, policy)

    def task_id_for(self, spec: SourceSpec) -> str:
        try:
            digest = file_sha256(spec.path)
        except OSError as exc:
            raise DataSourceError(f"无法读取输入文件: {exc}") from exc
        return "task-" + canonical_hash({
            "policy": self.policy.hash, "source_hash": digest,
            "spec": spec.to_dict(),
        })[:24]

    def run_source(self, spec: SourceSpec, actor: str = "CLI") -> Dict[str, Any]:
        task_id = self.task_id_for(spec)
        existing = self.store.get_task(task_id)
        if (existing and existing["status"] == "completed"
                and self._intact(task_id, existing.get("result") or {})):
            result = dict(existing.get("result") or {})
            result["cached"] = True
            self.audit.log("archive_cached", actor, task_id=task_id,
                           result="success", details={"source": spec.source_id})
            return result
        try:
            frame, report = read_source(spec, self.policy.quality)
            if not report.ok:
                raise DataSourceError("；".join(report.issues) or "数据质量校验未通过")
            start, end = _bounds(frame)
            decisions = self._precheck(spec, frame, report, start, end, actor)
            self.store.start_task(task_id, "archive", spec.source_id,
                                  self.policy.hash, start, end)
            self.store.update_task(task_id, status="running", stage="source_loaded",
                                   range_start=start, range_end=end)
            result = self._archive_frame(task_id, spec, frame, report, start, end,
                                         decisions=decisions)
            self.store.finish_task(task_id, "completed", result)
            details = {"source": spec.source_id, "policy": self.policy.name,
                       "tiers": {k: v["rows"] for k, v in result["tiers"].items()},
                       "quality": report.to_dict()}
            if decisions:
                details["aggregation"] = decisions
            self.audit.log(
                "archive", actor, task_id=task_id, range_start=start, range_end=end,
                input_hashes=[report.source_hash],
                output_hashes=result["output_hashes"], result="success",
                details=details)
            return result
        except Exception as exc:
            self.store.update_task(task_id, status="failed", stage="failed",
                                   finished_at=utc_now(), error=str(exc)[:1000])
            self.audit.log("archive", actor, task_id=task_id, input_hashes=[],
                           result="failure", error=str(exc)[:1000],
                           details={"source": spec.source_id})
            raise

    def _precheck(self, spec: SourceSpec, frame: pd.DataFrame, report: Any,
                  start: str, end: str, actor: str) -> Optional[Dict[str, str]]:
        """策略启用预检时执行基数/稀疏度检查并写审计；返回各层聚合决策。

        预检不写入任务表，不占用归档任务序号；未启用时返回 None，
        归档流程与未安装预检能力时完全一致。
        """
        config = self.policy.precheck
        if not config.enabled:
            return None
        precheck_report, digest, path = run_precheck(
            self.workdir, spec, self.policy, frame, report.source_hash)
        relative = path.relative_to(self.workdir).as_posix()
        self.audit.log(
            "precheck", actor, task_id=precheck_report["precheck_id"],
            range_start=start, range_end=end,
            input_hashes=[report.source_hash], output_hashes=[digest],
            result="success",
            details={"source": spec.source_id, "policy": self.policy.name,
                     "decisions": precheck_report["decisions"],
                     "exceeded": exceeded_months(precheck_report),
                     "report": relative,
                     "auto_apply": config.auto_apply})
        if not config.auto_apply:
            return None
        return dict(precheck_report["decisions"])

    def _archive_frame(self, task_id: str, spec: SourceSpec, frame: pd.DataFrame,
                       report: Any, start: str, end: str,
                       decisions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        tiers_result: Dict[str, Any] = {}
        output_hashes: list[str] = []
        for position, tier in enumerate(self.policy.tiers):
            self.store.update_task(task_id, stage=f"tier:{tier.name}")
            selected = select_tier_rows(frame, self.policy.tiers, position)
            mode = (decisions or {}).get(tier.name, "global")
            if selected.empty:
                tiers_result[tier.name] = {"rows": 0, "raw_rows": 0, "shards": 0,
                                           "missing": 0, "anomalies": 0}
                if decisions is not None:
                    tiers_result[tier.name]["mode"] = mode
                continue
            if mode == "local":
                tiers_result[tier.name] = self._archive_tier_local(
                    task_id, spec, tier, selected, report, output_hashes)
                continue
            aggregated = downsample_frame(
                selected, tier, spec.label_columns, spec.metric_columns,
                report.interval_seconds)
            if aggregated.empty:
                tiers_result[tier.name] = {"rows": 0, "raw_rows": len(selected),
                                           "shards": 0, "missing": 0, "anomalies": 0}
                if decisions is not None:
                    tiers_result[tier.name]["mode"] = mode
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
            if decisions is not None:
                tiers_result[tier.name]["mode"] = mode
        return {
            "task_id": task_id, "source": spec.source_id, "status": "completed",
            "range_start": start, "range_end": end,
            "input_hash": report.source_hash, "quality": report.to_dict(),
            "tiers": tiers_result, "output_hashes": sorted(output_hashes),
            "shards": sum(item["shards"] for item in tiers_result.values()),
            "cached": False,
        }

    def _archive_tier_local(self, task_id: str, spec: SourceSpec, tier: PolicyTier,
                            selected: pd.DataFrame, report: Any,
                            output_hashes: list[str]) -> Dict[str, Any]:
        """局部聚合层：先按自然月切分，再在分片内按标签组合独立聚合。

        每个分片附带组合覆盖率表（覆盖桶数与有效点数），四个质量列的
        口径与全局模式一致，仅理论槽位按组合自身节奏计算。
        """
        windowed = selected.copy()
        windowed["__partition"] = (pd.to_datetime(windowed[TIMESTAMP])
                                   .dt.strftime("%Y-%m"))
        rows = 0
        shards = 0
        missing = 0
        anomalies = 0
        for partition, raw_part in windowed.groupby("__partition", sort=True):
            raw_part = raw_part.drop(columns="__partition")
            aggregated = downsample_frame(
                raw_part, tier, spec.label_columns, spec.metric_columns,
                report.interval_seconds, mode="local")
            if aggregated.empty:
                continue
            coverage = combo_coverage(raw_part, tier, spec.label_columns,
                                      spec.metric_columns)
            record, entries = self._persist(task_id, spec, tier, str(partition),
                                            aggregated, coverage=coverage,
                                            mode="local")
            output_hashes.append(record.sha256)
            rows += record.row_count
            shards += 1
            missing += int(aggregated["missing_count"].sum())
            anomalies += int(aggregated["anomaly_count"].sum())
            if entries:
                entries = [replace(entry, shard_id=record.shard_id,
                                   package_path=record.path) for entry in entries]
                self.store.replace_index(entries)
        return {"rows": rows, "raw_rows": int(len(selected)), "shards": shards,
                "missing": missing, "anomalies": anomalies, "mode": "local"}

    def _persist(self, task_id: str, spec: SourceSpec, tier: PolicyTier,
                 partition: str, part: pd.DataFrame,
                 coverage: Optional[list] = None, mode: str = "global"):
        shard_id = canonical_hash([tier.name, spec.source_id, partition])[:24]
        staging = self.staging / f"{shard_id}-{uuid4().hex}.tmp"
        is_archive = tier is self.policy.archive_tier
        entries: list[IndexEntry] = []
        try:
            if is_archive:
                digest, _size, entries = build_archive_package(
                    part, staging, tier, spec.label_columns, spec.metric_columns,
                    spec.source_id, coverage=coverage, aggregation_mode=mode)
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
            sha256=digest, created_at=utc_now(), aggregation_mode=mode)
        self.store.save_shard(record)
        self.store.replace_coverage(shard_id, coverage or [])
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
                # 旧版归档包没有 coverage.json 时保持空覆盖率表。
                self.store.replace_coverage(
                    shard["shard_id"], read_package_coverage(package) or [])
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
