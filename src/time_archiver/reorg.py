"""预算方案的原子重排执行器。

两阶段提交与崩溃恢复：

1. materializing：按方案把全部新分片经 staging 校验后原子落到正式路径，
   此阶段不触碰任何既有正式分片与索引。
2. committed：单个 SQLite 事务统一切换分片指针与两级索引、停用陈旧分片；
   事务成功后才把陈旧文件移入 quarantine 隔离区并删除。

因此「清理陈旧正式文件和索引项」与「重排」要么整体成功、要么整体失败。
中断后依据 reorg_journals 重跑即可收敛到同一结果：文件内容寻址、
分片标识确定性、方案逐字节一致，重放不会产生第二份输出。
"""
from __future__ import annotations

import os
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .audit import AuditLog
from .budget import (
    MERGED_PARTITION, build_projection, months_in_range, plan_file_hash,
    validate_contiguity,
)
from .index_builder import atomic_write
from .metadata_store import MetadataStore, utc_now
from .schema import (
    ArchiveError, BudgetPlan, IndexEntry, Policy, ShardRecord, SourceSpec,
    bytes_sha256, canonical_hash, file_sha256, safe_name,
)

JOURNAL_MATERIALIZING = "materializing"
JOURNAL_COMMITTED = "committed"
JOURNAL_COMPLETED = "completed"


class Reorganizer:
    """依据持久化预算方案执行分层重排，可安全中断重跑。"""

    def __init__(self, workdir: Path, policy: Policy, store: MetadataStore,
                 audit: AuditLog):
        self.workdir = Path(workdir).resolve()
        self.policy = policy
        self.store = store
        self.audit = audit
        self.staging = self.workdir / "staging"
        self.staging.mkdir(parents=True, exist_ok=True)
        self.quarantine = self.workdir / "quarantine"
        # shard_id -> 合并归档包实际封装的月数，取自方案边界。
        self._merged_counts: Dict[str, int] = {}

    @property
    def _archive_pos(self) -> int:
        return len(self.policy.tiers) - 1

    def apply_plan(self, plan: BudgetPlan, spec: SourceSpec,
                   actor: str = "CLI") -> Dict[str, Any]:
        validate_contiguity(plan)
        if plan.policy_hash != self.policy.hash:
            raise ArchiveError("方案策略哈希与工作目录当前策略不一致，拒绝执行")
        if not plan.within_budget:
            raise ArchiveError("方案未能满足预算，拒绝执行；请放宽预算后重新推演")
        plan_hash = plan_file_hash(plan)
        projection = build_projection(spec, self.policy, self.staging)
        if projection.source_hash != plan.source_hash:
            raise ArchiveError("源文件内容已变化，方案失效；请重新推演")

        suffix = plan.plan_id[len("plan-"):]
        task_id = "reorg-" + suffix
        journal_id = "journal-" + suffix
        prior = self.store.get_journal(journal_id)
        if prior and prior["state"] == JOURNAL_COMPLETED \
                and prior["plan_hash"] == plan_hash:
            return self._idempotent_result(plan, prior, cached=True)

        new_shards, new_entries, stale = self._plan_targets(plan, spec, projection)
        self._guard_referenced_files(new_shards, stale)
        stale_ids = [item["shard_id"] for item in stale]
        journal = {
            "journal_id": journal_id, "plan_id": plan.plan_id, "task_id": task_id,
            "source_id": spec.source_id, "state": JOURNAL_MATERIALIZING,
            "stage": "started", "plan_hash": plan_hash,
            "new_shards": [record.to_dict() for record in new_shards],
            "stale_shards": stale_ids,
            # 提交前固化的陈旧文件路径：upsert 会复用 shard_id 改写指针，
            # 事后无法再从库中找回旧路径，必须在此快照以保证可回收。
            "stale_paths": [item["path"] for item in stale],
            "quarantined": [],
            "created_at": utc_now(),
        }
        self.store.save_journal(journal)
        self.store.start_task(task_id, "reorg", spec.source_id, self.policy.hash,
                              plan.data_months[0], plan.data_months[-1])

        self._materialize(plan, spec, projection, new_shards, journal_id)
        self._commit_and_clean(plan, task_id, journal_id, new_shards,
                               new_entries, stale)

        result = self._result(plan, task_id, new_shards, stale)
        self.store.finish_task(task_id, "completed", result)
        self.audit.log(
            "reorg", actor, task_id=task_id,
            range_start=plan.data_months[0], range_end=plan.data_months[-1],
            input_hashes=[plan.source_hash],
            output_hashes=sorted(record.sha256 for record in new_shards),
            result="success",
            details={"plan_id": plan.plan_id, "budget_bytes": plan.budget_bytes,
                     "projected_bytes": plan.projected_bytes,
                     "merge_oldest": plan.merge_oldest,
                     "deactivated_shards": len(stale_ids),
                     "moves": list(plan.moves)})
        return result

    # ------------------------------------------------------------------
    def _plan_targets(self, plan: BudgetPlan, spec: SourceSpec, projection
                      ) -> Tuple[List[ShardRecord], List[IndexEntry], List[Dict[str, str]]]:
        records: List[ShardRecord] = []
        entries: List[IndexEntry] = []
        for position, bound in enumerate(plan.bounds):
            if not bound.start_month:
                continue
            tier = self.policy.tiers[position]
            months = [m for m in months_in_range(bound.start_month, bound.end_month)
                      if m in projection.monthly_parts[position]]
            if position == self._archive_pos and plan.merge_oldest:
                record, entry = self._merged_target(spec, tier, projection, months)
                records.append(record)
                entries.append(entry)
                continue
            for month in months:
                record, entry = self._monthly_target(spec, tier, projection,
                                                     position, month)
                records.append(record)
                if entry is not None:
                    entries.append(entry)
        active = self.store.active_shards_for_source(spec.source_id)
        # 陈旧判据必须同时覆盖两种情况：
        # 1) 分片的 (层, 月分区) 不再被新方案覆盖（整月下沉到更老的层）；
        # 2) 分区仍被覆盖，但旧的天级年龄截断分片被整自然月版本替换，路径变化。
        # 因此以「当前活跃指针路径是否仍是新方案目标路径」为最终回收判据，
        # 既不会误删新方案仍引用的内容，也不会留下无指针的游离文件。
        target_paths = {record.path for record in records}
        covered = {(record.tier, record.partition) for record in records}
        stale = [
            {"shard_id": row["shard_id"], "path": row["path"]}
            for row in active
            if (row["tier"], row["partition"]) not in covered
            or row["path"] not in target_paths
        ]
        return records, entries, stale

    def _monthly_target(self, spec: SourceSpec, tier, projection, position, month
                        ) -> Tuple[ShardRecord, Optional[IndexEntry]]:
        candidate = projection.monthly_parts[position][month]
        is_archive = position == self._archive_pos
        shard_id = canonical_hash([tier.name, spec.source_id, month])[:24]
        if is_archive:
            digest = candidate["archive_digest"]
            extension = "tar.gz"
        else:
            digest = bytes_sha256(candidate["payload"])
            extension = candidate["extension"]
        filename = (f"{safe_name(spec.source_id)}__{safe_name(tier.name)}__"
                    f"{month}__{digest[:10]}.{extension}")
        relative = (Path(tier.path) / filename).as_posix()
        record = ShardRecord(
            shard_id=shard_id, task_id="", tier=tier.name, source=spec.source_id,
            partition=month, path=relative,
            fmt="tar.gz" if is_archive else extension,
            row_count=candidate["rows"], raw_count=candidate["raw_count"],
            missing_count=candidate["missing_count"],
            anomaly_count=candidate["anomaly_count"], sha256=digest,
            created_at=utc_now())
        entry = None
        if is_archive:
            entry = replace(candidate["archive_entries"][0], shard_id=shard_id,
                            package_path=relative)
        return record, entry

    def _merged_target(self, spec: SourceSpec, tier, projection, months
                       ) -> Tuple[ShardRecord, IndexEntry]:
        shard_id = canonical_hash(
            [tier.name, spec.source_id, MERGED_PARTITION])[:24]
        index = len(months) - 1
        self._merged_counts[shard_id] = len(months)
        built = projection.merged_archive_packages[index]
        filename = (f"{safe_name(spec.source_id)}__{safe_name(tier.name)}__"
                    f"{MERGED_PARTITION}__{built['sha256'][:10]}.tar.gz")
        relative = (Path(tier.path) / filename).as_posix()
        archive_parts = projection.monthly_parts[self._archive_pos]
        record = ShardRecord(
            shard_id=shard_id, task_id="", tier=tier.name, source=spec.source_id,
            partition=MERGED_PARTITION, path=relative, fmt="tar.gz",
            row_count=sum(archive_parts[m]["rows"] for m in months),
            raw_count=sum(archive_parts[m]["raw_count"] for m in months),
            missing_count=sum(archive_parts[m]["missing_count"] for m in months),
            anomaly_count=sum(archive_parts[m]["anomaly_count"] for m in months),
            sha256=built["sha256"], created_at=utc_now())
        entry = replace(built["entries"][0], shard_id=shard_id,
                        package_path=relative)
        return record, entry

    # ------------------------------------------------------------------
    def _guard_referenced_files(self, new_shards: List[ShardRecord],
                                stale: List[Dict[str, str]]) -> None:
        """任何被新方案引用的路径都绝不允许进入清理名单。"""
        keep_paths = {record.path for record in new_shards}
        if len(keep_paths) != len(new_shards):
            raise ArchiveError("安全校验失败：新方案存在重复目标路径")
        for item in stale:
            if item["path"] in keep_paths:
                raise ArchiveError(
                    f"安全校验失败：新方案仍引用 {item['path']}，禁止清理")

    def _materialize(self, plan: BudgetPlan, spec: SourceSpec, projection,
                     new_shards: List[ShardRecord], journal_id: str) -> None:
        for record in new_shards:
            target = self.workdir / record.path
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and file_sha256(target) == record.sha256:
                continue
            payload = self._payload_for(record, projection)
            stage = target.parent / f".stage_{uuid.uuid4().hex}.tmp"
            atomic_write(stage, payload)
            os.replace(stage, target)
            if file_sha256(target) != record.sha256:
                raise ArchiveError(f"落盘哈希与推演不一致: {record.path}")
            # 每次基于最新 journal 更新，避免循环内用旧快照互相覆盖。
            latest = self.store.get_journal(journal_id)
            self.store.save_journal({
                **latest, "stage": f"written:{record.shard_id}",
                "plan_hash": plan_file_hash(plan),
            })

    def _payload_for(self, record: ShardRecord, projection) -> bytes:
        position = next(i for i, tier in enumerate(self.policy.tiers)
                        if tier.name == record.tier)
        if record.fmt == "tar.gz":
            if record.partition == MERGED_PARTITION:
                count = self._merged_counts[record.shard_id]
                return projection.merged_archive_packages[count - 1]["bytes"]
            return projection.monthly_parts[position][record.partition][
                "package_bytes"]
        return projection.monthly_parts[position][record.partition]["payload"]

    def _commit_and_clean(self, plan: BudgetPlan, task_id: str, journal_id: str,
                          new_shards: List[ShardRecord], new_entries: List[IndexEntry],
                          stale: List[Dict[str, str]]) -> None:
        stale_ids = [item["shard_id"] for item in stale]
        stale_paths = [item["path"] for item in stale]
        prior = self.store.get_journal(journal_id)
        if prior and prior["state"] in (JOURNAL_COMMITTED, JOURNAL_COMPLETED):
            self._quarantine_leftovers(journal_id, plan)
            self._mark_completed(journal_id, plan, self.store.get_journal(journal_id))
            return
        for record in new_shards:
            record.task_id = task_id
        self.store.commit_reorg(task_id, new_shards, new_entries, stale_ids)
        self.store.save_journal({
            **prior, "state": JOURNAL_COMMITTED, "stage": "metadata_committed",
            "plan_hash": plan_file_hash(plan), "stale_shards": stale_ids,
            "stale_paths": stale_paths, "quarantined": [],
        })
        self._quarantine_leftovers(journal_id, plan)
        self._mark_completed(journal_id, plan, self.store.get_journal(journal_id))

    def _quarantine_leftovers(self, journal_id: str, plan: BudgetPlan) -> None:
        """按提交前固化的路径快照回收陈旧文件。

        不能在提交后回库查路径：复用 shard_id 的 upsert 已把指针改写到新
        路径，旧路径只存在于快照中。仍被任一活跃指针引用的路径绝不删除。
        """
        journal = self.store.get_journal(journal_id)
        stale_paths = list(journal.get("stale_paths", []))
        done = set(journal["quarantined"])
        active_paths = {row["path"] for row in self.store.list_shards(active_only=True)}
        self.quarantine.mkdir(parents=True, exist_ok=True)
        for relative in stale_paths:
            if relative in done:
                continue
            # 最后一道防线：仍被活跃指针引用的正式文件绝不删除。
            if relative in active_paths:
                raise ArchiveError(f"拒绝清理仍被引用的正式文件: {relative}")
            path = self.workdir / relative
            if path.exists():
                moved = self.quarantine / path.name
                moved_handle = moved
                counter = 1
                while moved_handle.exists():
                    moved_handle = self.quarantine / f"{counter}__{path.name}"
                    counter += 1
                os.replace(path, moved_handle)
                moved_handle.unlink()
            done.add(relative)
            latest = self.store.get_journal(journal_id)
            self.store.save_journal({
                **latest, "state": JOURNAL_COMMITTED,
                "stage": f"quarantined:{relative}",
                "quarantined": sorted(done),
            })

    def _mark_completed(self, journal_id: str, plan: BudgetPlan,
                        journal: Dict[str, Any]) -> None:
        self.store.save_journal({
            **journal, "state": JOURNAL_COMPLETED, "stage": "finished",
        })

    # ------------------------------------------------------------------
    def _result(self, plan: BudgetPlan, task_id: str,
                new_shards: List[ShardRecord],
                stale: List[Dict[str, str]]) -> Dict[str, Any]:
        actual_bytes = self._measure_active_bytes(plan)
        deviation = (abs(actual_bytes - plan.projected_bytes) /
                     plan.projected_bytes if plan.projected_bytes else 0.0)
        return {
            "task_id": task_id, "plan_id": plan.plan_id,
            "source": plan.source["source_id"], "status": "completed",
            "shards_written": len(new_shards),
            "deactivated_shards": len(stale),
            "projected_bytes": plan.projected_bytes,
            "actual_bytes": actual_bytes,
            "byte_deviation": round(deviation, 6),
            "within_tolerance": deviation <= plan.tolerance,
            "merge_oldest": plan.merge_oldest,
            "bounds": [bound.to_dict() for bound in plan.bounds],
            "cached": False,
        }

    def _measure_active_bytes(self, plan: BudgetPlan) -> int:
        total = 0
        for row in self.store.active_shards_for_source(plan.source["source_id"]):
            path = self.workdir / Path(row["path"])
            if path.exists():
                total += path.stat().st_size
        return total

    def _idempotent_result(self, plan: BudgetPlan, journal: Dict[str, Any],
                           cached: bool) -> Dict[str, Any]:
        actual_bytes = self._measure_active_bytes(plan)
        deviation = (abs(actual_bytes - plan.projected_bytes) /
                     plan.projected_bytes if plan.projected_bytes else 0.0)
        return {
            "task_id": journal["task_id"], "plan_id": plan.plan_id,
            "source": plan.source["source_id"], "status": "completed",
            "shards_written": len(journal["new_shards"]),
            "deactivated_shards": len(journal["stale_shards"]),
            "projected_bytes": plan.projected_bytes, "actual_bytes": actual_bytes,
            "byte_deviation": round(deviation, 6),
            "within_tolerance": deviation <= plan.tolerance,
            "merge_oldest": plan.merge_oldest,
            "bounds": [bound.to_dict() for bound in plan.bounds],
            "cached": cached,
        }
