"""可恢复的并发批量恢复：成员规划、恢复单元、独立 staging 与确定性落盘。

设计约束（见 ARCHITECTURE.md「并发恢复与断点续跑」）：

* 单元划分只依赖筛选条件与索引内容，与并行度无关，因此并行度变化不会改变
  输出文件集合，也不会改变恢复清单的字节内容。
* 成员一律按包内 tar 偏移定点读取（gzip 流 seek），不解压整个 tar.gz。
* 每个单元使用 ``staging/restore/<task_id>/unit_<seq>/`` 独立目录，残留按
  task_id + 单元序号隔离，重跑时先做哈希复核再复用，绝不静默删除。
* 已完成单元重跑时只复核输出哈希即跳过，不重复读取归档包。
* 单元全部结束后按 (min_ts, member_name, ...) 主序生成恢复清单，而不是
  按线程完成时间流式拼接。
"""
from __future__ import annotations

import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .index_builder import (
    atomic_write, member_hash_from_manifest, read_member_at_offset,
    verify_member_bytes,
)
from .metadata_store import MetadataStore
from .schema import ArchiveError, bytes_sha256, canonical_hash, safe_name

# 单个恢复单元的成员数与负载体积上限；只影响切分粒度，不影响输出确定性。
MAX_MEMBERS_PER_UNIT = 32
MAX_UNIT_BYTES = 256 * 1024 * 1024
MANIFEST_NAME = "restore_manifest.json"


@dataclass(frozen=True)
class RestoreMember:
    """一个命中成员的完整定位信息。"""

    shard_id: str
    package_path: str
    member_name: str
    offset: int
    size: int
    row_count: int
    min_ts: str
    max_ts: str
    member_sha256: str
    package_sha256: str
    tier: str
    source: str

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "RestoreMember":
        return cls(
            shard_id=str(row["shard_id"]), package_path=str(row["package_path"]),
            member_name=str(row["member_name"]), offset=int(row["offset"]),
            size=int(row["size"]), row_count=int(row["row_count"]),
            min_ts=str(row.get("min_ts", "")), max_ts=str(row.get("max_ts", "")),
            member_sha256=str(row.get("member_sha256") or ""),
            package_sha256=str(row.get("package_sha256") or ""),
            tier=str(row.get("tier", "")), source=str(row.get("source", "")))

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


def parse_labels(values: Optional[Sequence[str]]) -> List[Tuple[str, str]]:
    """解析 ``key=value`` 形式的重复标签参数。"""
    labels: List[Tuple[str, str]] = []
    for raw in values or []:
        text = str(raw).strip()
        if not text:
            continue
        if "=" not in text:
            raise ArchiveError(f"标签条件格式非法: {text!r}，应为 key=value")
        key, value = text.split("=", 1)
        key, value = key.strip(), value.strip()
        if not key or not value:
            raise ArchiveError(f"标签条件格式非法: {text!r}，键与值均不能为空")
        labels.append((key, value))
    return labels


def build_spec(start: str, end: str,
               labels: Sequence[Tuple[str, str]]) -> Dict[str, Any]:
    """规范化筛选条件；task_id 与输出清单都基于这份确定性结构。"""
    return {
        "start": str(start or ""), "end": str(end or ""),
        "labels": sorted((str(k), str(v)) for k, v in labels),
    }


def task_id_for_spec(spec: Dict[str, Any]) -> str:
    return "restore-" + canonical_hash(spec)[:24]


def plan_members(workdir: Path, store: MetadataStore, start: str, end: str,
                 labels: Sequence[Tuple[str, str]]) -> List[RestoreMember]:
    """从 archive_index 算出全部命中成员，并用包内 index.json 交叉核对。

    包内清单负责补齐旧版数据库缺失的成员 sha256；同一归档包的清单哈希
    只读取一次并缓存。
    """
    rows = store.search_members(start or None, end or None, labels, limit=None)
    members = [RestoreMember.from_row(row) for row in rows]
    base = Path(workdir).resolve()
    hash_cache: Dict[Tuple[str, str], str] = {}
    for member in members:
        package = (base / member.package_path).resolve()
        if base not in package.parents:
            raise ArchiveError(f"归档包路径越出工作目录: {member.package_path}")
        if not package.is_file():
            raise ArchiveError(f"归档包不存在: {member.package_path}")
        if not member.member_sha256:
            key = (member.package_path, member.member_name)
            if key not in hash_cache:
                hash_cache[key] = member_hash_from_manifest(
                    package, member.member_name)
            object.__setattr__(member, "member_sha256", hash_cache[key])
    return members


def split_units(members: Sequence[RestoreMember]) -> List[List[RestoreMember]]:
    """按归档包分组后做确定性贪心切分。

    同一归档包的成员始终落在同一单元，保证并发时不会有两个线程同时打开
    同一个包；切分依据仅为成员数量与索引登记的负载大小，与并行度无关。
    """
    ordered = sorted(members, key=lambda m: (m.min_ts, m.member_name,
                                             m.package_path, m.shard_id))
    groups: Dict[str, List[RestoreMember]] = {}
    for member in ordered:
        groups.setdefault(member.package_path, []).append(member)
    units: List[List[RestoreMember]] = []
    current: List[RestoreMember] = []
    current_bytes = 0
    for package_path in sorted(groups):
        group = groups[package_path]
        group_bytes = sum(item.size for item in group)
        if current and (len(current) + len(group) > MAX_MEMBERS_PER_UNIT
                        or current_bytes + group_bytes > MAX_UNIT_BYTES):
            units.append(current)
            current, current_bytes = [], 0
        current.extend(group)
        current_bytes += group_bytes
        # 单个包的成员组本身超上限时，整组独占一个单元并立即封存；
        # 绝不在单元边界处切开同一个包，杜绝并发打开同一个归档包。
        if len(current) > MAX_MEMBERS_PER_UNIT:
            units.append(current)
            current, current_bytes = [], 0
    if current:
        units.append(current)
    return units


def output_filenames(units: Sequence[Sequence[RestoreMember]]) -> List[List[str]]:
    """与旧 restore 同名规则一致地生成确定性文件名，重名追加序号。"""
    used: Dict[str, int] = {}
    result: List[List[str]] = []
    for unit in units:
        names = []
        for member in unit:
            suffix = Path(member.member_name).suffix or ".bin"
            base = safe_name(member.shard_id or Path(member.package_path).stem)
            name = f"{base}{suffix}"
            if name in used:
                used[name] += 1
                name = f"{Path(name).stem}__{used[name]}{suffix}"
            else:
                used[name] = 0
            names.append(name)
        result.append(names)
    return result


class RestorePlanner:
    """一次批量恢复的状态机：规划、调度、落盘、审计与续跑。"""

    def __init__(self, workdir: Path, store: MetadataStore, audit,
                 actor: str = "CLI"):
        self.workdir = Path(workdir).resolve()
        self.store = store
        self.audit = audit
        self.actor = actor
        self.restore_root = self.workdir / "staging" / "restore"

    # ---- 规划与持久化 ----

    def prepare(self, spec: Dict[str, Any], output_dir: Path
                ) -> Tuple[str, List[List[RestoreMember]], List[Dict[str, Any]]]:
        members = plan_members(self.workdir, self.store,
                               spec["start"], spec["end"],
                               [tuple(item) for item in spec["labels"]])
        if not members:
            raise ArchiveError("筛选条件没有命中任何归档成员")
        units = split_units(members)
        filenames = output_filenames(units)
        task_id = task_id_for_spec(spec)
        self.store.save_restore_task(
            task_id, spec, str(Path(output_dir)), len(units), len(members))
        previous = {row["unit_seq"]: row
                    for row in self.store.list_restore_units(task_id)}
        self.store.prune_restore_units(task_id, len(units))
        for seq, unit in enumerate(units):
            hashes = {member.member_name: member.member_sha256 for member in unit}
            staging_dir = self._staging_dir(task_id, seq)
            staging_dir.mkdir(parents=True, exist_ok=True)
            if seq in previous:
                status = previous[seq]["status"]
                error = previous[seq]["error"]
            else:
                status, error = "pending", ""
            self.store.save_restore_unit(
                task_id, seq, status, unit[0].package_path, len(unit),
                hashes, filenames[seq], str(staging_dir), error=error)
        return task_id, units, self.store.list_restore_units(task_id)

    # ---- 单元执行（工作线程内只做只读与 staging 写入）----

    def run_unit(self, task_id: str, seq: int,
                 unit: Sequence[RestoreMember],
                 row: Dict[str, Any]) -> Dict[str, Any]:
        staging_dir = Path(row["staging_dir"])
        staging_dir.mkdir(parents=True, exist_ok=True)
        for member, filename in zip(unit, row["output_files"]):
            staging_file = staging_dir / filename
            digest = self._reuse_or_extract(member, staging_file)
            if member.member_sha256 and digest != member.member_sha256:
                raise ArchiveError(
                    f"staging 成员哈希不一致: {member.package_path}!"
                    f"{member.member_name}")
        return {"task_id": task_id, "unit_seq": seq,
                "outputs": list(row["output_files"]),
                "staging_dir": str(staging_dir)}

    def _reuse_or_extract(self, member: RestoreMember,
                          staging_file: Path) -> str:
        """staging 残留先哈希复核后复用；否则按偏移重新读取并原子写入。"""
        if staging_file.is_file():
            digest = bytes_sha256(staging_file.read_bytes())
            if not member.member_sha256 or digest == member.member_sha256:
                return digest
        package = self.workdir / member.package_path
        data = read_member_at_offset(package, member.offset, member.size)
        verify_member_bytes(package, member.to_dict(), data)
        atomic_write(staging_file, data)
        return bytes_sha256(data)

    # ---- 主序线程：落盘与状态 ----

    def publish_unit(self, task_id: str, seq: int,
                     unit: Sequence[RestoreMember], row: Dict[str, Any],
                     output_dir: Path) -> str:
        """返回 'completed'（复核通过跳过）或 'published'（本次新落盘）。"""
        if row["status"] == "completed":
            if self._verify_published(unit, row, output_dir):
                return "completed"
            # 输出文件被外部删除：退回 staging 重建，staging 不在则重读包。
        result = self.run_unit(task_id, seq, unit, row)
        target_dir = Path(output_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        for filename in result["outputs"]:
            self._atomic_move(Path(result["staging_dir"]) / filename,
                              target_dir / filename)
        self.store.save_restore_unit(
            task_id, seq, "completed", unit[0].package_path, len(unit),
            {m.member_name: m.member_sha256 for m in unit},
            list(result["outputs"]), result["staging_dir"])
        return "published"

    def _verify_published(self, unit: Sequence[RestoreMember],
                          row: Dict[str, Any], output_dir: Path) -> bool:
        for member, filename in zip(unit, row["output_files"]):
            target = Path(output_dir) / filename
            if not target.is_file():
                return False
            digest = bytes_sha256(target.read_bytes())
            if member.member_sha256 and digest != member.member_sha256:
                raise ArchiveError(f"已落盘成员哈希复核失败: {filename}")
        return True

    @staticmethod
    def _atomic_move(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(source, target)
        except OSError:
            # 跨文件系统时 os.replace 失败，退回到同目录临时文件原子替换。
            atomic_write(target, source.read_bytes())
            source.unlink(missing_ok=True)

    # ---- 编排 ----

    def run(self, spec: Dict[str, Any], output_dir: Path, jobs: int = 1,
            actor: str = "CLI") -> Dict[str, Any]:
        jobs = max(1, int(jobs))
        self.actor = actor
        output_dir = Path(output_dir)
        task_id, units, rows = self.prepare(spec, output_dir)
        row_by_seq = {row["unit_seq"]: row for row in rows}
        failures: List[Dict[str, str]] = []
        skipped, executed = 0, 0
        try:
            if jobs == 1:
                for seq, unit in enumerate(units):
                    try:
                        state = self.publish_unit(
                            task_id, seq, unit, row_by_seq[seq], output_dir)
                        skipped += state == "completed"
                        executed += state != "completed"
                    except Exception as exc:  # 单单元失败不影响其它单元
                        failures.append(self._record_failure(task_id, seq, unit, exc))
            else:
                futures = {}
                with ThreadPoolExecutor(max_workers=jobs) as pool:
                    for seq, unit in enumerate(units):
                        row = row_by_seq[seq]
                        if row["status"] == "completed":
                            try:
                                if self._verify_published(unit, row, output_dir):
                                    skipped += 1
                                    continue
                            except Exception as exc:
                                failures.append(
                                    self._record_failure(task_id, seq, unit, exc))
                                continue
                        futures[pool.submit(self.run_unit, task_id, seq,
                                            unit, row)] = (seq, unit)
                    for future in as_completed(futures):
                        seq, unit = futures[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            failures.append(
                                self._record_failure(task_id, seq, unit, exc))
                            continue
                        # 落盘与 SQLite/审计写入只在主序线程发生。
                        for filename in result["outputs"]:
                            self._atomic_move(
                                Path(result["staging_dir"]) / filename,
                                output_dir / filename)
                        self.store.save_restore_unit(
                            task_id, seq, "completed", unit[0].package_path,
                            len(unit),
                            {m.member_name: m.member_sha256 for m in unit},
                            list(result["outputs"]), result["staging_dir"])
                        executed += 1
            status = "failed" if failures else "completed"
            self.store.update_restore_task(task_id, status,
                                           "; ".join(f"{i['unit_seq']}: {i['error']}"
                                                     for i in failures))
            manifest_path = self.write_manifest(spec, units, row_by_seq,
                                                output_dir, status)
        except KeyboardInterrupt:
            self.store.update_restore_task(task_id, "interrupted", "用户中断")
            raise
        result = {
            "task_id": task_id, "jobs": jobs,
            "total_units": len(units),
            "total_members": sum(len(unit) for unit in units),
            "executed_units": executed, "skipped_units": skipped,
            "failed_units": len(failures), "failures": failures,
            "output_dir": str(output_dir), "manifest": str(manifest_path),
            "status": "failed" if failures else "completed",
        }
        self.audit.log(
            "restore_batch", actor, task_id=task_id,
            range_start=spec.get("start", ""), range_end=spec.get("end", ""),
            input_hashes=sorted({m.package_sha256 for unit in units for m in unit
                                 if m.package_sha256}),
            output_hashes=self._output_hashes(row_by_seq, output_dir),
            result="failure" if failures else "success",
            error="; ".join(item["error"] for item in failures)[:1000],
            details={"jobs": jobs, "total_units": len(units),
                     "total_members": result["total_members"],
                     "executed_units": executed, "skipped_units": skipped,
                     "failed_units": len(failures),
                     "labels": spec.get("labels", [])})
        return result

    def _record_failure(self, task_id: str, seq: int,
                        unit: Sequence[RestoreMember], exc: Exception) -> Dict[str, str]:
        error = str(exc)[:1000]
        row = next(row for row in self.store.list_restore_units(task_id)
                   if row["unit_seq"] == seq)
        self.store.save_restore_unit(
            task_id, seq, "failed", unit[0].package_path, len(unit),
            {m.member_name: m.member_sha256 for m in unit},
            list(row["output_files"]), row["staging_dir"], error=error)
        self.audit.log(
            "restore_unit_failed", actor=self.actor, task_id=task_id,
            input_hashes=[m.package_sha256 for m in unit if m.package_sha256],
            result="failure", error=error,
            details={"unit_seq": seq, "package": unit[0].package_path,
                     "members": [m.member_name for m in unit]})
        return {"unit_seq": str(seq), "package": unit[0].package_path,
                "error": error}

    # ---- 确定性输出清单 ----

    def write_manifest(self, spec: Dict[str, Any],
                       units: Sequence[Sequence[RestoreMember]],
                       row_by_seq: Dict[int, Dict[str, Any]],
                       output_dir: Path, status: str) -> Path:
        """按 (min_ts, member_name, package_path) 主序写出恢复清单。

        清单只包含由筛选条件与索引决定的内容，不含运行时刻或并行度，
        因此同一筛选条件重复恢复、或更换并行度，清单都逐字节一致。
        """
        # 成员到输出文件名的唯一事实来源是 prepare 时登记的 output_files。
        filename_of = {}
        unit_seq_of = {}
        for seq, unit in enumerate(units):
            for index, member in enumerate(unit):
                filename_of[id(member)] = row_by_seq[seq]["output_files"][index]
                unit_seq_of[id(member)] = seq
        ordered = sorted(
            (member for unit in units for member in unit),
            key=lambda m: (m.min_ts, m.member_name, m.package_path, m.shard_id))
        records = []
        for member in ordered:
            filename = filename_of[id(member)]
            target = Path(output_dir) / filename
            records.append({
                "member_name": member.member_name,
                "output_file": filename,
                "package_path": member.package_path,
                "shard_id": member.shard_id,
                "offset": member.offset, "size": member.size,
                "row_count": member.row_count,
                "min_ts": member.min_ts, "max_ts": member.max_ts,
                "member_sha256": member.member_sha256,
                "output_sha256": (bytes_sha256(target.read_bytes())
                                  if target.is_file() else ""),
                "tier": member.tier, "source": member.source,
                "unit_seq": unit_seq_of[id(member)],
            })
        payload = {
            "task_id": task_id_for_spec(spec), "spec": spec,
            "status": status, "member_count": len(records),
            "members": records,
        }
        path = Path(output_dir) / MANIFEST_NAME
        atomic_write(path, (json.dumps(payload, ensure_ascii=False,
                                       sort_keys=True, indent=2) + "\n")
                     .encode("utf-8"))
        return path

    @staticmethod
    def _output_hashes(row_by_seq: Dict[int, Dict[str, Any]],
                       output_dir: Path) -> List[str]:
        hashes = set()
        for row in row_by_seq.values():
            if row["status"] != "completed":
                continue
            for filename in row["output_files"]:
                target = Path(output_dir) / filename
                if target.is_file():
                    hashes.add(bytes_sha256(target.read_bytes()))
        return sorted(hashes)

    def _staging_dir(self, task_id: str, seq: int) -> Path:
        return self.restore_root / safe_name(task_id) / f"unit_{seq:04d}"


def cleanup_staging(workdir: Path, store: MetadataStore,
                    task_id: str = "", remove_all: bool = False) -> Dict[str, Any]:
    """显式回收 restore staging 残留。

    默认只清理已完成/失败任务的目录；运行中或中断任务的目录保留，除非
    显式给出 task_id 或使用 remove_all。任何清理都必须经本命令显式触发，
    恢复流程本身不会静默删除残留。
    """
    root = Path(workdir) / "staging" / "restore"
    if not root.exists():
        return {"removed": [], "reclaimed_bytes": 0}
    if task_id:
        candidates = [root / safe_name(task_id)]
    else:
        candidates = [path for path in root.iterdir() if path.is_dir()]
    removed: List[str] = []
    reclaimed = 0
    for directory in candidates:
        if not directory.exists():
            continue
        if not remove_all and not task_id:
            task = _find_task(store, directory.name)
            if task and task["status"] in {"running", "interrupted"}:
                continue
        for path in directory.rglob("*"):
            if path.is_file():
                reclaimed += path.stat().st_size
        shutil.rmtree(directory)
        removed.append(directory.name)
    return {"removed": sorted(removed), "reclaimed_bytes": reclaimed}


def _find_task(store: MetadataStore, dirname: str):
    """staging 目录名经过 safe_name，按 restore_tasks 中全部任务反查。"""
    with store._connect() as connection:
        rows = connection.execute(
            "SELECT task_id, status FROM restore_tasks").fetchall()
    for row in rows:
        if safe_name(row["task_id"]) == dirname:
            return {"task_id": row["task_id"], "status": row["status"]}
    return None
