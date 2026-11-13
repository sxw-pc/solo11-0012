"""预算驱动的分层保留推演。

设计约束：

- 所有时间判断以数据源自身最大时间戳为锚，不使用机器时钟，保证可复现。
- 收缩边界只落在自然月分片上：以「数据月首日归属哪一层」做整月分配，
  层与层之间首尾相接，无空洞、无重叠。
- 每条候选分片都在内存中真实执行降采样与序列化，得到与正式落盘完全一致的
  字节数；推演与实际写入之间唯一的误差来源是压缩率波动，本工具的压缩参数
  （gzip mtime=0、固定 level、确定性 parquet 写出）使该误差实际为零，
  tolerance 仅作为验收口径保留。
- 搜索过程完全确定性：同一输入、同一预算，重复推演得到同一方案。
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .data_reader import TIMESTAMP, read_source
from .downsample import downsample_frame
from .index_builder import (
    archive_package_bytes, extension_for, serialize_frame, verify_package,
    verify_serialized,
)
from .schema import (
    BudgetPlan, DataSourceError, Policy, PolicyTier, SourceSpec, TierBounds,
    bytes_sha256, canonical_hash,
)

MERGED_PARTITION = "merged"


# ---------------------------------------------------------------------------
# 自然月工具
# ---------------------------------------------------------------------------

def month_str(stamp: pd.Timestamp) -> str:
    return stamp.strftime("%Y-%m")


def parse_month(value: str) -> pd.Timestamp:
    return pd.Timestamp(f"{value}-01")


def add_months(stamp: pd.Timestamp, months: int) -> pd.Timestamp:
    total = stamp.year * 12 + (stamp.month - 1) + months
    return pd.Timestamp(year=total // 12, month=total % 12 + 1, day=1)


def months_in_range(start: str, end: str) -> List[str]:
    """返回闭区间 [start, end] 内的全部自然月（YYYY-MM），含无数据的月份。"""
    current, last = parse_month(start), parse_month(end)
    output: List[str] = []
    while current <= last:
        output.append(month_str(current))
        current = add_months(current, 1)
    return output


def frame_months(frame: pd.DataFrame) -> List[str]:
    if frame.empty:
        return []
    stamps = pd.to_datetime(frame[TIMESTAMP], errors="coerce").dropna()
    if stamps.empty:
        return []
    return sorted(stamps.dt.strftime("%Y-%m").unique().tolist())


def select_month_window(frame: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """按自然月闭区间筛选原始行（YYYY-MM 字典序即时间序）。"""
    if frame.empty or not start or not end:
        return frame.iloc[0:0]
    months = pd.to_datetime(frame[TIMESTAMP], errors="coerce").dt.strftime("%Y-%m")
    return frame[(months >= start) & (months <= end)]


# ---------------------------------------------------------------------------
# 分层窗口与落盘物
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TierWindow:
    tier: PolicyTier
    position: int
    start_month: str
    end_month: str


def month_owner_index(age_days: int, policy: Policy) -> int:
    """数据月首日年龄（天）到层下标的确定性归属。

    层 i（非末层）持有 age_days <= 其保留上界、且大于上一层上界的月；
    末层持有所有更老的月。保留上界严格递增，因此每个月恰好命中一层。
    """
    for position, tier in enumerate(policy.tiers[:-1]):
        if age_days <= tier.retention_days:
            return position
    return len(policy.tiers) - 1


def baseline_windows(policy: Policy, anchor: pd.Timestamp,
                     oldest_month: str, anchor_month: str) -> List[TierWindow]:
    """按「数据月首日归属」计算各层在当前保留周期下的自然月覆盖。

    每个数据月以其首日相对 anchor 的年龄判定属主层；窗口即属主相同的
    连续自然月区间，因此全部数据月恰好覆盖一次，无空洞、无重叠，
    且边界落在自然月分片上。
    """
    owned: Dict[int, List[str]] = {i: [] for i in range(len(policy.tiers))}
    anchor_day = anchor.normalize()
    for month in months_in_range(oldest_month, anchor_month):
        first = parse_month(month)
        age_days = (anchor_day - first.normalize()).days
        owned[month_owner_index(age_days, policy)].append(month)
    windows: List[TierWindow] = []
    for position, tier in enumerate(policy.tiers):
        months = owned[position]
        if months:
            windows.append(TierWindow(tier, position, months[0], months[-1]))
        else:
            windows.append(TierWindow(tier, position, "", ""))
    return windows


def materialize_month(frame: pd.DataFrame, tier: PolicyTier, spec: SourceSpec,
                      interval_seconds: float, month: str) -> Optional[Dict[str, Any]]:
    """对单个自然月执行降采样并序列化，返回字节精确的候选分片描述。

    先按月筛选原始行再聚合，与正式重排执行走同一条代码路径，
    因此推演出的字节数即最终落盘字节数。
    """
    part_raw = select_month_window(frame, month, month)
    if part_raw.empty:
        return None
    aggregated = downsample_frame(
        part_raw, tier, spec.label_columns, spec.metric_columns, interval_seconds)
    if aggregated.empty:
        return None
    return {
        "month": month, "frame": aggregated, "rows": len(aggregated),
        "raw_count": int(aggregated["raw_count"].sum()),
        "missing_count": int(aggregated["missing_count"].sum()),
        "anomaly_count": int(aggregated["anomaly_count"].sum()),
    }


def serialized_cost(part: pd.DataFrame, tier: PolicyTier) -> Tuple[int, bytes, str]:
    """普通层级：返回 (字节数, 序列化字节, 扩展名)。"""
    data = serialize_frame(part, tier.format, tier.compression)
    if not verify_serialized(data, tier.format, len(part)):
        raise ValueError(f"输出完整性校验失败: {tier.name}")
    return len(data), data, extension_for(tier.format, tier.compression)


def _archive_bytes(parts: List[pd.DataFrame], tier: PolicyTier,
                   spec: SourceSpec) -> Dict[str, Any]:
    """合并若干月帧，纯内存构造归档包，返回字节、哈希与索引（同一确定性路径）。"""
    frame = pd.concat(parts, ignore_index=True)
    package, digest, entries = archive_package_bytes(
        frame, tier, spec.label_columns, spec.metric_columns, spec.source_id)
    return {"bytes": package, "sha256": digest, "entries": entries}


def _verify_probe(staging_dir: Path, package: bytes, prefix: str) -> None:
    """落一份临时探针包并完整校验可读性，确保推演产物是合法归档。"""
    target = staging_dir / f".{prefix}_{uuid.uuid4().hex}.tar.gz"
    try:
        target.write_bytes(package)
        if not verify_package(target):
            raise ValueError("归档包完整性校验失败")
    finally:
        target.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# 投影：一次读源，算出全部候选分片的真实字节
# ---------------------------------------------------------------------------

@dataclass
class Projection:
    spec: SourceSpec
    policy: Policy
    frame: pd.DataFrame
    anchor: pd.Timestamp
    anchor_month: str
    data_months: List[str]
    windows: List[TierWindow]
    baseline_owner: Dict[str, int]
    # monthly_cost[i][m]：层 i 持有月 m 时的分片字节数。
    # 月 m 只可能被基线属主及更老的层（下标更大）持有，构成三角网格。
    monthly_cost: List[Dict[str, int]]
    # 对应的完整候选物（帧与统计），执行阶段直接取来落盘，不再重算。
    monthly_parts: List[Dict[str, Dict[str, Any]]]
    # merged_archive_cost[k]：归档层持有最旧 k+1 个数据月时的单包字节数。
    merged_archive_cost: Dict[int, int]
    # merged_archive_packages[k]：同一合并包的可直接落盘字节与索引描述。
    merged_archive_packages: Dict[int, Dict[str, Any]]
    interval_seconds: float
    source_hash: str

    @property
    def tier_count(self) -> int:
        return len(self.policy.tiers)

    @property
    def archive_index(self) -> int:
        return self.tier_count - 1


def build_projection(spec: SourceSpec, policy: Policy,
                     staging_dir: Path) -> Projection:
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    frame, report = read_source(spec, policy.quality)
    if not report.ok:
        raise DataSourceError("；".join(report.issues) or "数据质量校验未通过")
    anchor = pd.to_datetime(frame[TIMESTAMP], errors="coerce").max()
    anchor_month = month_str(anchor)
    data_months = frame_months(frame)
    oldest_month = data_months[0]
    windows = baseline_windows(policy, anchor, oldest_month, anchor_month)

    baseline_owner: Dict[str, int] = {}
    data_month_set = set(data_months)
    for position, window in enumerate(windows):
        if not window.start_month:
            continue
        for month in months_in_range(window.start_month, window.end_month):
            if month in data_month_set:
                baseline_owner[month] = position

    tier_count = len(policy.tiers)
    archive_pos = tier_count - 1
    monthly_cost: List[Dict[str, int]] = [dict() for _ in range(tier_count)]
    monthly_parts: List[Dict[str, Dict[str, Any]]] = [dict() for _ in range(tier_count)]
    for month in data_months:
        owner = baseline_owner[month]
        # 月只能向更老的层下沉：预计算属主层到归档层的全部候选。
        for position in range(owner, tier_count):
            tier = policy.tiers[position]
            candidate = materialize_month(
                frame, tier, spec, report.interval_seconds, month)
            if candidate is None:
                continue
            if position == archive_pos:
                built = _archive_bytes([candidate["frame"]], tier, spec)
                _verify_probe(staging_dir, built["bytes"], "probe")
                candidate["archive_digest"] = built["sha256"]
                candidate["archive_entries"] = built["entries"]
                candidate["package_bytes"] = built["bytes"]
                candidate["extension"] = "tar.gz"
                size = len(built["bytes"])
            else:
                size, payload, extension = serialized_cost(candidate["frame"], tier)
                candidate["payload"] = payload
                candidate["extension"] = extension
            candidate["bytes"] = size
            monthly_cost[position][month] = size
            monthly_parts[position][month] = candidate

    # 预计算归档层在不同持有月数下的合并单包字节数（始终为最旧的 k+1 个月）。
    merged_archive_cost: Dict[int, int] = {}
    merged_archive_packages: Dict[int, Dict[str, Any]] = {}
    archive_tier = policy.archive_tier
    for k in range(len(data_months)):
        window_months = data_months[:k + 1]
        frames = [monthly_parts[archive_pos][m]["frame"] for m in window_months]
        built = _archive_bytes(frames, archive_tier, spec)
        _verify_probe(staging_dir, built["bytes"], f"probe_merge_{k}")
        merged_archive_cost[k] = len(built["bytes"])
        merged_archive_packages[k] = built

    return Projection(
        spec=spec, policy=policy, frame=frame, anchor=anchor,
        anchor_month=anchor_month, data_months=data_months, windows=windows,
        baseline_owner=baseline_owner, monthly_cost=monthly_cost,
        monthly_parts=monthly_parts,
        merged_archive_cost=merged_archive_cost,
        merged_archive_packages=merged_archive_packages,
        interval_seconds=report.interval_seconds, source_hash=report.source_hash)


# ---------------------------------------------------------------------------
# 确定性预算搜索
# ---------------------------------------------------------------------------

def windows_from_limits(data_months: List[str], policy: Policy,
                        limits: List[int]) -> List[Tuple[str, str]]:
    """把「各层保留月上界」翻译成自然月闭区间。

    data_months 由旧到新，热层（下标 0）持有最新的 limits[0] 个月，
    因此从最新端向前逐层分配；层间边界天然首尾相接。
    """
    total = len(data_months)
    windows: List[Tuple[str, str]] = [("", "")] * len(policy.tiers)
    cursor = total
    for position in range(len(policy.tiers)):
        count = min(limits[position], cursor)
        if count <= 0:
            continue
        start_index = cursor - count
        windows[position] = (data_months[start_index], data_months[cursor - 1])
        cursor = start_index
    return windows


def evaluate(projection: Projection, limits: List[int],
             merge_oldest: bool) -> Tuple[int, Dict[str, int]]:
    """计算给定保留上界与合并开关下的总字节与逐层字节。"""
    windows = windows_from_limits(projection.data_months, projection.policy, limits)
    total = 0
    per_tier: Dict[str, int] = {}
    archive_pos = projection.archive_index
    archive_window = windows[archive_pos]
    archive_months: List[str] = []
    for position, (start, end) in enumerate(windows):
        tier = projection.policy.tiers[position]
        if not start:
            per_tier[tier.name] = 0
            continue
        if position == archive_pos and merge_oldest:
            archive_months = [m for m in months_in_range(start, end)
                              if m in projection.monthly_cost[position]]
            continue
        amount = sum(projection.monthly_cost[position].get(m, 0)
                     for m in months_in_range(start, end))
        per_tier[tier.name] = amount
        total += amount
    if merge_oldest and archive_window[0]:
        # 合并包始终封装归档层持有的最旧 N 个月；N 与预计算表的 k+1 对齐，
        # 因为归档层窗口总是从 data_months 的最旧端连续开始。
        merged_bytes = (projection.merged_archive_cost[len(archive_months) - 1]
                        if archive_months else 0)
        per_tier[projection.policy.tiers[archive_pos].name] = merged_bytes
        total += merged_bytes
    return total, per_tier


def baseline_limits(projection: Projection) -> List[int]:
    limits: List[int] = []
    for window in projection.windows:
        if not window.start_month:
            limits.append(0)
        else:
            limits.append(len(months_in_range(window.start_month, window.end_month)))
    return limits


def candidate_moves(projection: Projection, limits: List[int],
                    merge_oldest: bool) -> List[Dict[str, Any]]:
    """生成全部合法的下一步动作；顺序固定，保证平局可复现。

    收缩第 i 层（非归档层）一个月时，该月整月下沉到归档层，因此归档层
    保留上界同步 +1，各层上界之和恒等于数据月总数——月只会下沉，不会被丢弃。
    """
    moves: List[Dict[str, Any]] = []
    last = len(limits) - 1
    for position in range(last):
        if limits[position] > 0:
            shrunk = list(limits)
            shrunk[position] -= 1
            shrunk[last] += 1
            moves.append({"kind": "shrink", "tier_index": position, "limits": shrunk})
    if not merge_oldest and projection.monthly_parts[last]:
        moves.append({"kind": "merge", "tier_index": last, "limits": list(limits)})
    return moves


def plan_budget(spec: SourceSpec, policy: Policy, budget_bytes: int,
                staging_dir: Path, tolerance: float = 0.05) -> BudgetPlan:
    """推演在总预算内的分层收缩方案；搜索过程与结果完全确定性。

    每一步在所有合法动作中选择推演字节最小者；平局先收缩后合并、
    再按层下标（细粒度优先）裁决。任何月都不会被丢弃，只会整月下沉
    到更老的层，或被并入归档单包，因此不存在粗暴截断。
    """
    projection = build_projection(spec, policy, staging_dir)
    limits = baseline_limits(projection)
    merge_oldest = False
    baseline_bytes, _ = evaluate(projection, limits, False)
    moves_log: List[str] = []

    while evaluate(projection, limits, merge_oldest)[0] > budget_bytes:
        candidates = candidate_moves(projection, limits, merge_oldest)
        if not candidates:
            break
        scored = []
        for move in candidates:
            cost, _ = evaluate(
                projection, move["limits"],
                merge_oldest or move["kind"] == "merge")
            scored.append((cost, move))
        _, best_move = min(scored, key=lambda item: (
            item[0], 0 if item[1]["kind"] == "shrink" else 1,
            item[1]["tier_index"]))
        tier_name = policy.tiers[best_move["tier_index"]].name
        if best_move["kind"] == "merge":
            merge_oldest = True
            moves_log.append(f"merge:{tier_name}")
        else:
            limits = best_move["limits"]
            moves_log.append(f"shrink:{tier_name}")

    projected_bytes, per_tier_bytes = evaluate(projection, limits, merge_oldest)
    windows = windows_from_limits(projection.data_months, policy, limits)
    bounds: List[TierBounds] = []
    for position, (start, end) in enumerate(windows):
        months = len(months_in_range(start, end)) if start else 0
        bounds.append(TierBounds(
            tier=policy.tiers[position].name, start_month=start, end_month=end,
            months=months, retention_days=policy.tiers[position].retention_days))
    feasible = projected_bytes <= budget_bytes
    identity = canonical_hash({
        "spec": spec.to_dict(), "source_hash": projection.source_hash,
        "policy_hash": policy.hash, "budget_bytes": int(budget_bytes),
        "tolerance": round(float(tolerance), 6),
    })
    return BudgetPlan(
        plan_id="plan-" + identity[:24], source=spec.to_dict(),
        source_hash=projection.source_hash, policy_name=policy.name,
        policy_hash=policy.hash,
        data_max=pd.Timestamp(projection.anchor).strftime("%Y-%m-%dT%H:%M:%SZ"),
        anchor_month=projection.anchor_month, budget_bytes=int(budget_bytes),
        tolerance=round(float(tolerance), 6),
        data_months=tuple(projection.data_months), bounds=tuple(bounds),
        tier_limits=tuple(limits), merge_oldest=merge_oldest, feasible=feasible,
        within_budget=feasible, projected_bytes=projected_bytes,
        baseline_bytes=baseline_bytes,
        per_tier_bytes=per_tier_bytes, moves=tuple(moves_log))


def plan_text(plan: BudgetPlan) -> str:
    """方案的规范 JSON 文本：键排序、无时间戳字段，逐字节可复现。"""
    return json.dumps(plan.to_dict(), ensure_ascii=False, sort_keys=True,
                      indent=2) + "\n"


def plan_file_hash(plan: BudgetPlan) -> str:
    return bytes_sha256(plan_text(plan).encode("utf-8"))


def validate_contiguity(plan: BudgetPlan) -> None:
    """校验方案的自然月覆盖首尾相接、无空洞、无重叠、月数与数据一致。

    层按从新到旧排列，因此不能直接按层顺序拼接；先收集各层区间再按
    月份排序，与由旧到新的 data_months 整体比较。
    """
    covered: List[str] = []
    for bound in plan.bounds:
        if not bound.start_month:
            if bound.months != 0:
                raise ValueError(f"层 {bound.tier} 空区间月数非零")
            continue
        months = months_in_range(bound.start_month, bound.end_month)
        if len(months) != bound.months:
            raise ValueError(f"层 {bound.tier} 月数与区间不一致")
        covered.extend(months)
    if sorted(covered) != list(plan.data_months):
        raise ValueError("分层覆盖与数据月不一致：存在空洞或重叠")
    if len(covered) != len(set(covered)):
        raise ValueError("分层覆盖存在重叠月份")
    if sum(plan.tier_limits) != len(plan.data_months):
        raise ValueError("各层保留月上界之和不等于数据月总数")
    # 逐层首尾相接：每一层的最早月必须紧贴更老一层的最晚月之后。
    ordered = [b for b in plan.bounds if b.start_month]
    for newer, older in zip(ordered, ordered[1:]):
        if older.end_month >= newer.start_month:
            raise ValueError(
                f"层 {newer.tier} 与 {older.tier} 的月份区间未首尾相接")


def load_plan(path: Path) -> BudgetPlan:
    """从规范 JSON 读取方案并做结构与连续性校验。"""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    payload = dict(data)
    payload["bounds"] = tuple(TierBounds(**item) for item in data["bounds"])
    payload["data_months"] = tuple(data["data_months"])
    payload["tier_limits"] = tuple(data["tier_limits"])
    payload["moves"] = tuple(data["moves"])
    plan = BudgetPlan(**payload)
    validate_contiguity(plan)
    return plan
