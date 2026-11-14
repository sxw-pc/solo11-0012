"""准入与资源预算：确定性成本预估、预算池与准入计划的纯函数部分。

预估模型只依赖源数据统计（行数、标签组合数、时间跨度）、策略层级参数和
可配置的安全系数，不读取当前时钟；同一份输入在同一工作目录状态下重复
预估会得到逐字节一致的结果。预算判定可选择性地把旧工作目录中同源同层
活跃分片的实际行数作为参照下界，但绝不因此重算已完成的源。
"""
from __future__ import annotations

import math
import sys
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .data_reader import TIMESTAMP
from .schema import ArchiveError, Policy, QualityReport, SourceSpec

ESTIMATE_VERSION = 1
DEFAULT_SAFETY_FACTOR = 1.5
MAX_SAFETY_FACTOR = 10.0
# 输出行固定宽度：datetime64 时间戳 8 字节 + 4 个 int64 质量列。
OUT_ROW_FIXED_BYTES = 8 + 4 * 8


def build_estimate(frame: pd.DataFrame, report: QualityReport, policy: Policy,
                   spec: SourceSpec, safety_factor: float = DEFAULT_SAFETY_FACTOR,
                   history_reference: Optional[Callable[[str], Optional[int]]] = None
                   ) -> Dict[str, Any]:
    """对单个数据源做逐层成本预估，返回可规范化序列化的字典。

    层级年龄窗口与 Archiver._archive_frame 完全一致；每层先递推桶数：
    每桶标签组合数 = floor(选中时间跨度 / 层粒度) + 1。
    - gap_strategy=skip：观测对数上界 = min(组合数 × 桶数, 选中行数)；
    - null / ffill：补桶后输出为全网格 = 组合数 × 桶数。
    历史参照（同源同层活跃分片的实际行数合计）作为下界取大者，
    最后统一乘安全系数。峰值内存按“源帧常驻 + 各层瞬时最大值”估算。
    """
    factor = float(safety_factor)
    row_total = max(1, len(frame))
    frame_bytes = int(frame.memory_usage(index=False, deep=True).sum())
    in_row_bytes = frame_bytes / row_total
    labels = [str(column) for column in spec.label_columns]
    if labels:
        label_bytes = int(frame[labels].memory_usage(
            index=False, deep=True).sum()) / row_total
    else:
        label_bytes = 0.0
    out_row_bytes = OUT_ROW_FIXED_BYTES + label_bytes + 8 * len(spec.metric_columns)

    timestamps = pd.to_datetime(frame[TIMESTAMP])
    anchor = timestamps.max()
    age = anchor - timestamps
    full_combinations = _combo_count(frame, labels)

    tiers: Dict[str, Any] = {}
    total_rows = 0
    total_shards = 0
    peak_transient = 0
    previous_days = 0
    for position, tier in enumerate(policy.tiers):
        mask = _tier_mask(age, policy, position, tier, previous_days)
        previous_days = tier.retention_days
        selected_rows = int(mask.sum())
        if selected_rows == 0:
            tiers[tier.name] = {
                "selected_rows": 0, "label_combinations": 0,
                "buckets_per_combo": 0, "gap_strategy": tier.gap_strategy,
                "history_rows": None, "estimated_rows": 0,
                "estimated_partitions": 0, "estimated_bytes": 0,
            }
            continue
        selected_ts = timestamps[mask]
        span_seconds = (selected_ts.max() - selected_ts.min()).total_seconds()
        buckets = int(span_seconds // tier.granularity_seconds) + 1
        combinations = _combo_count(frame.loc[mask, labels] if labels else frame,
                                    labels)
        partitions = _month_span(selected_ts.min(), selected_ts.max())
        if tier.gap_strategy == "skip":
            base_rows = min(combinations * buckets, selected_rows)
        else:
            base_rows = combinations * buckets
        history_rows: Optional[int] = None
        if history_reference is not None:
            history_rows = int(history_reference(tier.name) or 0)
            base_rows = max(base_rows, history_rows)
        estimated_rows = int(math.ceil(base_rows * factor))
        selected_bytes = 2 * selected_rows * in_row_bytes
        aggregated_bytes = estimated_rows * out_row_bytes
        shard_buffer = 2 * math.ceil(estimated_rows / partitions) * out_row_bytes
        tier_peak = int(math.ceil(selected_bytes + aggregated_bytes + shard_buffer))
        peak_transient = max(peak_transient, tier_peak)
        total_rows += estimated_rows
        total_shards += partitions
        tiers[tier.name] = {
            "selected_rows": selected_rows,
            "label_combinations": combinations,
            "buckets_per_combo": buckets,
            "gap_strategy": tier.gap_strategy,
            "history_rows": history_rows,
            "estimated_rows": estimated_rows,
            "estimated_partitions": partitions,
            "estimated_bytes": tier_peak,
        }
    return {
        "estimate_version": ESTIMATE_VERSION,
        "source": spec.source_id,
        "source_hash": report.source_hash,
        "policy_hash": policy.hash,
        "safety_factor": factor,
        "history_reference": history_reference is not None,
        "source_stats": {
            "rows": int(report.valid_rows),
            "interval_seconds": float(report.interval_seconds),
            "label_combinations": full_combinations,
            "frame_bytes": frame_bytes,
        },
        "tiers": tiers,
        "totals": {
            "estimated_output_rows": total_rows,
            "estimated_shards": total_shards,
            "estimated_peak_bytes": int(frame_bytes + peak_transient),
        },
    }


def estimate_summary(estimate: Dict[str, Any]) -> Dict[str, Any]:
    """预估的紧凑摘要，用于释放记录中与实际值对照校准。"""
    return {
        "estimated_output_rows": estimate["totals"]["estimated_output_rows"],
        "estimated_shards": estimate["totals"]["estimated_shards"],
        "estimated_peak_bytes": estimate["totals"]["estimated_peak_bytes"],
        "tiers": {
            name: {
                "estimated_rows": tier["estimated_rows"],
                "estimated_partitions": tier["estimated_partitions"],
            }
            for name, tier in estimate["tiers"].items()
        },
    }


def plan_admission(items: List[Tuple[Any, int]],
                   total_bytes: int) -> Tuple[List[Any], List[Any], List[Any]]:
    """按预估成本升序的贪心准入计划，返回 (准入顺序, 排队, 拒绝)。

    成本相同按键排序保证确定性；预估超过总预算的源直接拒绝，
    其余按累计占用不超过总预算依次准入，剩下的进入排队队列。
    """
    ordered = sorted(items, key=lambda item: (item[1], item[0]))
    admitted: List[Any] = []
    queued: List[Any] = []
    rejected: List[Any] = []
    held = 0
    for key, estimate_bytes in ordered:
        if estimate_bytes > total_bytes:
            rejected.append(key)
        elif held + estimate_bytes <= total_bytes:
            admitted.append(key)
            held += estimate_bytes
        else:
            queued.append(key)
    return admitted, queued, rejected


class BudgetPool:
    """单进程准入预算池：准入占用、完成或失败释放，可审计快照。"""

    def __init__(self, total_bytes: int):
        self.total_bytes = int(total_bytes)
        self._held: Dict[Any, int] = {}

    @property
    def held_bytes(self) -> int:
        return sum(self._held.values())

    @property
    def free_bytes(self) -> int:
        return self.total_bytes - self.held_bytes

    def acquire(self, key: Any, amount: int) -> None:
        amount = int(amount)
        if amount > self.free_bytes:
            raise ArchiveError(
                f"准入预算不足：需要 {amount} 字节，剩余 {self.free_bytes} 字节")
        self._held[key] = amount

    def release(self, key: Any) -> int:
        """释放占用并返回释放量；重复释放是安全的 no-op。"""
        return self._held.pop(key, 0)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "total_bytes": self.total_bytes,
            "held_bytes": self.held_bytes,
            "free_bytes": self.free_bytes,
            "held_count": len(self._held),
        }


def peak_rss_bytes() -> Optional[int]:
    """进程峰值常驻内存（尽力而为）；平台不支持时返回 None。"""
    try:
        import resource
    except ImportError:
        return None
    try:
        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except (OSError, ValueError):
        return None
    # Linux 报告 KiB，macOS 报告字节。
    return value if sys.platform == "darwin" else value * 1024


def _tier_mask(age: pd.Series, policy: Policy, position: int,
               tier: Any, previous_days: int) -> pd.Series:
    """与 Archiver._archive_frame 相同的层级年龄窗口。"""
    if len(policy.tiers) == 1:
        return pd.Series(True, index=age.index)
    if position == 0:
        return age <= timedelta(days=tier.retention_days)
    if position == len(policy.tiers) - 1:
        return age > timedelta(days=previous_days)
    return (age > timedelta(days=previous_days)) & (
        age <= timedelta(days=tier.retention_days))


def _combo_count(frame: pd.DataFrame, labels: List[str]) -> int:
    if not labels:
        return 1 if len(frame) else 0
    if frame.empty:
        return 0
    return int(frame[labels].drop_duplicates().shape[0])


def _month_span(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month) + 1
