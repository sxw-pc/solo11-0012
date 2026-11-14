"""标签组合基数与稀疏度预检：阈值判定、确定性报告与局部聚合决策。

预检在正式归档之前执行，只读取源数据与策略，不写入任务表，
因此不会污染归档任务序号。报告内容完全由输入数据与策略决定，
不含运行时刻、机器路径等不稳定因素，同一输入重复预检得到
逐字节一致的报告文件。
"""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .data_reader import TIMESTAMP
from .index_builder import atomic_write
from .schema import (
    Policy, SourceSpec, bytes_sha256, canonical_hash, canonical_json,
)

REPORT_VERSION = 1


def precheck_id_for(policy: Policy, source_hash: str, spec: SourceSpec) -> str:
    """预检标识：由策略哈希、源内容哈希与源描述共同决定。"""
    return "precheck-" + canonical_hash({
        "policy": policy.hash, "source_hash": source_hash,
        "spec": spec.to_dict(),
    })[:24]


def select_tier_rows(frame: pd.DataFrame, tiers, position: int) -> pd.DataFrame:
    """按数据最大时间戳为基准的层级年龄窗口选择，与归档流程完全一致。"""
    anchor = pd.to_datetime(frame[TIMESTAMP], errors="coerce").max()
    age = anchor - pd.to_datetime(frame[TIMESTAMP])
    if len(tiers) == 1:
        return frame
    if position == 0:
        return frame[age <= timedelta(days=tiers[0].retention_days)]
    if position == len(tiers) - 1:
        return frame[age > timedelta(days=tiers[position - 1].retention_days)]
    return frame[(age > timedelta(days=tiers[position - 1].retention_days)) &
                 (age <= timedelta(days=tiers[position].retention_days))]


def precheck_frame(frame: pd.DataFrame, spec: SourceSpec, policy: Policy,
                   source_hash: str = "") -> Dict[str, Any]:
    """对每个层级按自然月推算组合数、桶数与每组合平均覆盖桶数。

    返回确定性报告字典：组合数超过 max_combinations，或每组合平均覆盖
    桶数低于 min_avg_bucket_coverage（过稀疏）的月份记为超限；任一月份
    超限则该层决策为 local，否则为 global。
    """
    config = policy.precheck
    labels = [str(c) for c in spec.label_columns]
    tiers_report: List[Dict[str, Any]] = []
    decisions: Dict[str, str] = {}
    for position, tier in enumerate(policy.tiers):
        selected = select_tier_rows(frame, policy.tiers, position)
        months = []
        for stats in _month_stats(selected, tier.granularity_seconds, labels):
            reasons = []
            if stats["combinations"] > config.max_combinations:
                reasons.append("combinations")
            if stats["avg_buckets_per_combo"] < config.min_avg_bucket_coverage:
                reasons.append("sparsity")
            months.append({**stats, "exceeded": bool(reasons), "reasons": reasons})
        exceeded = any(month["exceeded"] for month in months)
        mode = "local" if exceeded else "global"
        decisions[tier.name] = mode
        tiers_report.append({
            "tier": tier.name, "mode": mode, "exceeded": exceeded,
            "months": months,
        })
    return {
        "version": REPORT_VERSION,
        "precheck_id": precheck_id_for(policy, source_hash, spec),
        "policy": policy.name,
        "policy_hash": policy.hash,
        "source": spec.source_id,
        "source_hash": source_hash,
        "spec": spec.to_dict(),
        "thresholds": {
            "max_combinations": config.max_combinations,
            "min_avg_bucket_coverage": config.min_avg_bucket_coverage,
        },
        "generated_from": {
            "rows": int(len(frame)),
            "range_start": _iso_bound(frame, "min"),
            "range_end": _iso_bound(frame, "max"),
        },
        "tiers": tiers_report,
        "decisions": decisions,
    }


def combo_coverage(frame: pd.DataFrame, tier, label_columns,
                   metric_columns) -> List[Dict[str, Any]]:
    """由分片原始数据计算每个标签组合的覆盖桶数与有效点数。

    覆盖桶数 = 该组合在分片内出现过的 distinct 时间桶个数；
    有效点数 = 该组合在分片内至少一个指标有效的原始行数。
    两者都可直接对原始数据回算，用于事后核对。
    """
    labels = [str(c) for c in label_columns]
    metrics = [str(c) for c in metric_columns]
    if frame is None or frame.empty:
        return []
    timestamps = pd.to_datetime(frame[TIMESTAMP], errors="coerce")
    work = frame.loc[timestamps.notna()].copy()
    if work.empty:
        return []
    width = pd.Timedelta(seconds=tier.granularity_seconds)
    work["__bucket"] = pd.to_datetime(work[TIMESTAMP]).dt.floor(width)
    if metrics:
        work["__valid"] = work[list(metrics)].notna().any(axis=1).astype("int64")
    else:
        work["__valid"] = 1
    entries: List[Dict[str, Any]] = []
    if labels:
        groups = work.groupby(labels, sort=True, dropna=False)
        for key, part in groups:
            key_tuple = key if isinstance(key, tuple) else (key,)
            label_map = {label: str(value) for label, value in zip(labels, key_tuple)}
            entries.append({
                "labels": label_map,
                "covered_buckets": int(part["__bucket"].nunique()),
                "valid_points": int(part["__valid"].sum()),
            })
    else:
        entries.append({
            "labels": {},
            "covered_buckets": int(work["__bucket"].nunique()),
            "valid_points": int(work["__valid"].sum()),
        })
    entries.sort(key=lambda entry: canonical_json(entry["labels"]))
    return entries


def report_bytes(report: Dict[str, Any]) -> bytes:
    """报告的确定性字节序列：键排序、无运行时刻，重复生成逐字节一致。"""
    return (json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n").encode("utf-8")


def report_path(workdir: Path, precheck_id: str) -> Path:
    return Path(workdir) / "state" / "precheck" / f"{precheck_id}.json"


def persist_report(workdir: Path, report: Dict[str, Any]) -> Tuple[Path, str]:
    """落盘预检报告；内容未变化时不重写，返回路径与内容哈希。"""
    path = report_path(workdir, report["precheck_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    data = report_bytes(report)
    digest = bytes_sha256(data)
    if path.exists() and bytes_sha256(path.read_bytes()) == digest:
        return path, digest
    atomic_write(path, data)
    return path, digest


def run_precheck(workdir: Path, spec: SourceSpec, policy: Policy,
                 frame: pd.DataFrame, source_hash: str) -> Tuple[Dict[str, Any], str, Path]:
    """计算并持久化预检报告，返回 (报告, 报告哈希, 报告路径)。"""
    report = precheck_frame(frame, spec, policy, source_hash)
    path, digest = persist_report(workdir, report)
    return report, digest, path


def exceeded_months(report: Dict[str, Any]) -> Dict[str, List[str]]:
    """提取每层超限月份，供审计详情使用。"""
    return {
        tier["tier"]: [month["month"] for month in tier["months"] if month["exceeded"]]
        for tier in report.get("tiers", []) if tier.get("exceeded")
    }


def _month_stats(frame: pd.DataFrame, granularity_seconds: int,
                 labels: List[str]) -> List[Dict[str, Any]]:
    columns = [TIMESTAMP, *labels] if labels else [TIMESTAMP]
    if frame is None or frame.empty:
        return []
    timestamps = pd.to_datetime(frame[TIMESTAMP], errors="coerce")
    work = frame.loc[timestamps.notna(), columns].copy()
    if work.empty:
        return []
    width = pd.Timedelta(seconds=granularity_seconds)
    work["__bucket"] = pd.to_datetime(work[TIMESTAMP]).dt.floor(width)
    work["__month"] = work["__bucket"].dt.strftime("%Y-%m")
    stats: List[Dict[str, Any]] = []
    for month, part in work.groupby("__month", sort=True):
        buckets = int(part["__bucket"].nunique())
        if labels:
            per_combo = part.groupby(labels, sort=True, dropna=False)["__bucket"].nunique()
        else:
            per_combo = pd.Series([buckets], dtype="int64")
        combinations = int(len(per_combo))
        avg = round(float(per_combo.mean()), 6) if combinations else 0.0
        stats.append({
            "month": str(month),
            "combinations": combinations,
            "buckets": buckets,
            "avg_buckets_per_combo": avg,
        })
    return stats


def _iso_bound(frame: pd.DataFrame, which: str) -> str:
    if frame is None or frame.empty or TIMESTAMP not in frame:
        return ""
    values = pd.to_datetime(frame[TIMESTAMP], errors="coerce").dropna()
    if values.empty:
        return ""
    stamp = pd.Timestamp(values.min() if which == "min" else values.max())
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")
