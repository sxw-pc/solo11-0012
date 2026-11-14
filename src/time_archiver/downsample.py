"""确定性时间桶降采样：7 种聚合算子、缺失点策略与质量标记。

聚合模式：
- global：整个输入共用一份源采样间隔推断，逐桶分组聚合（默认，历史行为）。
- local：按标签组合切分，每个组合独立推断采样间隔后分别聚合，
  用于高基数/稀疏组合场景；四个质量列的口径与 global 完全一致，
  仅 missing_count 的理论槽位按组合自身节奏计算。
"""
from __future__ import annotations

from typing import Iterable, List

import pandas as pd

from .schema import AGGREGATORS, ConfigError, PolicyTier
from .data_reader import ANOMALY, INTERVAL, TIMESTAMP

QUALITY_COLUMNS = ["raw_count", "valid_count", "missing_count", "anomaly_count"]
RESERVED = set(QUALITY_COLUMNS) | {TIMESTAMP, ANOMALY, INTERVAL}
AGGREGATION_MODES = ("global", "local")


def downsample_frame(frame: pd.DataFrame, tier: PolicyTier,
                     label_columns: Iterable[str], metric_columns: Iterable[str],
                     source_interval: float = 0.0,
                     mode: str = "global") -> pd.DataFrame:
    """按层级粒度聚合，返回稳定排序、带质量标记的 DataFrame。"""
    labels: List[str] = [str(c) for c in label_columns]
    metrics: List[str] = [str(c) for c in metric_columns]
    for metric in metrics:
        if metric in RESERVED:
            raise ConfigError(f"指标字段名与保留字段冲突: {metric}")
        if tier.aggregation_for(metric) not in AGGREGATORS:
            raise ConfigError(f"指标 {metric} 的聚合算子非法: {tier.aggregation_for(metric)}")
    if mode not in AGGREGATION_MODES:
        raise ConfigError(f"未知聚合模式: {mode}，支持 {', '.join(AGGREGATION_MODES)}")
    columns = [TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    if mode == "local" and labels:
        return _downsample_local(frame, tier, labels, metrics, source_interval)
    return _downsample_global(frame, tier, labels, metrics, source_interval)


def _downsample_local(frame: pd.DataFrame, tier: PolicyTier, labels: List[str],
                      metrics: List[str], source_interval: float) -> pd.DataFrame:
    """分片内按标签组合局部聚合：每个组合使用自身节奏推断理论槽位。"""
    parts = []
    for _, group in frame.groupby(labels, sort=True, dropna=False):
        interval = combo_interval(group[TIMESTAMP]) or float(source_interval or 0.0)
        parts.append(_downsample_global(group, tier, labels, metrics, interval))
    if not parts:
        return pd.DataFrame(columns=[TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS])
    combined = pd.concat(parts, ignore_index=True)
    return _stable_sort(combined, labels)


def combo_interval(timestamps) -> float:
    """由单个标签组合的时间戳推断采样间隔（正差值的众数），无法推断返回 0。"""
    values = pd.to_datetime(timestamps, errors="coerce").dropna()
    values = values.sort_values(kind="mergesort")
    if len(values) < 2:
        return 0.0
    diffs = values.diff().dt.total_seconds()
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return 0.0
    return float(diffs.mode().min())


def _downsample_global(frame: pd.DataFrame, tier: PolicyTier, labels: List[str],
                       metrics: List[str], source_interval: float) -> pd.DataFrame:
    """全局分组聚合：整个输入共用一份采样间隔推断。"""
    columns = [TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    missing_columns = [c for c in [TIMESTAMP, *labels, *metrics] if c not in frame.columns]
    if missing_columns:
        raise ConfigError(f"降采样输入缺少字段: {', '.join(missing_columns)}")
    working = frame.copy()
    working[TIMESTAMP] = pd.to_datetime(working[TIMESTAMP], errors="coerce")
    working = working[working[TIMESTAMP].notna()].copy()
    if working.empty:
        return pd.DataFrame(columns=columns)
    width = pd.Timedelta(seconds=tier.granularity_seconds)
    working["__bucket"] = working[TIMESTAMP].dt.floor(width)
    if not source_interval:
        source_interval = float(working[INTERVAL].iloc[0]) if INTERVAL in working else 0.0
    expected = _expected_points(tier.granularity_seconds, float(source_interval))
    keys = [*labels, "__bucket"]
    grouped = working.groupby(keys, sort=True, observed=True, dropna=False)
    base = grouped.size().rename("raw_count").reset_index()
    if ANOMALY in working.columns:
        anomaly = (grouped[ANOMALY].sum().astype("int64")
                   .rename("anomaly_count").reset_index())
        base = base.merge(anomaly, on=keys, how="left")
    else:
        base["anomaly_count"] = 0
    working["__valid"] = working[metrics].notna().any(axis=1).astype("int64")
    valid = grouped["__valid"].sum().rename("valid_count").reset_index()
    base = base.merge(valid, on=keys, how="left")
    base["raw_count"] = base["raw_count"].astype("int64")
    base["valid_count"] = base["valid_count"].astype("int64")
    base["anomaly_count"] = base["anomaly_count"].fillna(0).astype("int64")
    base["missing_count"] = (expected - base["raw_count"]).clip(lower=0).astype("int64")

    for metric in metrics:
        operator = tier.aggregation_for(metric)
        series = grouped[metric].agg(_aggregator(operator))
        aggregated = series.rename(metric).reset_index()
        base = base.merge(aggregated, on=keys, how="left")
    base = base.rename(columns={"__bucket": TIMESTAMP})
    base = base[[TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]]
    base = base.sort_values([*labels, TIMESTAMP], kind="mergesort").reset_index(drop=True)
    if tier.gap_strategy != "skip":
        base = _fill_gaps(base, labels, metrics, tier.gap_strategy, expected,
                          tier.granularity_seconds)
    return _stable_sort(base, labels)


def _expected_points(granularity_seconds: int, source_interval: float) -> int:
    if source_interval <= 0:
        return 1
    return max(1, int(round(granularity_seconds / source_interval)))


def _aggregator(operator: str):
    functions = {
        "mean": "mean", "max": "max", "min": "min", "count": "count",
        "first": "first", "last": "last",
    }
    if operator == "p95":
        return lambda values: values.quantile(0.95)
    return functions[operator]


def _fill_gaps(frame: pd.DataFrame, labels: List[str], metrics: List[str],
               strategy: str, expected: int, step_seconds: int) -> pd.DataFrame:
    """按标签补全首末观测之间的空时间桶。"""
    outputs = []
    for _, group in frame.groupby(labels, sort=True, dropna=False) if labels else [("", frame)]:
        group = group.sort_values(TIMESTAMP, kind="mergesort")
        full_index = pd.date_range(group[TIMESTAMP].min(), group[TIMESTAMP].max(),
                                   freq=pd.Timedelta(seconds=step_seconds))
        expanded = group.set_index(TIMESTAMP).reindex(full_index)
        expanded.index.name = TIMESTAMP
        if labels:
            for label in labels:
                expanded[label] = group[label].iloc[0]
        for metric in metrics:
            if strategy == "ffill":
                expanded[metric] = expanded[metric].ffill()
            else:
                expanded[metric] = expanded[metric].astype("float64")
        expanded["raw_count"] = expanded["raw_count"].fillna(0).astype("int64")
        expanded["valid_count"] = expanded["valid_count"].fillna(0).astype("int64")
        expanded["anomaly_count"] = expanded["anomaly_count"].fillna(0).astype("int64")
        expanded["missing_count"] = expanded["missing_count"].fillna(expected).astype("int64")
        outputs.append(expanded.reset_index())
    result = pd.concat(outputs, ignore_index=True)
    return result[[TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]]


def _stable_sort(frame: pd.DataFrame, labels: List[str]) -> pd.DataFrame:
    return (frame.sort_values([*labels, TIMESTAMP], kind="mergesort")
            .reset_index(drop=True))


def aggregate_values(values: Iterable[float], operator: str) -> float:
    """供测试和外部核对使用的单桶聚合函数。"""
    if operator not in AGGREGATORS:
        raise ConfigError(f"非法聚合算子: {operator}")
    series = pd.Series(list(values), dtype="float64").dropna()
    if series.empty:
        return float("nan")
    if operator == "mean":
        return float(series.mean())
    if operator == "max":
        return float(series.max())
    if operator == "min":
        return float(series.min())
    if operator == "count":
        return float(series.count())
    if operator == "first":
        return float(series.iloc[0])
    if operator == "last":
        return float(series.iloc[-1])
    return float(series.quantile(0.95))
