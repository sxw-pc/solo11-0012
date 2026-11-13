"""确定性时间桶降采样：7 种聚合算子、缺失点策略与质量标记。

pandas 2.x 适配约定：
- “标签组合 + 时间桶”分组使用 pandas 推荐的 ``pd.Grouper(freq=...)`` 重采样写法，
  与 ``Series.dt.floor`` 的桶边界完全等价，但不再依赖任何已移除的时区链式调用，
  对时区感知/时区无关时间戳都由 Grouper 统一对齐。
- 四个质量计数列（raw/valid/missing/anomaly_count）在每条产出路径上都强制为
  普通 ``int64``，缺失槽位写 0，不允许被升级成带 pd.NA 的可空整型，保证下游
  内容指纹与历史归档逐字节一致。
"""
from __future__ import annotations

from typing import Iterable, List

import pandas as pd

from .schema import AGGREGATORS, ConfigError, PolicyTier
from .data_reader import ANOMALY, INTERVAL, TIMESTAMP

QUALITY_COLUMNS = ["raw_count", "valid_count", "missing_count", "anomaly_count"]
RESERVED = set(QUALITY_COLUMNS) | {TIMESTAMP, ANOMALY, INTERVAL}


def downsample_frame(frame: pd.DataFrame, tier: PolicyTier,
                     label_columns: Iterable[str], metric_columns: Iterable[str],
                     source_interval: float = 0.0) -> pd.DataFrame:
    """按层级粒度聚合，返回稳定排序、带质量标记的 DataFrame。"""
    labels: List[str] = [str(c) for c in label_columns]
    metrics: List[str] = [str(c) for c in metric_columns]
    for metric in metrics:
        if metric in RESERVED:
            raise ConfigError(f"指标字段名与保留字段冲突: {metric}")
        if tier.aggregation_for(metric) not in AGGREGATORS:
            raise ConfigError(f"指标 {metric} 的聚合算子非法: {tier.aggregation_for(metric)}")
    columns = [TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    missing_columns = [c for c in [TIMESTAMP, *labels, *metrics] if c not in frame.columns]
    if missing_columns:
        raise ConfigError(f"降采样输入缺少字段: {', '.join(missing_columns)}")
    working = frame.copy()
    working[TIMESTAMP] = pd.to_datetime(working[TIMESTAMP], errors="coerce")
    working = working[pd.notna(working[TIMESTAMP])].copy()
    if working.empty:
        return pd.DataFrame(columns=columns)
    width = pd.Timedelta(seconds=tier.granularity_seconds)
    if not source_interval:
        source_interval = float(working[INTERVAL].iloc[0]) if INTERVAL in working else 0.0
    expected = _expected_points(tier.granularity_seconds, float(source_interval))

    # 新版推荐写法：Grouper(freq=) 在 groupby 内直接完成时间桶 floor 对齐，
    # 标签列与时间桶作为同一个分组键，排序语义与旧的 floor 列分组一致。
    group_keys = [*labels, pd.Grouper(key=TIMESTAMP, freq=width)]
    grouped = working.groupby(group_keys, sort=True, observed=True, dropna=False)

    base = grouped.size().rename("raw_count").reset_index()
    if ANOMALY in working.columns:
        anomaly = (grouped[ANOMALY].sum().rename("anomaly_count").reset_index())
        base = base.merge(anomaly, on=[*labels, TIMESTAMP], how="left")
    else:
        base["anomaly_count"] = 0
    working["__valid"] = working[metrics].notna().any(axis=1).astype("int64")
    valid = grouped["__valid"].sum().rename("valid_count").reset_index()
    base = base.merge(valid, on=[*labels, TIMESTAMP], how="left")

    for metric in metrics:
        operator = tier.aggregation_for(metric)
        series = grouped[metric].agg(_aggregator(operator))
        aggregated = series.rename(metric).reset_index()
        base = base.merge(aggregated, on=[*labels, TIMESTAMP], how="left")

    base = _force_count_int64(base)
    base["missing_count"] = (expected - base["raw_count"]).clip(lower=0).astype("int64")
    base = base[[TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]]
    base = base.sort_values([*labels, TIMESTAMP], kind="mergesort").reset_index(drop=True)
    if tier.gap_strategy != "skip":
        base = _fill_gaps(base, labels, metrics, tier.gap_strategy, expected,
                          tier.granularity_seconds)
    return _stable_sort(_force_count_int64(base), labels)


def _force_count_int64(frame: pd.DataFrame) -> pd.DataFrame:
    """把四个质量计数列统一收敛成普通 int64，空值写 0。

    pandas 新版可能在 merge/reindex 后把整数列升级成带 pd.NA 的可空整型；
    这里先 fillna(0) 再 astype("int64")，彻底消除 pd.NA，稳定 Parquet 指纹。
    """
    for column in QUALITY_COLUMNS:
        if column in frame.columns:
            frame[column] = frame[column].fillna(0).astype("int64")
    return frame


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
    return _force_count_int64(result)[[TIMESTAMP, *labels, *metrics, *QUALITY_COLUMNS]]


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
