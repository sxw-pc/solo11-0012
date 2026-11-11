"""Parquet / CSV / SQLite 多源时序数据只读接入与质量校验。"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import pandas as pd

from .schema import DataSourceError, QualityReport, SourceSpec, file_sha256

TIMESTAMP = "timestamp"
ANOMALY = "__anomaly"
INTERVAL = "__source_interval"


def read_source(spec: SourceSpec,
                quality_config: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, QualityReport]:
    """读取一个只读数据源，返回规范化 DataFrame 与质量报告。"""
    quality_config = quality_config or {}
    if spec.fmt not in ("parquet", "csv", "sqlite"):
        raise DataSourceError(f"不支持的数据源格式: {spec.fmt}")
    path = Path(spec.path)
    if not path.is_file():
        raise DataSourceError(f"数据源不存在: {path}")
    report = QualityReport(source_id=spec.source_id)
    try:
        report.source_hash = file_sha256(path)
    except OSError as exc:
        raise DataSourceError(f"无法读取数据源: {exc}") from exc
    raw = _read_raw(spec)
    report.rows = len(raw)
    required = [spec.timestamp_column, *spec.label_columns, *spec.metric_columns]
    missing = [column for column in required if column not in raw.columns]
    if missing:
        raise DataSourceError(f"数据源缺少字段: {', '.join(missing)}")
    if raw.empty:
        report.issues.append("严重: 数据源为空，无法接入")
        return pd.DataFrame(columns=[TIMESTAMP, *spec.label_columns,
                                     *spec.metric_columns, ANOMALY, INTERVAL]), report

    timestamps, bad_time = _parse_timestamps(raw[spec.timestamp_column])
    labels = raw[list(spec.label_columns)].copy()
    labels = labels.apply(lambda col: col.astype("string").str.strip())
    bad_labels = int(labels.isna().any(axis=1).sum())
    metrics = pd.DataFrame(index=raw.index)
    bad_metrics = 0
    for column in spec.metric_columns:
        converted = pd.to_numeric(raw[column], errors="coerce")
        bad_metrics += int((raw[column].notna() & converted.isna()).sum())
        metrics[column] = converted.astype("float64")
    frame = pd.DataFrame({TIMESTAMP: timestamps})
    for column in spec.label_columns:
        frame[column] = labels[column].astype(object)
    for column in spec.metric_columns:
        frame[column] = metrics[column]
    report.type_errors = int(bad_time + bad_labels + bad_metrics)
    if bad_time:
        report.issues.append(f"严重: {bad_time} 个时间戳无法解析，已剔除")
    if bad_labels:
        report.issues.append(f"警告: {bad_labels} 行标签为空，已剔除")
    if bad_metrics:
        report.issues.append(f"提示: {bad_metrics} 个指标值无法转为数字，已置空")
    frame = frame[frame[TIMESTAMP].notna()].copy()
    if spec.label_columns:
        frame = frame.dropna(subset=list(spec.label_columns))

    key = [*spec.label_columns, TIMESTAMP]
    duplicates = int(frame.duplicated(subset=key, keep="first").sum())
    if duplicates:
        frame = frame.drop_duplicates(subset=key, keep="first")
        report.issues.append(f"提示: {duplicates} 个重复时间戳，已保留首条")
    report.duplicate_timestamps = duplicates
    frame = frame.sort_values(key, kind="mergesort").reset_index(drop=True)
    interval = _infer_interval(frame, spec.label_columns)
    report.interval_seconds = interval
    frame[INTERVAL] = interval

    anomalies, masked = _mark_anomalies(frame, spec.metric_columns,
                                        quality_config.get("value_ranges", {}))
    frame[ANOMALY] = anomalies
    for column in spec.metric_columns:
        frame.loc[masked[column], column] = float("nan")
    report.anomalies = int(anomalies.sum())
    report.missing_points = int(_missing_slots(frame, spec.label_columns, interval))
    if report.missing_points:
        report.issues.append(f"提示: 检测到 {report.missing_points} 个缺失时间分片")
    if report.anomalies:
        report.issues.append(f"警告: 检测到 {report.anomalies} 行超阈值异常值，聚合时排除")
    report.valid_rows = len(frame)
    if report.valid_rows == 0:
        report.issues.append("严重: 无有效时间戳记录，无法接入")
    return frame, report


def _read_raw(spec: SourceSpec) -> pd.DataFrame:
    path = Path(spec.path)
    try:
        if spec.fmt == "parquet":
            return pd.read_parquet(path)
        if spec.fmt == "csv":
            return pd.read_csv(path)
        if spec.fmt == "sqlite":
            if not spec.table or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", spec.table):
                raise DataSourceError("SQLite 数据源必须提供合法表名（字母或下划线开头）")
            uri = path.resolve().as_uri() + "?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            try:
                connection.execute("PRAGMA query_only=ON")
                return pd.read_sql_query(f'SELECT * FROM "{spec.table}"', connection)
            finally:
                connection.close()
    except DataSourceError:
        raise
    except (OSError, ValueError, sqlite3.Error) as exc:
        raise DataSourceError(f"读取 {spec.fmt} 数据源失败: {exc}") from exc
    raise DataSourceError(f"不支持的数据源格式: {spec.fmt}")


def _parse_timestamps(series: pd.Series) -> Tuple[pd.Series, int]:
    if pd.api.types.is_numeric_dtype(series) and series.notna().any():
        magnitude = series.dropna().abs().median()
        unit = "ms" if magnitude > 1e11 else "s"
        parsed = pd.to_datetime(series, unit=unit, errors="coerce", utc=True)
    else:
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
    bad = int(parsed.isna().sum())
    if getattr(parsed.dt, "tz", None) is not None:
        parsed = parsed.dt.tz_convert(None)
    return parsed, bad


def _infer_interval(frame: pd.DataFrame, labels) -> float:
    labels = list(labels)
    if len(frame) < 2:
        return 0.0
    ordered = frame.sort_values([*labels, TIMESTAMP], kind="mergesort")
    diffs = ordered.groupby(labels, dropna=False)[TIMESTAMP].diff().dt.total_seconds()
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return 0.0
    return float(diffs.mode().min())


def _missing_slots(frame: pd.DataFrame, labels, interval: float) -> int:
    labels = list(labels)
    if interval <= 0 or frame.empty:
        return 0
    total = 0
    grouped = frame.groupby(labels, dropna=False)[TIMESTAMP] if labels else [frame[TIMESTAMP]]
    for values in grouped:
        ts = values[1] if isinstance(values, tuple) else values
        if ts.empty:
            continue
        span = (ts.max() - ts.min()).total_seconds()
        expected = int(round(span / interval)) + 1
        total += max(0, expected - len(ts))
    return total


def _mark_anomalies(frame: pd.DataFrame, metrics: list, ranges: Dict[str, Any]):
    result = pd.Series(False, index=frame.index, dtype=bool)
    masked = {metric: pd.Series(False, index=frame.index, dtype=bool) for metric in metrics}
    for metric in metrics:
        limits = ranges.get(metric)
        if not isinstance(limits, dict):
            continue
        values = frame[metric]
        current = pd.Series(False, index=frame.index, dtype=bool)
        if limits.get("min") is not None:
            current |= values < float(limits["min"])
        if limits.get("max") is not None:
            current |= values > float(limits["max"])
        masked[metric] = current.fillna(False)
        result |= masked[metric]
    return result, masked


def validate_sources(specs: list[SourceSpec], quality_config: Dict[str, Any]) -> list:
    """批量接入校验报告；单个源失败不阻断其他源。"""
    reports = []
    for spec in specs:
        try:
            _, report = read_source(spec, quality_config)
        except DataSourceError as exc:
            report = QualityReport(source_id=spec.source_id)
            report.issues.append(f"严重: {exc}")
        reports.append(report)
    return reports
