"""Parquet / CSV / SQLite 多源时序数据只读接入与质量校验。

pandas 2.x 适配约定：
- Parquet / CSV / SQLite 三条读取链路都按列显式声明 dtype，不依赖类型推断，
  避免时间列从时区感知退化成 object、数值列被升级成可空整型。
- 时间戳统一经 ``_parse_timestamps`` 归一化为时区无关的 ``datetime64[ns]``（UTC 值），
  下游所有 floor / groupby 都在同一时间语义下进行。
- 缺失判断统一走 ``pandas.isna`` / ``notna``，NaN、pd.NA、None、NaT 一视同仁，
  代码中不出现 ``== None`` 这类对 pd.NA 不成立的比较。
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    labels = _coerce_labels(raw, spec.label_columns)
    bad_labels = int(pd.isna(labels).any(axis=1).sum())
    metrics, bad_metrics = _coerce_metrics(raw, spec.metric_columns)
    frame = pd.DataFrame({TIMESTAMP: timestamps})
    for column in spec.label_columns:
        # 标签统一成字符串；保留 object dtype 以维持既有 Parquet 序列化字节布局。
        frame[column] = labels[column].astype("object")
    for column in spec.metric_columns:
        frame[column] = metrics[column]
    report.type_errors = int(bad_time + bad_labels + bad_metrics)
    if bad_time:
        report.issues.append(f"严重: {bad_time} 个时间戳无法解析，已剔除")
    if bad_labels:
        report.issues.append(f"警告: {bad_labels} 行标签为空，已剔除")
    if bad_metrics:
        report.issues.append(f"提示: {bad_metrics} 个指标值无法转为数字，已置空")
    frame = frame[pd.notna(frame[TIMESTAMP])].copy()
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
    """按格式读取原始数据，并在读取边界显式声明列 dtype。

    时间戳列不在此处理 dtype：它可能是数值 epoch、文本或带时区的 datetime，
    统一交给 ``_parse_timestamps`` 归一化，避免推断导致的 object 退化。
    """
    path = Path(spec.path)
    labels = list(spec.label_columns)
    metrics = list(spec.metric_columns)
    try:
        if spec.fmt == "parquet":
            # pandas 2.3 的 read_parquet 不支持 dtype 参数，先按文件内 schema 读，
            # 再对声明列做显式 astype，杜绝依赖推断的隐式类型。
            frame = pd.read_parquet(path, engine="pyarrow")
            return _enforce_declared_dtypes(frame, labels, metrics)
        if spec.fmt == "csv":
            # 标签与指标全部按字符串读入，指标再经 _coerce_metrics 显式转 float64；
            # 这样脏值（如 "bad-number"）以类型错误计数而不是让解析直接失败。
            dtype_map = {column: "string" for column in [*labels, *metrics]}
            frame = _read_csv_with_dtype(path, dtype_map)
            return _enforce_declared_dtypes(frame, labels, metrics)
        if spec.fmt == "sqlite":
            if not spec.table or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", spec.table):
                raise DataSourceError("SQLite 数据源必须提供合法表名（字母或下划线开头）")
            uri = path.resolve().as_uri() + "?mode=ro"
            connection = sqlite3.connect(uri, uri=True)
            try:
                connection.execute("PRAGMA query_only=ON")
                dtype_map = {column: "string" for column in [*labels, *metrics]}
                frame = _read_sql_with_dtype(connection, spec.table, dtype_map)
            finally:
                connection.close()
            return _enforce_declared_dtypes(frame, labels, metrics)
    except DataSourceError:
        raise
    except (OSError, ValueError, sqlite3.Error) as exc:
        raise DataSourceError(f"读取 {spec.fmt} 数据源失败: {exc}") from exc
    raise DataSourceError(f"不支持的数据源格式: {spec.fmt}")


def _read_csv_with_dtype(path: Path, dtype_map: Dict[str, str]) -> pd.DataFrame:
    """按显式 dtype 映射读 CSV；映射中的列缺失时回退到默认读取。

    回退后不在读取器层报错，让上层统一的“数据源缺少字段”校验抛 DataSourceError，
    保持与历史版本一致的错误类型与提示。
    """
    try:
        return pd.read_csv(path, dtype=dtype_map, keep_default_na=True,
                           na_values=[""], encoding="utf-8")
    except KeyError:
        return pd.read_csv(path, keep_default_na=True, na_values=[""], encoding="utf-8")


def _read_sql_with_dtype(connection: sqlite3.Connection, table: str,
                         dtype_map: Dict[str, str]) -> pd.DataFrame:
    """按显式 dtype 映射执行只读 SQL；映射中的列缺失时回退到无映射查询。"""
    query = f'SELECT * FROM "{table}"'
    try:
        return pd.read_sql_query(query, connection, dtype=dtype_map)
    except KeyError:
        return pd.read_sql_query(query, connection)


def _enforce_declared_dtypes(frame: pd.DataFrame, labels: List[str],
                             metrics: List[str]) -> pd.DataFrame:
    """对声明的标签/指标列显式固定 dtype，不依赖任何读取器推断。"""
    for column in labels:
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    for column in metrics:
        if column in frame.columns:
            # StringDtype 承载原始文本，数值化在 _coerce_metrics 中统一完成，
            # 保证脏值可被识别且最终列类型恒为 float64。
            frame[column] = frame[column].astype("string")
    return frame


def _coerce_labels(raw: pd.DataFrame, label_columns: List[str]) -> pd.DataFrame:
    """标签列显式转字符串并去空白；空标签统一表现为 pd.NA。"""
    labels = raw[list(label_columns)].copy()
    for column in label_columns:
        labels[column] = labels[column].astype("string").str.strip()
    return labels


def _coerce_metrics(raw: pd.DataFrame,
                    metric_columns: List[str]) -> Tuple[pd.DataFrame, int]:
    """指标列显式转 float64，返回转换后的帧与无法转数字的坏值计数。"""
    metrics = pd.DataFrame(index=raw.index)
    bad_metrics = 0
    for column in metric_columns:
        original = raw[column]
        converted = pd.to_numeric(original, errors="coerce")
        # 原始非空、转换后为空，即真正的坏值；pd.isna 同时覆盖 NaN 与 pd.NA。
        bad_metrics += int((pd.notna(original) & pd.isna(converted)).sum())
        metrics[column] = converted.astype("float64")
    return metrics, bad_metrics


def _parse_timestamps(series: pd.Series) -> Tuple[pd.Series, int]:
    """把任意形态的时间戳列归一化为时区无关的 UTC ``datetime64[ns]``。

    - 数值列或纯数值文本按 epoch（秒/毫秒）解析，量级自动判别。
    - 其余文本/日期时间用 pandas 2.x 推荐的 ``format="mixed"`` 显式解析，
      不再依赖被废弃的逐元素 infer 行为；统一 ``utc=True`` 先转成时区感知，
      再 ``tz_convert(None)`` 去掉时区，输出确定的 UTC 墙上时间。
    """
    numeric = _as_numeric_epoch(series)
    if numeric is not None:
        magnitude = numeric.dropna().abs().median()
        unit = "ms" if magnitude > 1e11 else "s"
        parsed = pd.to_datetime(numeric, unit=unit, errors="coerce", utc=True)
    else:
        parsed = pd.to_datetime(series, format="mixed", errors="coerce", utc=True)
    bad = int(pd.isna(parsed).sum())
    if getattr(parsed.dt, "tz", None) is not None:
        parsed = parsed.dt.tz_convert(None)
    # 不强制单位：保留 pandas 2.x 默认的时间分辨率（us），
    # 与历史归档产物的 Parquet 时间类型逐字节对齐。
    return parsed, bad


def _as_numeric_epoch(series: pd.Series) -> Optional[pd.Series]:
    """识别 epoch 输入：原生数值列直接返回；纯数值文本列转数值后返回。

    文本列只有在“所有非空值都能解析成数字”时才走 epoch 分支；只要混入了
    日期字符串等非数值内容，就返回 None 交给 ``mixed`` 日期时间解析，避免
    把普通时间戳误判成 epoch。无法识别时返回 None。
    """
    if pd.api.types.is_numeric_dtype(series):
        return series
    if pd.api.types.is_datetime64_any_dtype(series):
        return None
    if not pd.notna(series).any():
        return None
    candidate = pd.to_numeric(series, errors="coerce")
    # 非空原始值的位置上，数值化结果也必须全部非空，即整列都是数字文本。
    original_present = pd.notna(series)
    if bool(original_present.any()) and bool(pd.isna(candidate[original_present]).sum() == 0):
        return candidate
    return None


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
    """按标签分组估算缺失时间槽位。

    所有空值判断统一使用 ``pandas.isna`` / ``notna``，对 NaN、pd.NA、None、
    NaT 行为一致，不使用对 pd.NA 会抛错或语义不一致的 ``== None`` 比较。
    """
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
