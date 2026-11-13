"""pandas 2.x 行为兼容回归：显式 dtype、时区/空值混合输入与计数列稳定性。

这些用例锁定本次依赖升级后的关键不变量：
- Parquet / CSV / SQLite 读取后列类型由代码显式声明，不依赖推断；
- 时区感知时间列归一化后恒为时区无关的 datetime64（ns/us 均可），不退化成 object；
- NaN 与 pd.NA 的缺失判断统一，不出现 == None 风格比较错误；
- 四个质量计数列恒为普通 int64，空槽位写 0，不升级成带 pd.NA 的可空整型。
"""
from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from time_archiver.data_reader import ANOMALY, TIMESTAMP, read_source
from time_archiver.downsample import QUALITY_COLUMNS, downsample_frame
from time_archiver.schema import PolicyTier, SourceSpec


def assert_naive_datetime(series: pd.Series) -> None:
    """时间列必须是时区无关的 datetime64，且绝不退化成 object。"""
    assert pd.api.types.is_datetime64_ns_dtype(series) or \
        pd.api.types.is_datetime64_any_dtype(series)
    assert not pd.api.types.is_object_dtype(series)
    assert getattr(series.dt, "tz", None) is None


def _spec(path: Path, fmt: str, table: str | None = None) -> SourceSpec:
    return SourceSpec(
        path, fmt=fmt, table=table, timestamp_column="ts",
        label_columns=("sensor_id",), metric_columns=("temperature", "humidity"),
        source_id="mixed")


def _mixed_frame() -> pd.DataFrame:
    # 同时包含带时区的 ISO 字符串、朴素时间、空值、NaN 与 pd.NA。
    return pd.DataFrame({
        "ts": [
            "2024-01-01T00:00:00+00:00",
            "2024-01-01T00:10:00+08:00",
            "2024-01-01 00:20:00",
            "",
            "2024-01-01T00:30:00+00:00",
        ],
        "sensor_id": ["a", "a", None, "b", pd.NA],
        "temperature": ["1.0", "bad", "3.0", "4.0", "5.0"],
        "humidity": [10.0, np.nan, pd.NA, 40.0, 50.0],
    })


def test_mixed_timezone_and_null_csv_keeps_explicit_dtypes(tmp_path):
    path = tmp_path / "mixed.csv"
    _mixed_frame().to_csv(path, index=False)
    frame, report = read_source(_spec(path, "csv"))

    # 时间列归一化为时区无关 datetime64，绝不退化为 object。
    assert_naive_datetime(frame[TIMESTAMP])
    assert not pd.api.types.is_object_dtype(frame[TIMESTAMP])
    # 指标列恒为 float64；标签列是承载字符串的 object 列。
    assert frame["temperature"].dtype == "float64"
    assert frame["humidity"].dtype == "float64"
    # +08:00 的 00:10 归一化成 UTC 前一天 16:10（过滤后按时间排序，显式按值断言）。
    assert pd.Timestamp("2023-12-31 16:10:00") in set(frame[TIMESTAMP].map(pd.Timestamp))
    # 空时间戳行被剔除，空标签行（None / pd.NA）也被剔除。
    # 1 个坏时间戳 + 1 个坏指标值 + 2 个空标签行。
    assert report.type_errors == 4
    assert pd.notna(frame[TIMESTAMP]).all()
    assert not frame["sensor_id"].isna().any()
    # 坏指标值转成 NaN 而不是报错；NaN/pd.NA 统一按缺失处理。
    assert pd.isna(frame["temperature"]).sum() >= 1


def test_parquet_tz_aware_timestamp_is_normalized_not_object(tmp_path):
    frame = pd.DataFrame({
        "ts": pd.date_range("2024-01-01", periods=3, freq="1h", tz="UTC"),
        "sensor_id": pd.Series(["a", "b", "c"], dtype="string"),
        "temperature": [1.0, 2.0, 3.0],
        "humidity": [10.0, 20.0, 30.0],
    })
    path = tmp_path / "tz.parquet"
    frame.to_parquet(path, index=False, engine="pyarrow")
    result, _ = read_source(_spec(path, "parquet"))
    assert_naive_datetime(result[TIMESTAMP])
    assert not pd.api.types.is_object_dtype(result[TIMESTAMP])
    assert result["temperature"].dtype == "float64"
    assert result["humidity"].dtype == "float64"


def test_sqlite_explicit_dtypes_and_missing_values(tmp_path):
    path = tmp_path / "mixed.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE t (ts TEXT, sensor_id TEXT, temperature REAL, humidity REAL)")
        connection.executemany(
            "INSERT INTO t VALUES (?,?,?,?)",
            [("1704067200", "a", 1.0, 10.0),
             ("1704067260", None, None, np.nan),
             ("1704067800", "b", 3.0, 30.0)])
        connection.commit()
    finally:
        connection.close()
    frame, report = read_source(_spec(path, "sqlite", table="t"))
    assert_naive_datetime(frame[TIMESTAMP])
    assert frame["temperature"].dtype == "float64"
    assert frame["humidity"].dtype == "float64"
    # 纯数值文本列按 epoch 秒解析（1704067800 == 2024-01-01 00:10:00 UTC）。
    assert pd.Timestamp(frame[TIMESTAMP].iloc[-1]) == pd.Timestamp("2024-01-01 00:10:00")
    assert report.type_errors == 1  # 空标签行


def _downsample_input() -> pd.DataFrame:
    return pd.DataFrame({
        TIMESTAMP: pd.to_datetime([
            "2024-01-01 00:00:00", "2024-01-01 00:05:00",
            "2024-01-01 00:20:00", "2024-01-01 00:25:00"]),
        "sensor_id": ["a", "a", "b", "b"],
        "temperature": [1.0, 2.0, np.nan, 4.0],
        "humidity": [10.0, 20.0, 30.0, pd.NA],
        ANOMALY: [False, False, False, True],
    })


def _tier(gap: str = "null", granularity: str = "1h") -> PolicyTier:
    return PolicyTier("t", granularity, 30, "tiers/t", format="parquet",
                      aggregations={"default": "mean"}, gap_strategy=gap)


def _gap_input() -> pd.DataFrame:
    # 标签 a 在 00:00 与 00:20 两个 10 分钟桶各有点，中间 00:10 桶天然留空，
    # null 补桶应当补齐该空桶并把计数写 0。
    return pd.DataFrame({
        TIMESTAMP: pd.to_datetime([
            "2024-01-01 00:00:00", "2024-01-01 00:05:00",
            "2024-01-01 00:20:00", "2024-01-01 00:25:00"]),
        "sensor_id": ["a", "a", "a", "a"],
        "temperature": [1.0, 2.0, 3.0, 4.0],
        "humidity": [10.0, 20.0, 30.0, 40.0],
        ANOMALY: [False, False, False, True],
    })


def test_quality_count_columns_are_int64_with_zero_for_gaps():
    # 10 分钟粒度下，标签 a 占据 00:00 与 00:20 桶，中间 00:10 桶由 null 补齐。
    result = downsample_frame(_gap_input(), _tier(granularity="10m"),
                              ["sensor_id"], ["temperature", "humidity"],
                              source_interval=300)
    for column in QUALITY_COLUMNS:
        assert result[column].dtype == np.dtype("int64"), column
        assert result[column].isna().sum() == 0
    # 补出的空桶四个计数写 0 / missing 写期望值，绝不出现 pd.NA。
    gap_rows = result[result["raw_count"] == 0]
    assert len(gap_rows) == 1
    assert pd.Timestamp(gap_rows.iloc[0][TIMESTAMP]) == pd.Timestamp("2024-01-01 00:10:00")
    assert gap_rows.iloc[0]["valid_count"] == 0
    assert gap_rows.iloc[0]["anomaly_count"] == 0
    assert gap_rows.iloc[0]["missing_count"] > 0


def test_grouper_buckets_match_floor_semantics():
    frame = _downsample_input()
    result = downsample_frame(frame, _tier(gap="skip"), ["sensor_id"],
                              ["temperature"], source_interval=300)
    # 1h 桶边界必须落在整点，与旧的 dt.floor("1h") 语义一致。
    assert (result[TIMESTAMP] == result[TIMESTAMP].dt.floor("1h")).all()
    grouped = result.groupby("sensor_id", observed=True)["raw_count"].sum()
    # 每个标签两条原始记录都应进入各自的整点桶。
    assert grouped.to_dict() == {"a": 2, "b": 2}


def test_count_columns_survive_parquet_roundtrip(tmp_path):
    result = downsample_frame(_downsample_input(), _tier(), ["sensor_id"],
                              ["temperature", "humidity"], source_interval=300)
    path = tmp_path / "counts.parquet"
    result.to_parquet(path, index=False, engine="pyarrow")
    back = pd.read_parquet(path)
    for column in QUALITY_COLUMNS:
        assert back[column].dtype == np.dtype("int64"), column
        assert back[column].isna().sum() == 0
