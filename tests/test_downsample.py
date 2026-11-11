"""7 种聚合算子、质量标记、时间桶与缺失策略测试。"""
from __future__ import annotations

import math

import pandas as pd
import pytest

from time_archiver.data_reader import ANOMALY, TIMESTAMP
from time_archiver.downsample import (
    QUALITY_COLUMNS, aggregate_values, downsample_frame,
)
from time_archiver.schema import ConfigError, PolicyTier


def make_tier(operator: str = "mean", gap: str = "skip", granularity: str = "1m",
              strategy_for: dict | None = None) -> PolicyTier:
    aggregations = {"default": "mean", "temperature": operator}
    aggregations.update(strategy_for or {})
    return PolicyTier("test", granularity, 30, "tiers/test", aggregations=aggregations,
                      gap_strategy=gap)


def make_frame(values=(1.0, 2.0, 3.0), freq: str = "10s") -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=len(values), freq=freq),
        "sensor_id": ["a"] * len(values),
        "temperature": list(values), "humidity": [50.0] * len(values),
        ANOMALY: [False] * len(values),
    })


@pytest.mark.parametrize("operator,expected", [
    ("mean", 2.0), ("max", 3.0), ("min", 1.0), ("count", 3.0),
    ("first", 1.0), ("last", 3.0), ("p95", 2.9),
])
def test_all_aggregation_operators(operator, expected):
    result = downsample_frame(make_frame(), make_tier(operator), ["sensor_id"],
                              ["temperature", "humidity"], source_interval=10)
    assert math.isclose(result.loc[0, "temperature"], expected, abs_tol=1e-9)
    assert result.loc[0, "humidity"] == 50.0
    assert list(result.columns) == [TIMESTAMP, "sensor_id", "temperature",
                                    "humidity", *QUALITY_COLUMNS]


def test_metric_specific_aggregation():
    tier = make_tier(strategy_for={"humidity": "max"})
    tier = PolicyTier("x", "1m", 30, "tiers/x",
                      aggregations={"temperature": "min", "humidity": "max"})
    frame = make_frame((3.0, 1.0, 2.0))
    frame["humidity"] = [10.0, 30.0, 20.0]
    result = downsample_frame(frame, tier, ["sensor_id"],
                              ["temperature", "humidity"], source_interval=10)
    assert result.loc[0, "temperature"] == 1.0
    assert result.loc[0, "humidity"] == 30.0


def test_quality_markers_and_anomaly_exclusion():
    frame = make_frame((1.0, 2.0, float("nan")))
    frame.loc[2, ANOMALY] = True
    result = downsample_frame(frame, make_tier(), ["sensor_id"], ["temperature"],
                              source_interval=10)
    row = result.iloc[0]
    assert row["raw_count"] == 3
    assert row["valid_count"] == 2
    assert row["anomaly_count"] == 1
    assert row["missing_count"] == 3
    assert math.isclose(row["temperature"], 1.5, abs_tol=1e-9)


def test_empty_frame_has_expected_columns():
    result = downsample_frame(pd.DataFrame(), make_tier(), ["sensor_id"], ["temperature"])
    assert result.empty
    assert TIMESTAMP in result.columns and "missing_count" in result.columns


def test_invalid_inputs_raise_config_error():
    with pytest.raises(ConfigError, match="保留字段"):
        downsample_frame(make_frame(), make_tier(), ["sensor_id"], ["raw_count"])
    with pytest.raises(ConfigError, match="缺少字段"):
        downsample_frame(make_frame(), make_tier(), ["sensor_id"], ["absent"])
    bad = PolicyTier("bad", "1m", 30, "tiers/bad", aggregations={"default": "median"})
    with pytest.raises(ConfigError, match="非法"):
        downsample_frame(make_frame(), bad, ["sensor_id"], ["temperature"])


def test_skip_null_and_forward_fill_strategies():
    frame = make_frame((10.0, 30.0), freq="2min")
    skipped = downsample_frame(frame, make_tier(gap="skip"), ["sensor_id"],
                               ["temperature"], source_interval=60)
    filled = downsample_frame(frame, make_tier(gap="null"), ["sensor_id"],
                              ["temperature"], source_interval=60)
    forward = downsample_frame(frame, make_tier(gap="ffill"), ["sensor_id"],
                               ["temperature"], source_interval=60)
    assert len(skipped) == 2
    assert len(filled) == 3 and len(forward) == 3
    middle = filled.iloc[1]
    assert middle["raw_count"] == 0
    assert middle["missing_count"] == 1
    assert math.isnan(middle["temperature"])
    assert forward.iloc[1]["temperature"] == 10.0
    assert forward.iloc[1]["missing_count"] == 1


def test_multiple_labels_are_filled_independently():
    frame = pd.DataFrame({
        "timestamp": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:02"] * 2),
        "sensor_id": ["a", "a", "b", "b"],
        "temperature": [1.0, 2.0, 10.0, 20.0],
        ANOMALY: [False] * 4,
    })
    result = downsample_frame(frame, make_tier(gap="ffill"), ["sensor_id"],
                              ["temperature"], source_interval=60)
    assert len(result) == 6
    assert result.groupby("sensor_id").size().to_dict() == {"a": 3, "b": 3}
    assert result[result["sensor_id"] == "b"].iloc[1]["temperature"] == 10.0


def test_sorting_and_time_bucketing():
    frame = make_frame((3.0, 1.0, 2.0), freq="20s")
    frame = frame.iloc[[2, 0, 1]].reset_index(drop=True)
    result = downsample_frame(frame, make_tier("mean"), ["sensor_id"],
                              ["temperature"], source_interval=20)
    assert result.iloc[0][TIMESTAMP] == pd.Timestamp("2024-01-01 00:00:00")
    assert result.iloc[0]["raw_count"] == 3


@pytest.mark.parametrize("operator,expected", [
    ("mean", 2.0), ("max", 3.0), ("min", 1.0), ("count", 3.0),
    ("first", 1.0), ("last", 3.0), ("p95", 2.9),
])
def test_aggregate_values_all_operators(operator, expected):
    assert aggregate_values([1, 2, 3], operator) == pytest.approx(expected)


def test_aggregate_values_empty_and_errors():
    assert math.isnan(aggregate_values([], "mean"))
    assert aggregate_values([None, 2.0], "count") == 1
    with pytest.raises(ConfigError):
        aggregate_values([1], "median")


def test_no_anomaly_column_and_embedded_interval():
    frame = make_frame((1.0, 2.0, 3.0)).drop(columns=[ANOMALY])
    frame["__source_interval"] = 10
    result = downsample_frame(frame, make_tier(), ["sensor_id"], ["temperature"])
    assert result.iloc[0]["anomaly_count"] == 0
    assert result.iloc[0]["missing_count"] == 3


def test_invalid_timestamps_and_missing_source_interval():
    frame = make_frame((1.0,))
    frame["timestamp"] = "invalid"
    result = downsample_frame(frame, make_tier(), ["sensor_id"], ["temperature"])
    assert result.empty
    valid = downsample_frame(make_frame((1.0,)), make_tier(), ["sensor_id"],
                             ["temperature"], source_interval=0)
    assert valid.iloc[0]["missing_count"] == 0


def test_one_row_and_no_labels():
    frame = make_frame((5.0,))
    result = downsample_frame(frame, make_tier(), [], ["temperature"], 10)
    assert len(result) == 1
    assert result.iloc[0]["temperature"] == 5.0
