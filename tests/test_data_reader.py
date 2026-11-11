"""多格式只读接入、时间解析、异常/重复/缺失统计测试。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from time_archiver.data_reader import (
    ANOMALY, INTERVAL, TIMESTAMP, read_source, validate_sources,
)
from time_archiver.schema import DataSourceError, SourceSpec


def test_read_parquet_quality_and_original_untouched(source_file, tmp_path):
    spec = source_file(name="parquet-source")
    before = spec.path.read_bytes()
    frame, report = read_source(spec, {"value_ranges": {"temperature": {"min": 0, "max": 100}}})
    assert len(frame) == 48
    assert report.valid_rows == 48
    assert report.source_hash
    assert ANOMALY in frame and INTERVAL in frame
    assert frame[TIMESTAMP].is_monotonic_increasing
    assert spec.path.read_bytes() == before
    assert report.ok


def test_anomaly_range_and_missing_count(source_file, sensor_frame_factory):
    frame = sensor_frame_factory(periods=4, freq="1min", anomalies=1)
    frame = frame.drop(index=2).reset_index(drop=True)
    spec = source_file(frame=frame, name="anomaly")
    data, report = read_source(spec, {"value_ranges": {"temperature": {"min": -50, "max": 150}}})
    assert report.anomalies == 1
    assert data.loc[data[ANOMALY], "temperature"].isna().all()
    assert report.missing_points == 1  # 4 个期望分钟桶，删除一个内部桶


def test_duplicate_timestamps_removed(source_file, sensor_frame_factory):
    frame = sensor_frame_factory(periods=5, freq="1min")
    duplicated = pd.concat([frame, frame.iloc[[2]]], ignore_index=True)
    spec = source_file(frame=duplicated, name="dupes")
    data, report = read_source(spec)
    assert report.duplicate_timestamps == 1
    assert len(data) == 5
    assert any("重复时间戳" in issue for issue in report.issues)


def test_csv_input_and_type_errors(tmp_path, sensor_frame_factory):
    frame = sensor_frame_factory(periods=4, freq="1min").astype(object)
    frame.loc[0, "timestamp"] = "not-a-date"
    frame.loc[1, "temperature"] = "bad-number"
    frame.loc[2, "sensor_id"] = None
    path = tmp_path / "dirty.csv"
    frame.to_csv(path, index=False)
    spec = SourceSpec(path, fmt="csv", metric_columns=("temperature", "humidity", "pressure"),
                      source_id="dirty")
    data, report = read_source(spec, {"value_ranges": {}})
    assert report.type_errors >= 3
    assert report.valid_rows == len(data)
    assert report.valid_rows < 4
    assert any("无法解析" in issue for issue in report.issues)
    assert any("无法转为数字" in issue for issue in report.issues)


def test_sqlite_read_only_and_row_count(source_file):
    spec = source_file(fmt="sqlite", name="database")
    before = spec.path.read_bytes()
    data, report = read_source(spec)
    assert len(data) == 48
    assert spec.path.read_bytes() == before
    uri = spec.path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("DELETE FROM metrics")
    finally:
        connection.close()


def test_sqlite_table_validation(tmp_path, sensor_frame_factory):
    frame = sensor_frame_factory(periods=2)
    path = tmp_path / "db.sqlite"
    frame.to_sql("metrics", sqlite3.connect(path))
    with pytest.raises(DataSourceError, match="表名"):
        read_source(SourceSpec(path, fmt="sqlite", table="metrics; DROP TABLE metrics"))
    with pytest.raises(DataSourceError, match="缺少字段"):
        read_source(SourceSpec(path, fmt="sqlite", table="metrics", metric_columns=("nope",)))


def test_missing_source_and_columns(tmp_path, source_file):
    with pytest.raises(DataSourceError, match="不存在"):
        read_source(SourceSpec(tmp_path / "none.parquet"))
    spec = source_file(name="columns")
    with pytest.raises(DataSourceError, match="缺少字段"):
        read_source(SourceSpec(spec.path, metric_columns=("missing_metric",)))
    with pytest.raises(DataSourceError, match="不支持"):
        read_source(SourceSpec(spec.path, fmt="xml"))


def test_empty_file_returns_unusable_report(tmp_path):
    path = tmp_path / "empty.parquet"
    pd.DataFrame(columns=["timestamp", "sensor_id", "value"]).to_parquet(path, index=False)
    frame, report = read_source(SourceSpec(path, metric_columns=("value",)))
    assert frame.empty
    assert not report.ok
    assert any("为空" in issue for issue in report.issues)


def test_numeric_epoch_timestamps(tmp_path):
    frame = pd.DataFrame({
        "ts": [1704067200, 1704067260, 1704067320],
        "sensor_id": ["a", "a", "a"], "value": [1.0, 2.0, 3.0],
    })
    path = tmp_path / "epoch.csv"
    frame.to_csv(path, index=False)
    data, report = read_source(SourceSpec(path, fmt="csv", timestamp_column="ts",
                                          metric_columns=("value",)))
    assert report.interval_seconds == 60
    assert str(data[TIMESTAMP].iloc[0]) == "2024-01-01 00:00:00"
    assert len(data) == 3


def test_multiple_labels_and_single_row(source_file, sensor_frame_factory):
    frame = sensor_frame_factory(periods=1, labels=("a", "b"))
    spec = source_file(frame=frame, name="single")
    _, report = read_source(spec)
    assert report.interval_seconds == 0
    assert report.valid_rows == 1


def test_validate_sources_aggregates_failures(tmp_path, source_file):
    good = source_file(name="good")
    missing = SourceSpec(tmp_path / "missing.parquet", source_id="missing")
    reports = validate_sources([good, missing], {})
    assert len(reports) == 2
    assert reports[0].ok
    assert not reports[1].ok
    assert any("严重" in issue for issue in reports[1].issues)
