"""共享测试夹具：全部工作目录位于 pytest 临时目录。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from time_archiver.archiver import Archiver
from time_archiver.config_loader import init_workspace, load_policy
from time_archiver.schema import PolicyTier, SourceSpec


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    workspace = tmp_path / "work"
    init_workspace(workspace)
    return workspace


@pytest.fixture
def policy(workdir: Path):
    return load_policy(workdir / "policy.yaml", workdir)


@pytest.fixture
def archiver(workdir: Path, policy) -> Archiver:
    return Archiver(workdir, policy)


@pytest.fixture
def sensor_frame_factory():
    def build(start: str = "2024-01-01", periods: int = 48, freq: str = "1h",
              labels=("sensor-1",), anomalies=0, value: float = 20.0) -> pd.DataFrame:
        stamps = pd.date_range(start, periods=periods, freq=freq)
        rng = np.random.default_rng(42)
        frame = pd.DataFrame({
            "timestamp": stamps,
            "sensor_id": [labels[i % len(labels)] for i in range(periods)],
            "temperature": value + rng.normal(0, .2, periods),
            "humidity": 50 + rng.normal(0, 1, periods),
            "pressure": 1000 + rng.normal(0, 2, periods),
        })
        for index in range(min(anomalies, periods)):
            frame.loc[index, "temperature"] = 999.0
        return frame
    return build


@pytest.fixture
def source_file(tmp_path: Path, sensor_frame_factory):
    def build(frame: pd.DataFrame | None = None, fmt: str = "parquet",
              name: str = "input", table: str = "metrics") -> SourceSpec:
        frame = sensor_frame_factory() if frame is None else frame
        if fmt == "parquet":
            path = tmp_path / f"{name}.parquet"
            frame.to_parquet(path, index=False)
        elif fmt == "csv":
            path = tmp_path / f"{name}.csv"
            frame.to_csv(path, index=False)
        elif fmt == "sqlite":
            path = tmp_path / f"{name}.sqlite"
            connection = sqlite3.connect(path)
            try:
                frame.to_sql(table, connection, if_exists="replace", index=False)
            finally:
                connection.close()
        else:
            raise ValueError(fmt)
        return SourceSpec(path, fmt=fmt, table=table if fmt == "sqlite" else None,
                          metric_columns=("temperature", "humidity", "pressure"),
                          source_id=name)
    return build


@pytest.fixture
def long_source(tmp_path: Path, sensor_frame_factory) -> SourceSpec:
    frame = sensor_frame_factory(start="2024-01-01", periods=400 * 24, freq="1h",
                                 labels=("sensor-long",), anomalies=8)
    path = tmp_path / "long.parquet"
    frame.to_parquet(path, index=False)
    return SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                      source_id="sensor-long")


@pytest.fixture
def tier() -> PolicyTier:
    return PolicyTier(name="hot", granularity="1m", retention_days=7,
                      path="tiers/hot", format="parquet", compression="zstd",
                      aggregations={"default": "mean", "temperature": "max"},
                      gap_strategy="skip")
