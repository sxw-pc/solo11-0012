"""可复现地生成三组内置 mock 时序 fixture 与清单。"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "fixtures" / "mock_sensor"
SEED = 202501


def build_frame(start: str, end: str, freq: str, sensor: str, seed: int) -> pd.DataFrame:
    timestamps = pd.date_range(start, end, freq=freq)
    rng = np.random.default_rng(seed)
    phase = np.arange(len(timestamps)) / max(1, len(timestamps))
    temperature = 22 + 5 * np.sin(2 * np.pi * phase * 13) + rng.normal(0, 0.5, len(timestamps))
    humidity = 55 + 12 * np.sin(2 * np.pi * phase * 6 + 1.2) + rng.normal(0, 1.1, len(timestamps))
    pressure = 1008 + 8 * np.sin(2 * np.pi * phase * 3 + 0.4) + rng.normal(0, 0.8, len(timestamps))
    frame = pd.DataFrame({
        "timestamp": timestamps, "sensor_id": sensor,
        "metric_group": "environment", "temperature": temperature.round(6),
        "humidity": humidity.round(6), "pressure": pressure.round(6),
    })
    missing_count = int(round(len(frame) * 0.05))
    keep = np.sort(rng.choice(len(frame), size=len(frame) - missing_count, replace=False))
    frame = frame.iloc[keep].reset_index(drop=True)
    anomaly_count = int(round(len(frame) * 0.02))
    anomaly_rows = rng.choice(len(frame), size=anomaly_count, replace=False)
    metric_choices = rng.integers(0, 3, size=anomaly_count)
    spikes = {
        0: lambda: float(rng.choice([-80.0, 180.0])),
        1: lambda: float(rng.choice([-15.0, 135.0])),
        2: lambda: float(rng.choice([500.0, 1500.0])),
    }
    for row, metric_pos in zip(anomaly_rows, metric_choices):
        column = ["temperature", "humidity", "pressure"][int(metric_pos)]
        frame.loc[int(row), column] = round(spikes[int(metric_pos)](), 6)
    return frame


def describe(path: Path, spec: dict) -> dict:
    frame = pd.read_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        **spec, "file": path.name, "rows": int(len(frame)),
        "sha256": digest,
        "span_start": frame["timestamp"].min().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "span_end": frame["timestamp"].max().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "missing_rows_removed": int(round((spec["nominal_rows"] - len(frame)
                                            if "nominal_rows" in spec else 0))),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    specs = [
        {"source_id": "high-freq-edge", "sensor": "edge-gw-01",
         "start": "2025-12-18T00:00:00", "end": "2025-12-31T23:59:50",
         "freq": "10s", "seed": SEED + 1, "cadence": "10s"},
        {"source_id": "mid-freq-server", "sensor": "srv-metric-01",
         "start": "2025-10-01T00:00:00", "end": "2025-12-31T23:59:00",
         "freq": "1min", "seed": SEED + 2, "cadence": "1m"},
        {"source_id": "low-freq-iot", "sensor": "iot-node-01",
         "start": "2024-12-01T00:00:00", "end": "2025-12-31T23:00:00",
         "freq": "1h", "seed": SEED + 3, "cadence": "1h"},
    ]
    files = []
    for item in specs:
        frame = build_frame(item["start"], item["end"], item["freq"],
                            item["sensor"], item["seed"])
        nominal = len(pd.date_range(item["start"], item["end"], freq=item["freq"]))
        target = OUT / f"sensor_{item['source_id'].split('-')[0]}_freq.parquet"
        frame.to_parquet(target, index=False, compression="zstd")
        files.append(describe(target, {**item, "nominal_rows": nominal}))
    manifest = {
        "version": 1, "seed": SEED, "timezone": "UTC",
        "description": "13个月合成环境传感器数据，5%随机缺失、2%阈值异常",
        "span_start": "2024-12-01T00:00:00Z",
        "span_end": "2025-12-31T23:59:50Z",
        "files": files,
    }
    (ROOT / "fixtures" / "demo_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"files": [f["file"] for f in files]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
