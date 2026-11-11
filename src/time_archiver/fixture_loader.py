"""内置 mock 数据清单读取与演示数据源构造。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .config_loader import PROJECT_ROOT
from .schema import DataSourceError, SourceSpec

FIXTURE_DIR = PROJECT_ROOT / "fixtures" / "mock_sensor"
MANIFEST_PATH = PROJECT_ROOT / "fixtures" / "demo_manifest.json"


def fixture_dir() -> Path:
    return FIXTURE_DIR


def load_manifest(path: Path = MANIFEST_PATH) -> Dict[str, Any]:
    path = Path(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DataSourceError(f"演示数据清单不存在: {path}") from exc
    except (OSError, ValueError) as exc:
        raise DataSourceError(f"演示数据清单损坏: {exc}") from exc
    if not isinstance(data.get("files"), list) or not data["files"]:
        raise DataSourceError("演示数据清单缺少 files 列表")
    return data


def demo_sources(manifest_path: Path = MANIFEST_PATH) -> List[SourceSpec]:
    manifest = load_manifest(manifest_path)
    sources = []
    for item in manifest["files"]:
        path = FIXTURE_DIR / item["file"]
        if not path.is_file():
            raise DataSourceError(f"演示数据文件缺失: {path.name}，请重新生成 fixture")
        sources.append(SourceSpec(
            path=path, fmt="parquet", timestamp_column="timestamp",
            label_columns=("sensor_id", "metric_group"),
            metric_columns=("temperature", "humidity", "pressure"),
            source_id=item["source_id"],
        ))
    return sources


def fixture_summary(manifest_path: Path = MANIFEST_PATH) -> Dict[str, Any]:
    manifest = load_manifest(manifest_path)
    files = []
    for item in manifest["files"]:
        path = FIXTURE_DIR / item["file"]
        files.append({
            "file": item["file"], "source_id": item.get("source_id", path.stem),
            "cadence": item.get("cadence", ""), "rows": item.get("rows", 0),
            "missing_ratio": round(
                (item.get("nominal_rows", 0) - item.get("rows", 0)) /
                max(1, item.get("nominal_rows", 1)), 4),
            "span_start": item.get("span_start", ""),
            "span_end": item.get("span_end", ""),
            "exists": path.is_file(),
        })
    return {
        "description": manifest.get("description", ""),
        "span_start": manifest.get("span_start", ""),
        "span_end": manifest.get("span_end", ""),
        "files": files,
    }


def find_demo_source(source_id: str) -> SourceSpec:
    for spec in demo_sources():
        if spec.source_id == source_id:
            return spec
    raise DataSourceError(f"未知演示数据源: {source_id}")
