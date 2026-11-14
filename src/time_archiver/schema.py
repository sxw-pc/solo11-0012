"""核心数据模型、异常类型与确定性哈希工具。"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

AGGREGATORS = ("mean", "max", "min", "count", "first", "last", "p95")
SUPPORTED_FORMATS = ("parquet", "csv", "jsonl")
SUPPORTED_INPUT_FORMATS = ("parquet", "csv", "sqlite")
GAP_STRATEGIES = ("skip", "null", "ffill")
ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class TimeArchiverError(Exception):
    """项目基础异常。"""


class ConfigError(TimeArchiverError):
    """配置错误，附带字段级错误列表。"""

    def __init__(self, errors: Iterable[str] | str):
        if isinstance(errors, str):
            errors = [errors]
        self.errors = list(errors)
        super().__init__("；".join(self.errors))


class DataSourceError(TimeArchiverError):
    """数据源读取或质量校验失败。"""


class ArchiveError(TimeArchiverError):
    """归档流程错误。"""


@dataclass(frozen=True)
class SourceSpec:
    """一次数据接入的源描述。"""

    path: Path
    fmt: str = "parquet"
    timestamp_column: str = "timestamp"
    label_columns: Tuple[str, ...] = ("sensor_id",)
    metric_columns: Tuple[str, ...] = ("value",)
    table: Optional[str] = None
    source_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path))
        object.__setattr__(self, "fmt", self.fmt.lower())
        object.__setattr__(self, "label_columns", tuple(self.label_columns))
        object.__setattr__(self, "metric_columns", tuple(self.metric_columns))
        if not self.source_id:
            object.__setattr__(self, "source_id", self.path.stem or "source")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": str(self.path), "fmt": self.fmt,
            "timestamp_column": self.timestamp_column,
            "label_columns": list(self.label_columns),
            "metric_columns": list(self.metric_columns),
            "table": self.table, "source_id": self.source_id,
        }


@dataclass(frozen=True)
class PolicyTier:
    name: str
    granularity: str
    retention_days: int
    path: str
    format: str = "parquet"
    compression: str = "zstd"
    aggregations: Dict[str, str] = field(default_factory=lambda: {"default": "mean"})
    gap_strategy: str = "skip"

    @property
    def granularity_seconds(self) -> int:
        return parse_granularity(self.granularity)

    def aggregation_for(self, metric: str) -> str:
        return self.aggregations.get(metric, self.aggregations.get("default", "mean"))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name, "granularity": self.granularity,
            "retention_days": self.retention_days, "path": self.path,
            "format": self.format, "compression": self.compression,
            "aggregations": dict(sorted(self.aggregations.items())),
            "gap_strategy": self.gap_strategy,
        }


@dataclass(frozen=True)
class Policy:
    name: str
    tiers: Tuple[PolicyTier, ...]
    version: int = 1
    reference: str = "data_max"
    quality: Dict[str, Any] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def hash(self) -> str:
        payload = {
            "version": self.version, "name": self.name,
            "reference": self.reference, "quality": self.quality,
            "tiers": [t.to_dict() for t in self.tiers],
        }
        return canonical_hash(payload)

    @property
    def archive_tier(self) -> PolicyTier:
        return self.tiers[-1]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version, "name": self.name,
            "reference": self.reference, "quality": self.quality,
            "tiers": [t.to_dict() for t in self.tiers],
        }


@dataclass(frozen=True)
class Budget:
    """准入与资源预算配置；独立于策略，不参与策略哈希与任务标识。"""

    max_inflight_bytes: int
    safety_factor: float = 1.5
    history_reference: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_inflight_bytes": self.max_inflight_bytes,
            "safety_factor": self.safety_factor,
            "history_reference": self.history_reference,
        }


@dataclass
class QualityReport:
    source_id: str
    rows: int = 0
    valid_rows: int = 0
    duplicate_timestamps: int = 0
    missing_points: int = 0
    anomalies: int = 0
    type_errors: int = 0
    interval_seconds: float = 0.0
    source_hash: str = ""
    issues: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(str(i).startswith("严重") for i in self.issues) and self.valid_rows > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id, "rows": self.rows,
            "valid_rows": self.valid_rows,
            "duplicate_timestamps": self.duplicate_timestamps,
            "missing_points": self.missing_points, "anomalies": self.anomalies,
            "type_errors": self.type_errors,
            "interval_seconds": round(self.interval_seconds, 6),
            "source_hash": self.source_hash, "issues": list(self.issues),
            "ok": self.ok,
        }


@dataclass
class ShardRecord:
    shard_id: str
    task_id: str
    tier: str
    source: str
    partition: str
    path: str
    fmt: str
    row_count: int
    raw_count: int
    missing_count: int
    anomaly_count: int
    sha256: str
    created_at: str = ""
    active: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class IndexEntry:
    shard_id: str
    package_path: str
    member_name: str
    offset: int
    size: int
    row_count: int
    min_ts: str
    max_ts: str
    tags: Dict[str, list]
    tier: str
    source: str

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


def parse_granularity(value: str) -> int:
    """将 10s / 1m / 1h / 1d 解析为秒，非法值抛出 ConfigError。"""
    match = re.fullmatch(r"\s*([1-9]\d*)\s*([smhd])\s*", str(value).lower())
    if not match:
        raise ConfigError(f"时间粒度格式非法: {value!r}，示例: 10s、1m、1h、1d")
    number, unit = int(match.group(1)), match.group(2)
    return number * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def canonical_json(value: Any) -> str:
    """稳定序列化，用于配置、索引和审计哈希。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def bytes_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_name(value: str) -> str:
    """生成跨平台安全的文件名片段。"""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._")
    return cleaned[:80] or "item"
