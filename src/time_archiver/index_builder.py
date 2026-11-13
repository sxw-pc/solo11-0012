"""确定性序列化、tar.gz 冷数据封装、两级索引描述与定点恢复。"""
from __future__ import annotations

import gzip
import io
import json
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from .data_reader import TIMESTAMP
from .schema import IndexEntry, PolicyTier, bytes_sha256, safe_name

DATA_MEMBER = "data"
INDEX_MEMBER = "index.json"


def serialize_frame(frame: pd.DataFrame, fmt: str, compression: str = "zstd") -> bytes:
    """将 DataFrame 转为字节，gzip 头固定 mtime=0，保证哈希稳定。"""
    buffer = io.BytesIO()
    if fmt == "parquet":
        kwargs: Dict[str, Any] = {"index": False, "engine": "pyarrow"}
        kwargs["compression"] = None if compression == "none" else compression
        frame.to_parquet(buffer, **kwargs)
        return buffer.getvalue()
    if fmt == "csv":
        frame.to_csv(buffer, index=False, lineterminator="\n",
                     date_format="%Y-%m-%dT%H:%M:%SZ")
        raw = buffer.getvalue()
    elif fmt == "jsonl":
        text = frame.to_json(orient="records", lines=True, date_format="iso",
                             date_unit="s", force_ascii=False)
        raw = text.encode("utf-8")
    else:
        raise ValueError(f"不支持的输出格式: {fmt}")
    if compression == "gzip":
        raw = gzip.compress(raw, compresslevel=6, mtime=0)
    return raw


def deserialize_frame(data: bytes, fmt: str) -> pd.DataFrame:
    if fmt == "parquet":
        return pd.read_parquet(io.BytesIO(data))
    if data[:2] == bytes([31, 139]):
        data = gzip.decompress(data)
    if fmt == "csv":
        return pd.read_csv(io.BytesIO(data))
    if fmt == "jsonl":
        return pd.read_json(io.BytesIO(data), orient="records", lines=True)
    raise ValueError(f"不支持的输出格式: {fmt}")


def verify_serialized(data: bytes, fmt: str, expected_rows: int) -> bool:
    try:
        frame = deserialize_frame(data, fmt)
        return len(frame) == expected_rows
    except Exception:
        return False


def extension_for(fmt: str, compression: str) -> str:
    suffix = {"parquet": "parquet", "csv": "csv", "jsonl": "jsonl"}[fmt]
    return f"{suffix}.gz" if compression == "gzip" and fmt != "parquet" else suffix


def atomic_write(path: Path, data: bytes) -> str:
    """临时文件落盘后 fsync，再原子替换；返回 SHA256。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(dir=path.parent, prefix=".stage_", delete=False)
    temp = Path(handle.name)
    try:
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        digest = bytes_sha256(data)
        os.replace(temp, path)
        return digest
    except Exception:
        temp.unlink(missing_ok=True)
        raise


def build_archive_package(frame: pd.DataFrame, package_path: Path, tier: PolicyTier,
                          label_columns: Iterable[str], metric_columns: Iterable[str],
                          source_id: str,
                          index_path: str = "") -> Tuple[str, int, List[IndexEntry]]:
    """封装 data + index.json，注册时间偏移索引与标签索引。"""
    labels, metrics = list(label_columns), list(metric_columns)
    payload = serialize_frame(frame, tier.format, tier.compression)
    tags = _column_tags(frame, labels)
    min_ts, max_ts = _time_bounds(frame)
    manifest = {
        "source": source_id, "tier": tier.name, "format": tier.format,
        "compression": tier.compression, "row_count": int(len(frame)),
        "min_ts": min_ts, "max_ts": max_ts, "label_columns": labels,
        "metric_columns": metrics, "aggregations": dict(tier.aggregations),
        "tags": tags, "quality": {
            "raw_count": int(frame["raw_count"].sum()) if "raw_count" in frame else 0,
            "missing_count": int(frame["missing_count"].sum()) if "missing_count" in frame else 0,
            "anomaly_count": int(frame["anomaly_count"].sum()) if "anomaly_count" in frame else 0,
        },
    }
    suffix = extension_for(tier.format, tier.compression)
    member_name = f"{DATA_MEMBER}.{suffix}"
    members = [(member_name, payload),
               (INDEX_MEMBER, (json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                                          indent=2) + "\n").encode("utf-8"))]
    raw_tar = _make_tar(members)
    offset, size = _member_span(raw_tar, member_name)
    package_bytes = gzip.compress(raw_tar, compresslevel=6, mtime=0)
    atomic_write(package_path, package_bytes)
    relative = index_path or str(package_path)
    entry = IndexEntry(
        shard_id="", package_path=relative, member_name=member_name,
        offset=offset, size=size, row_count=int(len(frame)),
        min_ts=min_ts, max_ts=max_ts, tags=tags, tier=tier.name,
        source=source_id,
    )
    return bytes_sha256(package_bytes), len(package_bytes), [entry]


def _make_tar(members: List[Tuple[str, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, data in sorted(members, key=lambda item: item[0]):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def _member_span(raw_tar: bytes, member_name: str) -> Tuple[int, int]:
    with tarfile.open(fileobj=io.BytesIO(raw_tar), mode="r:") as archive:
        member = archive.getmember(member_name)
        return int(member.offset_data), int(member.size)


def _time_bounds(frame: pd.DataFrame) -> Tuple[str, str]:
    if frame.empty or TIMESTAMP not in frame:
        return "", ""
    # 显式 mixed 解析，避免 pandas 2.x 的 infer 回退警告；统一按 UTC 归一。
    values = pd.to_datetime(frame[TIMESTAMP], format="mixed",
                            errors="coerce", utc=True).dropna()
    if values.empty:
        return "", ""
    return (_iso(values.min()), _iso(values.max()))


def _iso(value: Any) -> str:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        # 统一去掉时区后按 UTC 墙上时间输出，与读取链路的归一化保持一致。
        stamp = stamp.tz_convert(None)
    return stamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _column_tags(frame: pd.DataFrame, labels: List[str]) -> Dict[str, List[str]]:
    tags: Dict[str, List[str]] = {}
    for label in labels:
        if label in frame:
            values = sorted({str(value) for value in frame[label].dropna().unique()})
            tags[label] = values
    return tags


def read_package_index(package_path: Path) -> Dict[str, Any]:
    with tarfile.open(Path(package_path), mode="r:gz") as archive:
        try:
            member = archive.extractfile(INDEX_MEMBER)
        except KeyError as exc:
            raise ValueError("归档包缺少 index.json") from exc
        if member is None:
            raise ValueError("归档包缺少 index.json")
        return json.loads(member.read().decode("utf-8"))


def extract_member(package_path: Path, member_name: str, output_dir: Path,
                   shard_id: str = "") -> Path:
    """按索引成员名定点提取，防止路径穿越。"""
    package_path = Path(package_path)
    safe_member = Path(member_name).name
    suffix = Path(safe_member).suffix or ".bin"
    target = Path(output_dir) / f"{safe_name(shard_id or package_path.stem)}{suffix}"
    target.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(package_path, mode="r:gz") as archive:
        try:
            source = archive.extractfile(safe_member)
        except KeyError as exc:
            raise FileNotFoundError(f"归档包内不存在成员: {safe_member}") from exc
        if source is None:
            raise FileNotFoundError(f"归档包内不存在成员: {safe_member}")
        data = source.read()
    atomic_write(target, data)
    return target


def restore_by_entry(workdir: Path, entry: Dict[str, Any], output_dir: Path) -> Path:
    package = Path(workdir) / entry["package_path"]
    return extract_member(package, entry["member_name"], output_dir,
                          shard_id=entry.get("shard_id", ""))


def verify_package(package_path: Path) -> bool:
    try:
        with tarfile.open(Path(package_path), mode="r:gz") as archive:
            names = archive.getnames()
            if INDEX_MEMBER not in names:
                return False
            for name in names:
                handle = archive.extractfile(name)
                if handle is None:
                    return False
                handle.read()
        return True
    except (OSError, tarfile.TarError, ValueError):
        return False


def entries_for_existing_package(package_path: Path, relative_path: str,
                                 shard_id: str) -> List[IndexEntry]:
    """回滚时从归档包内 index.json 重建两级索引。"""
    manifest = read_package_index(package_path)
    data_member = next((name for name in _archive_names(package_path)
                        if name.startswith(DATA_MEMBER + ".")), "")
    if not data_member:
        raise ValueError("归档包缺少数据成员")
    with tarfile.open(package_path, mode="r:gz") as archive:
        member = archive.getmember(data_member)
        offset, size = int(member.offset_data), int(member.size)
    return [IndexEntry(
        shard_id=shard_id, package_path=relative_path, member_name=data_member,
        offset=offset, size=size, row_count=int(manifest.get("row_count", 0)),
        min_ts=manifest.get("min_ts", ""), max_ts=manifest.get("max_ts", ""),
        tags=manifest.get("tags", {}), tier=manifest.get("tier", ""),
        source=manifest.get("source", ""),
    )]


def _archive_names(package_path: Path) -> List[str]:
    with tarfile.open(package_path, mode="r:gz") as archive:
        return archive.getnames()
