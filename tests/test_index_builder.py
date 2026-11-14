"""序列化确定性、归档包偏移、索引备份与定点恢复测试。"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from time_archiver.index_builder import (
    atomic_write, build_archive_package, deserialize_frame,
    entries_for_existing_package, extension_for, extract_member,
    member_hash_from_manifest, read_member_at_offset, read_package_index,
    restore_by_entry, serialize_frame, verify_member_bytes, verify_package,
    verify_serialized,
)
from time_archiver.schema import ArchiveError, PolicyTier, bytes_sha256

@pytest.fixture
def frame():
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=4, freq="1h"),
        "sensor_id": ["a", "a", "b", "b"],
        "temperature": [1.0, 2.0, 3.0, 4.0],
        "raw_count": [1, 1, 1, 1], "valid_count": [1, 1, 1, 1],
        "missing_count": [0, 0, 0, 0], "anomaly_count": [0, 0, 0, 0],
    })


@pytest.fixture
def archive_tier():
    return PolicyTier("archive", "1h", 365, "archives", format="parquet",
                      compression="gzip", aggregations={"default": "mean"})


@pytest.mark.parametrize("fmt,compression", [
    ("parquet", "zstd"), ("parquet", "none"), ("csv", "none"),
    ("csv", "gzip"), ("jsonl", "none"), ("jsonl", "gzip"),
])
def test_serialization_roundtrip_stable(frame, fmt, compression):
    first = serialize_frame(frame, fmt, compression)
    second = serialize_frame(frame, fmt, compression)
    assert first == second
    result = deserialize_frame(first, fmt)
    assert verify_serialized(first, fmt, 4)
    assert not verify_serialized(first, fmt, 99)
    assert len(result) == 4


def test_serialization_errors(frame):
    with pytest.raises(ValueError):
        serialize_frame(frame, "avro")
    with pytest.raises(ValueError):
        deserialize_frame(b"", "avro")
    assert not verify_serialized(b"broken", "parquet", 1)


def test_extensions():
    assert extension_for("parquet", "gzip") == "parquet"
    assert extension_for("csv", "gzip") == "csv.gz"
    assert extension_for("jsonl", "none") == "jsonl"


def test_atomic_write_and_hash(tmp_path):
    target = tmp_path / "nested" / "file.bin"
    digest = atomic_write(target, b"offline")
    assert target.read_bytes() == b"offline"
    assert digest == bytes_sha256(b"offline")
    assert not list(target.parent.glob(".stage_*"))


def test_archive_package_is_deterministic_and_indexed(frame, archive_tier, tmp_path):
    first_path = tmp_path / "archives" / "first.tar.gz"
    second_path = tmp_path / "archives" / "second.tar.gz"
    first_hash, size, entries = build_archive_package(
        frame, first_path, archive_tier, ["sensor_id"], ["temperature"],
        "source-a", index_path="archives/first.tar.gz")
    second_hash, second_size, second_entries = build_archive_package(
        frame, second_path, archive_tier, ["sensor_id"], ["temperature"],
        "source-a", index_path="archives/first.tar.gz")
    assert first_hash == second_hash == bytes_sha256(first_path.read_bytes())
    assert size == second_size
    assert first_path.name != second_path.name
    assert first_path.read_bytes() == second_path.read_bytes()
    assert verify_package(first_path)
    entry = entries[0]
    assert entry.member_name == "data.parquet"
    assert entry.offset >= 512
    assert entry.size > 0
    assert entry.tags == {"sensor_id": ["a", "b"]}
    assert entry.min_ts == "2024-01-01T00:00:00Z"
    assert second_entries[0].offset == entry.offset
    manifest = read_package_index(first_path)
    assert manifest["row_count"] == 4
    assert manifest["tags"]["sensor_id"] == ["a", "b"]
    assert manifest["quality"]["raw_count"] == 4


def test_rebuild_index_and_restore(frame, archive_tier, tmp_path):
    workdir = tmp_path / "work"
    package = workdir / "archives" / "sample.tar.gz"
    digest, _, entries = build_archive_package(
        frame, package, archive_tier, ["sensor_id"], ["temperature"],
        "source-a", index_path="archives/sample.tar.gz")
    rebuilt = entries_for_existing_package(package, "archives/sample.tar.gz", "s1")
    assert rebuilt[0].member_name == entries[0].member_name
    assert rebuilt[0].offset == entries[0].offset
    restored = restore_by_entry(workdir, {
        "package_path": "archives/sample.tar.gz", "member_name": "data.parquet",
        "shard_id": "shard/a"}, workdir / "restored")
    assert restored.exists()
    assert pd.read_parquet(restored).shape[0] == 4
    assert digest


def test_extract_missing_member_and_corruption(frame, archive_tier, tmp_path):
    package = tmp_path / "sample.tar.gz"
    build_archive_package(frame, package, archive_tier, ["sensor_id"],
                          ["temperature"], "source")
    with pytest.raises(FileNotFoundError):
        extract_member(package, "../missing.parquet", tmp_path / "out")
    broken = tmp_path / "broken.tar.gz"
    broken.write_bytes(b"not-a-tar")
    assert not verify_package(broken)
    with pytest.raises(Exception):
        read_package_index(broken)


def test_csv_archive_package(frame, tmp_path):
    tier = PolicyTier("archive", "1h", 365, "archives", format="csv",
                      compression="gzip")
    package = tmp_path / "sample.tar.gz"
    _, _, entries = build_archive_package(frame, package, tier, ["sensor_id"],
                                           ["temperature"], "source")
    assert entries[0].member_name == "data.csv.gz"
    assert verify_package(package)
    data = extract_member(package, entries[0].member_name, tmp_path / "out", "x")
    assert data.name.endswith(".gz")


def test_json_manifest_is_valid_utf8(frame, archive_tier, tmp_path):
    package = tmp_path / "sample.tar.gz"
    build_archive_package(frame, package, archive_tier, ["sensor_id"],
                          ["temperature"], "数据源")
    with __import__("tarfile").open(package, mode="r:gz") as archive:
        index_bytes = archive.extractfile("index.json").read()
    assert json.loads(index_bytes.decode("utf-8"))["source"] == "数据源"


def test_atomic_write_failure_cleans_staging(tmp_path, monkeypatch):
    import time_archiver.index_builder as builder
    def boom(*args, **kwargs):
        raise OSError("disk full")
    monkeypatch.setattr(builder.os, "replace", boom)
    target = tmp_path / "out.bin"
    with pytest.raises(OSError, match="disk full"):
        atomic_write(target, b"data")
    assert not target.exists()
    assert not list(tmp_path.glob(".stage_*"))


def test_time_bounds_empty_invalid_and_timezone(frame):
    from time_archiver.index_builder import _time_bounds
    start, end = _time_bounds(frame)
    assert start.startswith("2024-01-01T00:00:00Z")
    assert end.endswith("Z")
    assert _time_bounds(frame.iloc[0:0]) == ("", "")
    assert _time_bounds(frame.drop(columns=["timestamp"])) == ("", "")
    invalid = frame.copy()
    invalid["timestamp"] = "invalid"
    assert _time_bounds(invalid) == ("", "")
    aware = frame.copy()
    aware["timestamp"] = aware["timestamp"].dt.tz_localize("UTC")
    assert _time_bounds(aware)[1].endswith("Z")


def _make_tar(path: Path, members):
    import io
    import tarfile
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for info, data in members:
            archive.addfile(info, io.BytesIO(data) if data is not None else None)
    import gzip
    path.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))


def test_package_missing_index_and_data_member(tmp_path):
    import io
    import tarfile
    no_index = tmp_path / "no-index.tar.gz"
    data_info = tarfile.TarInfo("data.csv")
    data_info.size = 3
    _make_tar(no_index, [(data_info, b"a,b")])
    assert not verify_package(no_index)
    with pytest.raises(ValueError, match="index.json"):
        read_package_index(no_index)

    only_index = tmp_path / "only-index.tar.gz"
    payload = json.dumps({"row_count": 0, "tags": {}}).encode("utf-8")
    index_info = tarfile.TarInfo("index.json")
    index_info.size = len(payload)
    _make_tar(only_index, [(index_info, payload)])
    assert verify_package(only_index)
    with pytest.raises(ValueError, match="数据成员"):
        entries_for_existing_package(only_index, "x", "s1")


def test_directory_member_is_not_extractable(tmp_path):
    import tarfile
    package = tmp_path / "dir.tar.gz"
    index_payload = json.dumps({"row_count": 0}).encode("utf-8")
    index_info = tarfile.TarInfo("index.json")
    index_info.size = len(index_payload)
    dir_info = tarfile.TarInfo("data.parquet")
    dir_info.type = tarfile.DIRTYPE
    _make_tar(package, [(index_info, index_payload), (dir_info, None)])
    with pytest.raises(FileNotFoundError):
        extract_member(package, "data.parquet", tmp_path / "out")
    assert not verify_package(package)


def test_member_sha256_recorded_in_index_and_manifest(frame, archive_tier, tmp_path):
    package = tmp_path / "sample.tar.gz"
    _digest, _size, entries = build_archive_package(
        frame, package, archive_tier, ["sensor_id"], ["temperature"], "source")
    entry = entries[0]
    assert entry.member_sha256
    manifest = read_package_index(package)
    assert manifest["members"][0]["sha256"] == entry.member_sha256
    rebuilt = entries_for_existing_package(package, "sample.tar.gz", "s1")
    assert rebuilt[0].member_sha256 == entry.member_sha256
    assert member_hash_from_manifest(package, entry.member_name) == entry.member_sha256


def test_read_member_at_offset_matches_payload(frame, archive_tier, tmp_path):
    package = tmp_path / "sample.tar.gz"
    _digest, _size, entries = build_archive_package(
        frame, package, archive_tier, ["sensor_id"], ["temperature"], "source")
    entry = entries[0]
    data = read_member_at_offset(package, entry.offset, entry.size)
    assert bytes_sha256(data) == entry.member_sha256
    short = tmp_path / "short.tar.gz"
    short.write_bytes(package.read_bytes()[: len(package.read_bytes()) // 2])
    with pytest.raises(ArchiveError, match="偏移读取"):
        read_member_at_offset(short, entry.offset, entry.size)


def test_verify_member_bytes_member_and_package_level(frame, archive_tier, tmp_path):
    package = tmp_path / "sample.tar.gz"
    _digest, _size, entries = build_archive_package(
        frame, package, archive_tier, ["sensor_id"], ["temperature"], "source")
    entry = entries[0].to_dict()
    data = read_member_at_offset(package, entry["offset"], entry["size"])
    verify_member_bytes(package, entry, data)  # 成员哈希匹配，不抛异常
    with pytest.raises(ArchiveError, match="成员 sha256"):
        verify_member_bytes(package, entry, b"tampered-payload")
    # 旧条目没有成员哈希时回退整包哈希
    legacy = dict(entry)
    legacy["member_sha256"] = ""
    legacy["package_sha256"] = bytes_sha256(package.read_bytes())
    verify_member_bytes(package, legacy, data)
    other = tmp_path / "other.tar.gz"
    other.write_bytes(package.read_bytes())
    legacy["package_sha256"] = "0" * 64
    with pytest.raises(ArchiveError, match="整体 sha256"):
        verify_member_bytes(other, legacy, data)
