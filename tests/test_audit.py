"""审计哈希链、字段完整性、筛选和双格式导出测试。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from time_archiver.audit import REQUIRED_FIELDS, AuditLog


def test_required_fields_and_chain(tmp_path: Path):
    audit = AuditLog(tmp_path / "audit.jsonl")
    first = audit.log("config_validate", "CLI", details={"path": "policy.yaml"})
    second = audit.log("archive", "WEB", task_id="task-1", range_start="2024-01-01",
                       range_end="2024-01-31", input_hashes=["a"], output_hashes=["b"],
                       result="success", details={"rows": 10})
    assert set(REQUIRED_FIELDS) <= set(first)
    assert first["sequence"] == 1
    assert second["prev_hash"] == first["entry_hash"]
    assert second["input_hashes"] == ["a"]
    assert audit.verify() == {"valid": True, "entries": 2, "bad_sequence": 0, "reason": ""}
    assert audit.summary()["entries"] == 2


def test_failure_and_filters(tmp_path: Path):
    audit = AuditLog(tmp_path / "audit.jsonl")
    audit.log("archive", "CLI", task_id="t1", result="failure", error="boom")
    audit.log("restore", "WEB", task_id="t1", result="success")
    audit.log("archive", "CLI", task_id="t2", result="success")
    assert len(audit.read(task_id="t1")) == 2
    assert len(audit.read(operation="archive", result="success")) == 1
    assert audit.read(actor="WEB")[0]["operation"] == "restore"
    assert audit.read(result="failure")[0]["error"] == "boom"
    assert audit.read(since="2000-01-01", until="2999-01-01")
    assert len(audit.read(limit=1)) == 1


def test_exports_json_and_markdown(tmp_path: Path):
    audit = AuditLog(tmp_path / "audit.jsonl")
    audit.log("archive", "CLI", task_id="t|1", details={"中文": True})
    json_path = tmp_path / "out" / "audit.json"
    markdown_path = tmp_path / "out" / "audit.md"
    text = audit.export_json(json_path)
    assert json.loads(text)[0]["operation"] == "archive"
    assert json_path.exists()
    markdown = audit.export_markdown(markdown_path, task_id="t|1")
    assert "时档匠审计导出" in markdown
    assert "t\\|1" in markdown
    assert markdown_path.exists()
    assert audit.verify()["valid"]


def test_invalid_actor_rejected(tmp_path: Path):
    audit = AuditLog(tmp_path / "audit.jsonl")
    with pytest.raises(ValueError):
        audit.log("archive", "SYSTEM")


def test_tamper_is_detected(tmp_path: Path):
    path = tmp_path / "audit.jsonl"
    audit = AuditLog(path)
    audit.log("archive", "CLI", task_id="t1")
    audit.log("restore", "CLI", task_id="t1")
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    records[0]["details"]["changed"] = True
    path.write_text("\n".join(json.dumps(item, ensure_ascii=False) for item in records) + "\n",
                    encoding="utf-8")
    result = audit.verify()
    assert not result["valid"]
    assert result["bad_sequence"] == 1
    assert "entry_hash" in result["reason"]


def test_missing_field_and_malformed_lines(tmp_path: Path):
    path = tmp_path / "audit.jsonl"
    path.write_text("not-json\n{}\n", encoding="utf-8")
    audit = AuditLog(path)
    result = audit.read()
    assert result == [{}]
    assert not audit.verify()["valid"]
    assert "缺少字段" in audit.verify()["reason"]


def test_empty_log(tmp_path: Path):
    audit = AuditLog(tmp_path / "empty.jsonl")
    assert audit.verify()["valid"]
    assert audit.read() == []
    assert audit.summary()["operations"] == []
