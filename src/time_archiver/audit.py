"""独立追加式审计日志：哈希链、条件筛选与 JSON/Markdown 导出。"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .schema import canonical_hash, canonical_json

GENESIS = "0" * 64
REQUIRED_FIELDS = (
    "sequence", "timestamp", "actor", "operation", "task_id", "range_start",
    "range_end", "input_hashes", "output_hashes", "result", "error",
    "details", "prev_hash", "entry_hash",
)


class AuditLog:
    """单文件 JSONL 审计日志，每条记录链接上一条哈希。"""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self._lock = threading.Lock()

    def log(self, operation: str, actor: str = "CLI", task_id: str = "",
            range_start: str = "", range_end: str = "",
            input_hashes: Optional[Iterable[str]] = None,
            output_hashes: Optional[Iterable[str]] = None,
            result: str = "success", error: str = "",
            details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        actor = actor.upper() if actor else "CLI"
        if actor not in {"CLI", "WEB"}:
            raise ValueError("审计来源必须是 CLI 或 WEB")
        with self._lock:
            previous = self._last_entry()
            payload = {
                "sequence": int(previous.get("sequence", 0)) + 1,
                "timestamp": datetime.now(timezone.utc).replace(microsecond=0)
                               .isoformat().replace("+00:00", "Z"),
                "actor": actor, "operation": operation, "task_id": task_id,
                "range_start": range_start, "range_end": range_end,
                "input_hashes": sorted(set(input_hashes or [])),
                "output_hashes": sorted(set(output_hashes or [])),
                "result": result, "error": error or "",
                "details": details or {},
                "prev_hash": previous.get("entry_hash", GENESIS),
            }
            payload["entry_hash"] = _entry_hash(payload)
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(canonical_json(payload) + "\n")
            return payload

    def _last_entry(self) -> Dict[str, Any]:
        last = {}
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    try:
                        last = json.loads(line)
                    except ValueError:
                        continue
        return last

    def read(self, operation: str = "", task_id: str = "", result: str = "",
             actor: str = "", since: str = "", until: str = "",
             limit: int = 500) -> list[Dict[str, Any]]:
        records = self._all()
        output = []
        for record in records:
            if operation and record.get("operation") != operation:
                continue
            if task_id and record.get("task_id") != task_id:
                continue
            if result and record.get("result") != result:
                continue
            if actor and record.get("actor") != actor.upper():
                continue
            stamp = str(record.get("timestamp", ""))
            if since and stamp < since:
                continue
            if until and stamp > until:
                continue
            output.append(record)
        return output[-int(limit):]

    def _all(self) -> list[Dict[str, Any]]:
        records = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if isinstance(item, dict):
                    records.append(item)
        return records

    def verify(self) -> Dict[str, Any]:
        """校验字段完整性与哈希链，返回首个问题位置。"""
        previous = GENESIS
        records = self._all()
        for position, record in enumerate(records, start=1):
            missing = [field for field in REQUIRED_FIELDS if field not in record]
            if missing:
                return {"valid": False, "entries": len(records), "bad_sequence": position,
                        "reason": f"缺少字段 {','.join(missing)}"}
            if record.get("prev_hash") != previous:
                return {"valid": False, "entries": len(records), "bad_sequence": position,
                        "reason": "prev_hash 链断裂"}
            expected = _entry_hash(record)
            if record.get("entry_hash") != expected:
                return {"valid": False, "entries": len(records), "bad_sequence": position,
                        "reason": "entry_hash 不匹配，记录可能被篡改"}
            previous = record["entry_hash"]
        return {"valid": True, "entries": len(records), "bad_sequence": 0, "reason": ""}

    def export_json(self, target: Optional[Path] = None, **filters: Any) -> str:
        text = json.dumps(self.read(**filters), ensure_ascii=False, indent=2)
        if target:
            Path(target).parent.mkdir(parents=True, exist_ok=True)
            Path(target).write_text(text + "\n", encoding="utf-8")
        return text

    def export_markdown(self, target: Optional[Path] = None, **filters: Any) -> str:
        records = self.read(**filters)
        lines = [
            "# 时档匠审计导出", "",
            f"- 记录数量：{len(records)}",
            f"- 审计链状态：{'有效' if self.verify()['valid'] else '异常'}", "",
            "| 序号 | 时间 | 来源 | 操作 | 任务ID | 结果 |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for record in records:
            task = str(record.get("task_id", "")).replace("|", "\\|")
            lines.append(
                f"| {record.get('sequence')} | {record.get('timestamp')} | "
                f"{record.get('actor')} | {record.get('operation')} | "
                f"`{task}` | {record.get('result')} |")
        text = "\n".join(lines) + "\n"
        if target:
            Path(target).parent.mkdir(parents=True, exist_ok=True)
            Path(target).write_text(text, encoding="utf-8")
        return text

    def summary(self) -> Dict[str, Any]:
        records = self._all()
        return {
            "entries": len(records),
            "chain": self.verify(),
            "operations": sorted({r.get("operation", "") for r in records}),
        }


def _entry_hash(payload: Dict[str, Any]) -> str:
    unsigned = {key: value for key, value in payload.items() if key != "entry_hash"}
    return canonical_hash({"prev": payload.get("prev_hash", GENESIS), "record": unsigned})
