"""SQLite 元数据、任务断点、归档索引与回滚指针存储。"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .schema import IndexEntry, ShardRecord, canonical_json

from .storage_schema import COLUMN_MIGRATIONS, SCHEMA


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class MetadataStore:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(str(self.db_path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(SCHEMA)
            for table, columns in COLUMN_MIGRATIONS.items():
                existing = {row[1] for row in
                            connection.execute(f"PRAGMA table_info({table})")}
                for column, ddl in columns.items():
                    if column not in existing:
                        connection.execute(
                            f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def start_task(self, task_id: str, kind: str, source: str,
                   policy_hash: str, range_start: str = "",
                   range_end: str = "") -> Dict[str, Any]:
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if existing:
                return dict(existing)
            connection.execute(
                "INSERT INTO tasks(task_id,kind,source,status,started_at,policy_hash,"
                "range_start,range_end,stage) VALUES(?,?,?,?,?,?,?,?,?)",
                (task_id, kind, source, "running", utc_now(), policy_hash,
                 range_start, range_end, "started"))
            row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            return dict(row)

    def update_task(self, task_id: str, **fields: Any) -> None:
        allowed = {"status", "finished_at", "stage", "result_json", "error",
                   "range_start", "range_end"}
        values = {key: value for key, value in fields.items() if key in allowed}
        if not values:
            return
        assignments = ", ".join(f"{key}=?" for key in values)
        with self._connect() as connection:
            connection.execute(f"UPDATE tasks SET {assignments} WHERE task_id=?",
                               (*values.values(), task_id))

    def finish_task(self, task_id: str, status: str, result: Dict[str, Any],
                    error: str = "") -> None:
        self.update_task(task_id, status=status, finished_at=utc_now(),
                         stage="finished", error=error,
                         result_json=json.dumps(result, ensure_ascii=False, sort_keys=True))

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["result"] = _loads(result.pop("result_json", None), {})
        return result

    def list_tasks(self, limit: int = 50, status: Optional[str] = None,
                   kind: Optional[str] = None) -> list[Dict[str, Any]]:
        query = "SELECT * FROM tasks"
        clauses, values = [], []
        if status:
            clauses.append("status=?")
            values.append(status)
        if kind:
            clauses.append("kind=?")
            values.append(kind)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY started_at DESC, rowid DESC LIMIT ?"
        values.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        output = []
        for row in rows:
            item = dict(row)
            item["result"] = _loads(item.pop("result_json", None), {})
            output.append(item)
        return output

    def latest_task(self, kind: Optional[str] = None) -> Optional[Dict[str, Any]]:
        tasks = self.list_tasks(limit=1, kind=kind, status="completed")
        return tasks[0] if tasks else None

    def save_shard(self, record: ShardRecord) -> bool:
        """幂等写入分片指针；内容变化时先把旧指针写入历史。"""
        with self._connect() as connection:
            old = connection.execute(
                "SELECT * FROM shards WHERE shard_id=?", (record.shard_id,)).fetchone()
            if (old and old["sha256"] == record.sha256 and old["path"] == record.path
                    and old["active"]
                    and old["aggregation_mode"] == record.aggregation_mode):
                return False
            if old:
                connection.execute(
                    "INSERT INTO shard_history(shard_id,task_id,prev_task_id,prev_path,"
                    "prev_format,prev_sha256,prev_row_count,changed_at,"
                    "prev_aggregation_mode) VALUES(?,?,?,?,?,?,?,?,?)",
                    (record.shard_id, record.task_id, old["task_id"], old["path"],
                     old["format"], old["sha256"], old["row_count"], utc_now(),
                     old["aggregation_mode"]))
            connection.execute(
                "INSERT INTO shards(shard_id,task_id,tier,source,partition,path,format,"
                "row_count,raw_count,missing_count,anomaly_count,sha256,created_at,active,"
                "aggregation_mode) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,?) "
                "ON CONFLICT(shard_id) DO UPDATE SET task_id=excluded.task_id,"
                "tier=excluded.tier,source=excluded.source,partition=excluded.partition,"
                "path=excluded.path,format=excluded.format,row_count=excluded.row_count,"
                "raw_count=excluded.raw_count,missing_count=excluded.missing_count,"
                "anomaly_count=excluded.anomaly_count,sha256=excluded.sha256,"
                "created_at=excluded.created_at,active=1,"
                "aggregation_mode=excluded.aggregation_mode",
                (record.shard_id, record.task_id, record.tier, record.source,
                 record.partition, record.path, record.fmt, record.row_count,
                 record.raw_count, record.missing_count, record.anomaly_count,
                 record.sha256, record.created_at or utc_now(),
                 record.aggregation_mode))
            return True

    def shards_for_task(self, task_id: str) -> list[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM shards WHERE task_id=? AND active=1", (task_id,)).fetchall()
        return [dict(row) for row in rows]

    def get_shard(self, shard_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM shards WHERE shard_id=?", (shard_id,)).fetchone()
        return dict(row) if row else None

    def list_shards(self, tier: Optional[str] = None, active_only: bool = True) -> list[Dict[str, Any]]:
        query = "SELECT * FROM shards"
        clauses, values = [], []
        if tier:
            clauses.append("tier=?")
            values.append(tier)
        if active_only:
            clauses.append("active=1")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY tier, source, partition"
        with self._connect() as connection:
            return [dict(row) for row in connection.execute(query, values).fetchall()]

    def replace_index(self, entries: Iterable[IndexEntry]) -> None:
        entries = list(entries)
        with self._connect() as connection:
            shard_ids = {entry.shard_id for entry in entries}
            for shard_id in shard_ids:
                connection.execute("DELETE FROM archive_index WHERE shard_id=?", (shard_id,))
                connection.execute("DELETE FROM label_index WHERE shard_id=?", (shard_id,))
            for entry in entries:
                connection.execute(
                    "INSERT INTO archive_index(shard_id,package_path,member_name,offset,size,"
                    "row_count,min_ts,max_ts,tags_json,tier,source,coverage_member) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (entry.shard_id, entry.package_path, entry.member_name, entry.offset,
                     entry.size, entry.row_count, entry.min_ts, entry.max_ts,
                     json.dumps(entry.tags, ensure_ascii=False, sort_keys=True),
                     entry.tier, entry.source, entry.coverage_member))
                for key, values in sorted(entry.tags.items()):
                    for value in sorted(set(map(str, values))):
                        connection.execute(
                            "INSERT OR IGNORE INTO label_index(shard_id,tag_key,tag_value) "
                            "VALUES(?,?,?)", (entry.shard_id, key, value))

    def replace_coverage(self, shard_id: str,
                         entries: Iterable[Dict[str, Any]]) -> None:
        """按分片整体替换组合覆盖率表；空列表表示清除（全局模式分片）。"""
        rows = []
        for entry in entries or []:
            labels = {str(key): str(value)
                      for key, value in (entry.get("labels") or {}).items()}
            rows.append((shard_id, canonical_json(labels),
                         json.dumps(labels, ensure_ascii=False, sort_keys=True),
                         int(entry.get("covered_buckets", 0)),
                         int(entry.get("valid_points", 0))))
        with self._connect() as connection:
            connection.execute("DELETE FROM combo_coverage WHERE shard_id=?",
                               (shard_id,))
            connection.executemany(
                "INSERT OR REPLACE INTO combo_coverage(shard_id,combo_key,labels_json,"
                "covered_buckets,valid_points) VALUES(?,?,?,?,?)", rows)

    def coverage_for_shard(self, shard_id: str) -> list[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT labels_json,covered_buckets,valid_points FROM combo_coverage "
                "WHERE shard_id=? ORDER BY combo_key", (shard_id,)).fetchall()
        return [{"labels": _loads(row["labels_json"], {}),
                 "covered_buckets": int(row["covered_buckets"]),
                 "valid_points": int(row["valid_points"])} for row in rows]

    def search(self, start: Optional[str] = None, end: Optional[str] = None,
               tag_key: str = "", tag_value: str = "",
               limit: int = 200) -> list[Dict[str, Any]]:
        query = ("SELECT ai.*, s.sha256 FROM archive_index ai JOIN shards s "
                 "ON s.shard_id=ai.shard_id AND s.active=1 AND s.path=ai.package_path WHERE 1=1")
        values: list[Any] = []
        if start:
            query += " AND ai.max_ts >= ?"
            values.append(start)
        if end:
            query += " AND ai.min_ts <= ?"
            values.append(end)
        if tag_key and tag_value:
            query += (" AND EXISTS (SELECT 1 FROM label_index li WHERE "
                      "li.shard_id=ai.shard_id AND li.tag_key=? AND li.tag_value=?)")
            values.extend([tag_key, str(tag_value)])
        query += " ORDER BY ai.min_ts, ai.package_path LIMIT ?"
        values.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        output = []
        for row in rows:
            item = dict(row)
            item["tags"] = _loads(item.pop("tags_json"), {})
            output.append(item)
        return output

    def index_entries_for_shard(self, shard_id: str) -> list[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM archive_index WHERE shard_id=? ORDER BY id", (shard_id,)).fetchall()
        output = []
        for row in rows:
            item = dict(row)
            item["tags"] = _loads(item.pop("tags_json"), {})
            output.append(item)
        return output

    def index_summary(self) -> Dict[str, Any]:
        with self._connect() as connection:
            shards = connection.execute(
                "SELECT COUNT(*) AS c FROM archive_index").fetchone()["c"]
            labels = connection.execute(
                "SELECT COUNT(*) AS c FROM label_index").fetchone()["c"]
        return {"index_shards": shards, "label_entries": labels}

    def rollback_task(self, task_id: str) -> list[Dict[str, Any]]:
        """撤销任务的分片指针；优先恢复上一版指针，归档包不删除。"""
        changed = []
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM shards WHERE task_id=?", (task_id,)).fetchall()
            for row in rows:
                history = connection.execute(
                    "SELECT * FROM shard_history WHERE shard_id=? AND task_id=? "
                    "ORDER BY id DESC LIMIT 1", (row["shard_id"], task_id)).fetchone()
                changed.append({"shard_id": row["shard_id"], "current_path": row["path"],
                                "tier": row["tier"]})
                # 覆盖率表描述的是被撤销版本的内容，一律先清除；
                # 归档层分片由调用方从包内 coverage.json 重建。
                connection.execute("DELETE FROM combo_coverage WHERE shard_id=?",
                                   (row["shard_id"],))
                if history:
                    connection.execute(
                        "UPDATE shards SET task_id=?,path=?,format=?,row_count=?,sha256=?,"
                        "active=1,aggregation_mode=? WHERE shard_id=?",
                        (history["prev_task_id"], history["prev_path"],
                         history["prev_format"], history["prev_row_count"],
                         history["prev_sha256"], history["prev_aggregation_mode"],
                         row["shard_id"]))
                else:
                    connection.execute("UPDATE shards SET active=0 WHERE shard_id=?",
                                       (row["shard_id"],))
                    connection.execute("DELETE FROM archive_index WHERE shard_id=?",
                                       (row["shard_id"],))
                    connection.execute("DELETE FROM label_index WHERE shard_id=?",
                                       (row["shard_id"],))
        return changed

    def counts(self) -> Dict[str, int]:
        with self._connect() as connection:
            return {
                "tasks": connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
                "shards": connection.execute(
                    "SELECT COUNT(*) FROM shards WHERE active=1").fetchone()[0],
                "archives": connection.execute(
                    "SELECT COUNT(*) FROM archive_index").fetchone()[0],
            }


def _loads(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        return default
