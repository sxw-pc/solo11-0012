"""SQLite 元数据、两级索引与组合覆盖率表的建表语句及列迁移。"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, kind TEXT NOT NULL, source TEXT NOT NULL,
  status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
  policy_hash TEXT, range_start TEXT, range_end TEXT, stage TEXT,
  result_json TEXT, error TEXT
);
CREATE TABLE IF NOT EXISTS shards (
  shard_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tier TEXT NOT NULL,
  source TEXT NOT NULL, partition TEXT NOT NULL, path TEXT NOT NULL,
  format TEXT NOT NULL, row_count INTEGER NOT NULL, raw_count INTEGER NOT NULL,
  missing_count INTEGER NOT NULL, anomaly_count INTEGER NOT NULL,
  sha256 TEXT NOT NULL, created_at TEXT NOT NULL, active INTEGER DEFAULT 1,
  aggregation_mode TEXT NOT NULL DEFAULT 'global'
);
CREATE TABLE IF NOT EXISTS shard_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT, shard_id TEXT NOT NULL,
  task_id TEXT NOT NULL, prev_task_id TEXT, prev_path TEXT NOT NULL,
  prev_format TEXT, prev_sha256 TEXT, prev_row_count INTEGER,
  changed_at TEXT NOT NULL,
  prev_aggregation_mode TEXT NOT NULL DEFAULT 'global'
);
CREATE TABLE IF NOT EXISTS archive_index (
  id INTEGER PRIMARY KEY AUTOINCREMENT, shard_id TEXT NOT NULL,
  package_path TEXT NOT NULL, member_name TEXT NOT NULL, offset INTEGER NOT NULL,
  size INTEGER NOT NULL, row_count INTEGER NOT NULL, min_ts TEXT NOT NULL,
  max_ts TEXT NOT NULL, tags_json TEXT NOT NULL, tier TEXT NOT NULL,
  source TEXT NOT NULL, coverage_member TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS label_index (
  shard_id TEXT NOT NULL, tag_key TEXT NOT NULL, tag_value TEXT NOT NULL,
  PRIMARY KEY (shard_id, tag_key, tag_value)
);
CREATE TABLE IF NOT EXISTS combo_coverage (
  shard_id TEXT NOT NULL, combo_key TEXT NOT NULL, labels_json TEXT NOT NULL,
  covered_buckets INTEGER NOT NULL, valid_points INTEGER NOT NULL,
  PRIMARY KEY (shard_id, combo_key)
);
CREATE INDEX IF NOT EXISTS idx_archive_time ON archive_index(min_ts, max_ts);
CREATE INDEX IF NOT EXISTS idx_label_lookup ON label_index(tag_key, tag_value);
CREATE INDEX IF NOT EXISTS idx_shards_tier ON shards(tier, active);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, started_at);
"""

# 旧版本工作目录的列级迁移：CREATE TABLE IF NOT EXISTS 不会为已存在的表补列，
# 因此按表声明需要补齐的列，init_schema 时逐列 ALTER TABLE。
COLUMN_MIGRATIONS = {
    "shards": {
        "aggregation_mode": "TEXT NOT NULL DEFAULT 'global'",
    },
    "shard_history": {
        "prev_aggregation_mode": "TEXT NOT NULL DEFAULT 'global'",
    },
    "archive_index": {
        "coverage_member": "TEXT NOT NULL DEFAULT ''",
    },
}
