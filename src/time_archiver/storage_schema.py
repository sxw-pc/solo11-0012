"""SQLite 元数据与两级索引的建表语句。"""

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
  sha256 TEXT NOT NULL, created_at TEXT NOT NULL, active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS shard_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT, shard_id TEXT NOT NULL,
  task_id TEXT NOT NULL, prev_task_id TEXT, prev_path TEXT NOT NULL,
  prev_format TEXT, prev_sha256 TEXT, prev_row_count INTEGER,
  changed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_index (
  id INTEGER PRIMARY KEY AUTOINCREMENT, shard_id TEXT NOT NULL,
  package_path TEXT NOT NULL, member_name TEXT NOT NULL, offset INTEGER NOT NULL,
  size INTEGER NOT NULL, row_count INTEGER NOT NULL, min_ts TEXT NOT NULL,
  max_ts TEXT NOT NULL, tags_json TEXT NOT NULL, tier TEXT NOT NULL,
  source TEXT NOT NULL, member_sha256 TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS label_index (
  shard_id TEXT NOT NULL, tag_key TEXT NOT NULL, tag_value TEXT NOT NULL,
  PRIMARY KEY (shard_id, tag_key, tag_value)
);
CREATE INDEX IF NOT EXISTS idx_archive_time ON archive_index(min_ts, max_ts);
CREATE INDEX IF NOT EXISTS idx_label_lookup ON label_index(tag_key, tag_value);
CREATE INDEX IF NOT EXISTS idx_shards_tier ON shards(tier, active);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, started_at);
CREATE TABLE IF NOT EXISTS restore_tasks (
  task_id TEXT PRIMARY KEY, spec_json TEXT NOT NULL, output_dir TEXT NOT NULL,
  status TEXT NOT NULL, total_units INTEGER NOT NULL, total_members INTEGER NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT
);
CREATE TABLE IF NOT EXISTS restore_units (
  task_id TEXT NOT NULL, unit_seq INTEGER NOT NULL,
  status TEXT NOT NULL, package_path TEXT NOT NULL,
  member_count INTEGER NOT NULL, member_sha256_json TEXT NOT NULL,
  output_files_json TEXT NOT NULL, staging_dir TEXT NOT NULL,
  error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
  PRIMARY KEY (task_id, unit_seq)
);
CREATE INDEX IF NOT EXISTS idx_restore_units_status
  ON restore_units(task_id, status);
"""

# 旧版工作目录的增量迁移：只追加列，不重建表，历史数据与哈希链保持不变。
MIGRATIONS = (
    ("archive_index", "member_sha256",
     "ALTER TABLE archive_index ADD COLUMN member_sha256 TEXT NOT NULL DEFAULT ''"),
)

