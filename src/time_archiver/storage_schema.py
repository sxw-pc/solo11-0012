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
  source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS label_index (
  shard_id TEXT NOT NULL, tag_key TEXT NOT NULL, tag_value TEXT NOT NULL,
  PRIMARY KEY (shard_id, tag_key, tag_value)
);
CREATE INDEX IF NOT EXISTS idx_archive_time ON archive_index(min_ts, max_ts);
CREATE INDEX IF NOT EXISTS idx_label_lookup ON label_index(tag_key, tag_value);
CREATE INDEX IF NOT EXISTS idx_shards_tier ON shards(tier, active);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, started_at);
CREATE TABLE IF NOT EXISTS plans (
  plan_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, source_hash TEXT NOT NULL,
  policy_hash TEXT NOT NULL, budget_bytes INTEGER NOT NULL,
  merge_oldest INTEGER NOT NULL, within_budget INTEGER NOT NULL,
  projected_bytes INTEGER NOT NULL, baseline_bytes INTEGER NOT NULL,
  plan_path TEXT NOT NULL, plan_hash TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reorg_journals (
  journal_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL, task_id TEXT NOT NULL,
  source_id TEXT NOT NULL, state TEXT NOT NULL, stage TEXT NOT NULL,
  plan_hash TEXT NOT NULL, new_shards_json TEXT NOT NULL,
  stale_shards_json TEXT NOT NULL, stale_paths_json TEXT NOT NULL DEFAULT '[]',
  quarantined_json TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_journals_state ON reorg_journals(state);
"""

