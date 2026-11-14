# 变更记录

本文档记录时档匠的版本演进。日期使用本地项目日期，版本号遵循语义化版本的基本约定。

## 1.1.0 - 2026-11-14

### 新增

- 标签组合基数与稀疏度预检：按层按自然月推算组合数、时间桶数与每组合平均覆盖桶数，超阈值层自动从全局分组切换为分片内按组合局部聚合。
- 局部聚合模式：每个标签组合按自身节奏推断理论槽位后独立聚合；`raw_count`、`valid_count`、`missing_count`、`anomaly_count` 四个质量列口径与类型不变。
- 分片级组合覆盖率表：记录每个组合的覆盖桶数与有效点数，可对原始数据回算核对；SQLite 新增 `combo_coverage` 表，归档包新增 `coverage.json` 成员与清单声明，`archive_index` 新增 `coverage_member` 列。
- 策略可选 `precheck` 配置段（`enabled`、`max_combinations`、`min_avg_bucket_coverage`、`auto_apply`），配置段存在时参与策略哈希。
- 新 CLI 子命令 `precheck`：独立执行预检，确定性报告落盘 `state/precheck/`，结论写审计链但不写任务表。
- 旧版工作目录自动列迁移：`shards.aggregation_mode`、`shard_history.prev_aggregation_mode`、`archive_index.coverage_member`。

### 兼容性说明

- 未声明 `precheck` 段的策略哈希与 1.0.0 完全一致，归档输出逐字节兼容。
- 未启用预检或预检未超限时，归档包结构与分片文件和 1.0.0 逐字节一致。
- 没有覆盖率表的旧分片、旧归档包可正常检索、恢复与回滚重建。

## 1.0.0 - 2026-11-11

### 新增

- 声明式 1～5 级生命周期策略，支持按数据最大时间戳划分热、温、冷与归档层。
- Parquet、CSV、SQLite 三种本地输入接入，SQLite 以只读查询模式打开。
- 基于策略阈值的质量检查、异常标记、重复时间戳处理与缺失点估算。
- 七种确定性聚合算子：mean、max、min、count、first、last、p95。
- 三种缺失桶策略：skip、null、ffill，按标签独立补桶。
- 内容寻址的分层分片文件和确定性 tar.gz 归档包。
- 归档包一级时间偏移索引与二级标签索引，支持不解压检索和定点恢复。
- SQLite 任务、分片、历史指针与索引持久化，支持断点续跑和任务缓存。
- 两阶段提交、完整性校验、原子替换与 staging 清理。
- JSONL 哈希链审计，支持筛选、链校验、JSON 与 Markdown 导出。
- 任务级回滚，优先恢复上一版分片指针，归档包保留。
- Click CLI：init、load-demo、validate-config、archive、demo、schedule、tasks、search、restore、rollback、audit-export、web、test。
- Flask 本地 Web 控制台：任务、策略编辑、归档检索、下载恢复、审计导出。
- 三组确定性 mock 数据，合计 13 个月跨度，约 5% 缺失和 2% 阈值异常。
- 离线依赖 wheelhouse、fixture 重生成脚本、SHA256SUMS 校验清单。
- 单元、集成、幂等、边界、离线与隔离测试，整体覆盖率门槛 85%。

### 设计说明

- 生命周期基准固定为数据最大时间戳，不使用机器当前时间，确保历史回放可复现。
- 回滚只改变指针和索引，不删除归档包、原始输入或任何历史文件。
- Web 仅绑定 127.0.0.1，页面资源全部本地化，不引用外部服务。

### 已知限制

- 不支持分布式协调、实时流处理、云存储、多租户、自定义算子插件和非时序数据。
- 不提供自动删除原始数据或归档数据的能力。
- 前台调度器不安装系统服务，长期运行需由使用者自行托管进程。
