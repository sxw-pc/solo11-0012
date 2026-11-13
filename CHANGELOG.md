# 变更记录

本文档记录时档匠的版本演进。日期使用本地项目日期，版本号遵循语义化版本的基本约定。

## 1.1.0 - 2026-11-13

### 变更

- 依赖清单收敛为可复现的精确锁定表达：`requirements.txt` 改为 `包名==版本`，
  新增 `wheelhouse/constraints.txt` 锁定全部传递依赖，`pyproject.toml` 的
  `dependencies` 与实际轮子对齐，消除 pandas 静默滚动到未适配版本的风险。
- 核心数据栈锁定为 pandas 2.3.3、numpy 2.4.6、pyarrow 19.0.1、PyYAML 6.0.3。
- `wheelhouse/` 在原有 Windows x86_64 轮子之外，补齐同版本 Linux x86_64 /
  CPython 3.11 轮子，两套平台共用一份版本清单。
- 新增 `tools/download_wheels.sh` 固化联网机器按同一清单复现下载轮子的流程，
  新增 `tools/make_checksums.sh` 可复现地重新生成 `SHA256SUMS.txt`。

### 兼容性

- Parquet、CSV、SQLite 三条读取链路改为按列显式声明 dtype，不再依赖类型推断；
  时间戳统一用 `format="mixed"` 解析并归一化为时区无关 UTC 时间，带时区时间列
  不再退化成 object 列。
- 时间桶分组改用 pandas 2.x 推荐的 `groupby(pd.Grouper(freq=...))` 重采样写法，
  桶边界与旧的 `Series.dt.floor` 等价，不依赖已移除的时区链式调用。
- 缺失值判断统一为 `pandas.isna` / `notna`，NaN、`pd.NA`、None、NaT 行为一致，
  不再混用 `== None` 风格比较。
- `raw_count`、`valid_count`、`missing_count`、`anomaly_count` 四个计数列在所有
  产出路径上强制为普通 `int64`、空值写 0，避免被升级成带 `pd.NA` 的可空整型。

### 测试

- 新增 `tests/test_pandas2_compat.py`，覆盖显式 dtype、时区/空值混合 CSV、
  SQLite epoch、Grouper 分桶与计数列 int64 稳定性，共 6 项。
- 全量测试 129 项通过，整体语句覆盖率 94%，降采样模块 100%。
- 在干净虚拟环境从 `wheelhouse/` 离线安装并运行 demo，全部归档产物与升级前
  逐字节一致，对外命令行为不变。

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
