# 时档匠

**纯本地、离线、可复核的时序数据分层降质归档工具。**

时档匠面向边缘网关、本地服务器和离线物联网环境，把“数据越存越多、磁盘越来越紧、历史数据却很少被查询”的现实问题，收敛成一套可声明、可重复、可回滚的本地生命周期流程。它读取本地 Parquet、CSV 或 SQLite 中的时序数据，按照用户声明的 1～5 级策略执行确定性降采样，将热、温、冷、归档层分别写入独立工作目录，并为冷归档建立两级索引与全链路哈希审计。

时档匠不是采集器，不是实时流系统，不是云平台，也不是可视化大盘。它只做一件事：让历史时序数据在本地以更低的存储成本长期留存，同时保留足够的时间定位能力、标签检索能力、完整性证据和人工回退能力。

## 一、它解决什么问题

时序数据的典型困境是：最近几天需要秒级排查，最近几个月需要分钟级趋势，几年前的数据只关心“是否发生过、何时发生过、大致量级是多少”。如果所有粒度都按原始频率保存，磁盘会被大量低价值历史点占满；如果直接删除，又会丢失可追溯性。

时档匠的价值体现在四个方面：

1. **策略显式化**：用一份 YAML 声明生命周期边界、每层粒度和聚合方式，避免口头约定和散落脚本。
2. **结果确定化**：同一份输入、同一份策略、同一套依赖版本，重复运行得到逐字节一致的文件与元数据。
3. **归档可定位**：归档包不解压即可按时间偏移与标签找到目标成员，恢复时定点提取。
4. **操作可审计**：配置校验、归档、缓存命中、恢复、回滚和导出都写入带哈希链的本地审计日志。

## 二、适用场景

- 边缘网关磁盘容量有限，需要把 10 秒级数据逐级降为分钟、小时、天级。
- 工厂或实验室的本地服务器不能访问互联网，历史数据必须留在内网。
- 离线 IoT 设备按小时上报，数据跨度超过一年，需要低频长期归档。
- 小型运维团队希望保留“最近原始、历史降质”的能力，但不想引入分布式组件。
- 审计或复核场景需要证明“某个归档任务确实处理了哪些输入、产生了哪些输出”。

## 三、快速开始

在项目根目录准备 Python 3.9～3.11 环境，然后依次执行以下四条命令：

```bash
python -m pip install --no-index --find-links wheelhouse setuptools wheel -r requirements.txt
python -m pip install --no-index --no-build-isolation --config-settings editable_mode=compat -e .
time-archiver --workdir demo_work demo
python -m pytest
```

第一条命令完全从本地 `wheelhouse` 安装依赖，不访问网络。第二条命令以可编辑方式安装本项目，并生成 `time-archiver` 命令；`editable_mode=compat` 用于绕开部分 setuptools 版本在可编辑构建时报告元数据目录为空的问题。第三条命令自动初始化 `demo_work`、加载三组内置 mock 数据、执行全量分层归档、构建索引、检索并恢复一个样本。第四条命令运行全量测试并输出覆盖率报告。

如果不想安装项目，也可以在项目根目录使用源码方式运行。Windows PowerShell 可先执行 `$env:PYTHONPATH="src"`，Linux 或 macOS 可执行 `export PYTHONPATH=src`，随后运行 `python -m time_archiver --workdir demo_work demo`。测试配置已经声明 `pythonpath = src`，因此直接执行 `python -m pytest` 即可。若命令行未把 pip 的脚本目录加入 PATH，可使用 `python -m time_archiver` 代替 `time-archiver`。

`wheelhouse` 中的二进制轮子按 Windows x86_64、CPython 3.11 准备。若目标机器是 Linux、macOS 或其他 Python 小版本，需要在**联网的准备阶段**使用相同依赖清单重新下载对应平台的轮子，再拷贝到离线机器；归档工具运行期间本身不会发起任何外部请求。具体说明见 `wheelhouse/README.md`。

## 四、核心概念

### 4.1 以数据最大时间戳为基准

策略中的“最近 30 天”“30～90 天”不是相对机器当前时间，而是相对**当前输入数据的最大时间戳**。这个基准点由 `reference: data_max` 固定。这样做的原因是：离线环境可能没有可信时钟，历史数据回放也不应该因为运行日期不同而改变分层结果。相同输入多次运行，层级归属和输出哈希必须完全一致。

### 4.2 层级与保留周期

策略包含 1～5 个层级，按从新到旧排列。每层声明：

- `granularity`：该层的时间桶宽度，例如 `10s`、`1m`、`1h`、`1d`、`7d`。
- `retention_days`：该层覆盖的数据年龄上界。
- `path`：相对于工作目录的输出子目录。
- `format` 与 `compression`：Parquet、CSV 或 JSONL，以及相应压缩方式。
- `aggregations`：默认算子与逐指标覆盖算子。
- `gap_strategy`：缺失时间桶是跳过、补空还是前值填充。

粒度和保留周期都必须严格递增。最末一层就是归档层，其输出会被封装成确定性的 `tar.gz`，并建立归档包内偏移索引与标签索引。

### 4.3 确定性降采样

降采样以“标签组合 + 时间桶”为分组键，使用 `floor` 语义对齐桶边界。七个内置聚合算子为 `mean`、`max`、`min`、`count`、`first`、`last`、`p95`。异常值在读取阶段按策略阈值识别，不参与指标聚合，但会计入质量标记。

每个输出行包含 `raw_count`、`valid_count`、`missing_count`、`anomaly_count`。其中 `missing_count` 表示按源数据推断间隔和当前层粒度计算出的缺失槽位数，用于后续评估数据完整度。

### 4.4 热、温、冷、归档

默认策略提供四级：

| 层级 | 粒度 | 覆盖范围 | 用途 |
| --- | --- | --- | --- |
| hot | 10s | 最近 30 天 | 高频排障、原始节奏排查 |
| warm | 1m | 30～90 天 | 分钟级趋势与关联分析 |
| cold | 1h | 90～365 天 | 小时级容量与周期分析 |
| archive | 1d | 365 天以后 | 长期留存、低频追溯 |

需要注意，“热层保留原始粒度”是指策略粒度设为源数据粒度；工具仍然执行聚合，只是聚合后的时间桶不会比源数据更粗。

### 4.5 分片与内容寻址

每个层级按自然月分片。文件名由“数据源标识 + 层级 + 月份 + 内容 SHA256 前缀”组成，因此：

- 相同内容重复运行会命中已有文件，不产生冗余副本。
- 内容变化时新旧文件可以同时存在，便于回滚和历史对照。
- 文件名本身携带内容指纹，人工排查时能快速发现异常替换。

### 4.6 两级索引

归档包的一级索引记录包内数据成员的字节偏移、长度、最小与最大时间戳；二级索引记录标签键值与分片的关系。检索时先按时间范围和标签缩小候选包，再按偏移定点提取，不需要解压整个归档包。索引数据持久化在 SQLite 中，归档包内同时保存一份 `index.json` 作为自描述清单。

### 4.7 两阶段提交与断点续跑

写入流程为：先写 staging 临时文件，校验序列化结果或归档包完整性，再通过原子替换移动到正式路径，最后更新 SQLite 分片指针与索引。任务状态和阶段会写入元数据库；异常时任务标记为失败并记录错误。重复运行会优先检查已完成任务，并核对输出文件仍存在且哈希一致，只有在输出被删除或回滚后才重新执行。

### 4.8 哈希链审计

审计日志采用 JSONL 追加写，每条记录包含序号、时间、操作、执行来源、任务标识、范围、输入哈希、输出哈希、结果、错误与详情。每条记录的 `prev_hash` 指向上一条记录的 `entry_hash`，首条记录的前序哈希为空串。修改任何历史字段都会使后续哈希链校验失败。审计导出支持 JSON 与 Markdown。

### 4.9 标签组合基数与稀疏度预检

不同数据源的标签基数差异很大：同一层同一月份的分片里，稀疏组合可能被密集组合的全局采样节奏稀释，`missing_count` 等质量标记失去参考意义。预检在正式归档之前，对每个层级按自然月推算三个指标：

- `combinations`：该层该月出现的标签组合数。
- `buckets`：该层该月按层级粒度对齐后的时间桶数。
- `avg_buckets_per_combo`：每个组合平均覆盖的时间桶数（稀疏度）。

当任一月份的组合数超过 `precheck.max_combinations`，或每组合平均覆盖桶数低于 `precheck.min_avg_bucket_coverage`（过稀疏）时，该层的聚合方式从全局分组切换为**分片内按组合局部聚合**：每个标签组合使用自身节奏推断理论槽位，再独立聚合。局部聚合不改变 `raw_count`、`valid_count`、`missing_count`、`anomaly_count` 四个质量列的口径与类型，仅让 `missing_count` 的理论槽位按组合自身节奏计算。

局部聚合的分片会在分片元数据中记录一张**组合覆盖率表**（每个组合的覆盖桶数与有效点数），可直接对原始数据回算核对。覆盖率表持久化在 SQLite 的 `combo_coverage` 表；归档层分片同时在包内附加 `coverage.json` 成员，并在 `index.json` 清单与 `archive_index` 条目中声明，旧版本没有覆盖率表的分片与归档包仍可正常检索、恢复。

预检入口有两处，均不写入任务表、不占用归档任务序号：

- `precheck` 子命令：对任意数据源独立执行预检，报告确定性落盘到 `state/precheck/`，同一输入重复执行逐字节一致，结论写入审计链（`precheck` 操作）。
- 策略中的 `precheck` 配置段：`enabled: true` 时归档流程自动执行预检，`auto_apply: true` 时把超限层切换为局部聚合；预检未超限或配置缺席时，归档输出与未启用预检完全一致。

## 五、配置详解

默认策略位于 `config/default_policy.yaml`，示例策略位于 `config/example_policies/`。以下是一个最小可运行示例：

```yaml
version: 1
name: my_local_policy
reference: data_max
quality:
  value_ranges:
    temperature: {min: -50, max: 150}
    humidity: {min: 0, max: 100}
tiers:
  - name: hot
    granularity: 10s
    retention_days: 30
    path: tiers/hot
    format: parquet
    compression: zstd
    aggregations: {default: mean, vibration: max}
    gap_strategy: skip
  - name: warm
    granularity: 1m
    retention_days: 90
    path: tiers/warm
    format: parquet
    compression: zstd
    aggregations: {default: mean}
    gap_strategy: null
  - name: archive
    granularity: 1d
    retention_days: 730
    path: archives
    format: parquet
    compression: gzip
    aggregations: {default: mean, temperature: max}
    gap_strategy: skip
```

字段说明：

- `version`：当前仅支持整数 `1`。
- `name`：非空策略名称，参与任务标识计算。
- `reference`：仅允许 `data_max`，用于保证离线结果可复现。
- `quality.value_ranges`：可选指标阈值表；越界值标记为异常并排除出聚合，但不删除原始行。
- `tiers`：1～5 层，名称与路径不得重复，粒度和保留天数必须严格递增。
- `format`：`parquet`、`csv`、`jsonl`；输入支持 `parquet`、`csv`、`sqlite`。
- `compression`：Parquet 支持 `zstd`、`snappy`、`gzip`、`none`；文本格式支持 `none`、`gzip`。
- `aggregations`：必须包含 `default`，指标名可覆盖默认算子。
- `gap_strategy`：`skip` 不生成缺失桶，`null` 生成空值行，`ffill` 用同标签前一个有效值填充。
- `path`：必须是工作目录内的相对路径，禁止绝对路径和 `..` 越界。
- `precheck`：可选预检配置段，缺席时预检完全关闭且策略哈希与历史版本一致。子字段：
  - `enabled`：布尔值，默认 `false`；为 `true` 时归档流程自动执行预检并写审计。
  - `max_combinations`：正整数，默认 `5000`；单层单自然月标签组合数上限。
  - `min_avg_bucket_coverage`：非负数字，默认 `2.0`；每组合平均覆盖桶数低于该值视为过稀疏。
  - `auto_apply`：布尔值，默认 `true`；为 `true` 时超限层自动切换为局部聚合，为 `false` 时只出报告不改行为。

预检配置段存在时会参与策略哈希，因此调整阈值会形成新的任务标识；`precheck` 段不改变 `tiers` 的任何字段。层级窗口边缘的不完整月份可能只有很少时间桶，容易触发稀疏度判定，可按数据特点调低 `min_avg_bucket_coverage` 或将其设为 `0` 关闭稀疏度判定。

执行 `time-archiver --workdir demo_work validate-config` 可以校验工作目录中的策略；使用 `--config` 指定其他 YAML。校验失败会输出字段级错误并写入失败审计记录。

## 六、CLI 手册

在安装后使用 `time-archiver`，或在项目根目录使用 `python -m time_archiver`。以下命令均支持 `--workdir` 指定独立工作目录。

| 命令 | 作用 |
| --- | --- |
| `init` | 初始化工作目录、默认策略、审计与状态目录 |
| `load-demo` | 校验并列出三组内置演示数据 |
| `validate-config` | 校验策略字段、粒度、保留周期和路径 |
| `archive` | 对单个 Parquet、CSV 或 SQLite 源执行分层归档 |
| `precheck` | 归档前预检标签组合基数与稀疏度，输出确定性报告 |
| `demo` | 一键跑通加载、归档、索引、检索、恢复与审计验证 |
| `schedule` | 在本进程内按固定间隔重复归档演示数据 |
| `tasks` | 查询任务状态、阶段、质量和输出信息 |
| `search` | 按时间范围与标签组合检索冷归档索引 |
| `restore` | 按索引分片恢复，或从指定包定点提取成员 |
| `rollback` | 回滚最近或指定任务，归档包保留 |
| `audit-export` | 导出 JSON 或 Markdown 审计日志 |
| `web` | 启动仅绑定 127.0.0.1 的本地 Web 服务 |
| `test` | 调用 pytest 运行全量测试 |

归档示例：

```bash
time-archiver --workdir demo_work archive --source ./data/metrics.parquet \
  --format parquet --timestamp-column timestamp \
  --label-columns sensor_id,metric_group \
  --metric-columns temperature,humidity,pressure \
  --source-id edge-a
```

SQLite 输入默认只读打开，需通过 `--table` 指定表名。CSV 输入使用 UTF-8 文本。时间列支持常见日期时间字符串、Unix 秒和毫秒时间戳；解析失败的行会被计入类型错误，严重质量问题时任务失败且不会写入正式索引。

预检示例（与 `archive` 共享输入参数，不写任务表，报告落盘 `state/precheck/`）：

```bash
time-archiver --workdir demo_work precheck --source ./data/metrics.parquet \
  --format parquet --label-columns sensor_id,metric_group \
  --metric-columns temperature,humidity,pressure --source-id edge-a
```

检索示例：

```bash
time-archiver --workdir demo_work search \
  --start 2024-01-01 --end 2025-12-31 \
  --label-key sensor_id --label-value sensor-long --limit 20
```

恢复示例：

```bash
time-archiver --workdir demo_work restore --shard-id <分片标识> --out ./restored
```

回滚示例：

```bash
time-archiver --workdir demo_work rollback --task-id <任务标识>
```

回滚只会撤销分片指针与索引，不会删除归档包，也不会删除或修改任何原始输入。若没有更早版本可恢复，分片会标记为不活跃，归档包仍然留在磁盘上供人工核查。

## 七、Web 本地控制台

启动命令：

```bash
time-archiver --workdir demo_work web --port 8765
```

服务仅绑定 `127.0.0.1`，不提供登录、多租户或远程访问能力。页面使用原生 HTML、CSS 和 JavaScript，不引用外部字体、脚本、样式或图片。

页面功能包括：

- 任务页：查看任务历史、状态、阶段、层级统计与质量报告；选择内置演示源或自定义本地文件触发归档。
- 策略页：直接编辑 YAML，提交前执行完整校验；校验通过后原子保存并重建归档服务。
- 归档页：按时间范围与标签检索索引，下载恢复样本。
- 审计页：按操作、来源、结果过滤，导出 JSON 或 Markdown。

Web 测试通过 Flask 测试客户端完成，不监听真实端口，不产生外部访问。

## 八、演示数据

`fixtures/` 中包含三组确定性生成的 mock 环境传感器数据：

- 高频边缘数据：10 秒级，保留 114912 行，覆盖 2025-12-18 至 2025-12-31。
- 中频服务器数据：1 分钟级，保留 125856 行，覆盖 2025-10-01 至 2025-12-31。
- 低频 IoT 数据：1 小时级，保留 9029 行，覆盖 2024-12-01 至 2025-12-31。

三组数据合计跨度为 13 个月。生成过程会移除约 5% 的随机采样行，并在保留行中按阈值注入约 2% 的异常值。字段为 `timestamp`、`sensor_id`、`metric_group`、`temperature`、`humidity`、`pressure`。所有数据均为合成数据，不对应任何真实设备、组织或地点。更完整的生成规则和校验和见 `fixtures/README.md`。

## 九、测试与质量

测试使用 pytest，所有工作目录都位于 pytest 临时目录，测试结束自动清理。Web 测试使用 Flask 测试客户端，不监听真实端口。覆盖范围包括：

- 配置解析、字段级错误、路径越界、保存失败回滚。
- 三种输入格式、只读 SQLite、时间解析、重复时间戳、异常阈值、质量报告。
- 七种聚合算子、三种缺失策略、多标签补桶、异常排除和质量标记。
- 分层归档、内容寻址、幂等重跑、缓存命中、输出缺失后的安全重建、跨工作目录一致性。
- 归档包序列化、确定性 tar.gz、索引偏移、包校验、定点提取、损坏包处理。
- 元数据表、分片历史、索引替换、回滚指针恢复。
- 审计字段完整性、筛选、哈希链篡改检测、双格式导出。
- CLI 全命令链路与错误路径，Web 页面、策略编辑、任务触发、检索、下载和导出。
- 网络完全阻断时归档和 Web 页面仍可工作。
- 空数据、单行数据、只有标签、无异常列、非法时间、单层级策略、空层级、未完成任务回滚等边界。

最近一次完整测试结果为 123 项通过，整体语句覆盖率为 95%。核心模块覆盖率：降采样 100%，归档 99%，索引构建 99%，元数据存储 97%。执行 `python -m pytest` 会自动启用覆盖率门槛，整体低于 85% 时测试命令返回失败。

## 十、数据安全与操作边界

- 运行期间不发起任何出站网络请求；Web 服务仅绑定回环地址。
- 原始输入文件全程只读，不删除、不覆盖、不改写。
- 所有派生输出只写入用户指定的独立工作目录。
- 不提供自动删除原始数据或归档数据的能力；回滚只调整指针和索引。
- 归档文件采用内容寻址命名，出现异常时保留新旧版本供人工比对。
- 审计日志只追加，不提供通过应用修改历史记录的接口。
- 不收集遥测，不包含账号、密钥、真实地址或企业标识。
- 所有时间戳统一按 UTC 处理，避免本地时区差异破坏可复现性。

## 十一、常见问题

**问：为什么我的某些层级输出行数为零？**

答：说明该数据源在对应年龄区间没有数据。例如数据只覆盖最近两天，则温层、冷层和归档层都可能为空；任务结果会明确记录零行层级，不会伪造数据。

**问：重复执行会重复占磁盘吗？**

答：不会。相同策略、相同源哈希、相同内容会生成相同文件名，重复运行会复用已有文件；任务层面也会命中缓存，除非输出文件丢失或被回滚。

**问：修改策略后历史归档会怎样？**

答：策略哈希改变会产生新的任务标识。旧归档包不会被自动删除，新任务会按新策略生成新的分片文件；是否需要人工清理旧版本由使用者决定。

**问：异常值会被删除吗？**

答：不会。异常值只在聚合阶段被排除，并计入 `anomaly_count`；原始输入保持不变，归档数据中的异常计数用于后续质量分析。

**问：能否处理没有标签列的数据？**

答：可以。标签列允许为空，此时按全局时间桶聚合。CLI 中通过空的 `--label-columns` 传入空字符串即可。

**问：能否用于实时流？**

答：不能。时档匠是批处理归档工具，每次针对一个已落盘的本地文件快照执行。实时采集、消息队列和流计算不在范围内。

**问：能否删除原始数据？**

答：不能。工具默认仅做生命周期标记和降质归档，任何删除都需要使用者在本工具之外人工确认并执行。

## 十二、已知边界

- 不提供分布式任务协调，多个进程同时写同一工作目录时应由外部保证串行。
- 不提供实时监控、告警、可视化大盘或采集代理。
- 不支持对象存储、云数据库和远程传输。
- 不提供自定义 Python 算子插件；聚合算子固定为七种内置实现。
- 不处理非时序数据、关系建模、全文检索或图查询。
- 归档包的确定性依赖固定版本依赖栈；跨 pandas、pyarrow 大版本迁移后应重新建立基线并复核哈希。
- 调度命令是前台进程内调度，不安装系统服务，不修改操作系统计划任务。

## 十三、目录结构

```text
config/                 默认策略与三个场景示例
fixtures/               mock 数据、清单与生成说明
src/time_archiver/      核心库、CLI、Web 与模板静态资源
tests/                  单元、集成、边界、幂等、离线与隔离测试
tools/                  fixture 可复现生成脚本
wheelhouse/             离线依赖轮子与安装说明
README.md               本说明
ARCHITECTURE.md         架构、流程、数据模型与设计决策
CHANGELOG.md            版本变更记录
```

## 十四、免责声明

本工具用于本地时序数据的生命周期整理与降质归档。使用者应在正式环境执行前，在副本或测试目录验证策略边界、磁盘容量、时间解析方式和恢复流程。工具默认不删除原始数据，但归档输出仍需纳入本地备份与巡检制度。因策略配置不当、存储介质故障、依赖版本变化或未按流程人工确认造成的后果，由使用者自行承担。
