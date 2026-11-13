# 离线依赖轮子说明

本目录用于在无网络的机器上安装时档匠的运行与测试依赖。

## 当前内容

- 目标解释器：CPython 3.11。
- 同时包含两套平台的二进制轮子：
  - Windows x86_64（`win_amd64` / `cp311`）。
  - Linux x86_64（`manylinux_2_28_x86_64` 等 `cp311` 标签，兼容 glibc 2.28 及以上）。
- 已包含运行依赖、测试依赖、传递依赖，以及构建所需的 setuptools 与 wheel。
- 所有文件均为二进制轮子，不包含源码分发包。
- `constraints.txt` 是全量传递依赖的精确版本约束，与 `requirements.txt`、
  本目录下的实际轮子文件名三者一一对应。

## 版本锁定策略

`requirements.txt` 只写直接依赖的精确版本（`包名==版本`），不再使用范围区间；
`constraints.txt` 把传递依赖也钉到经离线验证的确切版本。这样任何机器、任何
时间重新解析依赖图，得到的都是同一组版本，pandas 不会再静默滚动到未适配的新版本。

当前锁定的核心数据栈：

| 包 | 版本 |
| --- | --- |
| pandas | 2.3.3 |
| numpy | 2.4.6 |
| pyarrow | 19.0.1 |
| PyYAML | 6.0.3 |

`pyproject.toml` 的 `dependencies` 与上述清单保持一致，避免包元数据与实际
轮子内容脱节。

## 离线安装

在项目根目录执行：

```bash
python -m pip install --no-index --find-links wheelhouse \
  setuptools wheel -c wheelhouse/constraints.txt -r requirements.txt
python -m pip install --no-index --no-build-isolation \
  --config-settings editable_mode=compat -e .
```

`--no-index` 禁止 pip 访问包索引，`--find-links wheelhouse` 只使用本目录中的文件，
`-c wheelhouse/constraints.txt` 锁定全部传递依赖。`--no-build-isolation` 让项目
构建复用已经安装的 setuptools，避免构建阶段尝试联网。`editable_mode=compat`
用于绕开部分 setuptools 版本在可编辑构建时报告元数据目录为空的问题。

pip 会根据当前解释器平台自动在两套轮子中选择匹配的那一套，无需手工区分目录。

## 联网机器重新下载（可复现）

轮子与平台和 Python 小版本相关。若部署目标是其他平台或其他 Python 版本，需要
在一台可联网的准备机上按同一份锁定清单下载对应轮子，再将整个 `wheelhouse`
复制到离线机器。准备阶段可以联网，归档工具运行阶段不会联网。

仓库提供脚本固化该流程，默认下载 Linux x86_64 / cp311：

```bash
tools/download_wheels.sh           # Linux manylinux_2_28_x86_64 / cp311
tools/download_wheels.sh windows   # Windows win_amd64 / cp311
```

脚本内部等价于下面的命令（平台、Python 版本、ABI 按目标环境替换）：

```bash
python -m pip download -d wheelhouse --only-binary=:all: \
  --platform manylinux_2_28_x86_64 --python-version 311 \
  --implementation cp --abi cp311 \
  -c wheelhouse/constraints.txt -r requirements.txt
```

由于版本由 `requirements.txt` 与 `constraints.txt` 固定，联网重下与离线安装
解析出的是同一组确切版本，只是二进制平台标签不同。下载完成后应：

1. 在干净虚拟环境用 `pip install --no-index --find-links wheelhouse` 做空环境验证；
2. 重新生成项目根目录的校验清单：`tools/make_checksums.sh`。

Python 3.9/3.10 等其他小版本需要把 `python-version` 和 `abi` 改为对应值，并同步
评估 pandas/pyarrow 是否提供对应轮子。

## 完整性

本目录不包含任何私有索引凭据、账号或真实服务地址。安装过程完全本地化。
轮子清单会随依赖更新而变化，发布前应重新生成项目根目录的 `SHA256SUMS.txt`。
