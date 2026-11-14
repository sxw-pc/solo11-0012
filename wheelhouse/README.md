# 离线依赖轮子说明

本目录用于在无网络的机器上安装时档匠的运行与测试依赖。

## 当前内容

- 目标平台：Windows x86_64。
- 目标解释器：CPython 3.11。
- 已包含运行依赖、测试依赖、传递依赖，以及构建所需的 setuptools 与 wheel。
- 所有文件均为二进制轮子，不包含源码分发包。

离线安装命令在项目根目录执行：

```bash
python -m pip install --no-index --find-links wheelhouse setuptools wheel -r requirements.txt
python -m pip install --no-index --no-build-isolation \
  --config-settings editable_mode=compat -e .
```

`--no-index` 禁止 pip 访问包索引，`--find-links wheelhouse` 只使用本目录中的文件。`--no-build-isolation` 让项目构建复用已经安装的 setuptools，避免构建阶段尝试联网。`editable_mode=compat` 用于绕开部分 setuptools 版本在可编辑构建时报告元数据目录为空的问题。

安装前可先运行部署自检。它会扫描本目录的 wheel 文件名标签（Python、ABI、平台）与 wheel 内 `METADATA` 的 `Requires-Dist`，对 `requirements.txt` 做完整依赖闭包解析，并明确指出缺失的依赖或不兼容的轮子：

```bash
time-archiver --workdir demo_work preflight --strict
```

严格模式下，只要任一依赖在当前解释器上无法由本目录满足，自检会以非零退出码结束，而不会等到归档执行中途才失败。若本目录与目标平台不匹配（例如在 Linux 上使用 Windows 轮子），该检查会直接列出这些轮子及其标签。

## 其他平台的准备方式

轮子与平台和 Python 小版本相关。若部署目标是 Linux、macOS 或不同的 Python 版本，需要在一台可联网的准备机上执行与目标平台对应的下载命令，再将整个 `wheelhouse` 复制到离线机器。准备阶段可以联网，归档工具运行阶段不会联网。

示例命令中的平台、Python 版本和 ABI 应按目标环境替换：

```bash
python -m pip download -d wheelhouse --only-binary=:all: \
  --platform win_amd64 --python-version 311 --implementation cp --abi cp311 \
  -r requirements.txt
```

Python 3.9 或 3.10 需要把 `python-version` 和 `abi` 改为对应值。Linux 与 macOS 需要改为对应平台标签。下载完成后，应在离线机器上使用 `pip install --no-index` 做一次空环境安装验证。

## 完整性

本目录不包含任何私有索引凭据、账号或真实服务地址。安装过程完全本地化。轮子清单会随依赖更新而变化，发布前应重新生成项目根目录的校验清单。
