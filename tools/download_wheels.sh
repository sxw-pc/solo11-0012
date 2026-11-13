#!/usr/bin/env bash
# 在联网的准备机上按锁定清单重新下载离线轮子。
#
# 同一套 requirements.txt + wheelhouse/constraints.txt 决定版本，平台只决定
# 下载哪一套二进制轮子；因此联网重下与离线安装得到的是同一组确切版本。
#
# 用法：
#   tools/download_wheels.sh                 # 默认下载 Linux x86_64 / cp311
#   tools/download_wheels.sh windows         # 下载 Windows x86_64 / cp311
#   tools/download_wheels.sh linux           # 下载 Linux x86_64 / cp311
#
# 可通过环境变量覆盖目标参数（换 Python 小版本或架构时使用）：
#   PY_VERSION=310 ABI=cp310 PLATFORM=manylinux_2_28_x86_64 tools/download_wheels.sh
#
# 下载完成后请：
#   1. 在干净虚拟环境执行 pip install --no-index --find-links wheelhouse 验证；
#   2. 重新生成根目录 SHA256SUMS.txt（见 tools/make_checksums.sh）。
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WHEEL_DIR="$ROOT_DIR/wheelhouse"
PY_BIN="${PYTHON:-python3}"

TARGET="${1:-linux}"
case "$TARGET" in
    linux)
        PLATFORM="${PLATFORM:-manylinux_2_28_x86_64}"
        ;;
    windows|win)
        PLATFORM="${PLATFORM:-win_amd64}"
        ;;
    *)
        PLATFORM="$TARGET"
        ;;
esac

PY_VERSION="${PY_VERSION:-311}"
IMPLEMENTATION="${IMPLEMENTATION:-cp}"
ABI="${ABI:-cp${PY_VERSION}}"

echo "下载目标：platform=$PLATFORM python=$PY_VERSION abi=$ABI"
echo "输出目录：$WHEEL_DIR"

# 先装构建期依赖本身需要的轮子，再装运行/测试依赖；-c 锁定全部传递依赖。
"$PY_BIN" -m pip download -d "$WHEEL_DIR" --only-binary=:all: \
    --platform "$PLATFORM" --python-version "$PY_VERSION" \
    --implementation "$IMPLEMENTATION" --abi "$ABI" \
    -c "$WHEEL_DIR/constraints.txt" \
    setuptools wheel

"$PY_BIN" -m pip download -d "$WHEEL_DIR" --only-binary=:all: \
    --platform "$PLATFORM" --python-version "$PY_VERSION" \
    --implementation "$IMPLEMENTATION" --abi "$ABI" \
    -c "$WHEEL_DIR/constraints.txt" \
    -r "$ROOT_DIR/requirements.txt"

echo "完成。当前 wheelhouse 内容："
ls -1 "$WHEEL_DIR"/*.whl
