#!/usr/bin/env bash
# 重新生成根目录 SHA256SUMS.txt。
#
# 枚举所有受 git 跟踪、以及未被 .gitignore 忽略的新文件（自动排除
# __pycache__、egg-info、虚拟环境等产物），按相对路径排序后计算二进制
# SHA256，输出格式与历史清单一致：<64 位哈希> *<相对路径>。
# 清单文件自身不计入，避免自引用。
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MANIFEST="SHA256SUMS.txt"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

git ls-files --cached --others --exclude-standard \
    | grep -v "^${MANIFEST}$" \
    | LC_ALL=C sort \
    | while IFS= read -r path; do
        # 只对普通文件计算，跳过目录或失效符号链接。
        [ -f "$path" ] || continue
        sha256sum "$path"
    done > "$TMP"

mv "$TMP" "$MANIFEST"
trap - EXIT
echo "已生成 $MANIFEST，共 $(wc -l < "$MANIFEST") 条记录。"
