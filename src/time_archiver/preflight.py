"""部署自检：离线轮子、策略配置、工作目录、分片指纹与残留清理。

设计约定：

- 自检本身只读。默认调用不会初始化工作目录、复制策略或写入元数据库，
  唯一新增内容是对既有哈希链审计日志的追加记录。因此连续两次同参数自检，
  审计日志之外的文件集合和内容保持不变。
- 自检结果使用独立操作类型 ``preflight`` 追加到现有审计链（复用
  ``AuditLog.log`` 入口），但不会创建元数据库任务行，也不会占用正式归档
  任务的序号或任务标识。哈希链必须保持单链，因此沿用同一个审计文件。
- 残留的 staging 临时文件、原子写临时文件和上次失败/中断任务属于
  “待清理”状态，记为警告而不是失败。只有显式传入 cleanup 时才会把文件
  移入隔离目录；隔离清单可被 restore 动作反向恢复。
"""
from __future__ import annotations

import email
import json
import os
import platform
import re
import sqlite3
import sys
import sysconfig
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .audit import AuditLog
from .config_loader import PROJECT_ROOT, load_policy
from .schema import ConfigError, canonical_hash, canonical_json, file_sha256

PREFLIGHT_OPERATION = "preflight"
DEFAULT_WHEELHOUSE = PROJECT_ROOT / "wheelhouse"
DEFAULT_REQUIREMENTS = PROJECT_ROOT / "requirements.txt"
DEFAULT_CONFIG_ROOT = PROJECT_ROOT / "config"
QUARANTINE_ROOT = ("state", "preflight_quarantine")
EXPECTED_DB_TABLES = {"tasks", "shards", "shard_history", "archive_index", "label_index"}
WRITE_PROBE_PREFIX = ".preflight_"
_NAME_RE = re.compile(
    r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[([^\]]*)\])?\s*(.*)$")
_SPEC_RE = re.compile(r"^(===|~=|==|!=|<=|>=|<|>)\s*([^,; ]+)")


class PreflightError(Exception):
    """自检或清理动作无法完成（不同于某项被检查事实不通过）。"""


def _check(name: str, status: str, summary: str,
           details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"name": name, "status": status, "summary": summary,
            "details": details or {}}


# ---------------------------------------------------------------------------
# 版本、依赖说明与环境标记
# ---------------------------------------------------------------------------

def _version_key(value: str) -> Tuple[int, ...]:
    text = str(value).strip().lower()
    if text.startswith("v"):
        text = text[1:]
    text = text.split("+", 1)[0]
    numbers = re.findall(r"\d+", text)
    return tuple(int(item) for item in numbers) or (0,)


def _compare_versions(left: str, right: str) -> int:
    first, second = _version_key(left), _version_key(right)
    width = max(len(first), len(second))
    first += (0,) * (width - len(first))
    second += (0,) * (width - len(second))
    return (first > second) - (first < second)


def _tilde_upper(target: str) -> str:
    """``~=X.Y.Z`` 的上界是 ``X.(Y+1)``，``~=X.Y`` 的上界是 ``(X+1)``。"""
    release = list(_version_key(target))
    if len(release) <= 1:
        return str(release[0] + 1)
    base = release[:-1]
    base[-1] += 1
    return ".".join(str(item) for item in base)


def _parse_spec_set(text: str) -> List[Tuple[str, str]]:
    output = []
    for token in (text or "").split(","):
        match = _SPEC_RE.match(token.strip())
        if match:
            output.append((match.group(1), match.group(2)))
    return output


def _satisfies(version: str, specifiers: Sequence[Tuple[str, str]]) -> bool:
    """按常见的 PEP 440 约束判断版本；未知约束按不满足处理。"""
    for operator, target in specifiers:
        target = str(target).strip()
        if operator == "===":
            if str(version) != target:
                return False
            continue
        if target.endswith(".*"):
            prefix = _version_key(target[:-2])
            key = _version_key(version)
            if key[: len(prefix)] != prefix:
                return False
            continue
        if operator == "==":
            if _compare_versions(version, target) != 0:
                return False
        elif operator == "!=":
            if _compare_versions(version, target) == 0:
                return False
        elif operator == ">=":
            if _compare_versions(version, target) < 0:
                return False
        elif operator == "<=":
            if _compare_versions(version, target) > 0:
                return False
        elif operator == ">":
            if _compare_versions(version, target) <= 0:
                return False
        elif operator == "<":
            if _compare_versions(version, target) >= 0:
                return False
        elif operator == "~=":
            if (_compare_versions(version, target) < 0
                    or _compare_versions(version, _tilde_upper(target)) >= 0):
                return False
    return True


def _normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", str(name)).strip().lower()


def _split_requirement(text: str) -> Optional[Dict[str, Any]]:
    """拆分 wheel 元数据或 requirements 行中的一条依赖。"""
    text = (text or "").strip()
    if not text or text.startswith("#"):
        return None
    marker = ""
    if ";" in text:
        text, marker = text.split(";", 1)
        marker = marker.strip()
    match = _NAME_RE.match(text)
    if not match:
        return None
    name, extras, remainder = match.group(1), match.group(2) or "", match.group(3).strip()
    return {"name": _normalize_name(name),
            "specifier": _parse_spec_set(remainder),
            "raw": text.strip(), "marker": marker,
            "extras": {item.strip() for item in extras.split(",") if item.strip()}}


_MARKER_TOKEN = re.compile(
    r"\s*(===|~=|==|!=|<=|>=|<|>|\(|\)|and\b|or\b|not\b|"
    r"'[^']*'|\"[^\"]*\"|[A-Za-z_][A-Za-z0-9_.]*)")
_MARKER_ENV = {
    "python_version": lambda: f"{sys.version_info.major}.{sys.version_info.minor}",
    "python_full_version": lambda: platform.python_version(),
    "sys_platform": lambda: sys.platform,
    "platform_machine": lambda: platform.machine(),
    "platform_system": lambda: platform.system(),
    "os_name": lambda: os.name,
    "implementation_name": lambda: sys.implementation.name,
    "extra": lambda: "",
}


def _marker_tokens(text: str) -> List[str]:
    tokens, position = [], 0
    while position < len(text):
        match = _MARKER_TOKEN.match(text, position)
        if not match:
            position += 1
            continue
        tokens.append(match.group(1))
        position = match.end()
    return tokens


def _marker_value(token: str) -> Tuple[str, bool]:
    if token.startswith(("'", '"')) and token.endswith(("'", '"')):
        return token[1:-1], True
    return _MARKER_ENV.get(token, lambda: token)(), False


def _marker_compare(text: str) -> bool:
    """朴素但保守的 PEP 508 标记求值：解析失败时返回 True（保留依赖）。"""
    tokens = _marker_tokens(text)
    index = 0

    def peek() -> str:
        return tokens[index] if index < len(tokens) else ""

    def parse_primary() -> bool:
        nonlocal index
        if peek() == "not":
            index += 1
            return not parse_primary()
        if peek() == "(":
            index += 1
            value = parse_or()
            if peek() == ")":
                index += 1
            return value
        left, left_quoted = _marker_value(peek())
        index += 1
        operator = peek()
        index += 1
        right, right_quoted = _marker_value(peek())
        index += 1
        return _compare_marker_values(left, operator, right,
                                      left_quoted or right_quoted)

    def parse_and() -> bool:
        nonlocal index
        value = parse_primary()
        while peek() == "and":
            index += 1
            value = parse_primary() and value
        return value

    def parse_or() -> bool:
        nonlocal index
        value = parse_and()
        while peek() == "or":
            index += 1
            value = parse_and() or value
        return value

    try:
        result = parse_or()
        return result
    except (IndexError, ValueError):
        return True


def _compare_marker_values(left: str, operator: str, right: str,
                           quoted: bool) -> bool:
    if operator in {"===", "~=", "==", "!=", "<=", ">=", "<", ">"}:
        if _looks_version(left) and _looks_version(right):
            if operator in {"==", "!="}:
                equal = _compare_versions(left, right) == 0
                return equal if operator == "==" else not equal
            return _satisfies(left, [(operator, right)])
        if operator == "==":
            return left == right
        if operator == "!=":
            return left != right
        if operator == "<":
            return left < right
        if operator == ">":
            return left > right
        if operator == "<=":
            return left <= right
        if operator == ">=":
            return left >= right
    return True


def _looks_version(value: str) -> bool:
    return bool(re.fullmatch(r"v?\d+(?:\.\d+)+", str(value).strip()))


def _marker_allows(marker: str, extras: Optional[set] = None) -> bool:
    """判断依赖标记是否适用于当前环境；显式 extra 约束按不适用处理。"""
    if not marker:
        return True
    if re.search(r"\bextra\b", marker):
        return False
    return _marker_compare(marker)


# ---------------------------------------------------------------------------
# wheel 文件名、平台标签与元数据
# ---------------------------------------------------------------------------

def _parse_wheel_filename(path: Path) -> Optional[Dict[str, Any]]:
    if not path.name.endswith(".whl"):
        return None
    stem = path.name[:-4]
    parts = stem.split("-")
    if len(parts) < 5:
        return None
    python_tag, abi_tag, platform_tag = parts[-3], parts[-2], parts[-1]
    head = parts[:-3]
    if len(head) >= 3 and re.fullmatch(r"\d+", head[-1]):
        head = head[:-1]
    if len(head) < 2:
        return None
    version = head[-1]
    name = "-".join(head[:-1])
    return {"name": name, "normalized": _normalize_name(name),
            "version": version, "python": python_tag, "abi": abi_tag,
            "platform": platform_tag, "path": path}


def _machine_aliases(machine: str) -> Tuple[str, ...]:
    machine = machine.lower()
    table = {
        "x86_64": ("x86_64", "amd64"),
        "amd64": ("amd64", "x86_64"),
        "aarch64": ("aarch64", "arm64"),
        "arm64": ("arm64", "aarch64"),
        "i386": ("i386", "i686", "x86"),
        "i686": ("i686", "i386", "x86"),
        "armv7l": ("armv7l", "armv7"),
    }
    return table.get(machine, (machine,))


def _current_machine() -> str:
    return platform.machine().lower()


def _platform_tag_compatible(tag: str) -> bool:
    tag = tag.lower()
    if tag == "any":
        return True
    system = platform.system().lower()
    machine = _current_machine()
    aliases = _machine_aliases(machine)
    if system == "windows":
        if not tag.startswith("win"):
            return False
        if machine in {"x86_64", "amd64"}:
            # x86_64 Windows 不能加载 arm64 轮子，只接受同架构标签。
            return tag.endswith(("_amd64", "_x86_64"))
        if machine in {"aarch64", "arm64"}:
            return tag.endswith("_arm64")
        return any(alias in tag for alias in aliases) or tag == "win32"
    if system == "linux":
        if not tag.startswith(("manylinux", "musllinux", "linux_")):
            return False
        return any(alias in tag for alias in aliases)
    if system == "darwin":
        if not tag.startswith("macosx_"):
            return False
        return any(alias in tag for alias in aliases) or "universal2" in tag
    return tag.replace("-", "_") == sysconfig.get_platform().lower().replace("-", "_")


def _python_tag_compatible(tag: str) -> bool:
    tag = tag.lower()
    major, minor = sys.version_info.major, sys.version_info.minor
    if tag in {f"cp{major}{minor}", f"py{major}{minor}", f"py{major}"}:
        return True
    if tag.startswith("cp") and tag[2:].isdigit():
        wheel_major, wheel_minor = int(tag[2]), int(tag[3:] or 0)
        return wheel_major == major and wheel_minor == minor
    return False


def _abi_tag_compatible(tag: str, python_tag: str) -> bool:
    tag = tag.lower()
    python_tag = python_tag.lower()
    current = f"cp{sys.version_info.major}{sys.version_info.minor}"
    if tag == "none":
        return True
    if tag == current:
        return True
    if tag == "abi3":
        if python_tag.startswith("cp") and python_tag[2:].isdigit():
            wheel_minor = int(python_tag[3:] or 0)
            return sys.version_info.major == 3 and wheel_minor <= sys.version_info.minor
        return sys.version_info.major == 3
    return False


def _wheel_is_compatible(info: Dict[str, Any]) -> bool:
    python_tags = [item for item in info["python"].lower().split(".") if item]
    abi_tags = [item for item in info["abi"].lower().split(".") if item]
    platform_tags = [item for item in info["platform"].lower().split(".") if item]
    for python_tag in python_tags:
        if not _python_tag_compatible(python_tag):
            continue
        for abi_tag in abi_tags:
            if not _abi_tag_compatible(abi_tag, python_tag):
                continue
            for platform_tag in platform_tags:
                if _platform_tag_compatible(platform_tag):
                    return True
    return False


def _wheel_requirements(path: Path) -> List[Dict[str, Any]]:
    try:
        with zipfile.ZipFile(path) as archive:
            metadata = next((name for name in archive.namelist()
                             if name.endswith(".dist-info/METADATA")), "")
            if not metadata:
                return []
            message = email.message_from_bytes(archive.read(metadata))
    except (OSError, zipfile.BadZipFile):
        return []
    requirements = []
    for item in message.get_all("Requires-Dist") or []:
        parsed = _split_requirement(item)
        if parsed:
            requirements.append(parsed)
    return requirements


def _scan_wheelhouse(wheelhouse: Path) -> List[Dict[str, Any]]:
    wheels = []
    if not wheelhouse.is_dir():
        return wheels
    for path in sorted(wheelhouse.iterdir(), key=lambda item: item.name):
        if not path.is_file() or not path.name.endswith(".whl"):
            continue
        info = _parse_wheel_filename(path)
        if not info:
            continue
        info["compatible"] = _wheel_is_compatible(info)
        info["requirements"] = _wheel_requirements(path)
        wheels.append(info)
    return wheels


def _strip_comment(line: str) -> str:
    """去掉 requirements 行尾注释，同时保留版本约束中的 #。"""
    if line.lstrip().startswith("#"):
        return ""
    return re.split(r"\s+#", line, maxsplit=1)[0].strip()


def _parse_requirements_lines(path: Path) -> List[Dict[str, Any]]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise PreflightError(f"依赖清单无法读取: {path}: {exc}") from exc
    output = []
    for line in text.splitlines():
        stripped = _strip_comment(line).strip()
        if not stripped or stripped.startswith(("-", "http:", "https:", "git+")):
            continue
        parsed = _split_requirement(stripped)
        if parsed and _marker_allows(parsed["marker"]):
            output.append(parsed)
    return output


def _candidate_key(info: Dict[str, Any]) -> Tuple[int, ...]:
    return _version_key(info["version"])


def resolve_wheelhouse(wheelhouse: Path, requirements: Path) -> Dict[str, Any]:
    """在当前解释器上解析离线依赖；汇总所有缺失与不兼容项。

    解析过程是一个固定点循环：每轮取当前约束下可满足的最高版本，把它的
    运行时依赖并入约束集合，直到版本选择不再变化。这样即使某个依赖先被
    一个较松的约束满足、随后又收到更紧的传递约束，也不会误报为缺失。
    `unresolved` 只保留在整个固定点结束后仍无法满足的依赖名。
    """
    wheels = _scan_wheelhouse(wheelhouse)
    by_name: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    incompatible: List[Dict[str, Any]] = []
    for info in wheels:
        if info["compatible"]:
            by_name[info["normalized"]].append(info)
        else:
            incompatible.append({
                "wheel": info["path"].name,
                "python": info["python"], "abi": info["abi"],
                "platform": info["platform"],
            })
    for items in by_name.values():
        items.sort(key=_candidate_key, reverse=True)

    if not Path(requirements).is_file():
        direct = []
        requirements_error = f"依赖清单不存在: {requirements}"
    else:
        direct = _parse_requirements_lines(Path(requirements))
        requirements_error = ""

    resolved: Dict[str, Dict[str, Any]] = {}
    failures: List[Dict[str, Any]] = []
    constraints: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    reasons: Dict[str, str] = {}
    pending: List[Dict[str, Any]] = list(direct)

    for _round in range(20):
        changed = False
        queue = list(pending)
        pending = []
        while queue:
            requirement = queue.pop(0)
            name = requirement["name"]
            new_specs = [item for item in requirement["specifier"]
                         if item not in constraints[name]]
            if new_specs:
                constraints[name].extend(new_specs)
                changed = True
            candidates = by_name.get(name, [])
            matching = [item for item in candidates
                        if _satisfies(item["version"], constraints[name])]
            if not matching:
                reasons.setdefault(
                    name, _resolution_reason(name, constraints[name], candidates, wheels))
                continue
            reasons.pop(name, None)
            selected = matching[0]
            already = resolved.get(name)
            if already and already["version"] == selected["version"]:
                merged = dict(already)
                merged["specifier"] = ",".join(
                    f"{operator}{value}" for operator, value in constraints[name])
                resolved[name] = merged
                continue
            resolved[name] = {
                "version": selected["version"],
                "wheel": selected["path"].name,
                "required": requirement["raw"],
                "specifier": ",".join(f"{operator}{value}"
                                      for operator, value in constraints[name]),
            }
            for child in selected["requirements"]:
                if _marker_allows(child["marker"]):
                    queue.append(child)
            changed = True
        if not changed:
            break

    # 固定点结束后，只有在约束仍无法满足且没有可解析版本时才算失败。
    for name in sorted(reasons):
        if name in resolved:
            continue
        available = sorted({item["version"] for item in by_name.get(name, [])},
                           key=_version_key)
        failures.append({
            "requirement": ",".join(f"{operator}{value}"
                                    for operator, value in constraints[name]) or name,
            "name": name,
            "reason": reasons[name],
            "available": available,
        })

    # 已解析版本若不再满足累积约束，也计入失败（约束冲突）。
    for name, chosen in list(resolved.items()):
        if not _satisfies(chosen["version"], constraints[name]):
            failures.append({
                "requirement": ",".join(f"{operator}{value}"
                                        for operator, value in constraints[name]),
                "name": name,
                "reason": (f"约束冲突：已选 {name}=={chosen['version']}，"
                           f"但累计约束为 {chosen['specifier']}"),
                "available": sorted({item["version"] for item in by_name.get(name, [])},
                                    key=_version_key),
            })
    failures.sort(key=lambda item: (item["name"], item["requirement"]))
    unresolved = {item["name"] for item in failures}
    payload = {
        "wheelhouse": str(wheelhouse),
        "requirements": str(requirements),
        "wheel_count": len(wheels),
        "compatible_wheel_count": len(wheels) - len(incompatible),
        "resolved": dict(sorted(resolved.items())),
        "unresolved": sorted(unresolved),
        "failures": failures,
        "incompatible_wheels": sorted(incompatible, key=lambda item: item["wheel"]),
        "python": platform.python_version(),
        "platform": sysconfig.get_platform(),
    }
    if requirements_error:
        payload["requirements_error"] = requirements_error
    return payload


def _resolution_reason(name: str, specifiers: Sequence[Tuple[str, str]],
                       candidates: Sequence[Dict[str, Any]],
                       wheels: Sequence[Dict[str, Any]]) -> str:
    wanted = ",".join(f"{operator}{value}" for operator, value in specifiers) or "任意版本"
    same_name = [item for item in wheels if item["normalized"] == name]
    if same_name and not candidates:
        tags = sorted({f"{item['python']}-{item['abi']}-{item['platform']}"
                       for item in same_name})
        return (f"wheelhouse 中存在 {name}，但没有与当前解释器兼容的轮子"
                f"（现有标签: {', '.join(tags)}）")
    if candidates:
        versions = ", ".join(sorted({item["version"] for item in candidates},
                                    key=_version_key))
        return f"wheelhouse 中 {name} 的版本 [{versions}] 不满足约束 {wanted}"
    return f"wheelhouse 缺少依赖 {name}（约束 {wanted}）"


# ---------------------------------------------------------------------------
# 配置、工作目录与元数据库
# ---------------------------------------------------------------------------

def _display_path(path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def _config_files(config_root: Path) -> List[Path]:
    config_root = Path(config_root)
    if config_root.is_file():
        return [config_root]
    files = []
    default_file = config_root / "default_policy.yaml"
    if default_file.is_file():
        files.append(default_file)
    examples = config_root / "example_policies"
    if examples.is_dir():
        files.extend(sorted(examples.glob("*.yaml"), key=lambda item: item.name))
    if not files and config_root.is_dir():
        files.extend(sorted(config_root.glob("*.yaml"), key=lambda item: item.name))
    return files


def _config_file_status(label: str, path: Path, policy,
                        error: Optional[ConfigError]) -> Tuple[str, List[str]]:
    """区分“文件缺失但可接受”和“文件存在但内容错误”。"""
    exists = Path(path).is_file()
    if not exists:
        if label == "workdir_policy":
            return "warn", ["工作目录尚未初始化策略，首次归档时会复制默认策略"]
        return "fail", [f"策略文件不存在: {path}"]
    if error is None:
        return "pass", []
    return "fail", list(error.errors)


def check_configs(workdir: Path, config_root: Path) -> Dict[str, Any]:
    """校验工作目录策略、默认策略与全部示例策略，逐份给出字段级错误。

    错误信息始终带具体文件路径，避免只报“配置损坏”而无法定位是哪一份。
    工作目录尚未初始化策略属于正常的首次部署状态，记为警告。
    """
    records: List[Dict[str, Any]] = []
    work_policy = Path(workdir) / "policy.yaml"
    entries: List[Tuple[str, Path]] = [("workdir_policy", work_policy)]
    for path in _config_files(config_root):
        entries.append(("config_bundle", path))

    for label, path in entries:
        record: Dict[str, Any] = {"label": label, "path": _display_path(path),
                                  "status": "pass", "errors": [], "policy": ""}
        policy, error = None, None
        if Path(path).is_file():
            try:
                policy = load_policy(path, Path(workdir), probe_write=False)
                record["policy"] = policy.name
                record["policy_hash"] = policy.hash
            except ConfigError as exc:
                error = exc
            except Exception as exc:  # 防御性兜底，仍明确指向具体文件
                error = ConfigError(f"{type(exc).__name__}: {exc}")
        record["status"], record["errors"] = _config_file_status(label, path, policy, error)
        records.append(record)

    failures = [item for item in records if item["status"] == "fail"]
    if failures:
        bad = [item["path"] for item in failures]
        return _check("config", "fail",
                      f"{len(failures)} 份策略未通过校验（{', '.join(bad)}）",
                      {"files": records, "failed": len(failures)})
    warnings = [item for item in records if item["status"] == "warn"]
    passed = [item for item in records if item["status"] == "pass"]
    status = "warn" if warnings else "pass"
    summary = f"{len(passed)} 份策略通过校验"
    if warnings:
        summary += "；工作目录策略尚未初始化，首次归档时会复制默认策略"
    return _check("config", status, summary,
                  {"files": records, "failed": 0})


def _probe_writable(directory: Path) -> Optional[str]:
    try:
        handle = tempfile.NamedTemporaryFile(dir=directory, prefix=WRITE_PROBE_PREFIX,
                                             delete=True)
        try:
            handle.write(b"preflight")
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            handle.close()
        return None
    except OSError as exc:
        return str(exc)


def check_workdir(workdir: Path) -> Dict[str, Any]:
    workdir = Path(workdir)
    details: Dict[str, Any] = {"path": str(workdir), "exists": workdir.is_dir(),
                               "subdirectories": {}, "problems": []}
    if not workdir.is_dir():
        ancestor = workdir
        while not ancestor.exists() and ancestor != ancestor.parent:
            ancestor = ancestor.parent
        problem = _probe_writable(ancestor) if ancestor.is_dir() else "父目录不存在"
        if problem:
            details["problems"].append(f"工作目录不存在且最近祖先不可写: {ancestor}: {problem}")
            return _check("workdir", "fail", "工作目录不存在且无法创建", details)
        details["nearest_existing"] = str(ancestor)
        return _check("workdir", "warn",
                      "工作目录尚未创建；首次归档会自动初始化",
                      details)

    problem = _probe_writable(workdir)
    if problem:
        details["problems"].append(f"工作目录不可写: {problem}")
        return _check("workdir", "fail", "工作目录不可写", details)

    for name in ("state", "audit", "staging", "archives"):
        candidate = workdir / name
        if not candidate.is_dir():
            details["subdirectories"][name] = "missing"
            continue
        sub_problem = _probe_writable(candidate)
        if sub_problem:
            details["subdirectories"][name] = f"unwritable: {sub_problem}"
            details["problems"].append(f"{name} 子目录不可写: {sub_problem}")
        else:
            details["subdirectories"][name] = "writable"
    if details["problems"]:
        return _check("workdir", "fail", "工作目录部分子目录不可写", details)
    return _check("workdir", "pass", "工作目录可写", details)


def _readonly_connection(db_path: Path) -> sqlite3.Connection:
    """以只读 URI 打开 SQLite；as_uri 兼容 Windows 盘符并转义特殊字符。"""
    uri = Path(db_path).resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10)
    connection.execute("PRAGMA query_only=ON")
    return connection


def check_metadata(workdir: Path) -> Dict[str, Any]:
    db_path = Path(workdir) / "state" / "archive.db"
    details: Dict[str, Any] = {"path": str(db_path), "exists": db_path.is_file(),
                               "integrity": "", "tables": [], "missing_tables": [],
                               "task_counts": {}}
    if not db_path.is_file():
        state_dir = Path(workdir) / "state"
        sidecars = sorted(item.name for item in state_dir.glob("archive.db-*")
                          if item.is_file()) if state_dir.is_dir() else []
        if sidecars:
            details["sidecars"] = sidecars
            return _check("metadata", "warn",
                          f"元数据库缺失但存在残留 sidecar 文件: {', '.join(sidecars)}",
                          details)
        return _check("metadata", "warn",
                      "尚未创建元数据库；首次归档会自动建库", details)
    try:
        connection = _readonly_connection(db_path)
    except sqlite3.Error as exc:
        details["error"] = str(exc)
        return _check("metadata", "fail", f"元数据库无法以只读方式打开: {exc}", details)
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        details["integrity"] = str(integrity[0]) if integrity else ""
        if details["integrity"] != "ok":
            return _check("metadata", "fail",
                          f"元数据库完整性检查失败: {details['integrity']}", details)
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        details["tables"] = sorted(tables)
        details["missing_tables"] = sorted(EXPECTED_DB_TABLES - tables)
        if "tasks" in tables:
            for status in ("running", "failed"):
                count = connection.execute(
                    "SELECT COUNT(*) FROM tasks WHERE status=?", (status,)).fetchone()[0]
                details["task_counts"][status] = int(count)
        if details["missing_tables"]:
            return _check("metadata", "fail",
                          f"元数据库缺少表: {', '.join(details['missing_tables'])}",
                          details)
        return _check("metadata", "pass", "元数据库可正常打开", details)
    except sqlite3.Error as exc:
        details["error"] = str(exc)
        return _check("metadata", "fail", f"元数据库查询失败: {exc}", details)
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# 分片指纹与残留
# ---------------------------------------------------------------------------

_CONTENT_NAME_RE = re.compile(
    r"^.+__(\d{4}-\d{2})__([0-9a-f]{10})\.([A-Za-z0-9]+(?:\.[A-Za-z0-9]+)?)$")


def _iter_shard_files(workdir: Path) -> Iterable[Path]:
    seen = set()
    for root_name in ("tiers", "archives"):
        root = Path(workdir) / root_name
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
            if path.is_file() and path not in seen:
                seen.add(path)
                yield path


def _metadata_shards(workdir: Path) -> List[Dict[str, Any]]:
    db_path = Path(workdir) / "state" / "archive.db"
    if not db_path.is_file():
        return []
    try:
        connection = _readonly_connection(db_path)
    except sqlite3.Error:
        return []
    try:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "shards" not in tables:
            return []
        rows = connection.execute(
            "SELECT shard_id,path,sha256,active FROM shards ORDER BY shard_id").fetchall()
    except sqlite3.Error:
        return []
    finally:
        connection.close()
    return [{"shard_id": row[0], "path": row[1], "sha256": row[2], "active": row[3]}
            for row in rows]


def check_shard_fingerprints(workdir: Path) -> Dict[str, Any]:
    workdir = Path(workdir)
    files: Dict[str, Dict[str, Any]] = {}
    problems: List[Dict[str, Any]] = []

    for path in _iter_shard_files(workdir):
        match = _CONTENT_NAME_RE.match(path.name)
        if not match:
            continue
        relative = path.relative_to(workdir).as_posix()
        entry = files.setdefault(relative, {
            "path": relative, "in_filename": True,
            "expected_sha256": "", "expected_fingerprint": match.group(2),
            "kind": "content_named"})
        _verify_fingerprint(workdir, entry, problems)

    for shard in _metadata_shards(workdir):
        relative = str(shard["path"])
        entry = files.setdefault(relative, {
            "path": relative, "in_filename": False,
            "expected_sha256": "", "expected_fingerprint": "",
            "kind": "metadata"})
        entry["shard_id"] = shard["shard_id"]
        entry["active"] = shard["active"]
        entry["recorded_sha256"] = shard["sha256"]
        match = _CONTENT_NAME_RE.match(Path(relative).name)
        if match and not entry["expected_fingerprint"]:
            entry["expected_fingerprint"] = match.group(2)
            entry["in_filename"] = True
        _verify_fingerprint(workdir, entry, problems)
        if entry.get("actual_sha256") and shard["sha256"]:
            if entry["actual_sha256"] != shard["sha256"]:
                entry["ok"] = False
                already = next((item for item in problems
                                if item.get("path") == relative
                                and item.get("kind") == "fingerprint_mismatch"), None)
                if already is not None:
                    already["recorded_sha256"] = shard["sha256"]
                    already["shard_id"] = shard["shard_id"]
                    already["kind"] = "fingerprint_and_metadata_mismatch"
                else:
                    problem = {"path": relative, "kind": "metadata_hash_mismatch",
                               "shard_id": shard["shard_id"],
                               "expected_sha256": shard["sha256"],
                               "actual_sha256": entry["actual_sha256"]}
                    entry["problem"] = problem
                    problems.append(problem)

    problematic = {item["path"] for item in problems}
    for entry in files.values():
        if entry["path"] in problematic:
            entry["ok"] = False

    records = [files[key] for key in sorted(files)]
    if problems:
        names = sorted({item["path"] for item in problems})
        return _check("shard_fingerprints", "fail",
                      f"{len(problems)} 个分片指纹异常（{', '.join(names)}）",
                      {"files": records, "problems": problems, "checked": len(records)})
    if not records:
        return _check("shard_fingerprints", "pass",
                      "未发现既有分片，无需校验指纹", {"files": [], "problems": [],
                                                       "checked": 0})
    return _check("shard_fingerprints", "pass",
                  f"{len(records)} 个既有分片指纹一致",
                  {"files": records, "problems": [], "checked": len(records)})


def _verify_fingerprint(workdir: Path, entry: Dict[str, Any],
                        problems: List[Dict[str, Any]]) -> None:
    if "actual_sha256" in entry:
        return
    path = Path(workdir) / entry["path"]
    if not path.is_file():
        entry["ok"] = False
        problem = {"path": entry["path"], "kind": "missing_file"}
        entry["problem"] = problem
        problems.append(problem)
        return
    try:
        actual = file_sha256(path)
    except OSError as exc:
        entry["ok"] = False
        problem = {"path": entry["path"], "kind": "unreadable", "error": str(exc)}
        entry["problem"] = problem
        problems.append(problem)
        return
    entry["actual_sha256"] = actual
    entry["size"] = path.stat().st_size
    fingerprint = entry.get("expected_fingerprint") or ""
    if fingerprint and not actual.startswith(fingerprint):
        entry["ok"] = False
        problem = {"path": entry["path"], "kind": "fingerprint_mismatch",
                   "expected_fingerprint": fingerprint, "actual_sha256": actual}
        entry["problem"] = problem
        problems.append(problem)
    else:
        entry["ok"] = True



# ---------------------------------------------------------------------------
# 残留检测与可回退清理
# ---------------------------------------------------------------------------

def _staging_files(workdir: Path) -> List[Path]:
    staging = Path(workdir) / "staging"
    if not staging.is_dir():
        return []
    return sorted((path for path in staging.rglob("*") if path.is_file()),
                  key=lambda item: item.as_posix())


def _atomic_temp_files(workdir: Path) -> List[Path]:
    candidates: List[Path] = []
    for root_name in ("tiers", "archives"):
        root = Path(workdir) / root_name
        if root.is_dir():
            for path in root.rglob("*"):
                if not path.is_file():
                    continue
                name = path.name
                if name.startswith((".stage_", ".write_")) or name.endswith(".tmp"):
                    candidates.append(path)
    policy_tmp = Path(workdir) / "policy.yaml.tmp"
    if policy_tmp.is_file():
        candidates.append(policy_tmp)
    return sorted(candidates, key=lambda item: item.as_posix())


def _interrupted_tasks(workdir: Path) -> List[Dict[str, Any]]:
    db_path = Path(workdir) / "state" / "archive.db"
    if not db_path.is_file():
        return []
    try:
        connection = _readonly_connection(db_path)
    except sqlite3.Error:
        return []
    try:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "tasks" not in tables:
            return []
        rows = connection.execute(
            "SELECT task_id,kind,source,status,stage,started_at,error FROM tasks "
            "WHERE status IN ('running','failed') ORDER BY task_id").fetchall()
    except sqlite3.Error:
        return []
    finally:
        connection.close()
    return [{"task_id": row[0], "kind": row[1], "source": row[2],
             "status": row[3], "stage": row[4], "started_at": row[5],
             "error": row[6] or ""} for row in rows]


def check_residue(workdir: Path) -> Dict[str, Any]:
    """把 staging 临时文件与失败/中断任务标记为待清理（警告而非失败）。"""
    workdir = Path(workdir)
    files = []
    for path in _staging_files(workdir) + _atomic_temp_files(workdir):
        relative = path.relative_to(workdir).as_posix()
        files.append({"path": relative, "kind": _residue_kind(relative),
                      "size": path.stat().st_size})
    tasks = _interrupted_tasks(workdir)
    details = {"files": files, "tasks": tasks, "pending_cleanup": bool(files or tasks),
               "cleanup_supported": bool(files), "cleanup_note": ""}
    if tasks:
        details["cleanup_note"] = (
            "失败或中断的任务行保留在元数据库中，由下一次归档自然覆盖；"
            "自动清理只移动文件，不改写任务状态，避免影响审计与断点语义")
    if not files and not tasks:
        return _check("residue", "pass", "未发现 staging 残留或失败任务", details)
    parts = []
    if files:
        parts.append(f"{len(files)} 个临时文件")
    if tasks:
        parts.append(f"{len(tasks)} 个失败/中断任务")
    return _check("residue", "warn",
                  "发现待清理状态：" + "、".join(parts) + "（不阻断归档）", details)


def _residue_kind(relative: str) -> str:
    name = Path(relative).name
    if relative.startswith("staging/"):
        return "staging_temp"
    if name == "policy.yaml.tmp":
        return "policy_temp"
    return "atomic_temp"


def _quarantine_dir(workdir: Path, files: Sequence[Dict[str, Any]]) -> Path:
    paths = sorted(str(item["path"]) for item in files)
    token = canonical_hash({"files": paths})[:16]
    return Path(workdir).joinpath(*QUARANTINE_ROOT) / f"q-{token}"


def cleanup_residue(workdir: Path, files: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """把待清理文件移入隔离目录，并写入可反向恢复的清单。"""
    workdir = Path(workdir)
    movable = sorted(files, key=lambda item: item["path"])
    if not movable:
        return {"moved": 0, "quarantined": [], "manifest": "", "skipped": [],
                "cleanup_id": ""}
    target_root = _quarantine_dir(workdir, movable)
    payload_root = target_root / "files"
    manifest_path = target_root / "manifest.json"
    existing = _load_manifest(manifest_path)
    entries = {item["original"]: item for item in existing.get("entries", [])}
    moved, skipped = [], []
    for item in sorted(movable, key=lambda value: value["path"]):
        original = workdir / item["path"]
        if not original.is_file():
            skipped.append({"path": item["path"], "reason": "文件已不存在"})
            continue
        destination = payload_root / item["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            digest = file_sha256(original)
            size = original.stat().st_size
            if destination.exists():
                suffix = canonical_hash([item["path"], digest])[:8]
                destination = destination.with_name(destination.name + f".{suffix}")
            os.replace(original, destination)
        except OSError as exc:
            skipped.append({"path": item["path"], "reason": str(exc)})
            continue
        entries[item["path"]] = {
            "original": item["path"],
            "quarantined": destination.relative_to(target_root).as_posix(),
            "sha256": digest, "size": size,
        }
        moved.append(item["path"])
    if entries:
        target_root.mkdir(parents=True, exist_ok=True)
        manifest = {
            "workdir": str(workdir), "entries": [entries[key] for key in sorted(entries)],
        }
        manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    return {"moved": len(moved), "quarantined": moved,
            "manifest": str(manifest_path) if entries else "",
            "cleanup_id": target_root.name, "skipped": skipped}


def restore_cleanup(workdir: Path, manifest: str) -> Dict[str, Any]:
    """按隔离清单把文件移回原路径；目标已存在时跳过，绝不覆盖。"""
    workdir = Path(workdir)
    manifest_path = _resolve_manifest(workdir, manifest)
    data = _load_manifest(manifest_path)
    if not data.get("entries"):
        raise PreflightError(f"隔离清单为空或不存在: {manifest_path}")
    base = manifest_path.parent
    base_resolved = base.resolve()
    workdir_resolved = workdir.resolve()
    restored, skipped = [], []
    for entry in data["entries"]:
        original_rel = str(entry.get("original", ""))
        quarantined_rel = str(entry.get("quarantined", ""))
        original = (workdir / original_rel).resolve()
        source = (base / quarantined_rel).resolve()
        # 清单同样视为外部输入：原路径必须落在工作目录内，
        # 隔离文件必须落在清单目录内，防止借助伪造清单越界移动文件。
        if not _is_within(original, workdir_resolved) or not _is_within(source, base_resolved):
            skipped.append({"path": original_rel, "reason": "清单路径越界，已拒绝"})
            continue
        if not source.is_file():
            skipped.append({"path": original_rel, "reason": "隔离文件已不存在"})
            continue
        if original.exists():
            skipped.append({"path": original_rel, "reason": "原路径已有文件，拒绝覆盖"})
            continue
        actual = file_sha256(source)
        if entry.get("sha256") and actual != entry["sha256"]:
            skipped.append({"path": original_rel, "reason": "隔离文件指纹不匹配"})
            continue
        original.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(source, original)
        except OSError as exc:
            skipped.append({"path": original_rel, "reason": str(exc)})
            continue
        restored.append(entry["original"])
    return {"restored": len(restored), "paths": sorted(restored),
            "manifest": str(manifest_path), "skipped": skipped}


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolve_manifest(workdir: Path, manifest: str) -> Path:
    text = str(manifest or "").strip()
    if text in {"", "latest"}:
        root = Path(workdir).joinpath(*QUARANTINE_ROOT)
        candidates = sorted(
            (path for path in root.glob("q-*/manifest.json") if path.is_file()),
            key=lambda item: item.stat().st_mtime, reverse=True)
        if not candidates:
            raise PreflightError("没有可恢复的自检隔离清单")
        return candidates[0]
    path = Path(text)
    if not path.is_absolute():
        path = Path(workdir) / path
    resolved = path.resolve()
    try:
        resolved.relative_to(Path(workdir).resolve())
    except ValueError as exc:
        raise PreflightError("隔离清单必须位于工作目录内") from exc
    if not resolved.is_file():
        raise PreflightError(f"隔离清单不存在: {resolved}")
    return resolved


def _load_manifest(path: Path) -> Dict[str, Any]:
    path = Path(path)
    if not path.is_file():
        return {"entries": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"entries": []}
    if not isinstance(data, dict) or not isinstance(data.get("entries"), list):
        return {"entries": []}
    return data


# ---------------------------------------------------------------------------
# 报告聚合、审计与入口
# ---------------------------------------------------------------------------

def _aggregate_status(checks: Iterable[Dict[str, Any]]) -> str:
    statuses = {check["status"] for check in checks}
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def build_report(workdir: Path, *, strict: bool = False,
                 wheelhouse: Optional[Path] = None,
                 requirements: Optional[Path] = None,
                 config_root: Optional[Path] = None) -> Dict[str, Any]:
    """执行全部只读检查并返回结构化报告（不写审计）。"""
    workdir = Path(workdir)
    wheelhouse = Path(wheelhouse) if wheelhouse else DEFAULT_WHEELHOUSE
    requirements = Path(requirements) if requirements else DEFAULT_REQUIREMENTS
    config_root = Path(config_root) if config_root else DEFAULT_CONFIG_ROOT

    resolution = resolve_wheelhouse(wheelhouse, requirements)
    if resolution.get("requirements_error"):
        dependency_check = _check(
            "dependencies", "fail", resolution["requirements_error"],
            {"resolution": resolution})
    elif resolution["failures"]:
        names = resolution["unresolved"]
        dependency_check = _check(
            "dependencies", "fail",
            f"{len(resolution['failures'])} 项依赖约束无法满足"
            f"（涉及: {', '.join(names)}）",
            {"resolution": resolution})
    else:
        incompatible = resolution["incompatible_wheels"]
        if incompatible:
            dependency_check = _check(
                "dependencies", "warn",
                f"全部依赖可满足；wheelhouse 中另有 {len(incompatible)} 个"
                f"与当前解释器不兼容的轮子（不会被使用）",
                {"resolution": resolution})
        else:
            dependency_check = _check(
                "dependencies", "pass",
                f"{len(resolution['resolved'])} 项依赖均可由 wheelhouse 满足",
                {"resolution": resolution})

    checks = [
        dependency_check,
        check_configs(workdir, config_root),
        check_workdir(workdir),
        check_metadata(workdir),
        check_shard_fingerprints(workdir),
        check_residue(workdir),
    ]
    status = _aggregate_status(checks)
    failures = [check for check in checks if check["status"] == "fail"]
    warnings = [check for check in checks if check["status"] == "warn"]
    report = {
        "status": status,
        "strict": bool(strict),
        "allowed": (status != "fail") if strict else True,
        "checks": checks,
        "failures": [{"name": check["name"], "summary": check["summary"]}
                     for check in failures],
        "warnings": [{"name": check["name"], "summary": check["summary"]}
                     for check in warnings],
        "python": platform.python_version(),
        "platform": sysconfig.get_platform(),
        "workdir": str(workdir),
    }
    report["report_id"] = _report_id(report)
    return report


def _report_id(report: Dict[str, Any]) -> str:
    payload = {
        "status": report["status"],
        "strict": report["strict"],
        "checks": [
            {"name": check["name"], "status": check["status"],
             "summary": check["summary"], "details": check["details"]}
            for check in report["checks"]
        ],
    }
    return "preflight-" + canonical_hash(payload)[:24]


def _audit_details(report: Dict[str, Any], cleanup: Optional[Dict[str, Any]],
                   restored: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    details: Dict[str, Any] = {
        "report_id": report["report_id"],
        "status": report["status"],
        "strict": report["strict"],
        "allowed": report["allowed"],
        "python": report["python"],
        "platform": report["platform"],
        "checks": [{"name": check["name"], "status": check["status"],
                    "summary": check["summary"]}
                   for check in report["checks"]],
        "failures": report["failures"],
        "warnings": report["warnings"],
        "actions": [],
    }
    if cleanup:
        details["actions"].append({
            "action": "cleanup_residue", "moved": cleanup.get("moved", 0),
            "quarantined": cleanup.get("quarantined", []),
            "manifest": cleanup.get("manifest", ""),
        })
    if restored:
        details["actions"].append({
            "action": "restore_cleanup", "restored": restored.get("restored", 0),
            "paths": restored.get("paths", []),
        })
    return details


def run_preflight(workdir: Path, *, strict: bool = False,
                  wheelhouse: Optional[Path] = None,
                  requirements: Optional[Path] = None,
                  config_root: Optional[Path] = None,
                  cleanup: bool = False,
                  restore: Optional[str] = None,
                  actor: str = "CLI") -> Dict[str, Any]:
    """执行自检并把结果追加到哈希链审计，返回报告。

    自检不创建任务行；``preflight`` 是独立操作类型，不占用归档任务序号。
    ``cleanup`` 把待清理文件移入工作目录内的隔离区，``restore`` 可反向恢复。
    """
    workdir = Path(workdir)
    report = build_report(workdir, strict=strict, wheelhouse=wheelhouse,
                          requirements=requirements, config_root=config_root)
    residue = next((check for check in report["checks"]
                    if check["name"] == "residue"), None)
    residue_files = (residue or {}).get("details", {}).get("files", [])

    cleanup_result = None
    restore_result = None
    actions_error = ""
    try:
        if cleanup:
            cleanup_result = cleanup_residue(workdir, residue_files)
        if restore:
            restore_result = restore_cleanup(workdir, restore)
    except (OSError, PreflightError) as exc:
        actions_error = str(exc)
        report["status"] = "fail"
        report["allowed"] = False if strict else report["allowed"]
        report["failures"].append({"name": "cleanup", "summary": actions_error})

    audit = AuditLog(workdir / "audit" / "audit.jsonl")
    result = "success" if report["status"] != "fail" else "failure"
    error = "" if report["status"] != "fail" else "；".join(
        item["summary"] for item in report["failures"])[:1000]
    record = audit.log(
        PREFLIGHT_OPERATION, actor, result=result, error=error,
        details=_audit_details(report, cleanup_result, restore_result))
    report["audit"] = {"operation": PREFLIGHT_OPERATION,
                       "sequence": record["sequence"],
                       "entry_hash": record["entry_hash"],
                       "result": result}
    if cleanup_result is not None:
        report["cleanup"] = cleanup_result
    if restore_result is not None:
        report["cleanup_restore"] = restore_result
    if actions_error:
        raise PreflightError(actions_error)
    return report
