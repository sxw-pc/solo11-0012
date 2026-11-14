"""YAML 生命周期策略的加载、字段级校验与工作目录初始化。"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from .schema import (
    AGGREGATORS, GAP_STRATEGIES, SUPPORTED_FORMATS, ConfigError, Policy,
    PolicyTier, parse_granularity,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "default_policy.yaml"
MAX_TIERS = 5
TOP_KEYS = {"version", "name", "reference", "quality", "tiers"}
TIER_KEYS = {
    "name", "granularity", "retention_days", "path", "format",
    "compression", "aggregations", "gap_strategy",
}
COMPRESSIONS = {
    "parquet": {"zstd", "snappy", "gzip", "none"},
    "csv": {"none", "gzip"},
    "jsonl": {"none", "gzip"},
}


def default_policy_path() -> Path:
    return DEFAULT_POLICY_PATH


def project_root() -> Path:
    return PROJECT_ROOT


def load_yaml(path: Path) -> Dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"配置文件不存在: {path}") from exc
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"配置文件无法解析: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("配置根节点必须是 YAML 映射")
    return data


def load_policy(path: Path, workdir: Optional[Path] = None,
                probe_write: bool = True) -> Policy:
    """加载并严格校验策略；错误一次性汇总为字段级信息。

    ``probe_write=False`` 用于部署自检等只读场景：仍会校验路径是否在
    工作目录内以及最近的已存在祖先是否可写，但不会创建目录或探测文件。
    """
    data = load_yaml(Path(path))
    errors: list[str] = []
    unknown = sorted(set(data) - TOP_KEYS)
    if unknown:
        errors.append(f"根字段未知: {', '.join(unknown)}")
    version = data.get("version", 1)
    if version != 1:
        errors.append("version: 当前仅支持整数 1")
    name = str(data.get("name") or "").strip()
    if not name:
        errors.append("name: 策略名称不能为空")
    reference = data.get("reference", "data_max")
    if reference != "data_max":
        errors.append("reference: 仅支持 data_max，保证离线与结果确定性")

    quality = data.get("quality") or {}
    if not isinstance(quality, dict):
        errors.append("quality: 必须是映射")
        quality = {}
    ranges = quality.get("value_ranges", {})
    if not isinstance(ranges, dict):
        errors.append("quality.value_ranges: 必须是映射")
    else:
        for metric, limits in ranges.items():
            if not isinstance(limits, dict):
                errors.append(f"quality.value_ranges.{metric}: 必须是映射")
                continue
            low, high = limits.get("min"), limits.get("max")
            if low is None and high is None:
                errors.append(f"quality.value_ranges.{metric}: 至少设置 min 或 max")
            if low is not None and not isinstance(low, (int, float)):
                errors.append(f"quality.value_ranges.{metric}.min: 必须是数字")
            if high is not None and not isinstance(high, (int, float)):
                errors.append(f"quality.value_ranges.{metric}.max: 必须是数字")
            if isinstance(low, (int, float)) and isinstance(high, (int, float)) and low >= high:
                errors.append(f"quality.value_ranges.{metric}: min 必须小于 max")

    raw_tiers = data.get("tiers")
    tiers: list[PolicyTier] = []
    if not isinstance(raw_tiers, list) or not 1 <= len(raw_tiers) <= MAX_TIERS:
        errors.append(f"tiers: 必须包含 1 至 {MAX_TIERS} 个层级")
        raw_tiers = []
    names: set[str] = set()
    paths: set[str] = set()
    for position, item in enumerate(raw_tiers, start=1):
        prefix = f"tiers[{position - 1}]"
        if not isinstance(item, dict):
            errors.append(f"{prefix}: 必须是映射")
            continue
        unknown_tier = sorted(set(item) - TIER_KEYS)
        if unknown_tier:
            errors.append(f"{prefix}: 未知字段 {', '.join(unknown_tier)}")
        tier_name = str(item.get("name") or "").strip()
        if not tier_name:
            errors.append(f"{prefix}.name: 不能为空")
        elif tier_name in names:
            errors.append(f"{prefix}.name: 层级名称重复 {tier_name}")
        names.add(tier_name)
        try:
            granularity = item.get("granularity")
            granularity_seconds = parse_granularity(granularity)
        except ConfigError as exc:
            errors.append(f"{prefix}.granularity: {exc}")
            granularity_seconds = -1
        retention = item.get("retention_days")
        if isinstance(retention, bool) or not isinstance(retention, int) or retention <= 0:
            errors.append(f"{prefix}.retention_days: 必须是正整数")
            retention = -1
        relative_path = str(item.get("path") or "").strip()
        if not relative_path:
            errors.append(f"{prefix}.path: 不能为空")
        elif Path(relative_path).is_absolute() or ".." in Path(relative_path).parts:
            errors.append(f"{prefix}.path: 必须是工作目录内的相对路径")
        elif relative_path in paths:
            errors.append(f"{prefix}.path: 层级路径重复 {relative_path}")
        paths.add(relative_path)
        fmt = str(item.get("format") or "parquet").lower()
        if fmt not in SUPPORTED_FORMATS:
            errors.append(f"{prefix}.format: 仅支持 {', '.join(SUPPORTED_FORMATS)}")
        compression = str(item.get("compression") or "none").lower()
        if fmt in COMPRESSIONS and compression not in COMPRESSIONS[fmt]:
            errors.append(f"{prefix}.compression: {fmt} 不支持 {compression}")
        aggregations = item.get("aggregations", {"default": "mean"})
        if not isinstance(aggregations, dict) or not aggregations:
            errors.append(f"{prefix}.aggregations: 必须是非空映射")
            aggregations = {"default": "mean"}
        else:
            aggregations = {str(k): str(v).lower() for k, v in aggregations.items()}
            for metric, operator in aggregations.items():
                if operator not in AGGREGATORS:
                    errors.append(
                        f"{prefix}.aggregations.{metric}: 非法算子 {operator}，"
                        f"支持 {', '.join(AGGREGATORS)}"
                    )
        gap_strategy = str(item.get("gap_strategy") or "skip").lower()
        if gap_strategy not in GAP_STRATEGIES:
            errors.append(f"{prefix}.gap_strategy: 仅支持 {', '.join(GAP_STRATEGIES)}")
        tiers.append(PolicyTier(
            name=tier_name, granularity=str(granularity or ""),
            retention_days=retention, path=relative_path, format=fmt,
            compression=compression, aggregations=aggregations,
            gap_strategy=gap_strategy,
        ))

    valid_seconds = [t.granularity_seconds for t in tiers if t.granularity_seconds > 0]
    valid_days = [t.retention_days for t in tiers if t.retention_days > 0]
    if len(valid_seconds) == len(tiers):
        for index in range(1, len(valid_seconds)):
            if valid_seconds[index] <= valid_seconds[index - 1]:
                errors.append(f"tiers[{index}].granularity: 时间粒度必须严格递增（由细到粗）")
    if len(valid_days) == len(tiers):
        for index in range(1, len(valid_days)):
            if valid_days[index] <= valid_days[index - 1]:
                errors.append(f"tiers[{index}].retention_days: 保留周期必须严格递增")
    if workdir is not None:
        errors.extend(_validate_paths(tiers, Path(workdir),
                                      probe_write=probe_write))
    if errors:
        raise ConfigError(errors)
    return Policy(name=name, tiers=tuple(tiers), version=version,
                  reference=reference, quality=quality, raw=data)


def _validate_paths(tiers: list[PolicyTier], workdir: Path,
                    probe_write: bool = True) -> list[str]:
    """在真实文件系统上验证路径可写且不越界。

    ``probe_write=False`` 时不创建目录、不写探测文件，只确认解析后的路径
    没有越界，并检查已存在的最近祖先目录是否可写。
    """
    errors: list[str] = []
    base = workdir.resolve()
    if probe_write:
        try:
            workdir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return [f"workdir: 无法创建工作目录: {exc}"]
    for index, tier in enumerate(tiers):
        target = (workdir / tier.path).resolve()
        try:
            target.relative_to(base)
        except ValueError:
            errors.append(f"tiers[{index}].path: 解析后越出工作目录")
            continue
        if not probe_write:
            ancestor = target
            while not ancestor.exists() and ancestor != ancestor.parent:
                ancestor = ancestor.parent
            if not os.access(ancestor, os.W_OK):
                errors.append(f"tiers[{index}].path: 最近的可写祖先不可写: {ancestor}")
            continue
        try:
            target.mkdir(parents=True, exist_ok=True)
            handle = tempfile.NamedTemporaryFile(dir=target, prefix=".write_", delete=True)
            handle.close()
        except OSError as exc:
            errors.append(f"tiers[{index}].path: 路径不可写: {exc}")
    return errors


def policy_to_yaml(policy: Policy) -> str:
    return yaml.safe_dump(policy.to_dict(), allow_unicode=True, sort_keys=False)


def save_policy_text(path: Path, text: str, workdir: Path) -> Policy:
    """先写临时文件再原子替换，避免半份非法配置落盘。"""
    temp = Path(path).with_suffix(Path(path).suffix + ".tmp")
    temp.write_text(text, encoding="utf-8")
    try:
        policy = load_policy(temp, workdir)
    except ConfigError:
        temp.unlink(missing_ok=True)
        raise
    os.replace(temp, path)
    return policy


def init_workspace(workdir: Path, policy_source: Optional[Path] = None) -> Path:
    """初始化独立工作目录并复制默认策略。"""
    workdir = Path(workdir)
    for relative in ("state", "audit", "staging", "tiers", "archives", "restored"):
        (workdir / relative).mkdir(parents=True, exist_ok=True)
    source = Path(policy_source) if policy_source else DEFAULT_POLICY_PATH
    target = workdir / "policy.yaml"
    if not target.exists():
        shutil.copyfile(source, target)
    load_policy(target, workdir)
    return target
