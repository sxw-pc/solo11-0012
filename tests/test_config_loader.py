"""策略加载、字段级校验、路径校验与原子保存测试。"""
from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from time_archiver.config_loader import (
    default_policy_path, init_workspace, load_policy, policy_to_yaml,
    save_policy_text, project_root,
)
from time_archiver.schema import ConfigError, PolicyTier, parse_granularity


def base_config() -> dict:
    return yaml.safe_load(default_policy_path().read_text(encoding="utf-8"))


def write_config(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


def test_default_policy_is_valid_and_stable(policy, workdir):
    assert policy.name == "default_hot_warm_cold_archive"
    assert len(policy.tiers) == 4
    assert [tier.name for tier in policy.tiers] == ["hot", "warm", "cold", "archive"]
    assert policy.tiers[0].aggregation_for("vibration") == "max"
    assert policy.tiers[0].aggregation_for("temperature") == "mean"
    assert policy.hash == load_policy(workdir / "policy.yaml", workdir).hash
    assert policy.archive_tier.name == "archive"
    assert "tiers:" in policy_to_yaml(policy)


@pytest.mark.parametrize("filename", ["edge_sensor.yaml", "server_metric.yaml", "iot_device.yaml"])
def test_example_policies_load(tmp_path, filename):
    path = project_root() / "config" / "example_policies" / filename
    policy = load_policy(path, tmp_path / filename.replace(".yaml", ""))
    assert 1 <= len(policy.tiers) <= 5
    assert policy.name


@pytest.mark.parametrize("value,seconds", [("10s", 10), ("1m", 60), ("2h", 7200), ("7d", 604800)])
def test_parse_granularity(value, seconds):
    assert parse_granularity(value) == seconds


@pytest.mark.parametrize("value", ["", "0s", "1y", "h", "1.5h", None])
def test_parse_granularity_rejects_invalid(value):
    with pytest.raises(ConfigError):
        parse_granularity(value)


def test_unknown_top_level_field_has_field_error(tmp_path):
    data = base_config()
    data["mystery"] = True
    with pytest.raises(ConfigError) as info:
        load_policy(write_config(tmp_path, data), tmp_path)
    assert any("mystery" in error for error in info.value.errors)


def test_invalid_granularity_and_retention_and_operator(tmp_path):
    data = base_config()
    data["tiers"][1]["granularity"] = "5s"
    data["tiers"][2]["retention_days"] = 1
    data["tiers"][0]["aggregations"]["humidity"] = "median"
    errors = _errors(tmp_path, data)
    assert any("granularity" in e for e in errors)
    assert any("retention_days" in e for e in errors)
    assert any("median" in e for e in errors)


def test_tier_count_and_duplicate_names_paths(tmp_path):
    data = base_config()
    data["tiers"] = data["tiers"] * 2
    errors = _errors(tmp_path, data)
    assert any("tiers:" in e for e in errors)


def test_duplicate_name_and_path(tmp_path):
    data = base_config()
    data["tiers"][1]["name"] = "hot"
    data["tiers"][1]["path"] = "tiers/hot"
    errors = _errors(tmp_path, data)
    assert any("名称重复" in e for e in errors)
    assert any("路径重复" in e for e in errors)


def test_path_traversal_rejected(tmp_path):
    data = base_config()
    data["tiers"][0]["path"] = "../outside"
    errors = _errors(tmp_path, data)
    assert any("相对路径" in e for e in errors)


@pytest.mark.parametrize("field,value", [
    ("format", "avro"), ("compression", "lz4"), ("gap_strategy", "interpolate"),
])
def test_invalid_storage_fields(tmp_path, field, value):
    data = base_config()
    data["tiers"][0][field] = value
    errors = _errors(tmp_path, data)
    assert any(field in error for error in errors)


def test_quality_ranges_validated(tmp_path):
    data = base_config()
    data["quality"]["value_ranges"]["temperature"] = {"min": 100, "max": -100}
    data["quality"]["value_ranges"]["broken"] = "high"
    errors = _errors(tmp_path, data)
    assert any("min" in error for error in errors)
    assert any("value_ranges.broken" in error for error in errors)


def test_malformed_configs(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="映射"):
        load_policy(path)
    path.write_text("tiers: [", encoding="utf-8")
    with pytest.raises(ConfigError, match="无法解析"):
        load_policy(path)
    with pytest.raises(ConfigError, match="不存在"):
        load_policy(tmp_path / "missing.yaml")


def test_version_and_reference_validated(tmp_path):
    data = base_config()
    data["version"] = 2
    data["reference"] = "now"
    errors = _errors(tmp_path, data)
    assert any("version" in error for error in errors)
    assert any("reference" in error for error in errors)


def test_tier_reporting_fields(tier):
    assert tier.granularity_seconds == 60
    assert tier.to_dict()["name"] == "hot"
    assert tier.aggregation_for("unknown") == "mean"
    fallback = PolicyTier("x", "1m", 1, "x")
    assert fallback.aggregation_for("anything") == "mean"


def test_save_policy_text_is_atomic_and_rolls_back(tmp_path):
    init_workspace(tmp_path / "work")
    target = tmp_path / "work" / "policy.yaml"
    original = target.read_text(encoding="utf-8")
    with pytest.raises(ConfigError):
        save_policy_text(target, "version: 1\nname: bad\ntiers: []\n", tmp_path / "work")
    assert target.read_text(encoding="utf-8") == original
    assert not target.with_suffix(".yaml.tmp").exists()
    policy = save_policy_text(target, original, tmp_path / "work")
    assert policy.name == "default_hot_warm_cold_archive"


def test_init_workspace_is_idempotent(tmp_path):
    work = tmp_path / "workspace"
    first = init_workspace(work)
    marker = work / "state" / "marker"
    marker.write_text("keep", encoding="utf-8")
    second = init_workspace(work)
    assert first == second
    assert marker.exists()
    assert (work / "audit").is_dir()


def _errors(tmp_path, data):
    config = copy.deepcopy(data)
    try:
        load_policy(write_config(tmp_path, config), tmp_path / "work")
    except ConfigError as exc:
        return exc.errors
    return []
