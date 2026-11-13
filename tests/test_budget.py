"""预算推演：自然月归属、无重无漏、确定性搜索与方案持久化测试。"""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from time_archiver.budget import (
    baseline_limits, baseline_windows, build_projection, frame_months,
    load_plan, month_owner_index, month_str, months_in_range, plan_budget,
    plan_file_hash, plan_text, validate_contiguity, windows_from_limits,
)
from time_archiver.config_loader import load_policy
from time_archiver.schema import PolicyTier, SourceSpec


def four_tiers():
    return (
        PolicyTier("hot", "1h", 40, "tiers/hot"),
        PolicyTier("warm", "6h", 100, "tiers/warm"),
        PolicyTier("cold", "1d", 160, "tiers/cold"),
        PolicyTier("archive", "1d", 300, "archives",
                   format="parquet", compression="gzip"),
    )


@pytest.fixture
def six_month_source(tmp_path: Path) -> SourceSpec:
    stamps = pd.date_range("2024-01-01", "2024-06-30", freq="6h")
    rng = np.random.default_rng(7)
    frame = pd.DataFrame({
        "timestamp": stamps,
        "sensor_id": ["s1"] * len(stamps),
        "temperature": 20 + rng.normal(0, .5, len(stamps)),
        "humidity": 50 + rng.normal(0, 1, len(stamps)),
        "pressure": 1000 + rng.normal(0, 2, len(stamps)),
    })
    path = tmp_path / "six.parquet"
    frame.to_parquet(path, index=False)
    return SourceSpec(path, metric_columns=("temperature", "humidity", "pressure"),
                      source_id="six")


@pytest.fixture
def policy(workdir: Path):
    return load_policy(workdir / "policy.yaml", workdir)


def test_month_helpers_and_owner_partition():
    assert months_in_range("2024-01", "2024-03") == ["2024-01", "2024-02", "2024-03"]
    policy = replace(_load_minimal_policy(), tiers=four_tiers())
    # 保留上界严格递增：年龄最小命中热层，最大落到归档层。
    assert month_owner_index(1, policy) == 0
    assert month_owner_index(40, policy) == 0
    assert month_owner_index(41, policy) == 1
    assert month_owner_index(161, policy) == 3
    assert month_owner_index(10_000, policy) == 3


def _load_minimal_policy():
    from time_archiver.config_loader import init_workspace
    import tempfile
    work = Path(tempfile.mkdtemp())
    return load_policy(init_workspace(work), work)


def test_baseline_windows_partition_every_month_once(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    projection = build_projection(six_month_source, pol,
                                  six_month_source.path.parent / "staging")
    covered: list[str] = []
    for window in projection.windows:
        if window.start_month:
            covered.extend(months_in_range(window.start_month, window.end_month))
    assert sorted(covered) == projection.data_months
    assert len(covered) == len(set(covered))
    assert set(projection.baseline_owner) == set(projection.data_months)


def test_projection_costs_cover_sink_grid(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    staging = six_month_source.path.parent / "staging"
    projection = build_projection(six_month_source, pol, staging)
    # 每个月在其基线属主及更老的每一层都有真实字节候选。
    for month, owner in projection.baseline_owner.items():
        for position in range(owner, len(pol.tiers)):
            assert projection.monthly_cost[position][month] > 0
    # 合并单包成本随持有月数单调给出，且键覆盖全部月数。
    assert sorted(projection.merged_archive_cost) == list(
        range(len(projection.data_months)))


def test_loose_budget_keeps_baseline(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    staging = six_month_source.path.parent / "staging"
    plan = plan_budget(six_month_source, pol, budget_bytes=10 ** 12,
                       staging_dir=staging)
    assert plan.within_budget is True
    assert plan.tier_limits == tuple(baseline_limits(
        build_projection(six_month_source, pol, staging)))
    assert plan.merge_oldest is False
    assert plan.moves == ()
    validate_contiguity(plan)


def test_tight_budget_shrinks_without_dropping_months(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    plan = plan_budget(six_month_source, pol, budget_bytes=20_000,
                       staging_dir=six_month_source.path.parent / "staging")
    validate_contiguity(plan)
    assert sum(plan.tier_limits) == len(plan.data_months)
    assert plan.projected_bytes <= 20_000
    assert plan.projected_bytes < plan.baseline_bytes
    # 所有数据月仍被恰好一层覆盖。
    covered = sorted(m for b in plan.bounds if b.start_month
                     for m in months_in_range(b.start_month, b.end_month))
    assert covered == list(plan.data_months)


def test_extreme_budget_merges_oldest_layer(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    plan = plan_budget(six_month_source, pol, budget_bytes=1,
                       staging_dir=six_month_source.path.parent / "staging")
    assert plan.within_budget is False  # 物理上压不到 1 字节
    assert plan.merge_oldest is True
    archive_bound = plan.bounds[-1]
    assert archive_bound.start_month == plan.data_months[0]
    assert archive_bound.end_month == plan.data_months[-1]
    assert sum(plan.tier_limits) == len(plan.data_months)
    validate_contiguity(plan)


def test_repeated_planning_is_byte_identical(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    staging = six_month_source.path.parent / "staging"
    first = plan_budget(six_month_source, pol, 20_000, staging)
    second = plan_budget(six_month_source, pol, 20_000, staging)
    assert first.plan_id == second.plan_id
    assert plan_text(first) == plan_text(second)
    assert plan_file_hash(first) == plan_file_hash(second)
    # 不同预算给出不同方案。
    other = plan_budget(six_month_source, pol, 12_000, staging)
    assert other.plan_id != first.plan_id


def test_plan_persistence_roundtrip(policy, six_month_source, tmp_path: Path):
    pol = replace(policy, tiers=four_tiers())
    plan = plan_budget(six_month_source, pol, 20_000,
                       six_month_source.path.parent / "staging")
    target = tmp_path / "plan.json"
    target.write_text(plan_text(plan), encoding="utf-8")
    loaded = load_plan(target)
    assert loaded.plan_id == plan.plan_id
    assert loaded.tier_limits == plan.tier_limits
    assert plan_text(loaded) == plan_text(plan)


def test_month_boundary_assignment_is_unambiguous(policy):
    """构造恰好卡在月边界的数据：边界月整体归属唯一一层。"""
    pol = replace(policy, tiers=four_tiers())
    anchor = pd.Timestamp("2024-06-30")
    windows = baseline_windows(pol, anchor, "2024-01", "2024-06")
    owners = {}
    for position, window in enumerate(windows):
        if window.start_month:
            for month in months_in_range(window.start_month, window.end_month):
                owners[month] = position
    assert sorted(owners) == months_in_range("2024-01", "2024-06")
    assert len(owners) == 6  # 无重无漏


def test_windows_from_limits_are_head_to_tail(policy):
    pol = replace(policy, tiers=four_tiers())
    data = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]
    windows = windows_from_limits(data, pol, [0, 0, 2, 4])
    # 热/温层为空；冷层持最新两月，归档层持最旧四月，首尾相接。
    assert windows[0] == ("", "")
    assert windows[1] == ("", "")
    assert windows[2] == ("2024-05", "2024-06")
    assert windows[3] == ("2024-01", "2024-04")


def test_validate_contiguity_detects_overlap_and_gap(policy, six_month_source):
    pol = replace(policy, tiers=four_tiers())
    plan = plan_budget(six_month_source, pol, 20_000,
                       six_month_source.path.parent / "staging")
    validate_contiguity(plan)
    # 篡改月上界使其之和不等于数据月数，必须被拒绝。
    broken = replace(plan, tier_limits=(0, 0, 2, 3))
    with pytest.raises(ValueError):
        validate_contiguity(broken)


def test_frame_months_handles_empty():
    assert frame_months(pd.DataFrame(columns=["timestamp"])) == []
