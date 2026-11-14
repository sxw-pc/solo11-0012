"""CLI 与命令模块共享的输出和工作目录初始化工具。"""
from __future__ import annotations

import json
from pathlib import Path

import click

from .archiver import Archiver
from .audit import AuditLog
from .config_loader import init_workspace, load_budget, load_policy
from .metadata_store import MetadataStore


def emit(value) -> None:
    click.echo(json.dumps(value, ensure_ascii=False, indent=2))


def runtime(workdir: Path):
    policy_path = init_workspace(workdir)
    policy = load_policy(policy_path, workdir)
    budget = load_budget(workdir)
    return (policy, Archiver(workdir, policy, budget=budget),
            MetadataStore(workdir / "state" / "archive.db"),
            AuditLog(workdir / "audit" / "audit.jsonl"))
