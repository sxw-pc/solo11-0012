"""Flask 本地 Web 应用工厂；服务仅应绑定 127.0.0.1。"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from flask import Flask, jsonify

from ..archiver import Archiver
from ..config_loader import init_workspace, load_budget, load_policy
from ..fixture_loader import fixture_summary
from ..metadata_store import MetadataStore
from ..schema import ConfigError


def create_app(workdir: Path,
               policy_path: Optional[Path] = None,
               testing: bool = False) -> Flask:
    workdir = Path(workdir).resolve()
    policy_file = init_workspace(workdir, policy_path)
    policy = load_policy(policy_file, workdir)
    archiver = Archiver(workdir, policy, budget=load_budget(workdir))
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.update(TESTING=testing, BIND_HOST="127.0.0.1",
                      WORKDIR=str(workdir), POLICY_FILE=str(policy_file))
    app.extensions["time_archiver"] = {
        "workdir": workdir, "policy_file": policy_file,
        "policy": policy, "archiver": archiver,
    }

    from .routes import bp
    app.register_blueprint(bp)

    @app.template_filter("short_hash")
    def short_hash(value):
        text = str(value or "")
        return text[:12]

    @app.errorhandler(ConfigError)
    def config_error(error):
        if testing:
            return jsonify({"error": str(error), "fields": error.errors}), 400
        return jsonify({"error": str(error), "fields": error.errors}), 400

    @app.errorhandler(404)
    def not_found(_error):
        return jsonify({"error": "资源不存在"}), 404

    @app.errorhandler(500)
    def server_error(_error):
        return jsonify({"error": "本地服务内部错误"}), 500

    @app.context_processor
    def inject_globals():
        return {
            "app_name": "时档匠",
            "fixture_summary": fixture_summary(),
            "bind_host": app.config["BIND_HOST"],
        }

    return app


def refresh_policy(app: Flask) -> None:
    """Web 修改策略后重建归档服务，保持配置与运行态一致。"""
    workdir = Path(app.extensions["time_archiver"]["workdir"])
    policy_file = Path(app.extensions["time_archiver"]["policy_file"])
    policy = load_policy(policy_file, workdir)
    app.extensions["time_archiver"].update({
        "policy": policy,
        "archiver": Archiver(workdir, policy, budget=load_budget(workdir)),
        "store": MetadataStore(workdir / "state" / "archive.db"),
    })


def store_for(app: Flask) -> MetadataStore:
    return app.extensions["time_archiver"]["archiver"].store
