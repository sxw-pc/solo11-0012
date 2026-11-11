"""Flask 测试客户端：本地页面、策略编辑、任务、检索、恢复与审计。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from time_archiver.web import create_app


@pytest.fixture
def app(tmp_path):
    return create_app(tmp_path / "web", testing=True)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def archived(client, long_source):
    response = client.post("/tasks/run", data={
        "mode": "custom", "source_path": str(long_source.path),
        "format": "parquet", "timestamp_column": "timestamp",
        "label_columns": "sensor_id", "metric_columns": "temperature,humidity,pressure",
        "source_id": "web-source",
    })
    assert response.status_code == 200
    return response


def test_local_pages_render_without_external_assets(client, app):
    assert app.config["BIND_HOST"] == "127.0.0.1"
    assert client.get("/").status_code == 302
    for path in ("/tasks", "/policy", "/archive", "/audit"):
        response = client.get(path)
        assert response.status_code == 200
        assert "时档匠" in response.get_data(as_text=True)
        assert "http://" not in response.get_data(as_text=True).replace("http://localhost", "")
        assert "https://" not in response.get_data(as_text=True)
    assert client.get("/static/style.css").status_code == 200
    assert client.get("/static/common.js").status_code == 200
    assert client.get("/not-found").status_code == 404


def test_manual_custom_task_search_restore_and_api(client, archived, long_source):
    text = archived.get_data(as_text=True)
    assert "已触发 1 个数据源" in text
    tasks = client.get("/api/tasks").get_json()
    assert tasks["counts"]["tasks"] == 1
    task_id = tasks["tasks"][0]["task_id"]
    assert client.get(f"/api/tasks/{task_id}").get_json()["status"] == "completed"
    assert client.get("/api/tasks/absent").status_code == 404

    search = client.get("/archive?label_key=sensor_id&label_value=sensor-long")
    assert search.status_code == 200
    assert "data.parquet" in search.get_data(as_text=True)
    page = client.get("/archive?start=2024-01-01&end=2025-12-31")
    assert "条时间索引" in page.get_data(as_text=True)


def test_restore_download_and_errors(client, archived, app):
    from time_archiver.web import store_for
    entries = store_for(app).search(tag_key="sensor_id", tag_value="sensor-long")
    response = client.post("/archive/restore", data={"shard_id": entries[0]["shard_id"]})
    assert response.status_code == 200
    assert response.headers["Content-Disposition"].startswith("attachment")
    assert len(response.data) > 0
    missing = client.post("/archive/restore", data={"shard_id": "none"})
    assert missing.status_code == 404


def test_policy_view_edit_success_and_failure(client, app):
    response = client.get("/policy")
    assert response.status_code == 200
    text = (Path(app.config["WORKDIR"]) / "policy.yaml").read_text(encoding="utf-8")
    changed = text.replace("name: default_hot_warm_cold_archive", "name: edited_policy")
    response = client.post("/policy", data={"yaml_text": changed})
    assert response.status_code == 200
    assert "策略已校验并保存" in response.get_data(as_text=True)
    assert app.extensions["time_archiver"]["policy"].name == "edited_policy"
    invalid = client.post("/policy", data={"yaml_text": "version: 1\nname: bad\ntiers: []\n"})
    assert invalid.status_code == 200
    assert "配置未保存" in invalid.get_data(as_text=True)
    empty = client.post("/policy", data={"yaml_text": "   "})
    assert "YAML 内容不能为空" in empty.get_data(as_text=True)
    assert app.extensions["time_archiver"]["archiver"].audit.verify()["valid"]


def test_custom_task_validation_and_demo_source_missing(client):
    response = client.post("/tasks/run", data={"mode": "custom", "source_path": ""})
    assert "路径不能为空" in response.get_data(as_text=True)


def test_audit_page_filters_and_exports(client, archived):
    page = client.get("/audit?actor=WEB&result=success&operation=archive")
    assert page.status_code == 200
    assert "archive" in page.get_data(as_text=True)
    json_export = client.get("/audit/export?format=json&actor=WEB")
    assert json_export.status_code == 200
    payload = json.loads(json_export.data)
    assert isinstance(payload, list)
    markdown = client.get("/audit/export?format=markdown")
    assert markdown.status_code == 200
    assert markdown.mimetype == "text/markdown"
    assert "时档匠审计导出" in markdown.get_data(as_text=True)
    assert client.get("/audit/export?format=xml").status_code == 400


def test_archive_parameter_errors(client):
    assert "时间格式非法" in client.get("/archive?start=notadate").get_data(as_text=True)
    assert "limit 必须是整数" in client.get("/archive?limit=abc").get_data(as_text=True)


def test_task_unknown_mode_still_uses_custom(client):
    response = client.post("/tasks/run", data={"mode": "unknown"})
    assert response.status_code == 200
    assert "路径不能为空" in response.get_data(as_text=True)
