"""离线约束：归档与 Web 页面不创建任何网络套接字。"""
from __future__ import annotations

import socket
import urllib.request

from time_archiver.archiver import Archiver
from time_archiver.web import create_app


def test_archive_and_web_work_with_network_blocked(policy, workdir, long_source, monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("离线工具不应发起网络连接")

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(urllib.request, "urlopen", blocked)

    result = Archiver(workdir, policy).run_source(long_source)
    assert result["status"] == "completed"
    assert result["shards"] > 0

    app = create_app(workdir / "web", testing=True)
    client = app.test_client()
    for path in ("/tasks", "/policy", "/archive", "/audit"):
        response = client.get(path)
        assert response.status_code == 200
        assert "http://" not in response.get_data(as_text=True)
        assert "https://" not in response.get_data(as_text=True)
    assert app.config["BIND_HOST"] == "127.0.0.1"
