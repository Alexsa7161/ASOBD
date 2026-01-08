import os
from fastapi.testclient import TestClient
from tracker.app import app, HTML_FILE

client = TestClient(app)


def test_tracker_root_returns_html():
    # файл реально существует
    assert os.path.exists(HTML_FILE)

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")

    # проверяем, что тело не пустое
    assert response.content
