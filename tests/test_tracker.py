import os
from fastapi.testclient import TestClient
from tracker.app import app, HTML_FILE

client = TestClient(app)


def test_tracker_root_returns_html():
    assert os.path.exists(HTML_FILE)

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.content
