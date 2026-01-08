import os
import uuid
import pytest
import psycopg2
from fastapi.testclient import TestClient
from api.app import app, save_event


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "clickstream")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "clickstream")
POSTGRES_DB = os.getenv("POSTGRES_DB", "clickstream_write")

def get_db_cursor():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB
    )
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "message": "API is running"}


def test_post_single_event():
    event_data = [
        {
            "event_id": str(uuid.uuid4()),
            "type": "click",
            "created_at": "2026-01-07T12:00:00",
            "received_at": "2026-01-07T12:01:00",
            "session_id": "sess1",
            "user_id": 42,
            "ip": "127.0.0.1",
            "url": "/home",
            "referrer": "/landing",
            "device_type": "desktop",
            "user_agent": "test-agent",
            "event_title": "button_click",
            "element_id": "btn-1",
            "x": 100,
            "y": 200
        }
    ]

    response = client.post("/events", json=event_data)
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "count": 1}


def test_save_event_defaults():
    event = {
        "event_id": str(uuid.uuid4()),
        "type": "view",
        "created_at": "2026-01-07T12:10:00",
        "received_at": "2026-01-07T12:11:00",
        "session_id": None,
        "user_id": None,
        "ip": None,
        "url": None,
        "referrer": None,
        "device_type": None,
        "user_agent": None
    }

    save_event(event)
    assert event["source"] == "http"
    assert isinstance(event["payload"], str)
