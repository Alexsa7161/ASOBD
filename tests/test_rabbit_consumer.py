# tests/test_rabbit_consumer.py
import os
import uuid
import pytest
import psycopg2
from consumer import rabbit_consumer as rc

# --------------------------
# Настройки БД
# --------------------------
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_USER = os.getenv("DB_USER", "clickstream")
DB_PASSWORD = os.getenv("DB_PASSWORD", "clickstream")
DB_NAME = os.getenv("DB_NAME", "clickstream")

# --------------------------
# Фикстура для курсора к БД
# --------------------------
@pytest.fixture
def db_cursor():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()

# --------------------------
# Подготовка таблицы перед тестами
# --------------------------
@pytest.fixture(scope="module", autouse=True)
def setup_table():
    rc.create_table()

# =========================
# Тест сохранения одиночного события
# =========================
def test_save_single_event(db_cursor):
    event = {
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

    rc.save_events_batch([event])

    # Проверяем, что событие добавилось в БД
    db_cursor.execute("SELECT * FROM raw_events WHERE event_id = %s", (event["event_id"],))
    result = db_cursor.fetchone()
    assert result is not None
    # Проверяем id и type
    assert str(result[0]) == event["event_id"]
    assert result[1] == event["type"]

# =========================
# Тест сохранения события с дефолтами
# =========================
def test_save_event_defaults(db_cursor):
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

    rc.save_events_batch([event])

    db_cursor.execute(
        "SELECT payload, source FROM raw_events WHERE event_id = %s",
        (event["event_id"],)
    )
    payload, source = db_cursor.fetchone()

    # payload уже dict, не нужно json.loads
    assert isinstance(payload, dict)
    assert source == "rabbitmq"  # default source
    # Проверяем, что поля payload есть даже если None
    assert "event_title" in payload
    assert "element_id" in payload
    assert "x" in payload
    assert "y" in payload
