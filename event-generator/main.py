import os
import csv
import json
import uuid
import time
import random
import threading
import queue
import requests
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

# ======================
# CONFIG
# ======================
EVENT_COUNT = int(os.getenv("EVENT_COUNT", 200_000))

CSV_ENABLED = True
CSV_PATH = "/data/events.csv"

HTTP_ENABLED = True
HTTP_ENDPOINT = "http://api:8000/events"
HTTP_RATE = 200  # events/sec
HTTP_BATCH_SIZE = 50
HTTP_SLEEP = HTTP_BATCH_SIZE / HTTP_RATE   # 0.25 sec

DB_HOST = "postgres"
DB_PORT = 5432
DB_NAME = "clickstream"
DB_USER = "clickstream"
DB_PASSWORD = "clickstream"

# ======================
# EVENT GENERATION
# ======================
URLS = ["/", "/catalog", "/product/1", "/checkout"]
DEVICES = ["desktop", "mobile", "tablet"]
EVENT_TITLES = ["page_view", "add_to_cart", "checkout"]

def generate_event():
    now = datetime.utcnow()
    return {
        "event_id": str(uuid.uuid4()),
        "type": random.choices(["view", "click"], weights=[0.7, 0.3])[0],
        "created_at": now,
        "received_at": now + timedelta(milliseconds=random.randint(50, 300)),
        "session_id": f"session-{random.randint(1, 50000)}",
        "user_id": random.randint(1, 10000),
        "ip": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        "url": random.choice(URLS),
        "referrer": "/",
        "device_type": random.choice(DEVICES),
        "user_agent": "Mozilla/5.0",
        "event_title": random.choice(EVENT_TITLES),
        "element_id": random.choice(["#btn", "#link", "#submit"]),
        "x": random.randint(0, 1920),
        "y": random.randint(0, 1080),
        "payload": {}
    }

# ======================
# DB
# ======================
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_table_if_not_exists():
    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_events (
                    event_id UUID PRIMARY KEY,
                    type TEXT,
                    created_at TIMESTAMP,
                    received_at TIMESTAMP,
                    session_id TEXT,
                    user_id BIGINT,
                    ip TEXT,
                    url TEXT,
                    referrer TEXT,
                    device_type TEXT,
                    user_agent TEXT,
                    event_title TEXT,
                    element_id TEXT,
                    x INT,
                    y INT,
                    payload JSONB,
                    source TEXT
                )
            """)
    conn.close()

def save_events_batch(events):
    if not events:
        return

    for e in events:
        e["payload"] = json.dumps(e["payload"])

    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw_events (
                    event_id, type, created_at, received_at,
                    session_id, user_id, ip, url, referrer,
                    device_type, user_agent, event_title, element_id,
                    x, y, payload, source
                ) VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                [
                    (
                        e["event_id"], e["type"], e["created_at"], e["received_at"],
                        e["session_id"], e["user_id"], e["ip"], e["url"], e["referrer"],
                        e["device_type"], e["user_agent"], e["event_title"], e["element_id"],
                        e["x"], e["y"], e["payload"], e["source"]
                    )
                    for e in events
                ]
            )
    conn.close()

# ======================
# CSV WORKER
# ======================
def csv_worker():
    print("[CSV] started")
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

    events = []
    for _ in range(EVENT_COUNT):
        e = generate_event()
        e["source"] = "csv"
        events.append(e)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=events[0].keys())
        writer.writeheader()
        writer.writerows(events)

    save_events_batch(events)
    print(f"[CSV] inserted {len(events)} events")

# ======================
# HTTP WORKER
# ======================
http_queue = queue.Queue()

def http_worker():
    print("[HTTP] started")
    while True:
        batch = []
        for _ in range(HTTP_BATCH_SIZE):
            try:
                e = http_queue.get(timeout=1)
                e["source"] = "http"
                batch.append(e)
                http_queue.task_done()
            except queue.Empty:
                break

        if batch:
            try:
                requests.post(HTTP_ENDPOINT, json=batch, timeout=5)
            except Exception as e:
                print(f"[HTTP] error: {e}")

            save_events_batch(batch)

        time.sleep(HTTP_SLEEP)

# ======================
# MAIN
# ======================
create_table_if_not_exists()

threads = []

if CSV_ENABLED:
    t = threading.Thread(target=csv_worker, daemon=True)
    t.start()
    threads.append(t)

if HTTP_ENABLED:
    t = threading.Thread(target=http_worker, daemon=True)
    t.start()
    threads.append(t)

# генерация событий для HTTP
for _ in range(EVENT_COUNT):
    e = generate_event()
    if HTTP_ENABLED:
        http_queue.put(e.copy())

http_queue.join()

for t in threads:
    t.join()

print("[Generator] all sources processed")
