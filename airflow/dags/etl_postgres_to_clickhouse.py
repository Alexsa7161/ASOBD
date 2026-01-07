from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from clickhouse_driver import Client
import json

# ======================
# CONFIG
# ======================
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "clickstream",
    "user": "clickstream",
    "password": "clickstream"
}

CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "port": 9000,
    "user": "default",
    "password": "",
    "database": "clickstream"
}

BATCH_SIZE = 100_000
LOOKBACK_DAYS = 30

# ======================
# FUNCTIONS
# ======================
def fetch_raw_events(batch_size=BATCH_SIZE):
    """Берем сырые события из Postgres"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT event_id, type, created_at, received_at, session_id, user_id,
               ip, url, referrer, device_type, user_agent,
               payload, source
        FROM raw_events
        WHERE created_at >= NOW() - INTERVAL '{LOOKBACK_DAYS} DAYS'
        ORDER BY created_at ASC
        LIMIT {batch_size}
    """)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    
    return [dict(zip(columns, r)) for r in rows]

def clean_and_parse_event(event):
    """Очистка и разбор payload"""
    payload = event.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}

    # Валидируем поля и вытаскиваем из payload
    event_title = payload.get("event_title") or ""
    element_id = payload.get("element_id") or ""
    x = int(payload.get("x") or 0)
    y = int(payload.get("y") or 0)

    return {
        "event_id": str(event.get("event_id") or ""),
        "type": event.get("type") or "unknown",
        "created_at": event.get("created_at") or datetime.utcnow(),
        "received_at": event.get("received_at") or datetime.utcnow(),
        "session_id": event.get("session_id") or "",
        "user_id": int(event.get("user_id") or 0),
        "ip": event.get("ip") or "",
        "url": event.get("url") or "",
        "referrer": event.get("referrer") or "",
        "device_type": event.get("device_type") or "",
        "user_agent": event.get("user_agent") or "",
        "event_title": event_title,
        "element_id": element_id,
        "x": x,
        "y": y,
        "payload": json.dumps(payload),  # оставляем составной payload как есть
        "source": event.get("source") or "unknown"
    }

def transfer_to_clickhouse(**kwargs):
    events = fetch_raw_events()
    if not events:
        print("No new events to transfer")
        return
    
    clean_events = [clean_and_parse_event(e) for e in events]
    
    client = Client(**CLICKHOUSE_CONFIG)
    
    # Вставляем данные по колонкам
    data = [
        (
            e["event_id"], e["type"], e["created_at"], e["received_at"], e["session_id"],
            e["user_id"], e["ip"], e["url"], e["referrer"], e["device_type"],
            e["user_agent"], e["event_title"], e["element_id"], e["x"], e["y"],
            e["payload"], e["source"]
        )
        for e in clean_events
    ]
    
    client.execute("""
        INSERT INTO events_cleansed (
            event_id, type, created_at, received_at, session_id,
            user_id, ip, url, referrer, device_type,
            user_agent, event_title, element_id, x, y,
            payload, source
        ) VALUES
    """, data)
    
    print(f"Transferred {len(clean_events)} events to ClickHouse")

# ======================
# DAG DEFINITION
# ======================
default_args = {
    'owner': 'clickstream',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 3),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id='pg_to_clickhouse_payload_parsing',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_raw_to_ch',
        python_callable=transfer_to_clickhouse
    )
