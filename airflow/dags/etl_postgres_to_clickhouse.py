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

BATCH_SIZE = 1000000  # можно увеличить для больших объёмов
LOOKBACK_DAYS = 30    # окно для late events

# ======================
# FUNCTIONS
# ======================
def fetch_raw_events(**kwargs):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    
    # берем события только за последние LOOKBACK_DAYS
    cur.execute(f"""
        SELECT event_id, type, created_at, received_at, session_id, user_id,
               ip, url, referrer, device_type, user_agent,
               event_title, element_id, x, y, payload, source
        FROM raw_events
        WHERE created_at >= NOW() - INTERVAL '{LOOKBACK_DAYS} DAYS'
        ORDER BY created_at ASC
        LIMIT {BATCH_SIZE}
    """)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    events = [dict(zip(columns, r)) for r in rows]
    return events

def clean_event(event):
    event_clean = {}
    event_clean["event_id"] = str(event.get("event_id") or "")
    event_clean["type"] = event.get("type") or "unknown"
    event_clean["created_at"] = event.get("created_at") or datetime.utcnow()
    event_clean["received_at"] = event.get("received_at") or datetime.utcnow()
    event_clean["session_id"] = event.get("session_id") or ""
    event_clean["user_id"] = int(event.get("user_id") or 0)
    event_clean["ip"] = event.get("ip") or ""
    event_clean["url"] = event.get("url") or ""
    event_clean["referrer"] = event.get("referrer") or ""
    event_clean["device_type"] = event.get("device_type") or ""
    event_clean["user_agent"] = event.get("user_agent") or ""
    event_clean["event_title"] = event.get("event_title") or ""
    event_clean["element_id"] = event.get("element_id") or ""
    event_clean["x"] = int(event.get("x") or 0)
    event_clean["y"] = int(event.get("y") or 0)
    event_clean["payload"] = json.dumps(event.get("payload") or {})
    event_clean["source"] = event.get("source") or "unknown"
    return event_clean

def transfer_to_clickhouse(**kwargs):
    events = fetch_raw_events()
    if not events:
        print("No new events to transfer")
        return
    
    clean_events = [clean_event(e) for e in events]
    
    client = Client(**CLICKHOUSE_CONFIG)
    
    # вставка с ReplacingMergeTree
    client.execute("""
        INSERT INTO events_cleansed (
            event_id, type, created_at, received_at, session_id, user_id,
            ip, url, referrer, device_type, user_agent,
            event_title, element_id, x, y, payload, source
        ) VALUES
    """, clean_events)
    
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
    dag_id='pg_to_clickhouse_sliding_window',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),  # можно чаще, чем раньше
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_raw_to_ch',
        python_callable=transfer_to_clickhouse
    )
