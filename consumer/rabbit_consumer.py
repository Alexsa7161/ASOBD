import json
import time
import pika
import psycopg2
import os
from psycopg2.extras import execute_values

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "clickstream-rabbitmq")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "events")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "clickstream_write")
DB_USER = os.getenv("DB_USER", "clickstream")
DB_PASSWORD = os.getenv("DB_PASSWORD", "clickstream")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def create_table():
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
                    payload JSONB,
                    source TEXT
                )
            """)
    conn.close()

def save_events_batch(events):
    if not events:
        return

    for e in events:
        e["payload"] = json.dumps({
            "event_title": e.get("event_title"),
            "element_id": e.get("element_id"),
            "x": e.get("x"),
            "y": e.get("y")
        })

    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw_events (
                    event_id, type, created_at, received_at,
                    session_id, user_id, ip, url, referrer,
                    device_type, user_agent, payload, source
                ) VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                [
                    (
                        e["event_id"],
                        e["type"],
                        e["created_at"],
                        e["received_at"],
                        e["session_id"],
                        e["user_id"],
                        e["ip"],
                        e["url"],
                        e["referrer"],
                        e["device_type"],
                        e["user_agent"],
                        e["payload"],
                        e.get("source", "rabbitmq")
                    )
                    for e in events
                ]
            )
    conn.close()

def main():
    create_table()

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
            )
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            print("[Consumer] connected to RabbitMQ")

            def callback(ch, method, properties, body):
                event = json.loads(body)
                save_events_batch([event])
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except Exception as e:
            print("[Consumer] RabbitMQ connection failed, retry in 5s:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()