import os
import json
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import psycopg2

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

def create_table_if_not_exists():
    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_events (
                event_id UUID PRIMARY KEY,
                type TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                received_at TIMESTAMP NOT NULL,
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

def save_event(event):
    event["payload"] = json.dumps({
        "event_title": event.get("event_title"),
        "element_id": event.get("element_id"),
        "x": event.get("x"),
        "y": event.get("y")
    })
    
    if "source" not in event:
        event["source"] = "http"

    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw_events (
                    event_id, type, created_at, received_at,
                    session_id, user_id, ip, url, referrer,
                    device_type, user_agent, payload, source
                ) VALUES (
                    %(event_id)s, %(type)s, %(created_at)s, %(received_at)s,
                    %(session_id)s, %(user_id)s, %(ip)s, %(url)s, %(referrer)s,
                    %(device_type)s, %(user_agent)s, %(payload)s, %(source)s
                ) ON CONFLICT (event_id) DO NOTHING
            """, event)
    conn.close()



app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

create_table_if_not_exists()

@app.post("/events")
async def receive_events(request: Request):
    events = await request.json()
    for e in events:
        save_event(e)
    return {"status": "ok", "count": len(events)}

@app.get("/")
def root():
    return {"status": "ok", "message": "API is running"}
