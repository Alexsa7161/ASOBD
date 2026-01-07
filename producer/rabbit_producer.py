import json
import time
import uuid
import random
import pika
from datetime import datetime, timedelta

RABBITMQ_HOST = "rabbitmq"
RABBITMQ_QUEUE = "events"
EVENT_COUNT = 200_000
RATE = 200  # сообщений в секунду

URLS = ["/", "/catalog", "/product/1", "/checkout"]
DEVICES = ["desktop", "mobile", "tablet"]
EVENT_TITLES = ["page_view", "add_to_cart", "checkout"]

def generate_event():
    now = datetime.utcnow()
    event_title = random.choice(EVENT_TITLES)
    element_id = random.choice(["#btn", "#link", "#submit"])
    x = random.randint(0, 1920)
    y = random.randint(0, 1080)

    # Составной payload
    payload = {
        "event_title": event_title,
        "element_id": element_id,
        "x": x,
        "y": y
    }

    return {
        "event_id": str(uuid.uuid4()),
        "type": random.choices(["view", "click"], weights=[0.7, 0.3])[0],
        "created_at": now.isoformat(),
        "received_at": (now + timedelta(milliseconds=random.randint(50, 300))).isoformat(),
        "session_id": f"session-{random.randint(1, 50000)}",
        "user_id": random.randint(1, 10000),
        "ip": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}",
        "url": random.choice(URLS),
        "referrer": "/",
        "device_type": random.choice(DEVICES),
        "user_agent": "Mozilla/5.0",
        "event_title": event_title,
        "element_id": element_id,
        "x": x,
        "y": y,
        "payload": payload,
        "source": "rabbitmq"
    }

def main():
    # Подключаемся к RabbitMQ
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
            )
            channel = connection.channel()
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            print("[Producer] connected to RabbitMQ")
            break
        except Exception as e:
            print("[Producer] connection failed, retry in 2s:", e)
            time.sleep(2)

    # Отправляем события пакетами
    for i in range(0, EVENT_COUNT, RATE):
        batch = [generate_event() for _ in range(RATE)]
        for event in batch:
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=json.dumps(event),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        print(f"[Producer] sent batch {i // RATE + 1}")
        time.sleep(1)

    print("[Producer] done sending")
    connection.close()

if __name__ == "__main__":
    main()
