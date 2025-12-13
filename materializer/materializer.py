import os
import json
import redis
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
EVENTS_TOPIC = "events"

r = redis.Redis.from_url(REDIS_URL)

consumer = KafkaConsumer(
    EVENTS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="materializer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

print("Materializer started...")

for msg in consumer:
    event = msg.value
    event_type = event["eventType"]
    nid = event["notificationId"]

    if event_type == "NotificationCreated":
        r.hset(f"notification:{nid}", mapping={
            "notificationId": nid,
            "message": event["message"],
            "priority": event["priority"],
            "mode": event["mode"],
            "createdAt": event["timestamp"]
        })

    elif event_type == "NotificationProcessed":
        r.hset(f"notification:{nid}", mapping={
            "workerProcessingMs": event["workerProcessingMs"],
            "processedAt": event["processedAt"]
        })
