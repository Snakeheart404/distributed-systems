import os
import json
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

EVENTS_TOPIC = "events"
HIGH_TOPIC = "notifications.high"
NORMAL_TOPIC = "notifications.normal"

consumer = KafkaConsumer(
    EVENTS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="process-manager",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def handle_event(event: dict):
    if event.get("eventType") == "NotificationCreated":
        correlation_id = event.get ("correlationId")
        if not correlation_id:
            return

        priority = event.get("priority", "normal")

        command = {
            "correlationId": correlation_id,
            "notificationId": event["notificationId"],
            "message": event["message"],
            "sentAt": event["timestamp"],
        }

        if priority == "high":
            producer.send(HIGH_TOPIC, command)
        else:
            producer.send(NORMAL_TOPIC, command)
    else:
        return

for msg in consumer:
    event = msg.value
    handle_event(event)
