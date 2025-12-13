import time
import random
import grpc
import os
import json
import threading
from concurrent import futures

from kafka import KafkaConsumer, KafkaProducer

import dispatcher_pb2
import dispatcher_pb2_grpc


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
GROUP_ID = os.getenv("WORKER_GROUP_ID", "workers")

HIGH_TOPIC = "notifications.high"
NORMAL_TOPIC = "notifications.normal"
REPLY_TOPIC = "notifications.reply"

WORKER_API_KEY = os.getenv("WORKER_API_KEY", "worker-secret-key")
LOGFILE = os.getenv("WORKER_LOG", "worker_logs.jsonl")

os.makedirs(os.path.dirname(LOGFILE), exist_ok=True)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

high_consumer = KafkaConsumer(
    HIGH_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

normal_consumer = KafkaConsumer(
    NORMAL_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
def log_worker(entry: dict):
    with open(LOGFILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def do_work(n: int = 50_000):
    start = time.perf_counter()
    acc = 0
    for i in range(1, n + 1):
        acc += i * i
    end = time.perf_counter()
    return acc, int((end - start) * 1000)

class WorkerService(dispatcher_pb2_grpc.WorkerServiceServicer):
    def ProcessNotification(self, request, context):
        md = dict(context.invocation_metadata())
        key = md.get("x-api-key", "")
        if WORKER_API_KEY and key != WORKER_API_KEY:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid API key for worker")

        #delay = random.randint (0, 500) / 1000.0
        #time.sleep (delay)

        _, processing_ms = do_work (90000000)
        processed_at = int (time.time () * 1000)

        log_worker({
            "timestamp": time.time(),
            "notificationId": request.notificationId,
            "workerProcessingMs": processing_ms,
            "processedAt": processed_at
        })

        return dispatcher_pb2.WorkerResponseDto(
            success=True,
            notificationId=request.notificationId,
            processedAt=processed_at,
            workerProcessingMs=processing_ms
        )

def handle_kafka_message(msg: dict, topic: str):
    _, processing_ms = do_work(90000000)
    processed_at = int(time.time() * 1000)

    reply = {
        "correlationId": msg["correlationId"],
        "notificationId": msg["notificationId"],
        "workerProcessingMs": processing_ms,
        "processedAt": processed_at,
        "result": "ok"
    }

    producer.send(REPLY_TOPIC, reply)

    log_worker({
        "mode": "kafka",
        "notificationId": msg["notificationId"],
        "processedAt": processed_at,
        "workerProcessingMs": processing_ms,
        "topic": topic
    })

def kafka_loop():
    print("Kafka worker started")

    while True:
        high_msgs = high_consumer.poll(timeout_ms=20, max_records=1)
        if high_msgs:
            for _, records in high_msgs.items():
                handle_kafka_message(records[0].value, HIGH_TOPIC)
            continue

        normal_msgs = normal_consumer.poll(timeout_ms=30, max_records=1)
        for _, records in normal_msgs.items():
            handle_kafka_message(records[0].value, NORMAL_TOPIC)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dispatcher_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    port = os.getenv("WORKER_PORT", "50052")
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Worker gRPC running at :{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    threading.Thread (target=kafka_loop, daemon=True).start ()
    serve()
