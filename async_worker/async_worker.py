import pika
import time
import json
import os

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
TASK_QUEUE = os.getenv("TASK_QUEUE", "tasks_queue")
WORKER_LOG = os.getenv("WORKER_LOG", "/logs/worker_async_logs.jsonl")

params = pika.ConnectionParameters(host=RABBIT_HOST)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.queue_declare(queue=TASK_QUEUE, durable=True, arguments={'x-max-priority': 10})

def log_worker(entry: dict)
    with open(WORKER_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def on_request(ch, method, props, body):
    payload = json.loads(body.decode('utf-8'))
    start = time.perf_counter()

    task = payload.get("task", {})
    n = task.get("n", 1000)
    s = 0
    for i in range(1, n+1):
        s += i * i

    end = time.perf_counter()
    processing_ms = int((end - start) * 1000)

    response = {
        "notificationId": payload.get("notificationId"),
        "result": s,
        "workerProcessingMs": processing_ms,
        "processedAt": int(time.time() * 1000)
    }

    log_worker({
        "timestamp": time.time(),
        "notificationId": payload.get("notificationId"),
        "workerProcessingMs": processing_ms,
        "task": task
    })

    if props.reply_to:
        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=json.dumps(response).encode('utf-8')
        )
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=TASK_QUEUE, on_message_callback=on_request)
print(" [x] Awaiting RPC requests (async worker)")
channel.start_consuming()
