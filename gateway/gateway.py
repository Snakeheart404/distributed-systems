from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os
import time
import grpc
import uuid
import json
import pika
import uuid as _uuid

import dispatcher_pb2
import dispatcher_pb2_grpc

app = FastAPI()

GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "client-secret-key")
DISPATCHER_PROXY_HOST = os.getenv("DISPATCHER_PROXY_HOST", "envoy")
DISPATCHER_PROXY_PORT = os.getenv("DISPATCHER_PROXY_PORT", "50051")
DISPATCHER_API_KEY = os.getenv("DISPATCHER_API_KEY", "dispatcher-secret-key")

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
TASK_QUEUE = os.getenv("TASK_QUEUE", "tasks_queue")
LOGS_DIR = os.getenv("LOGS_DIR", "/logs")

channel = grpc.insecure_channel(f"{DISPATCHER_PROXY_HOST}:{DISPATCHER_PROXY_PORT}")
dispatcher_stub = dispatcher_pb2_grpc.DispatcherServiceStub(channel)

pika_params = pika.ConnectionParameters(host=RABBIT_HOST)
pika_conn = pika.BlockingConnection(pika_params)
pika_chan = pika_conn.channel()
pika_chan.queue_declare(queue=TASK_QUEUE, durable=True, arguments={'x-max-priority': 10})

class NotificationRequest(BaseModel):
    message: str
    n: int = 1000
    priority: int = 0

def _log(path, entry: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

@app.get("/")
def root():
    return {"status": "gateway ok"}

@app.post("/notifications", status_code=201)
def create_notification(req: NotificationRequest, x_api_key: str = Header(None)):
    if GATEWAY_API_KEY and x_api_key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key for gateway")
    sent_at = int(time.time() * 1000)
    grpc_request = dispatcher_pb2.DispatchNotificationDto(message=req.message, sentAt=sent_at)
    start = time.perf_counter()
    try:
        grpc_resp = dispatcher_stub.DispatchNotification(grpc_request, metadata=(("x-api-key", DISPATCHER_API_KEY),))
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail="Dispatcher error: " + (e.details() or str(e)))

    return {
        "notificationId": grpc_resp.notificationId,
        "success": grpc_resp.success,
        "dispatcherProcessingMs": grpc_resp.dispatcherProcessingMs,
        "workerProcessingMs": grpc_resp.workerProcessingMs,
        "processedAt": grpc_resp.processedAt
    }

@app.post("/notifications/async", status_code=201)
def create_notification_async(req: NotificationRequest, x_api_key: str = Header(None)):
    if GATEWAY_API_KEY and x_api_key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key for gateway")

    notification_id = str(uuid.uuid4())
    payload = {
        "notificationId": notification_id,
        "task": {"n": req.n, "message": req.message},
        "sentAt": int(time.time() * 1000)
    }

    result = pika_chan.queue_declare('', exclusive=True)
    callback_queue = result.method.queue
    corr_id = str(_uuid.uuid4())
    body = json.dumps(payload).encode('utf-8')
    props = pika.BasicProperties(reply_to=callback_queue, correlation_id=corr_id, delivery_mode=2, priority=req.priority)
    start = time.perf_counter()
    pika_chan.basic_publish(exchange='', routing_key=TASK_QUEUE, properties=props, body=body)

    responses = {}
    def on_response(ch, method, props, body):
        if props.correlation_id == corr_id:
            responses['body'] = body

    pika_chan.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=True)

    timeout = 30.0
    waited = 0.0
    poll = 0.01
    while waited < timeout:
        pika_conn.process_data_events(time_limit=poll)
        if 'body' in responses:
            resp = json.loads(responses['body'].decode('utf-8'))
            end = time.perf_counter()
            total_ms = int((end - start) * 1000)
            return {
                "notificationId": resp.get("notificationId"),
                "result": resp.get("result"),
                "dispatcherTotalTimeMs": total_ms,
                "workerProcessingMs": resp.get("workerProcessingMs"),
                "processedAt": resp.get("processedAt")
            }
        waited += poll

    raise HTTPException(status_code=504, detail="Timeout waiting for worker response")
