from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import os
import time
import grpc
import redis
import uuid

import dispatcher_pb2
import dispatcher_pb2_grpc

app = FastAPI()

GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "client-secret-key")
DISPATCHER_PROXY_HOST = os.getenv("DISPATCHER_PROXY_HOST", "envoy")
DISPATCHER_PROXY_PORT = os.getenv("DISPATCHER_PROXY_PORT", "50051")
DISPATCHER_API_KEY = os.getenv("DISPATCHER_API_KEY", "dispatcher-secret-key")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL)

channel = grpc.insecure_channel(f"{DISPATCHER_PROXY_HOST}:{DISPATCHER_PROXY_PORT}")
dispatcher_stub = dispatcher_pb2_grpc.DispatcherServiceStub(channel)

class NotificationRequest(BaseModel):
    message: str
    priority: str = "normal"
    mode: str = "async"

@app.post("/notifications", status_code=201)
def create_notification(req: NotificationRequest, x_api_key: str = Header(None)):
    if GATEWAY_API_KEY and x_api_key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key for gateway")

    sent_at = int(time.time() * 1000)

    grpc_request = dispatcher_pb2.DispatchNotificationDto(
        message=req.message,
        sentAt=sent_at,
        priority=req.priority,
        mode=req.mode
    )
    try:
        resp = dispatcher_stub.DispatchNotification(grpc_request, metadata=(("x-api-key", DISPATCHER_API_KEY),))
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail="Dispatcher error: " + (e.details() or str(e)))

    return {
        "notificationId": resp.notificationId,
        "mode": req.mode,
        "priority": req.priority,
        "success": resp.success,
        "dispatcherProcessingMs": resp.dispatcherProcessingMs,
        "workerProcessingMs": resp.workerProcessingMs,
        "processedAt": resp.processedAt
    }

@app.get("/notifications/{notification_id}")
def get_notification(notification_id: str, x_api_key: str = Header(None)):
    if x_api_key != GATEWAY_API_KEY:
        raise HTTPException(401, "Invalid API key")

    data = redis_client.hgetall(f"notification:{notification_id}")
    if not data:
        raise HTTPException(404, "Not found")

    return {k.decode(): v.decode() for k, v in data.items()}