from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import os
import time
import grpc

import dispatcher_pb2
import dispatcher_pb2_grpc

app = FastAPI()

GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY", "client-secret-key")  # ключ що має надсилати клієнт
DISPATCHER_PROXY_HOST = os.getenv("DISPATCHER_PROXY_HOST", "envoy")
DISPATCHER_PROXY_PORT = os.getenv("DISPATCHER_PROXY_PORT", "50051")
DISPATCHER_API_KEY = os.getenv("DISPATCHER_API_KEY", "dispatcher-secret-key")  # ключ, який gateway відправляє dispatcher

channel = grpc.insecure_channel(f"{DISPATCHER_PROXY_HOST}:{DISPATCHER_PROXY_PORT}")
dispatcher_stub = dispatcher_pb2_grpc.DispatcherServiceStub(channel)

class NotificationRequest(BaseModel):
    message: str

@app.post("/notifications", status_code=201)
def create_notification(req: NotificationRequest, x_api_key: str = Header(None)):
    if GATEWAY_API_KEY and x_api_key != GATEWAY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key for gateway")

    sent_at = int(time.time() * 1000)
    grpc_request = dispatcher_pb2.DispatchNotificationDto(
        message=req.message,
        sentAt=sent_at
    )

    try:
        resp = dispatcher_stub.DispatchNotification(grpc_request, metadata=(("x-api-key", DISPATCHER_API_KEY),))
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail="Dispatcher error: " + (e.details() or str(e)))

    return {
        "notificationId": resp.notificationId,
        "success": resp.success,
        "dispatcherProcessingMs": resp.dispatcherProcessingMs,
        "workerProcessingMs": resp.workerProcessingMs,
        "processedAt": resp.processedAt
    }