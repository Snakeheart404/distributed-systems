from fastapi import FastAPI
from pydantic import BaseModel
import time
import os
import grpc

import dispatcher_pb2
import dispatcher_pb2_grpc

app = FastAPI()

dispatcher_host = os.getenv("DISPATCHER_HOST", "dispatcher")
dispatcher_port = os.getenv("DISPATCHER_PORT", "50051")

channel = grpc.insecure_channel(f"{dispatcher_host}:{dispatcher_port}")
dispatcher_client = dispatcher_pb2_grpc.DispatcherServiceStub(channel)

class NotificationRequest(BaseModel):
    message: str

@app.post("/notifications", status_code=201)
def create_notification(req: NotificationRequest):
    sent_at = int(time.time() * 1000)

    grpc_request = dispatcher_pb2.DispatchNotificationDto(
        message=req.message,
        sentAt=sent_at
    )

    grpc_resp = dispatcher_client.DispatchNotification(grpc_request)

    return {
        "notificationId": grpc_resp.notificationId,
        "success": grpc_resp.success,
        "dispatcherProcessingMs": grpc_resp.dispatcherProcessingMs,
        "workerProcessingMs": grpc_resp.workerProcessingMs,
        "processedAt": grpc_resp.processedAt
    }
