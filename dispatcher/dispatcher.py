import os
import time
import json
import uuid
import threading
import grpc
from concurrent import futures
from typing import Dict

from kafka import KafkaProducer, KafkaConsumer

import dispatcher_pb2
import dispatcher_pb2_grpc

WORKER_PROXY_HOST = os.getenv ("WORKER_PROXY_HOST", "envoy")
WORKER_PROXY_PORT = os.getenv ("WORKER_PROXY_PORT", "50052")
DISPATCHER_API_KEY = os.getenv ("DISPATCHER_API_KEY", "dispatcher-secret-key")
WORKER_API_KEY = os.getenv ("WORKER_API_KEY", "worker-secret-key")
LOGFILE = os.getenv ("DISPATCHER_LOG", "dispatcher_logs.jsonl")

KAFKA_BOOTSTRAP = os.getenv ("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
EVENTS_TOPIC = "events"
REPLY_TOPIC = "notifications.reply"

os.makedirs(os.path.dirname(LOGFILE), exist_ok=True)


def log_dispatcher(entry: dict):
    with open (LOGFILE, "a", encoding="utf-8") as f:
        f.write (json.dumps (entry, ensure_ascii=False) + "\n")

event_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
reply_consumer = KafkaConsumer (
    REPLY_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads (v.decode ("utf-8")),
    group_id="dispatcher-replies",
    auto_offset_reset="latest",
)

pending_replies: Dict[str, dict] = {}

def reply_listener():
    for msg in reply_consumer:
        data = msg.value
        corr_id = data.get ("correlationId")
        if corr_id:
            pending_replies[corr_id] = data


threading.Thread (target=reply_listener, daemon=True).start ()


class DispatcherService (dispatcher_pb2_grpc.DispatcherServiceServicer):
    def __init__(self):
        target = f"{WORKER_PROXY_HOST}:{WORKER_PROXY_PORT}"
        self.worker_stub = dispatcher_pb2_grpc.WorkerServiceStub (
            grpc.insecure_channel (target)
        )

    def DispatchNotification(self, request, context):
        md = dict (context.invocation_metadata ())
        key = md.get ("x-api-key", "")
        if DISPATCHER_API_KEY and key != DISPATCHER_API_KEY:
            context.abort (grpc.StatusCode.UNAUTHENTICATED, "Invalid API key for dispatcher")

        notification_id = str (uuid.uuid4 ())
        correlation_id = str(uuid.uuid4())

        dispatcher_start = time.perf_counter ()
        dispatcher_start_ts = int (time.time () * 1000)

        event_producer.send(EVENTS_TOPIC, {
            "eventType": "NotificationCreated",
            "notificationId": notification_id,
            "correlationId": correlation_id,
            "message": request.message,
            "priority": request.priority,
            "mode": request.mode,
            "timestamp": dispatcher_start_ts
        })
        event_producer.flush()

        if request.mode == "sync":
            worker_req = dispatcher_pb2.WorkerRequestDto (
                message=request.message,
                sentAt=request.sentAt,
                notificationId=notification_id,
                dispatcherStartedAt=dispatcher_start_ts
            )

            try:
                worker_resp = self.worker_stub.ProcessNotification (worker_req,
                                                                    metadata=(("x-api-key", WORKER_API_KEY),))
            except grpc.RpcError as e:
                dispatcher_end = time.perf_counter ()
                total_ms = int ((dispatcher_end - dispatcher_start) * 1000)
                log_dispatcher ({
                    "timestamp": time.time (),
                    "notificationId": notification_id,
                    "error": e.details () if hasattr (e, "details") else str (e),
                    "status_code": e.code ().name if hasattr (e, "code") else None,
                    "total_time_ms": total_ms
                })
                context.abort (grpc.StatusCode.INTERNAL, "Worker call failed")

            dispatcher_end = time.perf_counter ()
            dispatcher_ms = int ((dispatcher_end - dispatcher_start) * 1000)

            event_producer.send (EVENTS_TOPIC, {
                "eventType": "NotificationProcessed",
                "notificationId": notification_id,
                "workerProcessingMs": worker_resp.workerProcessingMs,
                "processedAt": worker_resp.processedAt
            })

            log_dispatcher ({
                "timestamp": time.time (),
                "notificationId": notification_id,
                "total_time_ms": dispatcher_ms,
                "workerProcessingMs": worker_resp.workerProcessingMs
            })

            return dispatcher_pb2.DispatchResponse (
                success=worker_resp.success,
                notificationId=notification_id,
                dispatcherProcessingMs=dispatcher_ms,
                workerProcessingMs=worker_resp.workerProcessingMs,
                processedAt=worker_resp.processedAt
            )

        # work with kafka

        timeout = 200.0
        waited = 0.0
        poll = 0.05

        while waited < timeout:
            if correlation_id in pending_replies:
                reply = pending_replies.pop (correlation_id)
                dispatcher_ms = int ((time.perf_counter () - dispatcher_start) * 1000)

                event_producer.send (EVENTS_TOPIC, {
                    "eventType": "NotificationProcessed",
                    "notificationId": notification_id,
                    "correlationId": correlation_id,
                    "workerProcessingMs": reply["workerProcessingMs"],
                    "processedAt": reply["processedAt"]
                })

                log_dispatcher ({
                    "notificationId": notification_id,
                    "mode": "async",
                    "priority": request.priority,
                    "dispatcherProcessingMs": dispatcher_ms,
                    "workerProcessingMs": reply["workerProcessingMs"]
                })

                return dispatcher_pb2.DispatchResponse (
                    success=True,
                    notificationId=notification_id,
                    dispatcherProcessingMs=dispatcher_ms,
                    workerProcessingMs=reply["workerProcessingMs"],
                    processedAt=reply["processedAt"]
                )

            time.sleep (poll)
            waited += poll

        context.abort (grpc.StatusCode.DEADLINE_EXCEEDED, "Async timeout")


def serve():
    server = grpc.server (futures.ThreadPoolExecutor (max_workers=10))
    dispatcher_pb2_grpc.add_DispatcherServiceServicer_to_server (DispatcherService (), server)
    port = os.getenv ("DISPATCHER_PORT", "50051")
    server.add_insecure_port (f"[::]:{port}")
    server.start ()
    print (f"Dispatcher gRPC running at :{port}")
    server.wait_for_termination ()


if __name__ == "__main__":
    serve ()
