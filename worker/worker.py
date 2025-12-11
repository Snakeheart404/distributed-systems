import time
import random
import grpc
import os
import json
from concurrent import futures

import dispatcher_pb2
import dispatcher_pb2_grpc

WORKER_API_KEY = os.getenv("WORKER_API_KEY", "worker-secret-key")
LOGFILE = os.getenv("WORKER_LOG", "worker_logs.jsonl")

log_dir = os.path.dirname(LOGFILE) or "."
os.makedirs(log_dir, exist_ok=True)

def log_worker(entry: dict):
    with open(LOGFILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

class WorkerService(dispatcher_pb2_grpc.WorkerServiceServicer):
    def ProcessNotification(self, request, context):
        md = dict(context.invocation_metadata())
        key = md.get("x-api-key", "")
        if WORKER_API_KEY and key != WORKER_API_KEY:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid API key for worker")

        start = time.perf_counter()
        delay = random.randint(0, 500) / 1000.0
        time.sleep(delay)
        end = time.perf_counter()
        worker_processing_ms = int((end - start) * 1000)

        processed_at = int(time.time() * 1000)
        log_worker({
            "timestamp": time.time(),
            "notificationId": request.notificationId,
            "workerProcessingMs": worker_processing_ms,
            "processedAt": processed_at
        })

        return dispatcher_pb2.WorkerResponseDto(
            success=True,
            notificationId=request.notificationId,
            processedAt=processed_at,
            workerProcessingMs=worker_processing_ms
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dispatcher_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    port = os.getenv("WORKER_PORT", "50052")
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Worker gRPC running at :{port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
