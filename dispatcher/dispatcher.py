import time
import uuid
import grpc
from concurrent import futures

import dispatcher_pb2
import dispatcher_pb2_grpc


class DispatcherService(dispatcher_pb2_grpc.DispatcherServiceServicer):

    def __init__(self):
        self.worker = dispatcher_pb2_grpc.WorkerServiceStub(
            grpc.insecure_channel("worker:50052")
        )

    def DispatchNotification(self, request, context):
        notification_id = str(uuid.uuid4())
        dispatcher_start = time.perf_counter()
        dispatcher_start_ts = int(time.time() * 1000)

        worker_req = dispatcher_pb2.WorkerRequestDto(
            message=request.message,
            sentAt=request.sentAt,
            notificationId=notification_id,
            dispatcherStartedAt=dispatcher_start_ts
        )

        worker_resp = self.worker.ProcessNotification(worker_req)

        dispatcher_end = time.perf_counter()
        dispatcher_ms = int((dispatcher_end - dispatcher_start) * 1000)

        return dispatcher_pb2.DispatchResponse(
            success=worker_resp.success,
            notificationId=notification_id,
            dispatcherProcessingMs=dispatcher_ms,
            workerProcessingMs=worker_resp.workerProcessingMs,
            processedAt=worker_resp.processedAt
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dispatcher_pb2_grpc.add_DispatcherServiceServicer_to_server(
        DispatcherService(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Dispatcher gRPC running at :50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
