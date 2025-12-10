import time
import random
import grpc
from concurrent import futures

import dispatcher_pb2
import dispatcher_pb2_grpc


class WorkerService(dispatcher_pb2_grpc.WorkerServiceServicer):
    def ProcessNotification(self, request, context):
        start = time.perf_counter()

        delay = random.randint(0, 500) / 1000
        time.sleep(delay)

        end = time.perf_counter()
        worker_processing_ms = int((end - start) * 1000)

        return dispatcher_pb2.WorkerResponseDto(
            success=True,
            notificationId=request.notificationId,
            processedAt=int(time.time() * 1000),
            workerProcessingMs=worker_processing_ms
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dispatcher_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    print("Worker gRPC running at :50052")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
