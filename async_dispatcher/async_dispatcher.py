import os
import json
import time
import uuid
import functools
import logging
from typing import Any, Dict
from concurrent.futures import ThreadPoolExecutor

import pika
from fastapi import FastAPI, Request, HTTPException
import uvicorn
import asyncio

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
TASK_QUEUE = os.getenv("TASK_QUEUE", "tasks_queue")
DISPATCHER_LOG = os.getenv("DISPATCHER_LOG", "/logs/dispatcher_async_logs.jsonl")
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT", "30.0"))
MAX_THREADS = int(os.getenv("DISPATCHER_MAX_THREADS", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("async_dispatcher")

def ensure_log_path(path: str):
    if not path:
        raise ValueError("DISPATCHER_LOG path is empty")
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    try:
        with open(path, "a", encoding="utf-8") as f:
            pass
    except Exception as e:
        logger.error("Unable to create/open dispatcher log file '%s': %s", path, e)
        raise

def log_dispatcher(entry: Dict[str, Any]):
    try:
        with open(DISPATCHER_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
    except Exception as e:
        logger.exception("Failed to write dispatcher log: %s", e)


def _rpc_call_sync(payload: dict, priority: int, timeout: float) -> dict:
    params = pika.ConnectionParameters(host=RABBIT_HOST)
    connection = None
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        result = channel.queue_declare('', exclusive=True)
        callback_queue = result.method.queue
        responses: Dict[str, bytes] = {}

        def on_response(ch, method, props, body):
            if props and props.correlation_id:
                responses[props.correlation_id] = body

        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=on_response,
            auto_ack=True
        )

        corr_id = str(uuid.uuid4())
        body = json.dumps(payload).encode("utf-8")
        props = pika.BasicProperties(
            reply_to=callback_queue,
            correlation_id=corr_id,
            delivery_mode=2,
            priority=priority
        )

        start = time.perf_counter()
        channel.basic_publish(
            exchange="",
            routing_key=TASK_QUEUE,
            properties=props,
            body=body
        )

        waited = 0.0
        poll_interval = 0.1
        while waited < timeout:
            connection.process_data_events(time_limit=poll_interval)
            if corr_id in responses:
                resp_body = responses.pop(corr_id)
                total_ms = int((time.perf_counter() - start) * 1000)
                try:
                    resp_json = json.loads(resp_body)
                except Exception:
                    resp_json = {"raw_response": resp_body.decode("utf-8", errors="replace")}
                log_dispatcher({
                    "timestamp": time.time(),
                    "notificationId": payload.get("notificationId"),
                    "total_time_ms": total_ms,
                    "priority": priority,
                    "worker_response": resp_json
                })
                return {"status": "ok", "worker_response": resp_json}
            waited += poll_interval


        log_dispatcher({
            "timestamp": time.time(),
            "notificationId": payload.get("notificationId"),
            "priority": priority,
            "error": "timeout",
            "detail": f"no response after {timeout} seconds"
        })
        raise TimeoutError(f"No response from worker after {timeout} seconds")

    finally:
        try:
            if connection and not connection.is_closed:
                connection.close()
        except Exception:
            logger.exception("Error closing pika connection")

class RpcClient:
    def __init__(self, executor: ThreadPoolExecutor):
        self._executor = executor

    async def call(self, payload: dict, priority: int = 0, timeout: float = RPC_TIMEOUT) -> dict:
        loop = asyncio.get_event_loop()
        func = functools.partial(_rpc_call_sync, payload, priority, timeout)
        try:
            result = await loop.run_in_executor(self._executor, func)
            return result
        except TimeoutError as e:
            logger.warning("RPC timeout: %s", e)
            raise
        except Exception as e:
            logger.exception("RPC call failed: %s", e)
            log_dispatcher({
                "timestamp": time.time(),
                "notificationId": payload.get("notificationId"),
                "priority": priority,
                "error": "rpc_error",
                "detail": str(e)
            })
            raise


app = FastAPI()
_executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
rpc_client = RpcClient(_executor)

@app.on_event("startup")
async def startup():
    try:
        ensure_log_path(DISPATCHER_LOG)
        log_dispatcher({"timestamp": time.time(), "event": "dispatcher_started"})
        logger.info("Dispatcher started; logging to %s", DISPATCHER_LOG)
    except Exception:
        logger.exception("Failed to initialize dispatcher log file; exiting")
        raise

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down: waiting for executor to finish")
    _executor.shutdown(wait=True)
    logger.info("Executor shut down")

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.post("/dispatch")
async def dispatch(request: Request):
    body = await request.json()
    prio = int(body.get("priority", 0) or 0)
    try:
        result = await rpc_client.call(body, priority=prio, timeout=RPC_TIMEOUT)
        return result
    except TimeoutError as te:
        raise HTTPException(status_code=504, detail=str(te))
    except Exception as ex:
        raise HTTPException(status_code=500, detail="dispatcher internal error")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7000)
