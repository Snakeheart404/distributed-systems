FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dispatcher.proto
CMD ["python", "worker.py"]
