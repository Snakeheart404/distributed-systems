FROM python:3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dispatcher.pr
CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "8000"]
