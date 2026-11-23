# Built from the repository root (see compose.yaml). In your own service: pip install trailbeam boto3.
FROM python:3.12-slim
COPY python /src/trailbeam
RUN pip install --no-cache-dir /src/trailbeam boto3
WORKDIR /app
COPY examples/python-lambda/producer.py ./
ENV PYTHONUNBUFFERED=1
