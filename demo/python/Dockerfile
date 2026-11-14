# qv2 demo driver for Python, with the qv2 package from this repository. Built from the repository root
# by demo/compose.yaml; local only, nothing is published.
FROM python:3.12-slim
WORKDIR /app
COPY python /src/python
RUN pip install --no-cache-dir /src/python boto3
COPY demo/python/qv2demo.py ./
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/qv2demo.py"]
