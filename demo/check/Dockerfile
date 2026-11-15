# The Trailbeam 0.1 concurrency checker (check.py, standard library only). Built from the repository root by
# demo/compose.yaml; local only.
FROM python:3.12-slim
COPY demo/check/check.py /app/check.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/check.py"]
