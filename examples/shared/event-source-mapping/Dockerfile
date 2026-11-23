FROM python:3.12-slim
RUN pip install --no-cache-dir boto3
COPY esm.py /esm.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "/esm.py"]
