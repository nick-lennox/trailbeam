# The AWS Lambda base image for Python, with its Runtime Interface Emulator. Built from the repository
# root (see compose.yaml). In your own function: add trailbeam to requirements.txt. boto3 is bundled
# with it so it matches the botocore that trailbeam brings (the runtime's own copy may be older).
FROM public.ecr.aws/lambda/python:3.12
COPY python /tmp/trailbeam
RUN pip install --no-cache-dir /tmp/trailbeam boto3 -t "${LAMBDA_TASK_ROOT}" && rm -rf /tmp/trailbeam
COPY examples/python-lambda/handler.py "${LAMBDA_TASK_ROOT}/"
CMD ["handler.handler"]
