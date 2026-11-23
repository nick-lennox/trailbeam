# The Python conformance function: the AWS Lambda base image (with its Runtime Interface Emulator), the
# trailbeam package from this checkout, and handler.py. The poller builds it from a context with python/
# and this directory (driver.json, "--context").
FROM public.ecr.aws/lambda/python:3.12
COPY python /tmp/trailbeam
RUN pip install --no-cache-dir -q /tmp/trailbeam -t "${LAMBDA_TASK_ROOT}" \
 && pip list --path "${LAMBDA_TASK_ROOT}" 2>/dev/null | grep -i -E '^(trailbeam|opentelemetry-sdk|botocore) ' > /kit-versions.txt \
 && rm -rf /tmp/trailbeam
COPY conformance/drivers/python-lambda/handler.py "${LAMBDA_TASK_ROOT}/"
CMD ["handler.handler"]
