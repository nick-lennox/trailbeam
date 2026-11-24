# The Node.js conformance function: the AWS Lambda base image (with its Runtime Interface Emulator), the
# trailbeam package from this checkout's node/ (its production dependencies installed, as from npm), and
# handler.js. The poller builds it from a context with node/ and this directory (driver.json, "--context").
FROM public.ecr.aws/lambda/nodejs:22
COPY node /tmp/trailbeam
RUN cd /tmp/trailbeam && npm ci --omit=dev --no-audit --no-fund -q >/dev/null \
 && mkdir -p "${LAMBDA_TASK_ROOT}/node_modules" && cp -r /tmp/trailbeam "${LAMBDA_TASK_ROOT}/node_modules/trailbeam" \
 && node -e "const p=require('${LAMBDA_TASK_ROOT}/node_modules/trailbeam/package.json'); console.log(p.name+' '+p.version)" > /kit-versions.txt \
 && rm -rf /tmp/trailbeam
COPY conformance/drivers/node-lambda/handler.js "${LAMBDA_TASK_ROOT}/"
CMD ["handler.handler"]
