# Built from the repository root (see compose.yaml), with two targets: `producer` and `function`.
# In your own function: npm install trailbeam, and deploy node_modules with handler.js.
FROM node:22-slim AS deps
COPY node /src/node
RUN cd /src/node && npm ci --omit=dev --no-audit --no-fund
WORKDIR /src/examples/node-lambda
COPY examples/node-lambda/package.json examples/node-lambda/package-lock.json ./
# Then a copy of trailbeam (with its own node_modules) in place of the link to ../../node, as a release
# from npm would be: the function image has only node_modules, not the repository.
RUN npm ci --omit=dev --no-audit --no-fund && rm node_modules/trailbeam && cp -r /src/node node_modules/trailbeam

FROM deps AS producer
COPY examples/node-lambda/producer.js examples/node-lambda/sqs.js ./

# The AWS Lambda base image for Node.js, with its Runtime Interface Emulator.
FROM public.ecr.aws/lambda/nodejs:22 AS function
COPY --from=deps /src/examples/node-lambda/node_modules "${LAMBDA_TASK_ROOT}/node_modules"
COPY examples/node-lambda/handler.js "${LAMBDA_TASK_ROOT}/"
CMD ["handler.handler"]
