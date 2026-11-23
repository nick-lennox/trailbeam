# Built from the repository root (see compose.yaml). In your own service: npm install trailbeam @aws-sdk/client-sqs.
FROM node:22-slim
COPY node /src/node
RUN cd /src/node && npm ci --omit=dev --no-audit --no-fund
WORKDIR /src/examples/node-sdk-v3
COPY examples/node-sdk-v3/package.json examples/node-sdk-v3/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY examples/node-sdk-v3/*.js ./
