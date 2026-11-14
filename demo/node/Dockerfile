# qv2 demo driver for Node.js, with @qv2/sdk from this repository. Built from the repository root by
# demo/compose.yaml; local only, nothing is published.
FROM node:22-slim
COPY node /qv2/node
RUN cd /qv2/node && npm ci --omit=dev --no-audit --no-fund
WORKDIR /qv2/demo/node
COPY demo/node/package.json demo/node/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY demo/node/qv2demo.js ./
ENTRYPOINT ["node", "/qv2/demo/node/qv2demo.js"]
