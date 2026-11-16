# Trailbeam demo driver for Node.js, with trailbeam from this repository. Built from the repository root by
# demo/compose.yaml; local only, nothing is published.
FROM node:22-slim
COPY node /trailbeam/node
RUN cd /trailbeam/node && npm ci --omit=dev --no-audit --no-fund
WORKDIR /trailbeam/demo/node
COPY demo/node/package.json demo/node/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY demo/node/trailbeamdemo.js ./
ENTRYPOINT ["node", "/trailbeam/demo/node/trailbeamdemo.js"]
