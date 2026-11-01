# syntax=docker/dockerfile:1

FROM node:26-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY main.ts kv.ts ./

RUN mkdir -p /data && chown node:node /data

# Token storage and the SQLite database live under /data; mount a volume there
# so weigh-in history and the Garmin session survive container restarts.
ENV GARMIN_STORAGE_PATH=/data/.garmin-tokens
ENV DATABASE_PATH=/data/apple-to-garmin.db

USER node
VOLUME ["/data"]
EXPOSE 8080

CMD ["node", "main.ts"]
