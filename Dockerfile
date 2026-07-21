# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# Compiles main.ts into a single, self-contained executable with `deno compile`.
# Permissions are baked in at compile time (least privilege) instead of shipping
# a binary that needs --allow-all at runtime.
FROM denoland/deno:2.9.3 AS build

WORKDIR /app

COPY deno.json deno.lock ./
COPY main.ts ./

RUN deno compile \
  --allow-env=DENO_KV_PATH,GARMIN_STORAGE_PATH,GARMIN_USERNAME,GARMIN_PASSWORD \
  --allow-net \
  --allow-read=/data \
  --allow-write=/data \
  --output apple-to-garmin \
  main.ts

# ---- Runtime stage ----------------------------------------------------------
# No Deno CLI needed: `deno compile` produces a self-contained binary.
FROM debian:bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --create-home --uid 1000 app \
  && mkdir -p /data \
  && chown app:app /data

WORKDIR /app
COPY --from=build /app/apple-to-garmin ./apple-to-garmin

# Token storage and the Deno KV database live under /data; mount a volume there
# so weigh-in history and the Garmin session survive container restarts.
ENV GARMIN_STORAGE_PATH=/data/.garmin-tokens
ENV DENO_KV_PATH=/data/apple-to-garmin.sqlite3

USER app
VOLUME ["/data"]
EXPOSE 8080

ENTRYPOINT ["./apple-to-garmin"]
