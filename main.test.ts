import { test, mock } from "node:test";
import assert from "node:assert/strict";
import type { AddWeighInInput } from "garmin-connect-sdk";
import {
  createHandler,
  createNodeServer,
  createQueueListener,
  parseWeighInPayload,
  recordNewWeights,
  weightKey,
} from "./main.ts";
import { openKv } from "./kv.ts";

test("weightKey builds a KV key from the date's ISO string", () => {
  const date = new Date("2026-01-01T08:00:00.000Z");

  assert.deepEqual(weightKey(date), ["weights", "2026-01-01T08:00:00.000Z"]);
});

test("parseWeighInPayload returns an error when weights is missing", () => {
  const result = parseWeighInPayload({ dates: "2026-01-01T00:00:00.000Z" });

  assert.deepEqual(result, { error: "no weights found" });
});

test("parseWeighInPayload returns an error when dates is missing", () => {
  const result = parseWeighInPayload({ weights: "70.5" });

  assert.deepEqual(result, { error: "no dates found" });
});

test("parseWeighInPayload parses newline-delimited weights and dates", () => {
  const result = parseWeighInPayload({
    weights: "70.55\n71.20",
    dates: "2026-01-01T00:00:00.000Z\n2026-01-02T00:00:00.000Z",
  });

  assert.deepEqual(result, {
    weights: [70.55, 71.2],
    dates: [
      new Date("2026-01-01T00:00:00.000Z"),
      new Date("2026-01-02T00:00:00.000Z"),
    ],
  });
});

test("recordNewWeights sends a new weight to Garmin and persists it in KV", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {});
  const garmin = { weight: { addWeighIn } };
  const date = new Date("2026-01-01T00:00:00.000Z");

  await recordNewWeights(kv, garmin, [70.5], [date]);

  assert.equal(addWeighIn.mock.callCount(), 1);
  const stored = await kv.get(weightKey(date));
  assert.deepEqual(stored.value, { date, weight: 70.5 });
});

test("recordNewWeights only sends a given date to Garmin once", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {});
  const garmin = { weight: { addWeighIn } };
  const date = new Date("2026-01-01T00:00:00.000Z");

  await recordNewWeights(kv, garmin, [70.5], [date]);
  // Simulates a redelivered queue message for the same date.
  await recordNewWeights(kv, garmin, [70.5], [date]);

  assert.equal(addWeighIn.mock.callCount(), 1);
});

test("recordNewWeights sends each distinct date independently", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {});
  const garmin = { weight: { addWeighIn } };
  const firstDate = new Date("2026-01-01T00:00:00.000Z");
  const secondDate = new Date("2026-01-02T00:00:00.000Z");

  await recordNewWeights(kv, garmin, [70.5, 71.2], [firstDate, secondDate]);

  assert.equal(addWeighIn.mock.callCount(), 2);
  assert.deepEqual(addWeighIn.mock.calls[0].arguments, [
    { value: 70.5, measuredAt: firstDate.toISOString(), unit: "kg" },
  ]);
  assert.deepEqual(addWeighIn.mock.calls[1].arguments, [
    { value: 71.2, measuredAt: secondDate.toISOString(), unit: "kg" },
  ]);
});

test("recordNewWeights rolls back the KV claim when Garmin rejects the write, allowing a retry", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  let callCount = 0;
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {
    callCount++;
    if (callCount === 1) {
      throw new Error("Garmin is down");
    }
  });
  const garmin = { weight: { addWeighIn } };
  const date = new Date("2026-01-01T00:00:00.000Z");

  await assert.rejects(
    () => recordNewWeights(kv, garmin, [70.5], [date]),
    /Garmin is down/,
  );
  assert.equal((await kv.get(weightKey(date))).value, null);

  await recordNewWeights(kv, garmin, [70.5], [date]);
  assert.equal(addWeighIn.mock.callCount(), 2);
  assert.deepEqual((await kv.get(weightKey(date))).value, { date, weight: 70.5 });
});

test("concurrent calls for the same date send to Garmin once", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {});
  const garmin = { weight: { addWeighIn } };
  const date = new Date("2026-01-01T00:00:00.000Z");

  await Promise.all([
    recordNewWeights(kv, garmin, [70.5], [date]),
    recordNewWeights(kv, garmin, [70.5], [date]),
  ]);

  assert.equal(addWeighIn.mock.callCount(), 1);
});

test("createQueueListener ignores messages that are not NEW_WEIGHT", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {});
  const garmin = { weight: { addWeighIn } };
  const listener = createQueueListener(kv, garmin);

  await listener({ type: "SOMETHING_ELSE" });

  assert.equal(addWeighIn.mock.callCount(), 0);
});

test("createQueueListener records weights for NEW_WEIGHT messages", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());
  const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {});
  const garmin = { weight: { addWeighIn } };
  const listener = createQueueListener(kv, garmin);
  const date = new Date("2026-01-01T00:00:00.000Z");

  await listener({
    type: "NEW_WEIGHT",
    payload: { weights: [70.5], dates: [date] },
  });

  assert.equal(addWeighIn.mock.callCount(), 1);
});

test("createHandler returns 404 for unknown routes", async () => {
  const enqueue = mock.fn(async (_value: unknown) => ({
    ok: true as const,
    versionstamp: "0",
  }));
  const handler = createHandler({ enqueue }, "top-secret");
  const request = new Request("http://localhost/unknown", {
    method: "POST",
    body: JSON.stringify({}),
  });

  const response = await handler(request);

  assert.equal(response.status, 404);
  assert.deepEqual(await response.json(), { error: "not found" });
  assert.equal(enqueue.mock.callCount(), 0);
});

test("createHandler returns 401 when /callback is missing the secret", async () => {
  const enqueue = mock.fn(async (_value: unknown) => ({
    ok: true as const,
    versionstamp: "0",
  }));
  const handler = createHandler({ enqueue }, "top-secret");
  const request = new Request("http://localhost/callback", {
    method: "POST",
    body: JSON.stringify({
      weights: "70.55",
      dates: "2026-01-01T00:00:00.000Z",
    }),
  });

  const response = await handler(request);

  assert.equal(response.status, 401);
  assert.deepEqual(await response.json(), { error: "unauthorized" });
  assert.equal(enqueue.mock.callCount(), 0);
});

test("createHandler returns 401 when /callback has the wrong secret", async () => {
  const enqueue = mock.fn(async (_value: unknown) => ({
    ok: true as const,
    versionstamp: "0",
  }));
  const handler = createHandler({ enqueue }, "top-secret");
  const request = new Request("http://localhost/callback", {
    method: "POST",
    body: JSON.stringify({
      secret: "not-the-secret",
      weights: "70.55",
      dates: "2026-01-01T00:00:00.000Z",
    }),
  });

  const response = await handler(request);

  assert.equal(response.status, 401);
  assert.deepEqual(await response.json(), { error: "unauthorized" });
  assert.equal(enqueue.mock.callCount(), 0);
});

test("createHandler returns 400 when /callback is missing weights", async () => {
  const enqueue = mock.fn(async (_value: unknown) => ({
    ok: true as const,
    versionstamp: "0",
  }));
  const handler = createHandler({ enqueue }, "top-secret");
  const request = new Request("http://localhost/callback", {
    method: "POST",
    body: JSON.stringify({
      secret: "top-secret",
      dates: "2026-01-01T00:00:00.000Z",
    }),
  });

  const response = await handler(request);

  assert.equal(response.status, 400);
  assert.deepEqual(await response.json(), { error: "no weights found" });
  assert.equal(enqueue.mock.callCount(), 0);
});

test("createHandler returns 400 when /callback is missing dates", async () => {
  const enqueue = mock.fn(async (_value: unknown) => ({
    ok: true as const,
    versionstamp: "0",
  }));
  const handler = createHandler({ enqueue }, "top-secret");
  const request = new Request("http://localhost/callback", {
    method: "POST",
    body: JSON.stringify({ secret: "top-secret", weights: "70.5" }),
  });

  const response = await handler(request);

  assert.equal(response.status, 400);
  assert.deepEqual(await response.json(), { error: "no dates found" });
  assert.equal(enqueue.mock.callCount(), 0);
});

test("createHandler enqueues a NEW_WEIGHT message for a valid /callback body", async () => {
  const enqueue = mock.fn(async (_value: unknown) => ({
    ok: true as const,
    versionstamp: "0",
  }));
  const handler = createHandler({ enqueue }, "top-secret");
  const request = new Request("http://localhost/callback", {
    method: "POST",
    body: JSON.stringify({
      secret: "top-secret",
      weights: "70.55",
      dates: "2026-01-01T00:00:00.000Z",
    }),
  });

  const response = await handler(request);

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { status: "ok" });
  assert.equal(enqueue.mock.callCount(), 1);
  assert.deepEqual(enqueue.mock.calls[0].arguments, [
    {
      type: "NEW_WEIGHT",
      payload: {
        weights: [70.55],
        dates: [new Date("2026-01-01T00:00:00.000Z")],
      },
    },
  ]);
});

test("createNodeServer passes the path and JSON body to the handler", async (t) => {
  const server = createNodeServer(async (req) =>
    Response.json({ path: new URL(req.url).pathname, body: await req.json() })
  );
  await new Promise<void>((resolve) => server.listen(0, resolve));
  t.after(() => server.close());
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  const response = await fetch(`http://127.0.0.1:${port}/callback`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ weights: "70.5" }),
  });

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    path: "/callback",
    body: { weights: "70.5" },
  });
});

test("createNodeServer returns 500 for an invalid JSON body", async (t) => {
  const server = createNodeServer(createHandler({ enqueue: async () => ({ ok: true as const, versionstamp: "0" }) }, "secret"));
  await new Promise<void>((resolve) => server.listen(0, resolve));
  t.after(() => server.close());
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  const response = await fetch(`http://127.0.0.1:${port}/callback`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "not json",
  });

  assert.equal(response.status, 500);

  const second = await fetch(`http://127.0.0.1:${port}/callback`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      secret: "secret",
      weights: "70.5",
      dates: "2026-01-01T00:00:00.000Z",
    }),
  });
  assert.equal(second.status, 200);
});
