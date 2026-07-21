import { assertEquals } from "@std/assert";
import { assertSpyCallArgs, assertSpyCalls, spy } from "@std/testing/mock";
import {
  createHandler,
  createQueueListener,
  parseWeighInPayload,
  recordNewWeights,
  weightKey,
} from "./main.ts";

Deno.test("weightKey builds a KV key from the date's ISO string", () => {
  const date = new Date("2026-01-01T08:00:00.000Z");

  assertEquals(weightKey(date), ["weights", "2026-01-01T08:00:00.000Z"]);
});

Deno.test(
  "parseWeighInPayload returns an error when weights is missing",
  () => {
    const result = parseWeighInPayload({ dates: "2026-01-01T00:00:00.000Z" });

    assertEquals(result, { error: "no weights found" });
  },
);

Deno.test("parseWeighInPayload returns an error when dates is missing", () => {
  const result = parseWeighInPayload({ weights: "70.5" });

  assertEquals(result, { error: "no dates found" });
});

Deno.test(
  "parseWeighInPayload parses newline-delimited weights and dates",
  () => {
    const result = parseWeighInPayload({
      weights: "70.55\n71.20",
      dates: "2026-01-01T00:00:00.000Z\n2026-01-02T00:00:00.000Z",
    });

    assertEquals(result, {
      weights: [70.55, 71.2],
      dates: [
        new Date("2026-01-01T00:00:00.000Z"),
        new Date("2026-01-02T00:00:00.000Z"),
      ],
    });
  },
);

Deno.test(
  "recordNewWeights sends a new weight to Garmin and persists it in KV",
  async () => {
    const kv = await Deno.openKv(":memory:");
    const addWeighIn = spy(() => Promise.resolve());
    const garmin = { weight: { addWeighIn } };
    const date = new Date("2026-01-01T00:00:00.000Z");

    await recordNewWeights(kv, garmin, [70.5], [date]);

    assertSpyCalls(addWeighIn, 1);
    const stored = await kv.get(weightKey(date));
    assertEquals(stored.value, { date, weight: 70.5 });

    kv.close();
  },
);

Deno.test(
  "recordNewWeights only sends a given date to Garmin once",
  async () => {
    const kv = await Deno.openKv(":memory:");
    const addWeighIn = spy(() => Promise.resolve());
    const garmin = { weight: { addWeighIn } };
    const date = new Date("2026-01-01T00:00:00.000Z");

    await recordNewWeights(kv, garmin, [70.5], [date]);
    // Simulates a redelivered queue message for the same date.
    await recordNewWeights(kv, garmin, [70.5], [date]);

    assertSpyCalls(addWeighIn, 1);

    kv.close();
  },
);

Deno.test(
  "recordNewWeights sends each distinct date independently",
  async () => {
    const kv = await Deno.openKv(":memory:");
    const addWeighIn = spy(() => Promise.resolve());
    const garmin = { weight: { addWeighIn } };
    const firstDate = new Date("2026-01-01T00:00:00.000Z");
    const secondDate = new Date("2026-01-02T00:00:00.000Z");

    await recordNewWeights(kv, garmin, [70.5, 71.2], [firstDate, secondDate]);

    assertSpyCalls(addWeighIn, 2);
    assertSpyCallArgs(addWeighIn, 0, [
      { value: 70.5, measuredAt: firstDate.toISOString(), unit: "kg" },
    ]);
    assertSpyCallArgs(addWeighIn, 1, [
      { value: 71.2, measuredAt: secondDate.toISOString(), unit: "kg" },
    ]);

    kv.close();
  },
);

Deno.test(
  "recordNewWeights rolls back the KV claim when Garmin rejects the write, allowing a retry",
  async () => {
    const kv = await Deno.openKv(":memory:");
    let callCount = 0;
    const addWeighIn = spy(() => {
      callCount++;
      return callCount === 1
        ? Promise.reject(new Error("Garmin is down"))
        : Promise.resolve();
    });
    const garmin = { weight: { addWeighIn } };
    const date = new Date("2026-01-01T00:00:00.000Z");

    await recordNewWeights(kv, garmin, [70.5], [date]);

    assertSpyCalls(addWeighIn, 1);
    const afterFailure = await kv.get(weightKey(date));
    assertEquals(afterFailure.value, null);

    await recordNewWeights(kv, garmin, [70.5], [date]);

    assertSpyCalls(addWeighIn, 2);
    const afterRetry = await kv.get(weightKey(date));
    assertEquals(afterRetry.value, { date, weight: 70.5 });

    kv.close();
  },
);

Deno.test(
  "createQueueListener ignores messages that are not NEW_WEIGHT",
  async () => {
    const kv = await Deno.openKv(":memory:");
    const addWeighIn = spy(() => Promise.resolve());
    const garmin = { weight: { addWeighIn } };
    const listener = createQueueListener(kv, garmin);

    await listener({ type: "SOMETHING_ELSE" });

    assertSpyCalls(addWeighIn, 0);

    kv.close();
  },
);

Deno.test(
  "createQueueListener records weights for NEW_WEIGHT messages",
  async () => {
    const kv = await Deno.openKv(":memory:");
    const addWeighIn = spy(() => Promise.resolve());
    const garmin = { weight: { addWeighIn } };
    const listener = createQueueListener(kv, garmin);
    const date = new Date("2026-01-01T00:00:00.000Z");

    await listener({
      type: "NEW_WEIGHT",
      payload: { weights: [70.5], dates: [date] },
    });

    assertSpyCalls(addWeighIn, 1);

    kv.close();
  },
);

Deno.test("createHandler returns 404 for unknown routes", async () => {
  const enqueue = spy(() =>
    Promise.resolve({ ok: true as const, versionstamp: "0" }),
  );
  const handler = createHandler({ enqueue });
  const request = new Request("http://localhost/unknown", {
    method: "POST",
    body: JSON.stringify({}),
  });

  const response = await handler(request);

  assertEquals(response.status, 404);
  assertEquals(await response.json(), { error: "not found" });
  assertSpyCalls(enqueue, 0);
});

Deno.test(
  "createHandler returns 400 when /callback is missing weights",
  async () => {
    const enqueue = spy(() =>
      Promise.resolve({ ok: true as const, versionstamp: "0" }),
    );
    const handler = createHandler({ enqueue });
    const request = new Request("http://localhost/callback", {
      method: "POST",
      body: JSON.stringify({ dates: "2026-01-01T00:00:00.000Z" }),
    });

    const response = await handler(request);

    assertEquals(response.status, 400);
    assertEquals(await response.json(), { error: "no weights found" });
    assertSpyCalls(enqueue, 0);
  },
);

Deno.test(
  "createHandler returns 400 when /callback is missing dates",
  async () => {
    const enqueue = spy(() =>
      Promise.resolve({ ok: true as const, versionstamp: "0" }),
    );
    const handler = createHandler({ enqueue });
    const request = new Request("http://localhost/callback", {
      method: "POST",
      body: JSON.stringify({ weights: "70.5" }),
    });

    const response = await handler(request);

    assertEquals(response.status, 400);
    assertEquals(await response.json(), { error: "no dates found" });
    assertSpyCalls(enqueue, 0);
  },
);

Deno.test(
  "createHandler enqueues a NEW_WEIGHT message for a valid /callback body",
  async () => {
    const enqueue = spy(() =>
      Promise.resolve({ ok: true as const, versionstamp: "0" }),
    );
    const handler = createHandler({ enqueue });
    const request = new Request("http://localhost/callback", {
      method: "POST",
      body: JSON.stringify({
        weights: "70.55",
        dates: "2026-01-01T00:00:00.000Z",
      }),
    });

    const response = await handler(request);

    assertEquals(response.status, 200);
    assertEquals(await response.json(), { status: "ok" });
    assertSpyCalls(enqueue, 1);
    assertSpyCallArgs(enqueue, 0, [
      {
        type: "NEW_WEIGHT",
        payload: {
          weights: [70.55],
          dates: [new Date("2026-01-01T00:00:00.000Z")],
        },
      },
    ]);
  },
);
