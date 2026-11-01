import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DatabaseSync } from "node:sqlite";
import { openKv } from "./kv.ts";

const key = ["weights", "2026-01-01T00:00:00.000Z"];
const date = new Date("2026-01-01T00:00:00.000Z");

test("get returns null for a missing key", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());

  assert.deepEqual(await kv.get(key), {
    key,
    value: null,
    versionstamp: null,
  });
});

test("atomic claim stores a Date value that get revives", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());

  const result = await kv
    .atomic()
    .check({ key, versionstamp: null })
    .set(key, { date, weight: 70.5 })
    .commit();

  assert.equal(result.ok, true);
  const entry = await kv.get<{ date: Date; weight: number }>(key);
  assert.deepEqual(entry.value, { date, weight: 70.5 });
  assert.equal(typeof entry.versionstamp, "string");
});

test("atomic claim rejects a second writer for the same key", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());

  const first = await kv
    .atomic()
    .check({ key, versionstamp: null })
    .set(key, { date, weight: 70.5 })
    .commit();
  const second = await kv
    .atomic()
    .check({ key, versionstamp: null })
    .set(key, { date, weight: 99 })
    .commit();

  assert.equal(first.ok, true);
  assert.equal(second.ok, false);
  assert.deepEqual((await kv.get(key)).value, { date, weight: 70.5 });
});

test("atomic rejects a mismatched check and set key", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());

  await assert.rejects(
    () =>
      kv
        .atomic()
        .check({ key, versionstamp: null })
        .set(["other", "2026-01-01T00:00:00.000Z"], 1)
        .commit(),
    /same key/,
  );
});

test("delete removes a stored key", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());

  await kv.atomic().check({ key, versionstamp: null }).set(key, 1).commit();
  await kv.delete(key);

  assert.equal((await kv.get(key)).value, null);
});

test("openKv refuses a Deno KV schema", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "apple-to-garmin-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, "deno.sqlite3");

  const db = new DatabaseSync(path);
  db.exec(
    "CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB, v_encoding INTEGER, version INTEGER)",
  );
  db.close();

  await assert.rejects(() => openKv(path), /Deno KV/);
});

test("listenQueue delivers enqueued values", async (t) => {
  const kv = await openKv(":memory:");
  t.after(() => kv.close());

  let resolve!: (value: unknown) => void;
  const delivered = new Promise<unknown>((r) => {
    resolve = r;
  });
  kv.listenQueue(async (value) => {
    resolve(value);
  });

  const message = { type: "NEW_WEIGHT", payload: { date } };
  await kv.enqueue(message);

  assert.deepEqual(await delivered, message);
});

test("listenQueue retries a throwing handler", async (t) => {
  t.mock.method(console, "error", () => {});
  const kv = await openKv(":memory:", { retryDelay: () => 1 });
  t.after(() => kv.close());

  let attempts = 0;
  let markDone!: () => void;
  const done = new Promise<void>((r) => {
    markDone = r;
  });
  kv.listenQueue(async () => {
    attempts += 1;
    if (attempts === 1) throw new Error("transient");
    markDone();
  });

  await kv.enqueue("job");
  await done;

  assert.equal(attempts, 2);
});

test("queued values survive close and reopen", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "apple-to-garmin-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, "queue.db");

  const first = await openKv(path);
  await first.enqueue({ date });
  first.close();

  const second = await openKv(path);
  t.after(() => second.close());
  const delivered = new Promise<unknown>((resolve) => {
    second.listenQueue(async (value) => {
      resolve(value);
    });
  });

  assert.deepEqual(await delivered, { date });
});
