# Deno → Node.js 26 Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run apple-to-garmin on stock Node.js 26 with the same callback contract, dedupe guarantee, and durable queue-then-deliver flow, with no Deno runtime or tooling left.

**Architecture:** Replace `Deno.Kv` with a small `kv.ts` shim over built-in `node:sqlite` (a `kv` table plus a persistent `queue` table), replace `Deno.serve` with a `node:http` ↔ web `Request`/`Response` adapter, and run the TypeScript sources directly through Node's type stripping.

**Tech Stack:** Node.js 26, TypeScript 7 (type-check only), `node:sqlite`, `node:test`, `node:http`, `node:v8`, `garmin-connect-sdk@1.0.0`, Docker `node:26-bookworm-slim`.

**Spec:** `docs/superpowers/specs/2026-11-01-deno-to-node-design.md`

## Global Constraints

- Node.js `>=26` (`engines.node`); no `tsx`, `ts-node`, `tsc` emit, or build output.
- No runtime dependency except `garmin-connect-sdk@1.0.0`.
- Every type-only import must use `import type` (Node strips annotations, not imports).
- Preserve `POST /callback` status codes and JSON bodies exactly.
- One Garmin write per weigh-in date, including redelivery and restarts.
- Queue survives process exit; Garmin failures are retried, not swallowed.
- `DATABASE_PATH` replaces `DENO_KV_PATH`; defaults are `./apple-to-garmin.db` and `/data/apple-to-garmin.db`.
- Old Deno KV files must never be opened or clobbered; incompatible schema fails at startup with a message containing `Deno KV`.
- Server port stays `8080`.

## Review Focus

- Invalid or non-JSON request bodies must return `500` and leave the server running, not crash or hang the process.
- Two callbacks containing the same date at the same time must produce exactly one Garmin write.
- A queued message must still be delivered after the process is closed and reopened.
- A rejected Garmin write must roll back the claim and be retried, not acked and dropped.
- `DATABASE_PATH` pointing at a Deno KV database must fail at startup with a clear error, not on the first request.

---

### Task 1: Project scaffolding + SQLite KV core

**Files:**
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `kv.ts`
- Create: `kv.test.ts`
- Delete: `main_test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces: `KvKey`, `KvEntry<T>`, `Kv`, `KvOptions`, `openKv(path, options?)`, with `get`, `delete`, `atomic().check().set().commit()`, `close`.
  - `KvKey = readonly (string | number | bigint | boolean)[]`
  - `KvEntry<T> = { key: KvKey; value: T | null; versionstamp: string | null }`
  - `KvOptions = { retryDelay?: (attempt: number) => number }`
  - `commit()` returns `Promise<{ ok: boolean; versionstamp: string | null }>`.
  - Only `check({ versionstamp: null })` is supported; any other versionstamp throws.

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "apple-to-garmin",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "engines": { "node": ">=26" },
  "scripts": {
    "dev": "node --watch --env-file=.env main.ts",
    "start": "node main.ts",
    "test": "node --test",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "garmin-connect-sdk": "1.0.0"
  },
  "devDependencies": {
    "@types/node": "^26.6.6",
    "typescript": "^7.0.2"
  }
}
```

- [ ] **Step 2: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "esnext",
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "strict": true,
    "noEmit": true,
    "allowImportingTsExtensions": true,
    "erasableSyntaxOnly": true,
    "verbatimModuleSyntax": true,
    "types": ["node"]
  },
  "include": ["kv.ts", "kv.test.ts"]
}
```

- [ ] **Step 3: Install dependencies**

Run: `npm install`
Expected: `package-lock.json` and `node_modules/` created; no errors.

Then remove the Deno-only test file so `node --test` and `tsc` only see ported code:

```bash
git rm main_test.ts
```

`main.test.ts` replaces it in Task 3; the old file remains in git history.

- [ ] **Step 4: Write the failing KV core tests in `kv.test.ts`**

```ts
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
```

- [ ] **Step 5: Run the tests to verify they fail**

Run: `node --test kv.test.ts`
Expected: FAIL — `openKv` is not exported / not a function.

- [ ] **Step 6: Implement `kv.ts` core**

Export the types and `openKv`. Define the two tables exactly as in the spec. Add `openKv(path: string, options?: KvOptions): Promise<Kv>` where `KvOptions = { retryDelay?: (attempt: number) => number }`.

Implementation notes:
- `new DatabaseSync(path)`; for `path !== ":memory:"` run `PRAGMA journal_mode = WAL` and `PRAGMA busy_timeout = 5000`.
- Inspect `PRAGMA table_info(kv)` and `PRAGMA table_info(queue)`. Empty result → create that table. Non-empty and columns differ from the expected lists → `throw new Error("DATABASE_PATH points at a Deno KV database or another incompatible schema; use a fresh path")`.
- Serialize keys with `JSON.stringify(key)`; serialize values with `v8.serialize` and read with `v8.deserialize`.
- `get` returns `{ key, value: null, versionstamp: null }` when absent.
- `atomic().commit()` requires exactly one `check` and one `set`. For `versionstamp: null`, run `INSERT ... ON CONFLICT(key) DO NOTHING`; `ok = result.changes === 1`.
- `delete` runs `DELETE FROM kv WHERE key = ?`.
- `close()` sets a stopped flag, wakes any waiter (no-op in this task), and calls `db.close()`.
- Add `enqueue` and `listenQueue` methods that throw `new Error("not implemented")` for now; Task 2 replaces them.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `node --test kv.test.ts`
Expected: PASS — 5 tests.

- [ ] **Step 8: Type-check**

Run: `npm run typecheck`
Expected: exit 0, no errors.

- [ ] **Step 9: Commit**

```bash
git add package.json package-lock.json tsconfig.json kv.ts kv.test.ts main_test.ts docs/superpowers/specs/2026-11-01-deno-to-node-design.md docs/superpowers/plans/2026-11-01-deno-to-node.md
git commit -m "feat: add SQLite-backed KV store and Node project scaffolding"
```

---

### Task 2: Persistent queue with retry and backoff

**Files:**
- Modify: `kv.ts`
- Modify: `kv.test.ts`

**Interfaces:**
- Consumes: `openKv`, `Kv`, `KvOptions` from Task 1.
- Produces:
  - `Kv.enqueue(value: unknown): Promise<{ ok: true; versionstamp: string }>`
  - `Kv.listenQueue(handler: (value: unknown) => Promise<void>): void`
  - `KvOptions.retryDelay?: (attempt: number) => number` (attempt starts at 1)

- [ ] **Step 1: Write the failing queue tests in `kv.test.ts`**

```ts
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test kv.test.ts`
Expected: FAIL — `enqueue` / `listenQueue` throw `not implemented`.

- [ ] **Step 3: Implement the queue in `kv.ts`**

- `enqueue` inserts `v8.serialize(value)` into `queue(value)` with defaults, calls `wake()`, returns `{ ok: true, versionstamp: crypto.randomUUID() }`.
- `listenQueue` allows one handler; a second call throws. It starts a single `void drain()` loop.
- `drain()` loops until stopped: select the oldest row where `next_attempt_at <= Date.now()` ordered by `id`. If none, `await waitForWork()`.
- For a row: deserialize, `await handler(value)`, then if `stopped` is still false delete the row. On throw: `attempts += 1`, compute `delay = options.retryDelay?.(attempts) ?? Math.min(1000 * 2 ** (attempts - 1), 300_000)`, update `attempts` and `next_attempt_at = Date.now() + delay`, and `console.error` the failure. Never let the loop reject.
- `waitForWork()`: read `MIN(next_attempt_at)` from `queue`. If null, wait for `wake()`. Otherwise `setTimeout` until that timestamp; `wake()` clears that timer and resolves early.
- `close()` sets `stopped`, calls `wake()`, and closes the database; the drain loop exits before touching the database again.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test kv.test.ts`
Expected: PASS — 8 tests, including the retry and reopen cases. The retry test completes in roughly a few milliseconds because `retryDelay` returns `1`.

- [ ] **Step 5: Type-check and commit**

Run: `npm run typecheck`
Expected: exit 0.

```bash
git add kv.ts kv.test.ts
git commit -m "feat: add durable SQLite queue with retry backoff"
```

---

### Task 3: Port application logic and its tests

**Files:**
- Modify: `main.ts`
- Create: `main.test.ts`

**Interfaces:**
- Consumes: `Kv`, `KvKey`, `openKv` from Tasks 1–2; `AddWeighInInput` from `garmin-connect-sdk`.
- Produces: `weightKey(date): KvKey`, `parseWeighInPayload(body)`, `recordNewWeights(kv, garmin, weights, dates)`, `createQueueListener(kv, garmin)`, `createHandler(kv, secret)`.
  - `recordNewWeights` rethrows after `kv.delete(key)` when the Garmin call fails.

- [ ] **Step 1: Port `main.test.ts` to `node:test`**

Replace the imports:

```ts
import { test, mock } from "node:test";
import assert from "node:assert/strict";
import type { AddWeighInInput } from "garmin-connect-sdk";
import {
  createHandler,
  createQueueListener,
  parseWeighInPayload,
  recordNewWeights,
  weightKey,
} from "./main.ts";
import { openKv } from "./kv.ts";
```

Mechanical replacements for every existing case (keep test names and expected values):
- `Deno.test("...", fn)` → `test("...", fn)`; `Deno.test("...", async () => {...})` → `test("...", async (t) => {...})`.
- `await Deno.openKv(":memory:")` → `await openKv(":memory:")`; `kv.close()` → `t.after(() => kv.close())`.
- `assertEquals(a, b)` → `assert.deepEqual(a, b)`.
- `const addWeighIn = spy(() => Promise.resolve())` → `const addWeighIn = mock.fn(async (_entry: AddWeighInInput) => {})`.
- `assertSpyCalls(fn, n)` → `assert.equal(fn.mock.callCount(), n)`.
- `assertSpyCallArgs(fn, i, args)` → `assert.deepEqual(fn.mock.calls[i].arguments, args)`.
- `const enqueue = spy(() => Promise.resolve({ ok: true as const, versionstamp: "0" }))` → `const enqueue = mock.fn(async (_value: unknown) => ({ ok: true as const, versionstamp: "0" }))`.

The rollback test changes to expect a rejection on the first call:

```ts
await assert.rejects(
  () => recordNewWeights(kv, garmin, [70.5], [date]),
  /Garmin is down/,
);
assert.equal((await kv.get(weightKey(date))).value, null);

await recordNewWeights(kv, garmin, [70.5], [date]);
assert.equal(addWeighIn.mock.callCount(), 2);
assert.deepEqual((await kv.get(weightKey(date))).value, { date, weight: 70.5 });
```

Add a concurrent-duplicate case:

```ts
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test main.test.ts`
Expected: FAIL — `main.ts` still uses `Deno.*` and its exports do not type/run under Node.

- [ ] **Step 3: Rewrite `main.ts` logic (no HTTP server or entrypoint yet)**

- `import type { AddWeighInInput } from "garmin-connect-sdk";`
- `import type { Kv, KvKey } from "./kv.ts";`
- `weightKey(date: Date): KvKey` returns `["weights", date.toISOString()]`.
- `parseWeighInPayload` stays byte-for-byte in behavior.
- `recordNewWeights(kv: Pick<Kv, "atomic" | "delete">, ...)` keeps the claim/rollback flow, and in the catch block logs, `await kv.delete(key)`, then `throw error` instead of returning.
- `createQueueListener` and `createHandler` keep their current bodies and only change types.
- Do not add `main()`, `node:http`, or `import.meta.main` yet.
- Change `tsconfig.json` `include` to `["*.ts"]` now that every remaining source file is Node-ready.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test main.test.ts`
Expected: PASS — 17 tests (the original 16 plus the concurrent case).

- [ ] **Step 5: Type-check and commit**

Run: `npm run typecheck`
Expected: exit 0.

```bash
git add main.ts main.test.ts tsconfig.json
git commit -m "feat: port application logic and tests to Node"
```

---

### Task 4: HTTP adapter and entrypoint

**Files:**
- Modify: `main.ts`
- Modify: `main.test.ts`

**Interfaces:**
- Consumes: `createHandler` from Task 3; `openKv` from Task 1; `FileTokenStorage`, `GarminConnectSDK` from `garmin-connect-sdk`.
- Produces:
  - `toRequest(req: IncomingMessage): Request`
  - `sendResponse(res: ServerResponse, response: Response): Promise<void>`
  - `createNodeServer(handler: (req: Request) => Promise<Response>): Server`
  - `main(): Promise<void>` guarded by `if (import.meta.main)`.

- [ ] **Step 1: Add failing adapter tests to `main.test.ts`**

Add `createNodeServer` to the existing import from `./main.ts`, then add:

```ts
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `node --test main.test.ts`
Expected: FAIL — `createNodeServer` is not exported.

- [ ] **Step 3: Implement the adapter and entrypoint in `main.ts`**

- `toRequest(req)`: build `new URL(req.url ?? "/", "http://" + (req.headers.host ?? "localhost"))`; copy headers (`Array.isArray` → join with `", "`); for `GET`/`HEAD` omit the body, otherwise `Readable.toWeb(req)` cast to `ReadableStream`; pass `duplex: "half"` with a cast to `RequestInit`.
- `sendResponse(res, response)`: `res.writeHead(response.status, Object.fromEntries(response.headers))`, then `res.end(Buffer.from(await response.arrayBuffer()))`.
- `createNodeServer(handler)`: `createServer` async callback; on success `await sendResponse`; on throw, `console.error`, `res.writeHead(500)` if headers not sent, `res.end()`.
- `main()`: read `DATABASE_PATH`, `GARMIN_STORAGE_PATH`, `CALLBACK_SECRET` from `process.env` and throw the same "must be set before loading the script." errors. Construct `GarminConnectSDK` with `FileTokenStorage`, `restoreSession()`, login with `GARMIN_USERNAME`/`GARMIN_PASSWORD` when not restored, `openKv(databasePath)`, `kv.listenQueue(createQueueListener(kv, garmin))`, then `createNodeServer(createHandler(kv, secret)).listen(8080)`. Log the listening URL.
- Keep `if (import.meta.main) { await main(); }`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `node --test main.test.ts`
Expected: PASS — 19 tests.

- [ ] **Step 5: Type-check and smoke-test the entrypoint**

Run: `npm run typecheck`
Expected: exit 0.

Run: `node main.ts`
Expected: exits non-zero with `DATABASE_PATH must be set before loading the script.` (no network access attempted).

- [ ] **Step 6: Commit**

```bash
git add main.ts main.test.ts
git commit -m "feat: serve the callback over node:http"
```

---

### Task 5: Runtime configuration and container image

**Files:**
- Modify: `.env.example`
- Modify: `.gitignore`
- Modify: `.dockerignore`
- Modify: `Dockerfile`

**Interfaces:**
- Consumes: `main.ts` entrypoint and `package-lock.json` from earlier tasks.
- Produces: container image that starts `node main.ts` as the non-root `node` user with `/data` for state.

- [ ] **Step 1: Update `.env.example`**

```dotenv
GARMIN_USERNAME=
GARMIN_PASSWORD=
GARMIN_STORAGE_PATH=./.garmin-tokens
DATABASE_PATH=./apple-to-garmin.db
CALLBACK_SECRET=
```

- [ ] **Step 2: Update `.gitignore`**

Keep the existing entries and add, next to `*.sqlite3*`:

```gitignore
*.db
*.db-shm
*.db-wal
```

- [ ] **Step 3: Update `.dockerignore`**

Replace the three `apple-to-garmin.sqlite3*` lines with `apple-to-garmin.db*`. Replace `main_test.ts` with `*.test.ts`. Keep excluding `.env`, `.garmin-tokens`, and Docker files.

- [ ] **Step 4: Rewrite `Dockerfile`**

```dockerfile
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
```

- [ ] **Step 5: Verify a clean install and test run**

Run: `npm ci && npm test`
Expected: clean install from `package-lock.json`; all tests pass.

- [ ] **Step 6: Build the image if Docker is available**

Run: `docker build -t apple-to-garmin:node .`
Expected: success. If Docker is not installed, record that this step was skipped and continue.

- [ ] **Step 7: Commit**

```bash
git add .env.example .gitignore .dockerignore Dockerfile
git commit -m "build: run the service on node:26 in Docker"
```

---

### Task 6: Documentation, Deno removal, and final verification

**Files:**
- Modify: `README.md`
- Delete: `deno.json`
- Delete: `deno.lock`

**Interfaces:**
- Consumes: the final code and commands from Tasks 1–5.
- Produces: a README with no Deno setup instructions and a repository with no Deno runtime files.

- [ ] **Step 1: Update the README**

- Intro: "A tiny Node.js HTTP server"; dedupe is "keyed by its date in SQLite".
- Mermaid: participant `KV as SQLite`.
- Requirements: Node.js 26+, Garmin account, optional Docker.
- Setup: `npm install`, `cp .env.example .env`, table row `DATABASE_PATH` → "Path to the local SQLite database file", `npm run dev`, `npm test`.
- Add a short note: the new database starts empty, so the first batch may re-send dates already sent by the Deno version.
- Docker: no compile step; the image runs `node main.ts`; env note uses `DATABASE_PATH`.
- Replace the Deno permissions section with "Runtime": this project does not enable Node's optional Permission Model (`--permission`); `npm run dev` runs with your own user's access, while the container runs as the non-root `node` user and only needs network access plus read/write on `/data`.

- [ ] **Step 2: Delete the Deno project files**

```bash
git rm deno.json deno.lock
```

- [ ] **Step 3: Verify no Deno APIs or tooling remain**

Run: `rg -n -e 'Deno\.' -e 'deno (task|run|test|compile)' -e 'deno\.json' -e 'deno\.lock' -e 'jsr:' -e '@std/' --glob '!node_modules/**' --glob '!.git/**' --glob '!docs/**' .`
Expected: no matches. (`docs/` keeps the historical design/plan references.)

- [ ] **Step 4: Run the full verification**

Run: `npm test`
Expected: all tests pass (8 KV tests, 19 app tests).

Run: `npm run typecheck`
Expected: exit 0.

Run: `node --env-file=.env.example main.ts`
Expected: exits non-zero because `CALLBACK_SECRET` is empty (fail-fast validation still works).

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: document the Node.js runtime and remove Deno tooling"
```
