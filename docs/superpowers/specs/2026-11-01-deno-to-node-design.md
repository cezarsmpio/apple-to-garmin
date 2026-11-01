# Port apple-to-garmin from Deno to Node.js 26 — design

Date: 2026-11-01
Status: awaiting approval
Approach chosen: **A — SQLite-backed KV/queue shim on built-in `node:sqlite`**

## Intent

The project is a small Deno HTTP server that receives Apple Health weight
samples from an iOS Shortcut, deduplicates them, and forwards new samples to
Garmin Connect. We want the same behavior on Node.js 26, with no Deno runtime
or tooling left in the repository.

The reason is not stated, so the design preserves behavior rather than
revisiting it: same routes, same payload contract, same dedupe guarantee, same
queue-then-deliver flow.

## Goals

- Run on stock Node.js 26 with `node main.ts`; no transpile/build step.
- Preserve `POST /callback` behavior, status codes, and JSON responses exactly.
- Preserve "a given weigh-in date is sent to Garmin at most once", including
  across process restarts and redelivered messages.
- Preserve the queue-then-process flow, with the message surviving a crash
  between the HTTP 200 and Garmin delivery.
- Keep the test suite covering the same behaviors, run with `node --test`.
- Ship a working Docker image without Deno or `deno compile`.
- Remove `deno.json`, `deno.lock`, and all Deno APIs.

## Non-goals

- No API contract, payload format, or Garmin SDK changes.
- No new features (auth changes, metrics, health endpoint, UI).
- No framework (Express/Hono/Fastify); use `node:http`.
- No external database or KV/queue service (Redis, Postgres, Deno Deploy KV).
- No Deno compatibility shim; the Deno code is replaced, not wrapped.
- No data migration tooling; there is no live database to migrate (see
  Migration decision).

## Anti-goals

A change that technically runs on Node but loses the dedupe guarantee, drops
queued messages on restart, weakens the tests, or leaves a Deno dependency
would be a failure even if `npm test` passes.

## Constraints

- Node 26.13 is available locally; `node:sqlite`, native TypeScript type
  stripping, `node:test`, `--env-file`, and `--watch` all work without flags.
- `garmin-connect-sdk@1.0.0` declares `engines.node >= 24` and depends only on
  `zod`, so it is used unchanged.
- Node's type stripping removes type annotations but not value imports. Imports
  used only as types (`AddWeighInInput`) must become `import type`.
- Node 26 supports `import.meta.main`, so the existing entrypoint guard stays.
- The existing Deno KV SQLite file uses Deno's own schema and cannot be read as
  the new schema.

## Approaches considered

### A — SQLite-backed KV/queue shim (chosen)

Add `kv.ts` implementing the small Deno KV surface the app uses
(`get`, `delete`, atomic check/set, `enqueue`, `listenQueue`) on top of
built-in `node:sqlite`. Two tables: `kv` and `queue`.

- Keeps the current architecture and nearly every test.
- Zero new runtime dependencies.
- Queue is durable across restarts; dedupe claim stays atomic via a unique key.
- Cost: ~120–150 lines of new storage code to test and maintain.

### B — Inline processing, no queue

Claim the date in SQLite and call Garmin before returning 200.

- Least code.
- Changes response latency and retry semantics; the Shortcut waits on Garmin.
- Rewrites most handler tests and loses crash-safe handoff.

Rejected: changes observable behavior, which the port should not do.

### C — Third-party KV store (LMDB/Level)

Swap Deno KV for an npm KV library.

- Extra dependency and native build concerns in Docker.
- Still needs a queue built on top; more total code than A.
- No benefit for a single-process, low-volume app.

Rejected: more moving parts for the same result.

## How it is built

### Runtime and project files

- `package.json`: `"type": "module"`, `"engines": { "node": ">=26" }`,
  dependency `garmin-connect-sdk@1.0.0`.
- Scripts:
  - `dev`: `node --watch --env-file=.env main.ts`
  - `start`: `node main.ts`
  - `test`: `node --test`
  - `typecheck`: `tsc --noEmit` (devDependency: `typescript`, `@types/node`)
- `tsconfig.json`: `module`/`moduleResolution` `nodenext`, `target` `esnext`,
  `strict`, `noEmit`, `allowImportingTsExtensions`,
  `erasableSyntaxOnly`, `types: ["node"]`.
- Delete `deno.json` and `deno.lock`; add `package-lock.json`.
- TypeScript runs directly via Node type stripping. No `tsx`, `ts-node`, or
  build output.

### KV shim (`kv.ts`)

Public surface is the subset `main.ts` and the tests use:

```ts
type KvKey = readonly (string | number | bigint | boolean)[];
type KvEntry<T> = { key: KvKey; value: T | null; versionstamp: string | null };

openKv(path: string): Promise<Kv>;

interface Kv {
  get<T>(key: KvKey): Promise<KvEntry<T>>;
  delete(key: KvKey): Promise<void>;
  atomic(): { check(...): this; set(key, value): this; commit(): Promise<{ok: boolean; versionstamp: string | null}> };
  enqueue(value: unknown): Promise<{ ok: true; versionstamp: string }>;
  listenQueue(handler: (value: unknown) => Promise<void>): void;
  close(): void;
}
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL,
  versionstamp TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  value BLOB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0
);
```

Implementation decisions:

- Keys are serialized with `JSON.stringify(key)` for the primary key.
- Values are serialized with `v8.serialize` / `v8.deserialize` so `Date`
  objects survive the round trip (the tests assert stored `Date` values).
- Atomic claim: `INSERT ... ON CONFLICT(key) DO NOTHING`; `ok` is whether a
  row was inserted. `check({ versionstamp: null })` means "must not exist",
  which is the only check the app uses. Other check forms may be rejected with
  a clear error rather than silently ignored.
- `delete` removes the row.
- `enqueue` inserts a queue row, wakes the worker, and returns
  `{ ok: true, versionstamp }`.
- `listenQueue` starts one sequential worker loop. It selects the oldest due
  row, deserializes it, awaits the handler, deletes on success, and on failure
  increments `attempts` and sets an exponential backoff (`next_attempt_at`).
  The loop is woken by `enqueue`; it does not busy-poll.
- `close` stops the worker and closes the database.
- File databases enable `PRAGMA journal_mode = WAL` and `busy_timeout`;
  `:memory:` is supported for tests.
- `weightKey` keeps returning `["weights", isoString]`.

### HTTP server

- Use `node:http` `createServer`, port 8080.
- Adapt `IncomingMessage` → web `Request` (URL from `req.url` + `Host`,
  headers copied, body via `Readable.toWeb(req)`, `duplex: "half"`) and
  `Response` → `ServerResponse` (status, headers, buffered body).
- `createHandler` keeps its current `(req: Request) => Promise<Response>`
  signature so tests exercise the same function.
- Unhandled errors return 500 instead of crashing the process.

### Configuration

- Read `process.env`; validate the same required variables at startup with the
  same fail-fast behavior.
- Rename `DENO_KV_PATH` → `DATABASE_PATH` (this project now owns the schema and
  the name should not reference Deno). Update `.env.example`, Dockerfile, and
  README.
- Default filename changes to `apple-to-garmin.db` (local) and
  `/data/apple-to-garmin.db` (Docker), so an old Deno KV file at
  `apple-to-garmin.sqlite3` is never opened or clobbered.
- On open, inspect the existing `kv`/`queue` tables. If a table exists but its
  columns are not the ones below (e.g. Deno KV's `kv(k, v, v_encoding,
  version)`), exit with a clear error naming `DATABASE_PATH` and explaining
  that this is a Deno KV database and a fresh path is required.
- Keep `GARMIN_STORAGE_PATH`, `GARMIN_USERNAME`, `GARMIN_PASSWORD`,
  `CALLBACK_SECRET` unchanged.

### Delivery-failure behavior

`recordNewWeights` currently catches Garmin errors, logs, deletes the claim, and
returns — so the queue acks and the weight is lost even though the log says
"will retry later". The port fixes that:

- After rolling back the claim, rethrow the error so `listenQueue` retries the
  message with exponential backoff (capped at 5 minutes, retried indefinitely;
  each failure is logged).
- The rollback still allows the retry to re-claim the date.
- Accepted trade-off: a crash after the claim commits but before the Garmin call
  leaves the date claimed, so redelivery skips it. That is the existing
  at-most-once behavior and is preserved.

### Tests

- Port `main_test.ts` to `node:test` + `node:assert/strict` + `mock.fn`.
- Keep the same cases: key shape, payload parsing/errors, dedupe, per-date
  sends, rollback on Garmin failure, listener filtering, all handler responses.
- The rollback case now expects the first `recordNewWeights` call to reject
  (`assert.rejects`) while the claim is gone, and the second call to succeed.
- Add tests for the new `kv.ts`: atomic claim rejects a duplicate, queue
  delivery runs the handler, a throwing handler is retried and eventually
  succeeds, and data survives close/reopen of a file-backed database.
- Replace `Deno.openKv(":memory:")` with `openKv(":memory:")`; close in
  `afterEach` or via `t.after`.
- `assertSpyCalls` → `spy.mock.callCount()`;
  `assertSpyCallArgs` → `assert.deepEqual(spy.mock.calls[i].arguments, ...)`.

### Docker

Single stage on `node:26-slim`:

- `COPY package*.json`, `npm ci --omit=dev`, copy `main.ts` and `kv.ts`.
- Create `/data`, chown to the built-in `node` user, `USER node`.
- `ENV GARMIN_STORAGE_PATH=/data/.garmin-tokens`,
  `DATABASE_PATH=/data/apple-to-garmin.db`.
- `EXPOSE 8080`, `CMD ["node", "main.ts"]`.
- No compile step, no Deno image, no multi-stage build.

### Documentation

- README: requirements (Node 26), `npm install`, `npm run dev`,
  `npm test`, mermaid participant `SQLite` instead of `Deno KV`, Docker
  description, and a replacement for the Deno permissions section.
- Note the env var rename and the fresh-database caveat.

## Migration decision

There is no live Deno KV database to carry over. The new SQLite schema starts
empty. The Docker volume and local defaults use the new `apple-to-garmin.db`
filename, and startup refuses to use a file whose schema is Deno KV's, so the
old `apple-to-garmin.sqlite3` is left untouched.

Consequence, documented in the README: the first batch after switching has no
dedupe history, so dates already sent from the Deno version could be sent to
Garmin again. No importer is part of this work.

## Left to the builder

- Exact backoff constants for the queue (capped at 5 minutes).
- Logging wording for startup and delivery failures.
- Whether to add graceful shutdown (`SIGINT`/`SIGTERM`) around `server.close`
  and `kv.close`.
- Test helper layout for opening/closing in-memory databases.
- Whether `npm run typecheck` is wired into CI (none exists today).
