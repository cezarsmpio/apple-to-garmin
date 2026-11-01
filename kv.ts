import { randomBytes, randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type { StatementSync } from "node:sqlite";
import { deserialize, serialize } from "node:v8";

export type KvKey = readonly (string | number | bigint | boolean)[];

export type KvEntry<T> = {
  key: KvKey;
  value: T | null;
  versionstamp: string | null;
};

export type KvOptions = {
  retryDelay?: (attempt: number) => number;
};

export type KvCheck = {
  key: KvKey;
  versionstamp: string | null;
};

export type KvCommitResult = {
  ok: boolean;
  versionstamp: string | null;
};

export interface AtomicOperation {
  check(check: KvCheck): this;
  set(key: KvKey, value: unknown): this;
  commit(): Promise<KvCommitResult>;
}

export interface Kv {
  get<T>(key: KvKey): Promise<KvEntry<T>>;
  delete(key: KvKey): Promise<void>;
  atomic(): AtomicOperation;
  enqueue(value: unknown): Promise<{ ok: true; versionstamp: string }>;
  listenQueue(handler: (value: unknown) => Promise<void>): void;
  close(): void;
}

const KV_COLUMNS = ["key", "value", "versionstamp"];
const QUEUE_COLUMNS = ["id", "value", "attempts", "next_attempt_at"];

const CREATE_KV_TABLE = `CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value BLOB NOT NULL,
  versionstamp TEXT NOT NULL
)`;

const CREATE_QUEUE_TABLE = `CREATE TABLE IF NOT EXISTS queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  value BLOB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0
)`;

const INCOMPATIBLE_SCHEMA_MESSAGE =
  "DATABASE_PATH points at a Deno KV database or another incompatible schema; use a fresh path";

function newVersionstamp(): string {
  const time = Date.now().toString(16).padStart(12, "0");
  return time + randomBytes(8).toString("hex");
}

type WakeSignal = {
  promise: Promise<void>;
  resolve: () => void;
};

function newWakeSignal(): WakeSignal {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function tableColumns(db: DatabaseSync, table: string): string[] {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all();
  return rows.map((row) => String(row.name));
}

function columnsMatch(actual: string[], expected: string[]): boolean {
  return (
    actual.length === expected.length &&
    actual.every((name, index) => name === expected[index])
  );
}

function ensureSchema(db: DatabaseSync): void {
  const kvColumns = tableColumns(db, "kv");
  if (kvColumns.length === 0) {
    db.exec(CREATE_KV_TABLE);
  } else if (!columnsMatch(kvColumns, KV_COLUMNS)) {
    throw new Error(INCOMPATIBLE_SCHEMA_MESSAGE);
  }

  const queueColumns = tableColumns(db, "queue");
  if (queueColumns.length === 0) {
    db.exec(CREATE_QUEUE_TABLE);
  } else if (!columnsMatch(queueColumns, QUEUE_COLUMNS)) {
    throw new Error(INCOMPATIBLE_SCHEMA_MESSAGE);
  }
}

class SqliteAtomicOperation implements AtomicOperation {
  kv: SqliteKv;
  checkEntry: KvCheck | undefined;
  setEntry: { key: KvKey; value: Uint8Array } | undefined;

  constructor(kv: SqliteKv) {
    this.kv = kv;
  }

  check(check: KvCheck): this {
    if (this.checkEntry !== undefined) {
      throw new Error("atomic() supports exactly one check()");
    }
    if (check.versionstamp !== null) {
      throw new Error(
        "atomic().check() only supports versionstamp: null (key must not exist)",
      );
    }
    this.checkEntry = check;
    return this;
  }

  set(key: KvKey, value: unknown): this {
    if (this.setEntry !== undefined) {
      throw new Error("atomic() supports exactly one set()");
    }
    this.setEntry = { key, value: serialize(value) };
    return this;
  }

  async commit(): Promise<KvCommitResult> {
    const check = this.checkEntry;
    const set = this.setEntry;
    if (check === undefined) {
      throw new Error("atomic().commit() requires exactly one check()");
    }
    if (set === undefined) {
      throw new Error("atomic().commit() requires exactly one set()");
    }
    if (JSON.stringify(check.key) !== JSON.stringify(set.key)) {
      throw new Error("atomic().check() and set() must use the same key");
    }

    const versionstamp = newVersionstamp();
    const result = this.kv.insertClaim.run(
      JSON.stringify(set.key),
      set.value,
      versionstamp,
    );

    if (Number(result.changes) === 1) {
      return { ok: true, versionstamp };
    }
    return { ok: false, versionstamp: null };
  }
}

class SqliteKv implements Kv {
  db: DatabaseSync;
  stopped: boolean;
  options: KvOptions;
  handler: ((value: unknown) => Promise<void>) | undefined;
  getStatement: StatementSync;
  deleteStatement: StatementSync;
  insertClaim: StatementSync;
  insertQueueStatement: StatementSync;
  nextQueueStatement: StatementSync;
  minNextAttemptStatement: StatementSync;
  retryQueueStatement: StatementSync;
  deleteQueueStatement: StatementSync;
  wakeSignal: WakeSignal;
  wakeTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(db: DatabaseSync, options: KvOptions = {}) {
    this.db = db;
    this.stopped = false;
    this.options = options;
    this.handler = undefined;
    this.wakeSignal = newWakeSignal();
    this.wakeTimer = undefined;
    this.getStatement = db.prepare(
      "SELECT value, versionstamp FROM kv WHERE key = ?",
    );
    this.deleteStatement = db.prepare("DELETE FROM kv WHERE key = ?");
    this.insertClaim = db.prepare(
      "INSERT INTO kv (key, value, versionstamp) VALUES (?, ?, ?) ON CONFLICT(key) DO NOTHING",
    );
    this.insertQueueStatement = db.prepare(
      "INSERT INTO queue (value) VALUES (?)",
    );
    this.nextQueueStatement = db.prepare(
      "SELECT id, value, attempts FROM queue WHERE next_attempt_at <= ? ORDER BY id LIMIT 1",
    );
    this.minNextAttemptStatement = db.prepare(
      "SELECT MIN(next_attempt_at) AS next FROM queue",
    );
    this.retryQueueStatement = db.prepare(
      "UPDATE queue SET attempts = ?, next_attempt_at = ? WHERE id = ?",
    );
    this.deleteQueueStatement = db.prepare("DELETE FROM queue WHERE id = ?");
  }

  async get<T>(key: KvKey): Promise<KvEntry<T>> {
    const row = this.getStatement.get(JSON.stringify(key));
    if (row === undefined) {
      return { key, value: null, versionstamp: null };
    }
    return {
      key,
      value: deserialize(row.value as Uint8Array) as T,
      versionstamp: String(row.versionstamp),
    };
  }

  async delete(key: KvKey): Promise<void> {
    this.deleteStatement.run(JSON.stringify(key));
  }

  atomic(): AtomicOperation {
    return new SqliteAtomicOperation(this);
  }

  async enqueue(value: unknown): Promise<{ ok: true; versionstamp: string }> {
    this.insertQueueStatement.run(serialize(value));
    this.wakeWorker();
    return { ok: true, versionstamp: randomUUID() };
  }

  listenQueue(handler: (value: unknown) => Promise<void>): void {
    if (this.handler !== undefined) {
      throw new Error("listenQueue() supports exactly one handler");
    }
    this.handler = handler;
    void this.drain();
  }

  close(): void {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    this.wakeWorker();
    this.db.close();
  }

  wakeWorker(): void {
    if (this.wakeTimer !== undefined) {
      clearTimeout(this.wakeTimer);
      this.wakeTimer = undefined;
    }
    const signal = this.wakeSignal;
    this.wakeSignal = newWakeSignal();
    signal.resolve();
  }

  async drain(): Promise<void> {
    try {
      while (!this.stopped) {
        const row = this.nextQueueStatement.get(Date.now());
        if (row === undefined) {
          await this.waitForWork();
          continue;
        }
        await this.processQueueRow(
          Number(row.id),
          Number(row.attempts),
          row.value as Uint8Array,
        );
      }
    } catch (error) {
      if (!this.stopped) {
        console.error("queue worker stopped unexpectedly", error);
      }
    }
  }

  async waitForWork(): Promise<void> {
    const wake = this.wakeSignal.promise;
    const row = this.minNextAttemptStatement.get();
    const next = row === undefined ? null : row.next;
    if (next === null || next === undefined) {
      await wake;
      return;
    }

    const delay = Math.max(0, Number(next) - Date.now());
    const timerDone = new Promise<void>((resolve) => {
      this.wakeTimer = setTimeout(() => {
        this.wakeTimer = undefined;
        resolve();
      }, delay);
    });
    await Promise.race([wake, timerDone]);
    if (this.wakeTimer !== undefined) {
      clearTimeout(this.wakeTimer);
      this.wakeTimer = undefined;
    }
  }

  async processQueueRow(
    id: number,
    attempts: number,
    serialized: Uint8Array,
  ): Promise<void> {
    const handler = this.handler;
    if (handler === undefined) {
      throw new Error("the queue worker requires a handler");
    }

    try {
      await handler(deserialize(serialized));
    } catch (error) {
      if (this.stopped) {
        return;
      }
      const nextAttempts = attempts + 1;
      const delay = Math.max(
        1,
        this.options.retryDelay?.(nextAttempts) ??
          Math.min(1000 * 2 ** (nextAttempts - 1), 300_000),
      );
      this.retryQueueStatement.run(nextAttempts, Date.now() + delay, id);
      console.error(
        `queue handler failed (attempt ${nextAttempts}); retrying in ${delay}ms`,
        error,
      );
      return;
    }

    if (!this.stopped) {
      this.deleteQueueStatement.run(id);
    }
  }
}

export async function openKv(
  path: string,
  options: KvOptions = {},
): Promise<Kv> {
  const db = new DatabaseSync(path);

  try {
    ensureSchema(db);

    if (path !== ":memory:") {
      db.exec("PRAGMA busy_timeout = 5000");
      db.exec("PRAGMA journal_mode = WAL");
    }
  } catch (error) {
    db.close();
    throw error;
  }

  return new SqliteKv(db, options);
}
