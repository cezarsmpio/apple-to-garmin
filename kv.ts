import { randomBytes } from "node:crypto";
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
  getStatement: StatementSync;
  deleteStatement: StatementSync;
  insertClaim: StatementSync;

  constructor(db: DatabaseSync) {
    this.db = db;
    this.stopped = false;
    this.getStatement = db.prepare(
      "SELECT value, versionstamp FROM kv WHERE key = ?",
    );
    this.deleteStatement = db.prepare("DELETE FROM kv WHERE key = ?");
    this.insertClaim = db.prepare(
      "INSERT INTO kv (key, value, versionstamp) VALUES (?, ?, ?) ON CONFLICT(key) DO NOTHING",
    );
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

  enqueue(_value: unknown): Promise<{ ok: true; versionstamp: string }> {
    throw new Error("not implemented");
  }

  listenQueue(_handler: (value: unknown) => Promise<void>): void {
    throw new Error("not implemented");
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
    // The queue worker is added in a later task; enqueue() will wake it here.
  }
}

export async function openKv(
  path: string,
  _options?: KvOptions,
): Promise<Kv> {
  const db = new DatabaseSync(path);

  try {
    if (path !== ":memory:") {
      db.exec("PRAGMA journal_mode = WAL");
      db.exec("PRAGMA busy_timeout = 5000");
    }
    ensureSchema(db);
  } catch (error) {
    db.close();
    throw error;
  }

  return new SqliteKv(db);
}
