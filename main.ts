import type { AddWeighInInput } from "garmin-connect-sdk";
import type { Kv, KvKey } from "./kv.ts";

export function weightKey(date: Date): KvKey {
  return ["weights", date.toISOString()];
}

export function parseWeighInPayload(body: {
  weights?: unknown;
  dates?: unknown;
}): { weights: number[]; dates: Date[] } | { error: string } {
  if (typeof body.weights !== "string") {
    return { error: "no weights found" };
  }

  if (typeof body.dates !== "string") {
    return { error: "no dates found" };
  }

  const weights = body.weights
    .split("\n")
    .map((weight: string) => weight.slice(0, 5))
    .map(Number);

  const dates = body.dates.split("\n").map((date: string) => new Date(date));

  return { weights, dates };
}

interface WeighInWriter {
  weight: {
    addWeighIn(entry: AddWeighInInput): Promise<unknown>;
  };
}

export async function recordNewWeights(
  kv: Pick<Kv, "atomic" | "delete">,
  garmin: WeighInWriter,
  weights: number[],
  dates: Date[],
): Promise<void> {
  for (const [index, weight] of weights.entries()) {
    const date = dates[index];
    const key = weightKey(date);

    const claim = await kv
      .atomic()
      .check({ key, versionstamp: null })
      .set(key, { date, weight })
      .commit();

    if (!claim.ok) {
      // Already recorded (e.g. a redelivered queue message) - skip.
      continue;
    }

    try {
      await garmin.weight.addWeighIn({
        value: weight,
        measuredAt: date.toISOString(),
        unit: "kg",
      });
    } catch (error) {
      console.error(
        `Failed to send weight for ${date.toISOString()} to Garmin, will retry later`,
        error,
      );
      await kv.delete(key);
      throw error;
    }
  }
}

interface NewWeightMessage {
  type: "NEW_WEIGHT";
  payload: { weights: number[]; dates: Date[] };
}

function isNewWeightMessage(value: unknown): value is NewWeightMessage {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { type?: unknown }).type === "NEW_WEIGHT"
  );
}

export function createQueueListener(
  kv: Pick<Kv, "atomic" | "delete">,
  garmin: WeighInWriter,
): (value: unknown) => Promise<void> {
  return async (value: unknown) => {
    if (!isNewWeightMessage(value)) {
      return;
    }

    const { weights, dates } = value.payload;
    await recordNewWeights(kv, garmin, weights, dates);
  };
}

function isSecretValid(provided: unknown, expected: string): boolean {
  return (
    typeof provided === "string" && provided.length > 0 && provided === expected
  );
}

export function createHandler(
  kv: Pick<Kv, "enqueue">,
  secret: string,
): (req: Request) => Promise<Response> {
  return async (req: Request) => {
    const url = new URL(req.url);

    if (url.pathname !== "/callback") {
      return Response.json({ error: "not found" }, { status: 404 });
    }

    const body = await req.json();

    if (!isSecretValid(body?.secret, secret)) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }

    const parsed = parseWeighInPayload(body);

    if ("error" in parsed) {
      return Response.json({ error: parsed.error }, { status: 400 });
    }

    await kv.enqueue({
      type: "NEW_WEIGHT",
      payload: parsed,
    });

    return Response.json({ status: "ok" });
  };
}
