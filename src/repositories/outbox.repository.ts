import { randomUUID } from "node:crypto";
import type { Prisma, PrismaClient } from "@prisma/client";
import { prisma } from "./prisma";

export interface ClaimedOutboxEvent {
  id: string;
  jobId: string;
  eventType: string;
  payload: Prisma.JsonValue;
  attempts: number;
  lockToken: string;
}

interface ClaimedOutboxRow {
  id: string;
  jobId: string;
  eventType: string;
  payload: Prisma.JsonValue;
  attempts: number;
}

export class OutboxRepository {
  constructor(private readonly client: PrismaClient = prisma) {}

  claimDue(
    batchSize: number,
    leaseDurationMs: number,
    onlyEventId?: string,
  ): Promise<ClaimedOutboxEvent[]> {
    return this.client.$transaction(async (transaction) => {
      const due = await transaction.$queryRaw<ClaimedOutboxRow[]>`
        SELECT "id", "jobId", "eventType", "payload", "attempts"
        FROM "OutboxEvent"
        WHERE "publishedAt" IS NULL
          AND (${onlyEventId ?? null}::uuid IS NULL OR "id" = ${onlyEventId ?? null}::uuid)
          AND "availableAt" <= clock_timestamp()
          AND (
            "lockedUntil" IS NULL
            OR "lockedUntil" <= clock_timestamp()
          )
        ORDER BY "availableAt", "createdAt"
        LIMIT ${batchSize}
        FOR UPDATE SKIP LOCKED
      `;

      const claimed: ClaimedOutboxEvent[] = [];
      for (const event of due) {
        const lockToken = randomUUID();
        const updated = await transaction.$queryRaw<
          Array<{ attempts: number }>
        >`
          UPDATE "OutboxEvent"
          SET "lockedUntil" = clock_timestamp()
                + (${leaseDurationMs} * INTERVAL '1 millisecond'),
              "lockToken" = ${lockToken}::uuid,
              "attempts" = "attempts" + 1
          WHERE "id" = ${event.id}::uuid
            AND "publishedAt" IS NULL
          RETURNING "attempts"
        `;
        if (updated.length !== 1) {
          throw new Error("Outbox event claim changed unexpectedly");
        }
        claimed.push({ ...event, attempts: updated[0]!.attempts, lockToken });
      }

      return claimed;
    });
  }

  async markPublished(eventId: string, lockToken: string): Promise<boolean> {
    const updated = await this.client.$executeRaw`
      UPDATE "OutboxEvent"
      SET "publishedAt" = clock_timestamp(),
          "lockedUntil" = NULL,
          "lockToken" = NULL,
          "lastError" = NULL
      WHERE "id" = ${eventId}::uuid
        AND "publishedAt" IS NULL
        AND "lockToken" = ${lockToken}::uuid
        AND "lockedUntil" > clock_timestamp()
    `;
    return updated === 1;
  }

  async scheduleRetry(
    eventId: string,
    lockToken: string,
    safeError: string,
    retryDelayMs: number,
  ): Promise<boolean> {
    const updated = await this.client.$executeRaw`
      UPDATE "OutboxEvent"
      SET "availableAt" = clock_timestamp()
            + (${retryDelayMs} * INTERVAL '1 millisecond'),
          "lockedUntil" = NULL,
          "lockToken" = NULL,
          "lastError" = ${safeError}
      WHERE "id" = ${eventId}::uuid
        AND "publishedAt" IS NULL
        AND "lockToken" = ${lockToken}::uuid
        AND "lockedUntil" > clock_timestamp()
    `;
    return updated === 1;
  }
}
