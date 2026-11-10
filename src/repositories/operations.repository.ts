import type { PrismaClient } from "@prisma/client";
import { prisma } from "./prisma";

export interface OperationsMetrics {
  pendingOutboxEvents: number;
  oldestUnpublishedEventAgeMs: number | null;
  failedJobs: number;
}

export class OperationsRepository {
  constructor(private readonly client: PrismaClient = prisma) {}

  async getMetrics(): Promise<OperationsMetrics> {
    const [row] = await this.client.$queryRaw<
      Array<{
        pendingOutboxEvents: bigint;
        oldestUnpublishedEventAgeMs: number | null;
        failedJobs: bigint;
      }>
    >`
      SELECT
        (SELECT COUNT(*) FROM "OutboxEvent" WHERE "publishedAt" IS NULL)
          AS "pendingOutboxEvents",
        (
          SELECT GREATEST(
            0,
            FLOOR(EXTRACT(EPOCH FROM (clock_timestamp() - MIN("createdAt"))) * 1000)
          )
          FROM "OutboxEvent"
          WHERE "publishedAt" IS NULL
        )::double precision AS "oldestUnpublishedEventAgeMs",
        (SELECT COUNT(*) FROM "Job" WHERE "status" = 'FAILED')
          AS "failedJobs"
    `;
    return {
      pendingOutboxEvents: Number(row?.pendingOutboxEvents ?? 0),
      oldestUnpublishedEventAgeMs: row?.oldestUnpublishedEventAgeMs ?? null,
      failedJobs: Number(row?.failedJobs ?? 0),
    };
  }
}
