import {
  JobAttemptStatus,
  JobStatus,
  type Job,
  type JobAttempt,
  type PrismaClient,
} from "@prisma/client";
import { randomUUID } from "node:crypto";
import { prisma } from "./prisma";

export interface ClaimedJob {
  job: Job;
  attempt: JobAttempt;
  leaseToken: string;
}

export interface ClaimJobOptions {
  leaseDurationMs: number;
}

export type LeaseMutationResult = "updated" | "lost_lease";
export type FailureMutationResult = LeaseMutationResult | "retry_scheduled";

export class JobExecutionRepository {
  constructor(private readonly client: PrismaClient = prisma) {}

  findById(id: string): Promise<Job | null> {
    return this.client.job.findUnique({ where: { id } });
  }

  claimAvailable(
    id: string,
    options: ClaimJobOptions,
  ): Promise<ClaimedJob | null> {
    return this.client.$transaction(async (transaction) => {
      const currentRows = await transaction.$queryRaw<
        Array<{
          status: JobStatus;
          attempts: number;
          leaseToken: string | null;
          leaseExpiresAt: Date | null;
          scheduledAt: Date | null;
          maxAttempts: number;
          databaseNow: Date;
        }>
      >`
        SELECT "status", "attempts", "maxAttempts", "scheduledAt",
               "leaseToken", "leaseExpiresAt",
               clock_timestamp() AS "databaseNow"
        FROM "Job"
        WHERE "id" = ${id}::uuid
        FOR UPDATE
      `;
      const current = currentRows[0];
      if (!current) {
        return null;
      }

      const expiredProcessing =
        current.status === JobStatus.PROCESSING &&
        current.leaseExpiresAt !== null &&
        current.leaseExpiresAt <= current.databaseNow;
      if (
        current.status === JobStatus.PENDING &&
        current.scheduledAt !== null &&
        current.scheduledAt > current.databaseNow
      ) {
        return null;
      }
      if (current.status !== JobStatus.PENDING && !expiredProcessing) {
        return null;
      }

      if (current.attempts >= current.maxAttempts) {
        if (expiredProcessing) {
          await transaction.jobAttempt.updateMany({
            where: { jobId: id, status: JobAttemptStatus.PROCESSING },
            data: {
              status: JobAttemptStatus.FAILED,
              finishedAt: current.databaseNow,
              error: "Execution lease expired; retry limit exhausted",
            },
          });
        }
        await transaction.$executeRaw`
          UPDATE "Job"
          SET "status" = 'FAILED',
              "failedAt" = clock_timestamp(),
              "updatedAt" = clock_timestamp(),
              "lastError" = COALESCE("lastError", 'Maximum attempts exhausted'),
              "leaseToken" = NULL,
              "leaseExpiresAt" = NULL
          WHERE "id" = ${id}::uuid
        `;
        return null;
      }

      const attemptNumber = current.attempts + 1;
      const leaseToken = randomUUID();
      const updated = await transaction.$queryRaw<Array<{ attempts: number }>>`
        UPDATE "Job"
        SET "status" = 'PROCESSING',
            "attempts" = "attempts" + 1,
            "startedAt" = clock_timestamp(),
            "updatedAt" = clock_timestamp(),
            "leaseToken" = ${leaseToken}::uuid,
            "scheduledAt" = NULL,
            "leaseExpiresAt" = clock_timestamp()
              + (${options.leaseDurationMs} * INTERVAL '1 millisecond')
        WHERE "id" = ${id}::uuid
          AND "attempts" = ${current.attempts}
          AND "status" = ${current.status}::"JobStatus"
          AND (
            "status" = 'PENDING'
            OR (
              "status" = 'PROCESSING'
              AND "leaseToken" IS NOT DISTINCT FROM ${current.leaseToken}::uuid
              AND "leaseExpiresAt" <= clock_timestamp()
            )
          )
        RETURNING "attempts"
      `;

      if (updated.length !== 1) {
        return null;
      }

      if (expiredProcessing) {
        await transaction.jobAttempt.updateMany({
          where: { jobId: id, status: JobAttemptStatus.PROCESSING },
          data: {
            status: JobAttemptStatus.FAILED,
            finishedAt: current.databaseNow,
            error: "Execution lease expired",
          },
        });
      }

      const job = await transaction.job.findUniqueOrThrow({ where: { id } });
      const attempt = await transaction.jobAttempt.create({
        data: {
          jobId: id,
          attemptNumber,
          status: JobAttemptStatus.PROCESSING,
        },
      });

      return { job, attempt, leaseToken };
    });
  }

  async renewLease(
    jobId: string,
    leaseToken: string,
    leaseDurationMs: number,
  ): Promise<LeaseMutationResult> {
    const updated = await this.client.$executeRaw`
      UPDATE "Job"
      SET "leaseExpiresAt" = clock_timestamp()
            + (${leaseDurationMs} * INTERVAL '1 millisecond'),
          "updatedAt" = clock_timestamp()
      WHERE "id" = ${jobId}::uuid
        AND "status" = 'PROCESSING'
        AND "leaseToken" = ${leaseToken}::uuid
        AND "leaseExpiresAt" > clock_timestamp()
    `;
    return updated === 1 ? "updated" : "lost_lease";
  }

  async markCompleted(
    jobId: string,
    attemptId: string,
    leaseToken: string,
  ): Promise<LeaseMutationResult> {
    return this.client.$transaction(async (transaction) => {
      const jobUpdate = await transaction.$executeRaw`
        UPDATE "Job"
        SET "status" = 'COMPLETED',
            "completedAt" = clock_timestamp(),
            "updatedAt" = clock_timestamp(),
            "leaseToken" = NULL,
            "leaseExpiresAt" = NULL
        WHERE "id" = ${jobId}::uuid
          AND "status" = 'PROCESSING'
          AND "leaseToken" = ${leaseToken}::uuid
          AND "leaseExpiresAt" > clock_timestamp()
      `;
      if (jobUpdate !== 1) {
        return "lost_lease";
      }

      const attemptUpdate = await transaction.jobAttempt.updateMany({
        where: {
          id: attemptId,
          jobId,
          status: JobAttemptStatus.PROCESSING,
        },
        data: {
          status: JobAttemptStatus.COMPLETED,
          finishedAt: new Date(),
        },
      });

      if (attemptUpdate.count !== 1) {
        throw new Error("Job completion state changed unexpectedly");
      }
      return "updated";
    });
  }

  async markFailed(
    jobId: string,
    attemptId: string,
    leaseToken: string,
    safeError: string,
    retryable = false,
    retryDelayMs = 0,
  ): Promise<FailureMutationResult> {
    return this.client.$transaction(async (transaction) => {
      const currentRows = await transaction.$queryRaw<
        Array<{ attempts: number; maxAttempts: number }>
      >`
        SELECT "attempts", "maxAttempts"
        FROM "Job"
        WHERE "id" = ${jobId}::uuid
          AND "status" = 'PROCESSING'
          AND "leaseToken" = ${leaseToken}::uuid
          AND "leaseExpiresAt" > clock_timestamp()
        FOR UPDATE
      `;
      const current = currentRows[0];
      if (!current) {
        return "lost_lease";
      }
      const retryScheduled =
        retryable && current.attempts < current.maxAttempts;
      const jobUpdate = await transaction.$executeRaw`
        UPDATE "Job"
        SET "status" = CASE
              WHEN ${retryScheduled} THEN 'PENDING'::"JobStatus"
              ELSE 'FAILED'::"JobStatus"
            END,
            "failedAt" = CASE WHEN ${retryScheduled} THEN NULL ELSE clock_timestamp() END,
            "scheduledAt" = CASE
              WHEN ${retryScheduled}
                THEN clock_timestamp() + (${retryDelayMs} * INTERVAL '1 millisecond')
              ELSE NULL
            END,
            "updatedAt" = clock_timestamp(),
            "lastError" = ${safeError},
            "leaseToken" = NULL,
            "leaseExpiresAt" = NULL
        WHERE "id" = ${jobId}::uuid
          AND "status" = 'PROCESSING'
          AND "leaseToken" = ${leaseToken}::uuid
          AND "leaseExpiresAt" > clock_timestamp()
      `;
      if (jobUpdate !== 1) {
        return "lost_lease";
      }

      const attemptUpdate = await transaction.jobAttempt.updateMany({
        where: {
          id: attemptId,
          jobId,
          status: JobAttemptStatus.PROCESSING,
        },
        data: {
          status: JobAttemptStatus.FAILED,
          finishedAt: new Date(),
          error: safeError,
        },
      });

      if (attemptUpdate.count !== 1) {
        throw new Error("Job failure state changed unexpectedly");
      }
      return retryScheduled ? "retry_scheduled" : "updated";
    });
  }
}
