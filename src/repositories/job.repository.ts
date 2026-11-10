import { Prisma, type Job, type PrismaClient } from "@prisma/client";
import { prisma } from "./prisma";
import type { JobSubmissionInput } from "../types/job-submission";
import type { JobMessage } from "../types/job-message";

export const JOB_SUBMITTED_EVENT_TYPE = "job.submitted";

export class JobRepository {
  constructor(private readonly client: PrismaClient = prisma) {}

  async createSubmission(input: JobSubmissionInput): Promise<Job> {
    return this.client.$transaction(async (transaction) => {
      const job = await transaction.job.create({
        data: {
          type: input.type,
          payload: input.payload,
          priority: input.priority,
          maxAttempts: input.maxAttempts,
          idempotencyKey: input.idempotencyKey ?? null,
        },
      });
      const message: JobMessage = {
        jobId: job.id,
        type: job.type,
        payload: input.payload,
        priority: job.priority,
        createdAt: job.createdAt.toISOString(),
      };

      await transaction.outboxEvent.create({
        data: {
          jobId: job.id,
          eventType: JOB_SUBMITTED_EVENT_TYPE,
          payload: message,
        },
      });

      return job;
    });
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<Job | null> {
    return this.client.job.findUnique({ where: { idempotencyKey } });
  }

  isUniqueConstraintError(error: unknown): boolean {
    return (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === "P2002"
    );
  }
}
