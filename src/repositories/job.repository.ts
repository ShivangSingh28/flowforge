import { Prisma, type Job } from "@prisma/client";
import { prisma } from "./prisma";
import type { JobSubmissionInput } from "../types/job-submission";

export class JobRepository {
  async create(input: JobSubmissionInput): Promise<Job> {
    return prisma.job.create({
      data: {
        type: input.type,
        payload: input.payload,
        priority: input.priority,
        maxAttempts: input.maxAttempts,
        idempotencyKey: input.idempotencyKey ?? null,
      },
    });
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<Job | null> {
    return prisma.job.findUnique({ where: { idempotencyKey } });
  }

  isUniqueConstraintError(error: unknown): boolean {
    return (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === "P2002"
    );
  }
}
