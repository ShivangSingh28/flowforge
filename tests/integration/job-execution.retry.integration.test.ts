import { randomUUID } from "node:crypto";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
} from "@jest/globals";
import { JobAttemptStatus, JobStatus } from "@prisma/client";
import { JobExecutionRepository } from "../../src/repositories/job-execution.repository";
import { prisma } from "../../src/repositories/prisma";
import {
  JobExecutionService,
  TerminalJobError,
  type JobHandler,
} from "../../src/services/job-execution.service";
import type { JobMessage } from "../../src/types/job-message";

const createdJobIds: string[] = [];

const createJob = async (maxAttempts: number): Promise<JobMessage> => {
  const job = await prisma.job.create({
    data: {
      type: "retry-integration",
      payload: { marker: randomUUID() },
      maxAttempts,
      idempotencyKey: `retry-integration:${randomUUID()}`,
    },
  });
  createdJobIds.push(job.id);
  return {
    jobId: job.id,
    type: job.type,
    payload: { marker: "retry-integration" },
    priority: job.priority,
    createdAt: job.createdAt.toISOString(),
  };
};

const cleanCreatedJobs = async (): Promise<void> => {
  if (createdJobIds.length === 0) {
    return;
  }
  await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
  createdJobIds.length = 0;
};

const waitOptions = {
  leaseDurationMs: 3000,
  heartbeatIntervalMs: 500,
  nodeEnvironment: "test" as const,
  retryBaseDelayMs: 20,
  retryMaxDelayMs: 30,
};

describe("job execution retry policy against real PostgreSQL", () => {
  beforeAll(async () => {
    const databases = await prisma.$queryRaw<Array<{ database: string }>>`
      SELECT current_database() AS database
    `;
    if (databases[0]?.database !== "flowforge_integration") {
      throw new Error(
        "Retry integration tests require the isolated flowforge_integration database",
      );
    }
  });

  afterEach(cleanCreatedJobs);

  afterAll(async () => {
    await cleanCreatedJobs();
    await prisma.$disconnect();
  });

  it("retries retryable handler errors with persisted backoff and accurate attempts", async () => {
    const message = await createJob(3);
    let executions = 0;
    const handler: JobHandler = async () => {
      executions += 1;
      if (executions < 3) {
        throw new Error("temporary integration failure");
      }
    };
    const service = new JobExecutionService(
      new JobExecutionRepository(),
      new Map([[message.type, handler]]),
      waitOptions.leaseDurationMs,
      waitOptions.heartbeatIntervalMs,
      waitOptions.nodeEnvironment,
      waitOptions.retryBaseDelayMs,
      waitOptions.retryMaxDelayMs,
    );

    await expect(service.execute(message)).resolves.toBe("ack");

    const job = await prisma.job.findUniqueOrThrow({
      where: { id: message.jobId },
      include: { jobAttempts: { orderBy: { attemptNumber: "asc" } } },
    });
    expect(job).toMatchObject({
      status: JobStatus.COMPLETED,
      attempts: 3,
      maxAttempts: 3,
      scheduledAt: null,
    });
    expect(job.jobAttempts.map((attempt) => attempt.attemptNumber)).toEqual([
      1, 2, 3,
    ]);
    expect(job.jobAttempts.map((attempt) => attempt.status)).toEqual([
      JobAttemptStatus.FAILED,
      JobAttemptStatus.FAILED,
      JobAttemptStatus.COMPLETED,
    ]);
  });

  it("marks a retryable failure terminal after maxAttempts is reached", async () => {
    const message = await createJob(2);
    const service = new JobExecutionService(
      new JobExecutionRepository(),
      new Map([
        [message.type, async () => Promise.reject(new Error("failure"))],
      ]),
      waitOptions.leaseDurationMs,
      waitOptions.heartbeatIntervalMs,
      waitOptions.nodeEnvironment,
      waitOptions.retryBaseDelayMs,
      waitOptions.retryMaxDelayMs,
    );

    await expect(service.execute(message)).resolves.toBe("ack");

    const job = await prisma.job.findUniqueOrThrow({
      where: { id: message.jobId },
      include: { jobAttempts: { orderBy: { attemptNumber: "asc" } } },
    });
    expect(job).toMatchObject({
      status: JobStatus.FAILED,
      attempts: 2,
      maxAttempts: 2,
      scheduledAt: null,
    });
    expect(job.failedAt).not.toBeNull();
    expect(job.jobAttempts.map((attempt) => attempt.status)).toEqual([
      JobAttemptStatus.FAILED,
      JobAttemptStatus.FAILED,
    ]);
  });

  it("does not retry explicitly terminal handler failures", async () => {
    const message = await createJob(3);
    let executions = 0;
    const service = new JobExecutionService(
      new JobExecutionRepository(),
      new Map([
        [
          message.type,
          async () => {
            executions += 1;
            throw new TerminalJobError("invalid job data");
          },
        ],
      ]),
      waitOptions.leaseDurationMs,
      waitOptions.heartbeatIntervalMs,
      waitOptions.nodeEnvironment,
      waitOptions.retryBaseDelayMs,
      waitOptions.retryMaxDelayMs,
    );

    await expect(service.execute(message)).resolves.toBe("ack");
    const job = await prisma.job.findUniqueOrThrow({
      where: { id: message.jobId },
    });
    expect(job.status).toBe(JobStatus.FAILED);
    expect(job.attempts).toBe(1);
    expect(executions).toBe(1);
  });

  it("fails an expired final attempt instead of exceeding maxAttempts", async () => {
    const message = await createJob(1);
    const repository = new JobExecutionRepository();
    const claim = await repository.claimAvailable(message.jobId, {
      leaseDurationMs: 30000,
    });
    if (!claim) {
      throw new Error("Expected final attempt to be claimed");
    }

    await prisma.$executeRaw`
      UPDATE "Job"
      SET "leaseExpiresAt" = clock_timestamp() - INTERVAL '1 millisecond'
      WHERE "id" = ${message.jobId}::uuid
        AND "leaseToken" = ${claim.leaseToken}::uuid
    `;
    await expect(
      repository.claimAvailable(message.jobId, { leaseDurationMs: 30000 }),
    ).resolves.toBeNull();

    const job = await prisma.job.findUniqueOrThrow({
      where: { id: message.jobId },
      include: { jobAttempts: true },
    });
    expect(job.status).toBe(JobStatus.FAILED);
    expect(job.attempts).toBe(1);
    expect(job.jobAttempts).toMatchObject([
      {
        attemptNumber: 1,
        status: JobAttemptStatus.FAILED,
        error: "Execution lease expired; retry limit exhausted",
      },
    ]);
  });
});
