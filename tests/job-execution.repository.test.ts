import {
  JobAttemptStatus,
  JobPriority,
  JobStatus,
  type Job,
  type JobAttempt,
} from "@prisma/client";
import { describe, expect, it, jest } from "@jest/globals";
import type { PrismaClient } from "@prisma/client";
import { JobExecutionRepository } from "../src/repositories/job-execution.repository";

const jobId = "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0";
const leaseToken = "d23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0";

const makeJob = (overrides: Partial<Job> = {}): Job => ({
  id: jobId,
  type: "demo",
  payload: {},
  status: JobStatus.PENDING,
  priority: JobPriority.MEDIUM,
  attempts: 2,
  maxAttempts: 3,
  idempotencyKey: null,
  leaseToken: null,
  leaseExpiresAt: null,
  scheduledAt: null,
  startedAt: null,
  completedAt: null,
  failedAt: null,
  lastError: null,
  createdAt: new Date("2026-10-31T12:00:00.000Z"),
  updatedAt: new Date("2026-10-31T12:00:00.000Z"),
  ...overrides,
});

const attempt: JobAttempt = {
  id: "c23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
  jobId,
  attemptNumber: 3,
  status: JobAttemptStatus.PROCESSING,
  workerId: null,
  startedAt: new Date("2026-10-31T12:00:00.000Z"),
  finishedAt: null,
  error: null,
  createdAt: new Date("2026-10-31T12:00:00.000Z"),
};

const createHarness = (
  currentJob = makeJob(),
  claimUpdateSucceeds = true,
  executeCount = 1,
  attemptUpdateCount = 1,
  databaseNow = new Date(),
) => {
  const updatedJob = {
    ...currentJob,
    status: JobStatus.PROCESSING,
    attempts: currentJob.attempts + 1,
    leaseToken,
  };
  const findUniqueOrThrow = jest
    .fn<() => Promise<Job>>()
    .mockResolvedValue(updatedJob);
  let claimWon = false;
  const queryRaw = jest.fn(async (query: TemplateStringsArray) => {
    const sql = query.join("");
    if (sql.includes("SELECT")) {
      return [
        {
          status: currentJob.status,
          attempts: currentJob.attempts,
          leaseToken: currentJob.leaseToken,
          leaseExpiresAt: currentJob.leaseExpiresAt,
          databaseNow,
        },
      ];
    }

    if (!claimUpdateSucceeds || claimWon) {
      return [];
    }
    claimWon = true;
    return [{ attempts: currentJob.attempts + 1 }];
  });
  const executeRaw = jest
    .fn<
      (query: TemplateStringsArray, ...values: unknown[]) => Promise<number>
    >()
    .mockResolvedValue(executeCount);
  const attemptUpdateMany = jest
    .fn<() => Promise<{ count: number }>>()
    .mockResolvedValue({ count: attemptUpdateCount });
  const create = jest
    .fn<() => Promise<JobAttempt>>()
    .mockResolvedValue(attempt);
  const transaction = {
    $queryRaw: queryRaw,
    $executeRaw: executeRaw,
    job: { findUniqueOrThrow },
    jobAttempt: { create, updateMany: attemptUpdateMany },
  };
  const client = {
    $transaction: jest.fn(
      async (callback: (tx: typeof transaction) => unknown) =>
        callback(transaction),
    ),
    $queryRaw: queryRaw,
    $executeRaw: executeRaw,
  } as unknown as PrismaClient;

  return {
    repository: new JobExecutionRepository(client),
    findUniqueOrThrow,
    queryRaw,
    executeRaw,
    attemptUpdateMany,
    create,
  };
};

describe("JobExecutionRepository", () => {
  it("claims a pending job with a new lease and creates its attempt atomically", async () => {
    const { repository, queryRaw, create } = createHarness();

    const claimed = await repository.claimAvailable(jobId, {
      leaseDurationMs: 30000,
    });

    expect(claimed).toMatchObject({
      job: { id: jobId, status: JobStatus.PROCESSING },
      attempt: { attemptNumber: 3, status: JobAttemptStatus.PROCESSING },
    });
    expect(claimed?.leaseToken).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );
    expect(queryRaw.mock.calls[0]?.[0].join("")).toContain("FOR UPDATE");
    const claimSql = queryRaw.mock.calls[1]?.[0].join("");
    expect(claimSql).toContain('"leaseExpiresAt" <= clock_timestamp()');
    expect(claimSql).toContain("INTERVAL '1 millisecond'");
    expect(create).toHaveBeenCalledWith({
      data: {
        jobId,
        attemptNumber: 3,
        status: JobAttemptStatus.PROCESSING,
      },
    });
  });

  it("reclaims an expired processing job and closes its previous attempt", async () => {
    const now = new Date("2026-10-31T12:00:30.000Z");
    const { repository, queryRaw, attemptUpdateMany } = createHarness(
      makeJob({
        status: JobStatus.PROCESSING,
        leaseToken,
        leaseExpiresAt: new Date(now.getTime() - 1),
      }),
      true,
      1,
      1,
      now,
    );

    const claimed = await repository.claimAvailable(jobId, {
      leaseDurationMs: 30000,
    });

    expect(claimed?.leaseToken).not.toBe(leaseToken);
    expect(queryRaw).toHaveBeenCalledTimes(2);
    expect(attemptUpdateMany).toHaveBeenCalledWith({
      where: { jobId, status: JobAttemptStatus.PROCESSING },
      data: {
        status: JobAttemptStatus.FAILED,
        finishedAt: now,
        error: "Execution lease expired",
      },
    });
  });

  it("does not steal an active processing lease", async () => {
    const { repository, queryRaw, create } = createHarness(
      makeJob({
        status: JobStatus.PROCESSING,
        leaseToken,
        leaseExpiresAt: new Date(Date.now() + 60000),
      }),
    );

    await expect(
      repository.claimAvailable(jobId, { leaseDurationMs: 30000 }),
    ).resolves.toBeNull();
    expect(queryRaw).toHaveBeenCalledTimes(1);
    expect(create).not.toHaveBeenCalled();
  });

  it("allows only one concurrent claimant to win", async () => {
    let hasClaimed = false;
    const now = new Date();
    const currentJob = makeJob();
    const queryRaw = jest.fn(async (query: TemplateStringsArray) => {
      if (query.join("").includes("SELECT")) {
        return [
          {
            status: currentJob.status,
            attempts: currentJob.attempts,
            leaseToken: currentJob.leaseToken,
            leaseExpiresAt: currentJob.leaseExpiresAt,
            databaseNow: now,
          },
        ];
      }
      if (hasClaimed) {
        return [];
      }
      hasClaimed = true;
      return [{ attempts: currentJob.attempts + 1 }];
    });
    const transaction = {
      $queryRaw: queryRaw,
      job: {
        findUniqueOrThrow: jest.fn(async () =>
          makeJob({ status: JobStatus.PROCESSING }),
        ),
      },
      jobAttempt: {
        create: jest.fn(async () => attempt),
        updateMany: jest.fn(async () => ({ count: 1 })),
      },
    };
    const client = {
      $transaction: jest.fn(
        async (callback: (tx: typeof transaction) => unknown) =>
          callback(transaction),
      ),
      $queryRaw: queryRaw,
    } as unknown as PrismaClient;
    const repository = new JobExecutionRepository(client);

    const results = await Promise.all([
      repository.claimAvailable(jobId, { leaseDurationMs: 30000 }),
      repository.claimAvailable(jobId, { leaseDurationMs: 30000 }),
    ]);

    expect(results.filter(Boolean)).toHaveLength(1);
    expect(transaction.jobAttempt.create).toHaveBeenCalledTimes(1);
  });

  it("renews only the matching processing lease", async () => {
    const { repository, executeRaw } = createHarness();

    await expect(repository.renewLease(jobId, leaseToken, 30000)).resolves.toBe(
      "updated",
    );
    expect(executeRaw.mock.calls[0]?.[0].join("")).toContain(
      '"leaseExpiresAt" > clock_timestamp()',
    );

    executeRaw.mockResolvedValueOnce(0);
    await expect(
      repository.renewLease(jobId, "another-token", 30000),
    ).resolves.toBe("lost_lease");
  });

  it("does not renew an expired lease when the token still matches", async () => {
    const { repository, executeRaw } = createHarness(
      makeJob({
        status: JobStatus.PROCESSING,
        leaseToken,
        leaseExpiresAt: new Date(Date.now() - 1),
      }),
      true,
      0,
    );

    await expect(repository.renewLease(jobId, leaseToken, 30000)).resolves.toBe(
      "lost_lease",
    );
    expect(executeRaw.mock.calls[0]?.[0].join("")).toContain('"leaseToken" = ');
    expect(executeRaw.mock.calls[0]?.[0].join("")).toContain(
      '"leaseExpiresAt" > clock_timestamp()',
    );
  });

  it.each([
    { operation: "completion", method: "markCompleted" },
    { operation: "failure", method: "markFailed" },
  ])(
    "fences stale $operation persistence and clears leases on terminal persistence",
    async ({ operation, method }) => {
      const { repository, executeRaw } = createHarness();
      const persist =
        operation === "completion"
          ? repository.markCompleted(jobId, attempt.id, leaseToken)
          : repository.markFailed(
              jobId,
              attempt.id,
              leaseToken,
              "Job execution failed",
            );

      await expect(persist).resolves.toBe("updated");
      const updateSql = executeRaw.mock.calls[0]?.[0].join("");
      expect(updateSql).toContain('"leaseExpiresAt" > clock_timestamp()');
      expect(updateSql).toContain(
        operation === "completion" ? "'COMPLETED'" : "'FAILED'",
      );

      const staleHarness = createHarness(makeJob(), true, 0);
      const stalePersist =
        method === "markCompleted"
          ? staleHarness.repository.markCompleted(jobId, attempt.id, leaseToken)
          : staleHarness.repository.markFailed(
              jobId,
              attempt.id,
              leaseToken,
              "Job execution failed",
            );
      await expect(stalePersist).resolves.toBe("lost_lease");
      expect(staleHarness.attemptUpdateMany).not.toHaveBeenCalled();
    },
  );

  it.each([
    { operation: "completion", method: "markCompleted" },
    { operation: "failure", method: "markFailed" },
  ])(
    "does not persist $operation for an expired matching lease",
    async ({ method }) => {
      const { repository, executeRaw, attemptUpdateMany } = createHarness(
        makeJob({
          status: JobStatus.PROCESSING,
          leaseToken,
          leaseExpiresAt: new Date(Date.now() - 1),
        }),
        true,
        0,
      );
      const persist =
        method === "markCompleted"
          ? repository.markCompleted(jobId, attempt.id, leaseToken)
          : repository.markFailed(
              jobId,
              attempt.id,
              leaseToken,
              "Job execution failed",
            );

      await expect(persist).resolves.toBe("lost_lease");
      expect(executeRaw.mock.calls[0]?.[0].join("")).toContain(
        '"leaseExpiresAt" > clock_timestamp()',
      );
      expect(attemptUpdateMany).not.toHaveBeenCalled();
    },
  );

  it("does not let an old token overwrite the job after reclaim", async () => {
    const newOwnerToken = "e23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0";
    const { repository, executeRaw, attemptUpdateMany } = createHarness(
      makeJob({
        status: JobStatus.PROCESSING,
        leaseToken: newOwnerToken,
        leaseExpiresAt: new Date(Date.now() + 30000),
      }),
      true,
      0,
    );

    await expect(
      repository.markCompleted(jobId, attempt.id, leaseToken),
    ).resolves.toBe("lost_lease");
    expect(executeRaw.mock.calls[0]?.[0].join("")).toContain('"leaseToken" = ');
    expect(attemptUpdateMany).not.toHaveBeenCalled();
  });
});
