import { randomUUID } from "node:crypto";
import { JobAttemptStatus, JobStatus, PrismaClient } from "@prisma/client";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
} from "@jest/globals";
import { JobExecutionRepository } from "../../src/repositories/job-execution.repository";
import type { ClaimedJob } from "../../src/repositories/job-execution.repository";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  throw new Error("Missing isolated integration DATABASE_URL");
}

const createClientUrl = (applicationName: string): string => {
  const url = new URL(databaseUrl);
  url.searchParams.set("application_name", applicationName);
  return url.toString();
};

const firstApplicationName = `flowforge_claim_a_${randomUUID()}`;
const secondApplicationName = `flowforge_claim_b_${randomUUID()}`;
const firstClient = new PrismaClient({
  datasources: {
    db: { url: createClientUrl(firstApplicationName) },
  },
});
const secondClient = new PrismaClient({
  datasources: {
    db: { url: createClientUrl(secondApplicationName) },
  },
});
const lockClient = new PrismaClient({
  datasources: {
    db: { url: createClientUrl(`flowforge_claim_lock_${randomUUID()}`) },
  },
});
const firstRepository = new JobExecutionRepository(firstClient);
const secondRepository = new JobExecutionRepository(secondClient);
const createdJobIds: string[] = [];

const createPendingJob = async (): Promise<string> => {
  const job = await firstClient.job.create({
    data: {
      type: "postgres-integration-test",
      payload: { marker: "repository-integration" },
      idempotencyKey: `repository-integration:${randomUUID()}`,
    },
  });
  createdJobIds.push(job.id);
  return job.id;
};

const waitForDatabaseCondition = async (
  predicate: () => Promise<boolean>,
  description: string,
  timeoutMs = 5000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

const leaseHasExpired = async (jobId: string): Promise<boolean> => {
  const rows = await firstClient.$queryRaw<Array<{ expired: boolean }>>`
    SELECT "leaseExpiresAt" <= clock_timestamp() AS expired
    FROM "Job"
    WHERE "id" = ${jobId}::uuid
  `;
  return rows[0]?.expired === true;
};

const closeTrackedJobs = async (): Promise<void> => {
  if (createdJobIds.length === 0) {
    return;
  }
  await firstClient.job.deleteMany({ where: { id: { in: createdJobIds } } });
  createdJobIds.length = 0;
};

describe("JobExecutionRepository against real PostgreSQL", () => {
  beforeAll(async () => {
    const databases = await firstClient.$queryRaw<Array<{ database: string }>>`
      SELECT current_database() AS database
    `;
    if (databases[0]?.database !== "flowforge_integration") {
      throw new Error(
        "Repository integration tests require the isolated flowforge_integration database",
      );
    }
    await Promise.all([
      firstClient.$queryRaw`SELECT 1`,
      secondClient.$queryRaw`SELECT 1`,
    ]);
  });

  afterEach(closeTrackedJobs);

  afterAll(async () => {
    try {
      await closeTrackedJobs();
    } finally {
      await Promise.all([
        firstClient.$disconnect(),
        secondClient.$disconnect(),
        lockClient.$disconnect(),
      ]);
    }
  });

  it("allows exactly one of two independent clients to claim a pending job", async () => {
    const jobId = await createPendingJob();
    let releaseRowLock = (): void => {};
    const rowLockReleased = new Promise<void>((resolve) => {
      releaseRowLock = resolve;
    });
    let signalRowLocked = (): void => {};
    const rowLocked = new Promise<void>((resolve) => {
      signalRowLocked = resolve;
    });
    const lockTransaction = lockClient.$transaction(async (transaction) => {
      await transaction.$queryRaw`
        SELECT "id" FROM "Job" WHERE "id" = ${jobId}::uuid FOR UPDATE
      `;
      signalRowLocked();
      await rowLockReleased;
    });
    await rowLocked;

    const claims: Array<Promise<ClaimedJob | null>> = [];
    try {
      claims.push(
        firstRepository.claimAvailable(jobId, { leaseDurationMs: 30000 }),
        secondRepository.claimAvailable(jobId, { leaseDurationMs: 30000 }),
      );
      await waitForDatabaseCondition(async () => {
        const waitingClaims = await firstClient.$queryRaw<
          Array<{ application_name: string; pid: number }>
        >`
          SELECT application_name, pid
          FROM pg_stat_activity
          WHERE application_name IN (
            ${firstApplicationName},
            ${secondApplicationName}
          )
            AND state = 'active'
            AND wait_event_type = 'Lock'
            AND query LIKE '%FOR UPDATE%'
        `;
        return (
          waitingClaims.length === 2 &&
          waitingClaims[0]?.pid !== waitingClaims[1]?.pid &&
          waitingClaims.some(
            (activity) => activity.application_name === firstApplicationName,
          ) &&
          waitingClaims.some(
            (activity) => activity.application_name === secondApplicationName,
          )
        );
      }, "both claim transactions waiting on the locked job row");
    } finally {
      releaseRowLock();
      await lockTransaction;
      await Promise.allSettled(claims);
    }

    const winners = (await Promise.all(claims)).filter(
      (claim): claim is NonNullable<typeof claim> => claim !== null,
    );

    expect(winners).toHaveLength(1);
    expect(winners[0]?.leaseToken).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );
    expect(winners[0]?.job).toMatchObject({
      id: jobId,
      status: JobStatus.PROCESSING,
      attempts: 1,
      leaseToken: winners[0]?.leaseToken,
    });
    expect(winners[0]?.job.leaseExpiresAt?.getTime()).toBeGreaterThan(
      Date.now(),
    );

    const stored = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
      include: { jobAttempts: true },
    });
    expect(stored.status).toBe(JobStatus.PROCESSING);
    expect(stored.attempts).toBe(1);
    expect(stored.leaseToken).toBe(winners[0]?.leaseToken);
    expect(stored.jobAttempts).toHaveLength(1);
    expect(
      stored.jobAttempts.filter(
        (attempt) => attempt.status === JobAttemptStatus.PROCESSING,
      ),
    ).toHaveLength(1);
  });

  it("rolls back a claim when attempt creation fails", async () => {
    const jobId = await createPendingJob();
    const conflictAttempt = await firstClient.jobAttempt.create({
      data: {
        jobId,
        attemptNumber: 1,
        status: JobAttemptStatus.FAILED,
        finishedAt: new Date(),
        error: "Test-only unique-key conflict",
      },
    });

    await expect(
      firstRepository.claimAvailable(jobId, { leaseDurationMs: 30000 }),
    ).rejects.toMatchObject({ code: "P2002" });

    const rolledBack = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
    });
    expect(rolledBack).toMatchObject({
      status: JobStatus.PENDING,
      attempts: 0,
      leaseToken: null,
      leaseExpiresAt: null,
    });

    await firstClient.jobAttempt.delete({ where: { id: conflictAttempt.id } });
    const claimAfterFailure = await firstRepository.claimAvailable(jobId, {
      leaseDurationMs: 30000,
    });
    expect(claimAfterFailure).toMatchObject({
      job: { status: JobStatus.PROCESSING, attempts: 1 },
      attempt: { attemptNumber: 1, status: JobAttemptStatus.PROCESSING },
    });
  });

  it("renews only the matching unexpired lease", async () => {
    const jobId = await createPendingJob();
    const claim = await firstRepository.claimAvailable(jobId, {
      leaseDurationMs: 30000,
    });
    if (!claim) {
      throw new Error("Expected pending job to be claimed");
    }

    const original = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
    });
    expect(
      await firstRepository.renewLease(jobId, claim.leaseToken, 60000),
    ).toBe("updated");
    const renewed = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
    });
    expect(renewed.leaseExpiresAt?.getTime()).toBeGreaterThan(
      original.leaseExpiresAt?.getTime() ?? 0,
    );

    expect(await firstRepository.renewLease(jobId, randomUUID(), 60000)).toBe(
      "lost_lease",
    );
    const afterWrongToken = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
    });
    expect(afterWrongToken.leaseExpiresAt).toEqual(renewed.leaseExpiresAt);

    const shortJobId = await createPendingJob();
    const shortClaim = await firstRepository.claimAvailable(shortJobId, {
      leaseDurationMs: 250,
    });
    if (!shortClaim) {
      throw new Error("Expected short-lease job to be claimed");
    }
    await waitForDatabaseCondition(
      () => leaseHasExpired(shortJobId),
      "short lease to expire in PostgreSQL",
    );
    const expiredBeforeRenewal = await firstClient.job.findUniqueOrThrow({
      where: { id: shortJobId },
    });
    expect(expiredBeforeRenewal.leaseExpiresAt).not.toBeNull();
    expect(
      await firstRepository.renewLease(
        shortJobId,
        shortClaim.leaseToken,
        30000,
      ),
    ).toBe("lost_lease");
    const expiredAfterRenewal = await firstClient.job.findUniqueOrThrow({
      where: { id: shortJobId },
    });
    expect(expiredAfterRenewal.leaseExpiresAt).toEqual(
      expiredBeforeRenewal.leaseExpiresAt,
    );
  });

  it("fences stale Worker A completion and failure after Worker B reclaims", async () => {
    const jobId = await createPendingJob();
    const workerA = await firstRepository.claimAvailable(jobId, {
      leaseDurationMs: 250,
    });
    if (!workerA) {
      throw new Error("Expected Worker A to claim the pending job");
    }

    await waitForDatabaseCondition(
      () => leaseHasExpired(jobId),
      "Worker A lease to expire in PostgreSQL",
    );
    const workerB = await secondRepository.claimAvailable(jobId, {
      leaseDurationMs: 30000,
    });
    if (!workerB) {
      throw new Error("Expected Worker B to reclaim the expired job");
    }
    const workerBState = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
    });
    const expectedWorkerBState = {
      status: JobStatus.PROCESSING,
      leaseToken: workerB.leaseToken,
      leaseExpiresAt: workerBState.leaseExpiresAt,
      lastError: workerBState.lastError,
    };

    expect(
      await firstRepository.markCompleted(
        jobId,
        workerA.attempt.id,
        workerA.leaseToken,
      ),
    ).toBe("lost_lease");
    await expect(
      firstClient.job.findUniqueOrThrow({ where: { id: jobId } }),
    ).resolves.toMatchObject(expectedWorkerBState);
    expect(
      await firstRepository.markFailed(
        jobId,
        workerA.attempt.id,
        workerA.leaseToken,
        "Stale Worker A failure",
      ),
    ).toBe("lost_lease");
    await expect(
      firstClient.job.findUniqueOrThrow({ where: { id: jobId } }),
    ).resolves.toMatchObject(expectedWorkerBState);

    expect(
      await secondRepository.markCompleted(
        jobId,
        workerB.attempt.id,
        workerB.leaseToken,
      ),
    ).toBe("updated");

    const stored = await firstClient.job.findUniqueOrThrow({
      where: { id: jobId },
      include: { jobAttempts: { orderBy: { attemptNumber: "asc" } } },
    });
    expect(stored).toMatchObject({
      status: JobStatus.COMPLETED,
      attempts: 2,
      leaseToken: null,
      leaseExpiresAt: null,
    });
    expect(stored.jobAttempts).toHaveLength(2);
    expect(stored.jobAttempts[0]).toMatchObject({
      attemptNumber: 1,
      status: JobAttemptStatus.FAILED,
      error: "Execution lease expired",
    });
    expect(stored.jobAttempts[0]?.finishedAt).not.toBeNull();
    expect(stored.jobAttempts[1]).toMatchObject({
      attemptNumber: 2,
      status: JobAttemptStatus.COMPLETED,
      error: null,
    });
    expect(stored.jobAttempts[1]?.finishedAt).not.toBeNull();
  });

  it("closes the expired attempt and increments the attempt number on reclaim", async () => {
    const jobId = await createPendingJob();
    const first = await firstRepository.claimAvailable(jobId, {
      leaseDurationMs: 250,
    });
    if (!first) {
      throw new Error("Expected first attempt to claim the pending job");
    }

    await waitForDatabaseCondition(
      () => leaseHasExpired(jobId),
      "first lease to expire in PostgreSQL",
    );
    const second = await secondRepository.claimAvailable(jobId, {
      leaseDurationMs: 30000,
    });
    expect(second).not.toBeNull();
    expect(second).toMatchObject({
      job: { attempts: 2, status: JobStatus.PROCESSING },
      attempt: { attemptNumber: 2, status: JobAttemptStatus.PROCESSING },
    });

    const attempts = await firstClient.jobAttempt.findMany({
      where: { jobId },
      orderBy: { attemptNumber: "asc" },
    });
    expect(attempts).toHaveLength(2);
    expect(attempts[0]).toMatchObject({
      attemptNumber: 1,
      status: JobAttemptStatus.FAILED,
      error: "Execution lease expired",
    });
    expect(attempts[0]?.finishedAt).not.toBeNull();
    expect(attempts[1]).toMatchObject({
      attemptNumber: 2,
      status: JobAttemptStatus.PROCESSING,
      finishedAt: null,
      error: null,
    });
  });

  it("rejects a PROCESSING job with a null lease expiry", async () => {
    const jobId = await createPendingJob();
    await expect(
      firstClient.job.findUniqueOrThrow({ where: { id: jobId } }),
    ).resolves.toMatchObject({
      status: JobStatus.PENDING,
      leaseExpiresAt: null,
    });
    await expect(
      firstClient.$executeRaw`
        UPDATE "Job"
        SET "status" = 'PROCESSING',
            "leaseToken" = NULL,
            "leaseExpiresAt" = NULL
        WHERE "id" = ${jobId}::uuid
      `,
    ).rejects.toMatchObject({ code: "P2010" });

    await expect(
      firstClient.job.findUniqueOrThrow({ where: { id: jobId } }),
    ).resolves.toMatchObject({
      status: JobStatus.PENDING,
      leaseToken: null,
      leaseExpiresAt: null,
    });
  });
});
