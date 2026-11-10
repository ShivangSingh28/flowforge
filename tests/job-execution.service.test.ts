import {
  JobAttemptStatus,
  JobPriority,
  JobStatus,
  type Job,
} from "@prisma/client";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import type {
  ClaimedJob,
  FailureMutationResult,
  LeaseMutationResult,
} from "../src/repositories/job-execution.repository";
import type { JobMessage } from "../src/types/job-message";
import {
  JobExecutionService,
  createCrashTestHandler,
  createJobHandlers,
  type JobHandler,
} from "../src/services/job-execution.service";

const message: JobMessage = {
  jobId: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
  type: "demo",
  payload: { value: "private" },
  priority: "MEDIUM",
  createdAt: "2026-10-31T12:00:00.000Z",
};

const makeJob = (
  status: JobStatus = JobStatus.PENDING,
  overrides: Partial<Job> = {},
): Job => ({
  id: message.jobId,
  type: message.type,
  payload: message.payload,
  status,
  priority: JobPriority.MEDIUM,
  attempts: 0,
  maxAttempts: 3,
  idempotencyKey: null,
  leaseToken: status === JobStatus.PROCESSING ? claimed.leaseToken : null,
  leaseExpiresAt:
    status === JobStatus.PROCESSING ? new Date(Date.now() + 30000) : null,
  scheduledAt: null,
  startedAt: null,
  completedAt: null,
  failedAt: null,
  lastError: null,
  createdAt: new Date(message.createdAt),
  updatedAt: new Date(message.createdAt),
  ...overrides,
});

const claimed: ClaimedJob = {
  job: {
    id: message.jobId,
    type: message.type,
    payload: message.payload,
    status: JobStatus.PROCESSING,
    priority: JobPriority.MEDIUM,
    attempts: 1,
    maxAttempts: 3,
    idempotencyKey: null,
    leaseToken: "d23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
    leaseExpiresAt: new Date(Date.now() + 30000),
    scheduledAt: null,
    startedAt: new Date(message.createdAt),
    completedAt: null,
    failedAt: null,
    lastError: null,
    createdAt: new Date(message.createdAt),
    updatedAt: new Date(message.createdAt),
  },
  leaseToken: "d23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
  attempt: {
    id: "c23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
    jobId: message.jobId,
    attemptNumber: 1,
    status: JobAttemptStatus.PROCESSING,
    workerId: null,
    startedAt: new Date(message.createdAt),
    finishedAt: null,
    error: null,
    createdAt: new Date(message.createdAt),
  },
};

const createHarness = (
  status: JobStatus = JobStatus.PENDING,
  handler?: JobHandler,
  handlers?: ReadonlyMap<string, JobHandler>,
  jobType = message.type,
  nodeEnvironment: "development" | "test" | "production" = "test",
) => {
  const findById = jest
    .fn<(id: string) => Promise<Job | null>>()
    .mockResolvedValue(makeJob(status, { type: jobType }));
  const claimAvailable = jest
    .fn<
      (
        id: string,
        options: { leaseDurationMs: number },
      ) => Promise<ClaimedJob | null>
    >()
    .mockResolvedValue({
      ...claimed,
      job: { ...claimed.job, type: jobType },
    });
  const renewLease = jest
    .fn<
      (
        jobId: string,
        leaseToken: string,
        leaseDurationMs: number,
      ) => Promise<LeaseMutationResult>
    >()
    .mockResolvedValue("updated");
  const markCompleted = jest
    .fn<
      (
        jobId: string,
        attemptId: string,
        leaseToken: string,
      ) => Promise<LeaseMutationResult>
    >()
    .mockResolvedValue("updated");
  const markFailed = jest
    .fn<
      (
        jobId: string,
        attemptId: string,
        leaseToken: string,
        safeError: string,
      ) => Promise<FailureMutationResult>
    >()
    .mockResolvedValue("updated");
  const repository = {
    findById,
    claimAvailable,
    renewLease,
    markCompleted,
    markFailed,
  };
  const service = new JobExecutionService(
    repository,
    handlers ?? (handler ? new Map([[message.type, handler]]) : undefined),
    30,
    10,
    nodeEnvironment,
  );

  return {
    service,
    findById,
    claimAvailable,
    renewLease,
    markCompleted,
    markFailed,
  };
};

describe("JobExecutionService", () => {
  beforeEach(() => {
    jest.spyOn(console, "info").mockImplementation(() => {});
    jest.spyOn(console, "error").mockImplementation(() => {});
    jest.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });

  it("waits for the configured crash-test handler duration", async () => {
    jest.useFakeTimers();
    const handler = createCrashTestHandler(1000);
    let completed = false;
    const execution = handler(claimed.job).then(() => {
      completed = true;
    });

    await Promise.resolve();
    expect(completed).toBe(false);
    await jest.advanceTimersByTimeAsync(999);
    expect(completed).toBe(false);
    await jest.advanceTimersByTimeAsync(1);
    await execution;

    expect(completed).toBe(true);
    expect(console.info).toHaveBeenCalledWith(
      JSON.stringify({
        event: "worker.crash_test_completed",
        jobId: claimed.job.id,
      }),
    );
  });

  it("cancels the crash-test handler using AbortSignal", async () => {
    jest.useFakeTimers();
    const controller = new AbortController();
    const handler = createCrashTestHandler(60000);
    const execution = handler(claimed.job, controller.signal);

    await Promise.resolve();
    controller.abort();
    await execution;

    expect(jest.getTimerCount()).toBe(0);
    expect(console.info).toHaveBeenCalledWith(
      JSON.stringify({
        event: "worker.crash_test_cancelled",
        jobId: claimed.job.id,
      }),
    );
    expect(console.info).not.toHaveBeenCalledWith(
      JSON.stringify({
        event: "worker.crash_test_completed",
        jobId: claimed.job.id,
      }),
    );
  });

  it("does not execute crash-test handlers in production", async () => {
    const crashTest = jest.fn<JobHandler>().mockResolvedValue(undefined);
    const handlers = new Map<string, JobHandler>([["crash-test", crashTest]]);
    const { service, markFailed } = createHarness(
      JobStatus.PENDING,
      undefined,
      handlers,
      "crash-test",
      "production",
    );

    await expect(
      service.execute({ ...message, type: "crash-test" }),
    ).resolves.toBe("ack");

    expect(createJobHandlers("production", 60000).has("crash-test")).toBe(
      false,
    );
    expect(crashTest).not.toHaveBeenCalled();
    expect(markFailed).toHaveBeenCalledWith(
      message.jobId,
      claimed.attempt.id,
      claimed.leaseToken,
      "Job execution failed",
      false,
      1000,
    );
  });

  it("claims a pending job and persists completion before returning ACK", async () => {
    const calls: string[] = [];
    const { service, claimAvailable, markCompleted } = createHarness(
      JobStatus.PENDING,
      async (job) => {
        calls.push("handler");
        return { jobId: job.id };
      },
    );
    markCompleted.mockImplementation(async () => {
      calls.push("completed");
      return "updated";
    });

    await expect(service.execute(message)).resolves.toBe("ack");
    expect(calls).toEqual(["handler", "completed"]);
    expect(claimAvailable).toHaveBeenCalledWith(message.jobId, {
      leaseDurationMs: 30,
    });
    expect(markCompleted).toHaveBeenCalledWith(
      message.jobId,
      claimed.attempt.id,
      claimed.leaseToken,
    );
  });

  it("renews the lease while a handler is executing", async () => {
    jest.useFakeTimers();
    let finishHandler: (() => void) | undefined;
    const { service, renewLease } = createHarness(
      JobStatus.PENDING,
      () =>
        new Promise((resolve) => {
          finishHandler = () => resolve(undefined);
        }),
    );
    const execution = service.execute(message);

    await Promise.resolve();
    await Promise.resolve();
    await jest.advanceTimersByTimeAsync(10);
    expect(renewLease).toHaveBeenCalledWith(
      message.jobId,
      claimed.leaseToken,
      30,
    );

    finishHandler?.();
    await expect(execution).resolves.toBe("ack");
  });

  it("retries a transient heartbeat error without aborting the handler", async () => {
    jest.useFakeTimers();
    let finishHandler: (() => void) | undefined;
    let handlerSignal: AbortSignal | undefined;
    const { service, renewLease, markCompleted } = createHarness(
      JobStatus.PENDING,
      (_job, signal) =>
        new Promise((resolve) => {
          handlerSignal = signal;
          finishHandler = () => resolve(undefined);
        }),
    );
    renewLease
      .mockRejectedValueOnce(new Error("Database temporarily unavailable"))
      .mockResolvedValueOnce("updated");

    const execution = service.execute(message);

    await Promise.resolve();
    await Promise.resolve();
    await jest.advanceTimersByTimeAsync(10);
    expect(renewLease).toHaveBeenCalledTimes(1);
    expect(handlerSignal?.aborted).toBe(false);

    await jest.advanceTimersByTimeAsync(10);
    expect(renewLease).toHaveBeenCalledTimes(2);
    expect(handlerSignal?.aborted).toBe(false);

    finishHandler?.();
    await expect(execution).resolves.toBe("ack");
    expect(markCompleted).toHaveBeenCalledWith(
      message.jobId,
      claimed.attempt.id,
      claimed.leaseToken,
    );
  });

  it("stops a handler when its heartbeat discovers another lease owner", async () => {
    jest.useFakeTimers();
    let handlerSignal: AbortSignal | undefined;
    const { service, findById, renewLease, markCompleted } = createHarness(
      JobStatus.PENDING,
      (_job, signal) => {
        handlerSignal = signal;
        return new Promise(() => {});
      },
    );
    renewLease.mockResolvedValue("lost_lease");
    findById
      .mockResolvedValueOnce(makeJob(JobStatus.PENDING))
      .mockResolvedValueOnce(makeJob(JobStatus.COMPLETED));
    const execution = service.execute(message);

    await Promise.resolve();
    await Promise.resolve();
    await jest.advanceTimersByTimeAsync(10);

    await expect(execution).resolves.toBe("ack");
    expect(handlerSignal?.aborted).toBe(true);
    expect(markCompleted).not.toHaveBeenCalled();
  });

  it("does not start a handler when cancellation occurs during an async claim", async () => {
    let finishClaim: ((job: ClaimedJob) => void) | undefined;
    const sideEffect = jest.fn<() => Promise<void>>().mockResolvedValue();
    const { service, claimAvailable } = createHarness(
      JobStatus.PENDING,
      async () => sideEffect(),
    );
    claimAvailable.mockImplementation(
      () =>
        new Promise((resolve) => {
          finishClaim = resolve;
        }),
    );
    const controller = new AbortController();
    const execution = service.execute(message, controller.signal);

    await Promise.resolve();
    await Promise.resolve();
    expect(claimAvailable).toHaveBeenCalledTimes(1);
    controller.abort();
    finishClaim?.(claimed);

    await expect(execution).resolves.toBe("defer");
    expect(sideEffect).not.toHaveBeenCalled();
  });

  it("checks cancellation immediately before invoking the handler", async () => {
    const controller = new AbortController();
    const sideEffect = jest.fn<() => Promise<void>>().mockResolvedValue();
    const handlers = new Map<string, JobHandler>([
      [message.type, async () => sideEffect()],
    ]);
    const getHandler = handlers.get.bind(handlers);
    jest.spyOn(handlers, "get").mockImplementation((type) => {
      controller.abort();
      return getHandler(type);
    });
    const { service } = createHarness(JobStatus.PENDING, undefined, handlers);

    await expect(service.execute(message, controller.signal)).resolves.toBe(
      "defer",
    );
    expect(sideEffect).not.toHaveBeenCalled();
  });

  it("does not persist completion or failure after losing the lease", async () => {
    const completedHarness = createHarness(
      JobStatus.PENDING,
      async () => "stale result",
    );
    completedHarness.markCompleted.mockResolvedValue("lost_lease");
    completedHarness.findById
      .mockResolvedValueOnce(makeJob(JobStatus.PENDING))
      .mockResolvedValueOnce(makeJob(JobStatus.COMPLETED));

    await expect(completedHarness.service.execute(message)).resolves.toBe(
      "ack",
    );
    expect(completedHarness.markCompleted).toHaveBeenCalledTimes(1);

    const failedHarness = createHarness(JobStatus.PENDING, async () => {
      throw new Error("stale failure");
    });
    failedHarness.markFailed.mockResolvedValue("lost_lease");
    failedHarness.findById
      .mockResolvedValueOnce(makeJob(JobStatus.PENDING))
      .mockResolvedValueOnce(makeJob(JobStatus.FAILED));

    await expect(failedHarness.service.execute(message)).resolves.toBe("ack");
    expect(failedHarness.markFailed).toHaveBeenCalledTimes(1);
  });

  it("persists terminal failure and ACKs only after it is durable", async () => {
    const { service, markFailed } = createHarness(
      JobStatus.PENDING,
      async () => {
        throw new Error("sensitive value");
      },
    );

    await expect(service.execute(message)).resolves.toBe("ack");
    expect(markFailed).toHaveBeenCalledWith(
      message.jobId,
      claimed.attempt.id,
      claimed.leaseToken,
      "Job execution failed",
      true,
      1000,
    );
  });

  it("continues after durably scheduling a retry", async () => {
    const { service, findById, markFailed } = createHarness(
      JobStatus.PENDING,
      async () => {
        throw new Error("temporary failure");
      },
    );
    findById
      .mockResolvedValueOnce(makeJob(JobStatus.PENDING))
      .mockResolvedValueOnce(makeJob(JobStatus.COMPLETED));
    markFailed.mockResolvedValueOnce("retry_scheduled");

    await expect(service.execute(message)).resolves.toBe("ack");
    expect(markFailed).toHaveBeenCalledWith(
      message.jobId,
      claimed.attempt.id,
      claimed.leaseToken,
      "Job execution failed",
      true,
      1000,
    );
  });

  it("does not log handler results or sensitive payload data", async () => {
    const info = jest.spyOn(console, "info").mockImplementation(() => {});
    const { service } = createHarness(JobStatus.PENDING, async () => ({
      secret: "sensitive handler result",
    }));

    await service.execute(message);

    const logs = info.mock.calls.flat().join(" ");
    expect(logs).not.toContain("sensitive");
    expect(logs).not.toContain(claimed.leaseToken);
  });

  it("ACKs missing and terminal jobs without claiming them", async () => {
    const missing = createHarness();
    missing.findById.mockResolvedValue(null);
    await expect(missing.service.execute(message)).resolves.toBe("ack");
    expect(missing.claimAvailable).not.toHaveBeenCalled();

    for (const status of [
      JobStatus.COMPLETED,
      JobStatus.FAILED,
      JobStatus.CANCELLED,
    ]) {
      const terminal = createHarness(status);
      await expect(terminal.service.execute(message)).resolves.toBe("ack");
      expect(terminal.claimAvailable).not.toHaveBeenCalled();
    }
  });

  it("waits instead of ACKing a redelivered message with an active lease", async () => {
    const controller = new AbortController();
    const { service, claimAvailable } = createHarness(JobStatus.PROCESSING);
    claimAvailable.mockResolvedValue(null);
    const execution = service.execute(message, controller.signal);

    await Promise.resolve();
    await Promise.resolve();
    controller.abort();

    await expect(execution).resolves.toBe("defer");
    expect(claimAvailable).toHaveBeenCalledTimes(1);
  });

  it("continues observing when the active lease is renewed", async () => {
    jest.useFakeTimers();
    const activeJob = makeJob(JobStatus.PROCESSING, {
      leaseExpiresAt: new Date(Date.now() + 5),
    });
    const renewedJob = makeJob(JobStatus.PROCESSING, {
      leaseExpiresAt: new Date(Date.now() + 100),
    });
    const { service, findById, claimAvailable } = createHarness(
      JobStatus.PROCESSING,
    );
    findById.mockResolvedValueOnce(activeJob).mockResolvedValue(renewedJob);
    claimAvailable.mockResolvedValue(null);
    const controller = new AbortController();
    const execution = service.execute(message, controller.signal);

    await Promise.resolve();
    await Promise.resolve();
    await jest.advanceTimersByTimeAsync(10);
    expect(claimAvailable.mock.calls.length).toBeGreaterThan(1);

    controller.abort();
    await expect(execution).resolves.toBe("defer");
  });

  it("reclaims an expired processing job and ACKs when it completes", async () => {
    const expired = makeJob(JobStatus.PROCESSING, {
      leaseExpiresAt: new Date(Date.now() - 1),
    });
    const { service, findById, claimAvailable } = createHarness(
      JobStatus.PROCESSING,
    );
    findById.mockResolvedValue(expired);

    await expect(service.execute(message)).resolves.toBe("ack");
    expect(claimAvailable).toHaveBeenCalledTimes(1);
  });

  it("ACKs a redelivery once another worker makes the job terminal", async () => {
    const { service, findById, claimAvailable } = createHarness(
      JobStatus.PROCESSING,
    );
    claimAvailable.mockResolvedValue(null);
    findById
      .mockResolvedValueOnce(makeJob(JobStatus.PROCESSING))
      .mockResolvedValueOnce(makeJob(JobStatus.COMPLETED))
      .mockResolvedValueOnce(makeJob(JobStatus.COMPLETED));

    await expect(service.execute(message)).resolves.toBe("ack");
    expect(claimAvailable).toHaveBeenCalledTimes(1);
  });

  it("cancels a waiting delivery during graceful shutdown without ACKing it", async () => {
    const controller = new AbortController();
    const { service, claimAvailable } = createHarness(JobStatus.PROCESSING);
    claimAvailable.mockResolvedValue(null);
    const execution = service.execute(message, controller.signal);

    await Promise.resolve();
    await Promise.resolve();
    controller.abort();

    await expect(execution).resolves.toBe("defer");
  });

  it("cancels an active handler and its heartbeat on shutdown", async () => {
    jest.useFakeTimers();
    let handlerSignal: AbortSignal | undefined;
    let markHandlerStarted: (() => void) | undefined;
    const handlerStarted = new Promise<void>((resolve) => {
      markHandlerStarted = resolve;
    });
    const { service, renewLease } = createHarness(
      JobStatus.PENDING,
      (_job, signal) => {
        handlerSignal = signal;
        markHandlerStarted?.();
        return new Promise(() => {});
      },
    );
    const controller = new AbortController();
    const execution = service.execute(message, controller.signal);

    await handlerStarted;
    controller.abort();

    await expect(execution).resolves.toBe("defer");
    expect(handlerSignal?.aborted).toBe(true);
    await jest.advanceTimersByTimeAsync(100);
    expect(renewLease).not.toHaveBeenCalled();
  });

  it("propagates database failures so the consumer cannot ACK", async () => {
    const { service, markCompleted } = createHarness();
    markCompleted.mockRejectedValue(new Error("Database unavailable"));

    await expect(service.execute(message)).rejects.toThrow(
      "Database unavailable",
    );
  });
});
