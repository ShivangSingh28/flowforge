import { JobStatus } from "@prisma/client";
import type { Job } from "@prisma/client";
import { env } from "../config/env";
import type {
  ClaimedJob,
  ClaimJobOptions,
  FailureMutationResult,
  LeaseMutationResult,
} from "../repositories/job-execution.repository";
import type { JobMessage } from "../types/job-message";
import { abortableDelay } from "../utils/abortable-delay";

export type JobExecutionDisposition = "ack" | "defer";
export type JobHandler = (job: Job, signal?: AbortSignal) => Promise<unknown>;
export type WorkerEnvironment = "development" | "test" | "production";

interface JobExecutionRepositoryPort {
  findById(id: string): Promise<Job | null>;
  claimAvailable(
    id: string,
    options: ClaimJobOptions,
  ): Promise<ClaimedJob | null>;
  renewLease(
    jobId: string,
    leaseToken: string,
    leaseDurationMs: number,
  ): Promise<LeaseMutationResult>;
  markCompleted(
    jobId: string,
    attemptId: string,
    leaseToken: string,
  ): Promise<LeaseMutationResult>;
  markFailed(
    jobId: string,
    attemptId: string,
    leaseToken: string,
    safeError: string,
    retryable?: boolean,
    retryDelayMs?: number,
  ): Promise<FailureMutationResult>;
}

type ClaimedExecutionResult =
  "terminal" | "retry_scheduled" | "ownership_lost" | "cancelled";

export class TerminalJobError extends Error {}

const demoHandler: JobHandler = async (job) => {
  const payloadFields =
    typeof job.payload === "object" &&
    job.payload !== null &&
    !Array.isArray(job.payload)
      ? Object.keys(job.payload).length
      : 0;

  return { jobId: job.id, payloadFields };
};

export const createCrashTestHandler = (durationMs: number): JobHandler => {
  return async (job, signal) => {
    if (signal?.aborted) {
      console.info(
        JSON.stringify({
          event: "worker.crash_test_cancelled",
          jobId: job.id,
        }),
      );
      return;
    }

    console.info(
      JSON.stringify({ event: "worker.crash_test_started", jobId: job.id }),
    );
    const completed = await abortableDelay(durationMs, signal);
    console.info(
      JSON.stringify({
        event: completed
          ? "worker.crash_test_completed"
          : "worker.crash_test_cancelled",
        jobId: job.id,
      }),
    );
  };
};

export const createJobHandlers = (
  nodeEnvironment: WorkerEnvironment,
  crashTestDurationMs: number,
): Map<string, JobHandler> => {
  const handlers = new Map<string, JobHandler>([["demo", demoHandler]]);
  if (nodeEnvironment !== "production") {
    handlers.set("crash-test", createCrashTestHandler(crashTestDurationMs));
  }
  return handlers;
};

export const jobHandlers = createJobHandlers(
  env.NODE_ENV,
  env.WORKER_CRASH_TEST_DURATION_MS,
);

const isTerminal = (status: JobStatus): boolean =>
  status === JobStatus.COMPLETED ||
  status === JobStatus.FAILED ||
  status === JobStatus.CANCELLED;

export class JobExecutionService {
  constructor(
    private readonly repository: JobExecutionRepositoryPort,
    private readonly handlers: ReadonlyMap<string, JobHandler> = jobHandlers,
    private readonly leaseDurationMs = env.WORKER_LEASE_DURATION_MS,
    private readonly heartbeatIntervalMs = env.WORKER_HEARTBEAT_INTERVAL_MS,
    private readonly nodeEnvironment: WorkerEnvironment = env.NODE_ENV,
    private readonly retryBaseDelayMs = env.JOB_RETRY_BASE_DELAY_MS,
    private readonly retryMaxDelayMs = env.JOB_RETRY_MAX_DELAY_MS,
  ) {}

  async execute(
    message: JobMessage,
    signal?: AbortSignal,
  ): Promise<JobExecutionDisposition> {
    while (!signal?.aborted) {
      const existing = await this.repository.findById(message.jobId);
      if (signal?.aborted) {
        return "defer";
      }
      if (!existing) {
        console.info(
          JSON.stringify({ event: "worker.job_missing", jobId: message.jobId }),
        );
        return "ack";
      }

      if (isTerminal(existing.status)) {
        console.info(
          JSON.stringify({
            event: "worker.job_terminal",
            jobId: message.jobId,
            status: existing.status,
          }),
        );
        return "ack";
      }

      const claimed = await this.repository.claimAvailable(message.jobId, {
        leaseDurationMs: this.leaseDurationMs,
      });

      if (signal?.aborted) {
        return "defer";
      }
      if (claimed) {
        const result = await this.runClaimedJob(claimed, signal);
        if (result === "terminal") {
          return "ack";
        }
        if (result === "retry_scheduled") {
          continue;
        }
        if (result === "cancelled") {
          return "defer";
        }
      }

      const current = await this.repository.findById(message.jobId);
      if (signal?.aborted) {
        return "defer";
      }
      if (!current) {
        return "ack";
      }
      if (isTerminal(current.status)) {
        return "ack";
      }

      const untilExpiry =
        current.status === JobStatus.PROCESSING && current.leaseExpiresAt
          ? Math.max(1, current.leaseExpiresAt.getTime() - Date.now())
          : current.status === JobStatus.PENDING && current.scheduledAt
            ? Math.max(1, current.scheduledAt.getTime() - Date.now())
            : this.heartbeatIntervalMs;
      const shouldContinue = await abortableDelay(
        Math.min(this.heartbeatIntervalMs, untilExpiry),
        signal,
      );
      if (!shouldContinue) {
        return "defer";
      }
    }

    return "defer";
  }

  private async runClaimedJob(
    claimed: ClaimedJob,
    signal?: AbortSignal,
  ): Promise<ClaimedExecutionResult> {
    const handler =
      this.nodeEnvironment === "production" && claimed.job.type === "crash-test"
        ? undefined
        : this.handlers.get(claimed.job.type);
    const handlerController = new AbortController();
    const heartbeatController = new AbortController();
    let ownershipLost = false;
    let heartbeatResult: "ownership_lost" | "cancelled" | undefined;
    let resolveHeartbeat: (
      result: "ownership_lost" | "cancelled",
    ) => void = () => {};
    const heartbeatOutcome = new Promise<"ownership_lost" | "cancelled">(
      (resolve) => {
        resolveHeartbeat = resolve;
      },
    );

    const abortForShutdown = (): void => {
      handlerController.abort();
      heartbeatController.abort();
      heartbeatResult = "cancelled";
      resolveHeartbeat("cancelled");
    };
    signal?.addEventListener("abort", abortForShutdown, { once: true });
    if (signal?.aborted) {
      abortForShutdown();
    }

    const heartbeatTask = (async (): Promise<void> => {
      while (!heartbeatController.signal.aborted) {
        const shouldContinue = await abortableDelay(
          this.heartbeatIntervalMs,
          heartbeatController.signal,
        );
        if (!shouldContinue) {
          return;
        }

        try {
          const renewed = await this.repository.renewLease(
            claimed.job.id,
            claimed.leaseToken,
            this.leaseDurationMs,
          );
          if (renewed === "updated") {
            continue;
          }
        } catch (error: unknown) {
          console.error(
            JSON.stringify({
              event: "worker.lease_heartbeat_failed",
              jobId: claimed.job.id,
              errorName: error instanceof Error ? error.name : "UnknownError",
            }),
          );
          continue;
        }

        ownershipLost = true;
        heartbeatResult = "ownership_lost";
        handlerController.abort();
        resolveHeartbeat("ownership_lost");
        return;
      }
    })();

    const work = Promise.resolve()
      .then(async () => {
        if (handlerController.signal.aborted || signal?.aborted) {
          return { cancelled: true as const };
        }
        if (!handler) {
          throw new TerminalJobError("No handler registered");
        }
        const result = await handler(claimed.job, handlerController.signal);
        return { cancelled: false as const, succeeded: true as const, result };
      })
      .then(
        (outcome) => outcome,
        (error: unknown) => ({
          cancelled: false as const,
          succeeded: false as const,
          error,
        }),
      );

    try {
      const outcome = await Promise.race([
        work,
        heartbeatOutcome.then((result) => ({ heartbeat: result })),
      ]);
      heartbeatController.abort();
      await heartbeatTask;
      signal?.removeEventListener("abort", abortForShutdown);

      if ("heartbeat" in outcome) {
        return outcome.heartbeat;
      }
      if (signal?.aborted || heartbeatResult === "cancelled") {
        return "cancelled";
      }
      if (ownershipLost || heartbeatResult === "ownership_lost") {
        return "ownership_lost";
      }

      if (outcome.cancelled || signal?.aborted) {
        return "cancelled";
      }

      if (!outcome.succeeded) {
        if (signal?.aborted) {
          return "cancelled";
        }
        const persisted = await this.repository.markFailed(
          claimed.job.id,
          claimed.attempt.id,
          claimed.leaseToken,
          "Job execution failed",
          !(outcome.error instanceof TerminalJobError),
          Math.min(
            this.retryMaxDelayMs,
            this.retryBaseDelayMs *
              2 ** Math.min(Math.max(claimed.job.attempts - 1, 0), 30),
          ),
        );
        if (persisted !== "updated") {
          if (persisted === "retry_scheduled") {
            console.warn(
              JSON.stringify({
                event: "worker.job_retry_scheduled",
                jobId: claimed.job.id,
                attemptId: claimed.attempt.id,
                attempt: claimed.job.attempts,
                retryDelayMs: Math.min(
                  this.retryMaxDelayMs,
                  this.retryBaseDelayMs *
                    2 ** Math.min(Math.max(claimed.job.attempts - 1, 0), 30),
                ),
              }),
            );
            return "retry_scheduled";
          }
          return "ownership_lost";
        }
        console.error(
          JSON.stringify({
            event: "worker.job_failed",
            jobId: claimed.job.id,
            attemptId: claimed.attempt.id,
            errorName:
              outcome.error instanceof Error
                ? outcome.error.name
                : "UnknownError",
          }),
        );
        return "terminal";
      }

      if (signal?.aborted) {
        return "cancelled";
      }
      const persisted = await this.repository.markCompleted(
        claimed.job.id,
        claimed.attempt.id,
        claimed.leaseToken,
      );
      if (persisted !== "updated") {
        return "ownership_lost";
      }
      console.info(
        JSON.stringify({
          event: "worker.job_completed",
          jobId: claimed.job.id,
          attemptId: claimed.attempt.id,
        }),
      );
      return "terminal";
    } finally {
      heartbeatController.abort();
      handlerController.abort();
      signal?.removeEventListener("abort", abortForShutdown);
      await heartbeatTask;
    }
  }
}
