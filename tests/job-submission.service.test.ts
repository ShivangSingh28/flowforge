import { JobPriority, JobStatus, type Job } from "@prisma/client";
import { describe, expect, it, jest } from "@jest/globals";
import type { JobSubmissionInput } from "../src/types/job-submission";
import { JobSubmissionService } from "../src/services/job-submission.service";

const input: JobSubmissionInput = {
  type: "email.send",
  payload: { recipient: "test@example.com", metadata: { source: "api" } },
  priority: JobPriority.MEDIUM,
  maxAttempts: 3,
  idempotencyKey: "request-123",
};

const makeJob = (overrides: Partial<Job> = {}): Job => ({
  id: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
  type: input.type,
  payload: input.payload,
  status: JobStatus.PENDING,
  priority: input.priority,
  attempts: 0,
  maxAttempts: input.maxAttempts,
  idempotencyKey: input.idempotencyKey ?? null,
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

const createHarness = () => {
  const createSubmission = jest
    .fn<(submission: JobSubmissionInput) => Promise<Job>>()
    .mockResolvedValue(makeJob());
  const findByIdempotencyKey = jest
    .fn<(key: string) => Promise<Job | null>>()
    .mockResolvedValue(null);
  const isUniqueConstraintError = jest
    .fn<(error: unknown) => boolean>()
    .mockReturnValue(false);
  const service = new JobSubmissionService({
    createSubmission,
    findByIdempotencyKey,
    isUniqueConstraintError,
  });

  return {
    service,
    createSubmission,
    findByIdempotencyKey,
    isUniqueConstraintError,
  };
};

describe("JobSubmissionService", () => {
  it("returns the job after its atomic submission persistence succeeds", async () => {
    const { service, createSubmission } = createHarness();

    await expect(service.submit(input)).resolves.toEqual({
      job: {
        id: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
        status: JobStatus.PENDING,
      },
      created: true,
    });

    expect(createSubmission).toHaveBeenCalledWith(input);
  });

  it("does not return a job when atomic persistence fails", async () => {
    const { service, createSubmission } = createHarness();
    createSubmission.mockRejectedValue(new Error("Database unavailable"));

    await expect(service.submit(input)).rejects.toThrow("Database unavailable");
  });

  it("returns an identical idempotent job without creating or publishing again", async () => {
    const existing = makeJob({
      payload: { metadata: { source: "api" }, recipient: "test@example.com" },
    });
    const { service, createSubmission, findByIdempotencyKey } = createHarness();
    findByIdempotencyKey.mockResolvedValue(existing);

    await expect(service.submit(input)).resolves.toEqual({
      job: { id: existing.id, status: existing.status },
      created: false,
    });

    expect(findByIdempotencyKey).toHaveBeenCalledWith(input.idempotencyKey);
    expect(createSubmission).not.toHaveBeenCalled();
  });

  it("rejects a repeated idempotency key with conflicting submission data", async () => {
    const { service, findByIdempotencyKey } = createHarness();
    findByIdempotencyKey.mockResolvedValue(makeJob());

    await expect(
      service.submit({ ...input, type: "email.delete" }),
    ).rejects.toMatchObject({
      statusCode: 409,
      message: "Idempotency key was already used with different job data",
    });
  });

  it("handles a unique-key race without publishing the duplicate", async () => {
    const existing = makeJob();
    const uniqueError = new Error("Unique constraint");
    const {
      service,
      createSubmission,
      findByIdempotencyKey,
      isUniqueConstraintError,
    } = createHarness();
    findByIdempotencyKey
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(existing);
    createSubmission.mockRejectedValue(uniqueError);
    isUniqueConstraintError.mockReturnValue(true);

    await expect(service.submit(input)).resolves.toEqual({
      job: { id: existing.id, status: existing.status },
      created: false,
    });

    expect(createSubmission).toHaveBeenCalledTimes(1);
  });
});
