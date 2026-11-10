import { JobPriority, JobStatus, type Job } from "@prisma/client";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import type { JobMessage } from "../src/types/job-message";
import type { JobSubmissionInput } from "../src/types/job-submission";
import { HttpError } from "../src/utils/http-error";
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
  const create = jest
    .fn<(submission: JobSubmissionInput) => Promise<Job>>()
    .mockResolvedValue(makeJob());
  const findByIdempotencyKey = jest
    .fn<(key: string) => Promise<Job | null>>()
    .mockResolvedValue(null);
  const isUniqueConstraintError = jest
    .fn<(error: unknown) => boolean>()
    .mockReturnValue(false);
  const publish = jest
    .fn<(message: JobMessage) => Promise<void>>()
    .mockResolvedValue(undefined);
  const service = new JobSubmissionService(
    { create, findByIdempotencyKey, isUniqueConstraintError },
    { publish },
  );

  return {
    service,
    create,
    findByIdempotencyKey,
    isUniqueConstraintError,
    publish,
  };
};

describe("JobSubmissionService", () => {
  beforeEach(() => {
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("persists a pending job and publishes its typed message", async () => {
    const { service, create, publish } = createHarness();

    await expect(service.submit(input)).resolves.toEqual({
      job: {
        id: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
        status: JobStatus.PENDING,
      },
      created: true,
    });

    expect(create).toHaveBeenCalledWith(input);
    expect(publish).toHaveBeenCalledWith({
      jobId: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
      type: input.type,
      payload: input.payload,
      priority: input.priority,
      createdAt: "2026-10-31T12:00:00.000Z",
    });
  });

  it("does not publish when database persistence fails", async () => {
    const { service, create, publish } = createHarness();
    create.mockRejectedValue(new Error("Database unavailable"));

    await expect(service.submit(input)).rejects.toThrow("Database unavailable");
    expect(publish).not.toHaveBeenCalled();
  });

  it("keeps the database job pending and returns 503 when publishing fails", async () => {
    const { service, create, publish } = createHarness();
    publish.mockRejectedValue(new Error("Broker unavailable"));
    const log = jest.spyOn(console, "error").mockImplementation(() => {});

    const error = await service
      .submit(input)
      .catch((caught: unknown) => caught);

    expect(create).toHaveBeenCalledTimes(1);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(error).toBeInstanceOf(HttpError);
    expect(error).toMatchObject({
      statusCode: 503,
      message: "Job was saved but could not be published; it remains pending",
    });
    expect(log).toHaveBeenCalledWith(
      JSON.stringify({
        event: "jobs.publish_failed",
        jobId: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
        errorName: "Error",
      }),
    );
    expect(log.mock.calls.flat().join(" ")).not.toContain("test@example.com");
  });

  it("returns an identical idempotent job without creating or publishing again", async () => {
    const existing = makeJob({
      payload: { metadata: { source: "api" }, recipient: "test@example.com" },
    });
    const { service, create, findByIdempotencyKey, publish } = createHarness();
    findByIdempotencyKey.mockResolvedValue(existing);

    await expect(service.submit(input)).resolves.toEqual({
      job: { id: existing.id, status: existing.status },
      created: false,
    });

    expect(findByIdempotencyKey).toHaveBeenCalledWith(input.idempotencyKey);
    expect(create).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
  });

  it("rejects a repeated idempotency key with conflicting submission data", async () => {
    const { service, findByIdempotencyKey, publish } = createHarness();
    findByIdempotencyKey.mockResolvedValue(makeJob());

    await expect(
      service.submit({ ...input, type: "email.delete" }),
    ).rejects.toMatchObject({
      statusCode: 409,
      message: "Idempotency key was already used with different job data",
    });

    expect(publish).not.toHaveBeenCalled();
  });

  it("handles a unique-key race without publishing the duplicate", async () => {
    const existing = makeJob();
    const uniqueError = new Error("Unique constraint");
    const {
      service,
      create,
      findByIdempotencyKey,
      isUniqueConstraintError,
      publish,
    } = createHarness();
    findByIdempotencyKey
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(existing);
    create.mockRejectedValue(uniqueError);
    isUniqueConstraintError.mockReturnValue(true);

    await expect(service.submit(input)).resolves.toEqual({
      job: { id: existing.id, status: existing.status },
      created: false,
    });

    expect(create).toHaveBeenCalledTimes(1);
    expect(publish).not.toHaveBeenCalled();
  });
});
