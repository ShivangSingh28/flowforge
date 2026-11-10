import type { Job, Prisma } from "@prisma/client";
import { HttpError } from "../utils/http-error";
import type { JobSubmissionInput } from "../types/job-submission";

interface JobRepositoryPort {
  createSubmission(input: JobSubmissionInput): Promise<Job>;
  findByIdempotencyKey(idempotencyKey: string): Promise<Job | null>;
  isUniqueConstraintError(error: unknown): boolean;
}

export interface JobSubmissionResult {
  job: Pick<Job, "id" | "status">;
  created: boolean;
}

const canonicalize = (value: Prisma.JsonValue | undefined): string => {
  if (value === undefined) {
    return "undefined";
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalize(item)).join(",")}]`;
  }

  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => `${JSON.stringify(key)}:${canonicalize(item)}`);
    return `{${entries.join(",")}}`;
  }

  return JSON.stringify(value) ?? "null";
};

const isSameSubmission = (job: Job, input: JobSubmissionInput): boolean =>
  job.type === input.type &&
  job.priority === input.priority &&
  job.maxAttempts === input.maxAttempts &&
  canonicalize(job.payload) === canonicalize(input.payload as Prisma.JsonValue);

export class JobSubmissionService {
  constructor(private readonly repository: JobRepositoryPort) {}

  async submit(input: JobSubmissionInput): Promise<JobSubmissionResult> {
    if (input.idempotencyKey) {
      const existing = await this.repository.findByIdempotencyKey(
        input.idempotencyKey,
      );
      if (existing) {
        return this.existingResult(existing, input);
      }
    }

    let job: Job;
    try {
      job = await this.repository.createSubmission(input);
    } catch (error: unknown) {
      if (
        !input.idempotencyKey ||
        !this.repository.isUniqueConstraintError(error)
      ) {
        throw error;
      }

      const existing = await this.repository.findByIdempotencyKey(
        input.idempotencyKey,
      );
      if (!existing) {
        throw error;
      }

      return this.existingResult(existing, input);
    }

    return { job: { id: job.id, status: job.status }, created: true };
  }

  private existingResult(
    job: Job,
    input: JobSubmissionInput,
  ): JobSubmissionResult {
    if (!isSameSubmission(job, input)) {
      throw new HttpError(
        409,
        "Idempotency key was already used with different job data",
      );
    }

    return { job: { id: job.id, status: job.status }, created: false };
  }
}
