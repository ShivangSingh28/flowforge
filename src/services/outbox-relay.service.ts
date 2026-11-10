import type { Prisma } from "@prisma/client";
import type { JobMessage } from "../types/job-message";
import type { ClaimedOutboxEvent } from "../repositories/outbox.repository";

interface OutboxRepositoryPort {
  claimDue(
    batchSize: number,
    leaseDurationMs: number,
    onlyEventId?: string,
  ): Promise<ClaimedOutboxEvent[]>;
  markPublished(eventId: string, lockToken: string): Promise<boolean>;
  scheduleRetry(
    eventId: string,
    lockToken: string,
    safeError: string,
    retryDelayMs: number,
  ): Promise<boolean>;
}

interface OutboxPublisherPort {
  publish(message: JobMessage, messageId: string): Promise<void>;
}

export interface OutboxRelayOptions {
  batchSize: number;
  leaseDurationMs: number;
  retryBaseDelayMs: number;
  retryMaxDelayMs: number;
  pollIntervalMs: number;
  onlyEventId?: string;
}

const isJobMessage = (payload: Prisma.JsonValue): payload is JobMessage =>
  typeof payload === "object" &&
  payload !== null &&
  !Array.isArray(payload) &&
  "jobId" in payload &&
  "type" in payload &&
  "payload" in payload &&
  "priority" in payload &&
  "createdAt" in payload;

const retryDelay = (attempts: number, options: OutboxRelayOptions): number =>
  Math.min(
    options.retryBaseDelayMs * 2 ** Math.min(Math.max(0, attempts - 1), 30),
    options.retryMaxDelayMs,
  );

export class OutboxRelayService {
  constructor(
    private readonly repository: OutboxRepositoryPort,
    private readonly publisher: OutboxPublisherPort,
    private readonly options: OutboxRelayOptions,
  ) {}

  async publishBatch(): Promise<number> {
    const events = await this.repository.claimDue(
      this.options.batchSize,
      this.options.leaseDurationMs,
      this.options.onlyEventId,
    );
    const results = await Promise.allSettled(
      events.map((event) => this.publishOne(event)),
    );
    const failures = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : [],
    );
    if (failures.length > 0) {
      throw new AggregateError(failures, "One or more outbox events failed");
    }
    return events.length;
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        const claimed = await this.publishBatch();
        if (claimed > 0) {
          continue;
        }
      } catch (error: unknown) {
        console.error(
          JSON.stringify({
            event: "outbox.relay_iteration_failed",
            errorName: error instanceof Error ? error.name : "UnknownError",
          }),
        );
      }

      await new Promise<void>((resolve) => {
        if (signal.aborted) {
          resolve();
          return;
        }
        const onAbort = (): void => {
          clearTimeout(timer);
          resolve();
        };
        const timer = setTimeout(() => {
          signal.removeEventListener("abort", onAbort);
          resolve();
        }, this.options.pollIntervalMs);
        signal.addEventListener("abort", onAbort, { once: true });
      });
    }
  }

  private async publishOne(event: ClaimedOutboxEvent): Promise<void> {
    try {
      if (!isJobMessage(event.payload)) {
        throw new Error("Invalid outbox job message");
      }
      await this.publisher.publish(event.payload, event.id);
      const marked = await this.repository.markPublished(
        event.id,
        event.lockToken,
      );
      if (!marked) {
        console.warn(
          JSON.stringify({
            event: "outbox.publish_ownership_lost",
            eventId: event.id,
          }),
        );
      }
    } catch (error: unknown) {
      const safeError = error instanceof Error ? error.name : "UnknownError";
      const scheduled = await this.repository.scheduleRetry(
        event.id,
        event.lockToken,
        safeError,
        retryDelay(event.attempts, this.options),
      );
      console.error(
        JSON.stringify({
          event: "outbox.publish_failed",
          eventId: event.id,
          errorName: safeError,
          retryScheduled: scheduled,
        }),
      );
    }
  }
}
