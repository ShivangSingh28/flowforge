import type { ConsumeMessage, ConfirmChannel } from "amqplib";
import { DEFAULT_JOBS_QUEUE } from "../config/rabbitmq-topology";
import { jobMessageSchema } from "../types/job-message";
import type { JobExecutionService } from "../services/job-execution.service";
import { abortableDelay } from "../utils/abortable-delay";

export class JobConsumer {
  private consumerTag: string | undefined;
  private accepting = false;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly controllers = new Set<AbortController>();

  constructor(
    private readonly channel: ConfirmChannel,
    private readonly executionService: JobExecutionService,
    private readonly prefetchCount: number,
    private readonly retryDelayMs: number,
  ) {}

  async start(): Promise<void> {
    if (this.consumerTag) {
      return;
    }

    await this.channel.prefetch(this.prefetchCount);
    this.accepting = true;
    const result = await this.channel.consume(
      DEFAULT_JOBS_QUEUE,
      (message) => {
        if (!message || !this.accepting) {
          return;
        }

        const controller = new AbortController();
        this.controllers.add(controller);
        const processing = this.process(message, controller.signal).finally(
          () => this.controllers.delete(controller),
        );
        this.inFlight.add(processing);
        void processing.then(
          () => this.inFlight.delete(processing),
          () => this.inFlight.delete(processing),
        );
      },
      { noAck: false },
    );
    this.consumerTag = result.consumerTag;

    console.info(
      JSON.stringify({
        event: "worker.consumer_started",
        queue: DEFAULT_JOBS_QUEUE,
        prefetch: this.prefetchCount,
      }),
    );
  }

  async stop(): Promise<void> {
    this.accepting = false;
    this.controllers.forEach((controller) => controller.abort());
    const consumerTag = this.consumerTag;
    this.consumerTag = undefined;
    let cancelError: unknown;

    if (consumerTag) {
      try {
        await this.channel.cancel(consumerTag);
      } catch (error: unknown) {
        cancelError = error;
      }
    }

    while (this.inFlight.size > 0) {
      await Promise.all([...this.inFlight]);
    }

    if (cancelError !== undefined) {
      throw cancelError;
    }
  }

  private async process(
    message: ConsumeMessage,
    signal: AbortSignal,
  ): Promise<void> {
    let deliverySettled = false;
    const settle = (action: () => void): void => {
      if (deliverySettled) {
        return;
      }
      deliverySettled = true;
      try {
        action();
      } catch (error: unknown) {
        console.error(
          JSON.stringify({
            event: "worker.delivery_settlement_failed",
            errorName: error instanceof Error ? error.name : "UnknownError",
          }),
        );
      }
    };

    let decoded: unknown;
    try {
      decoded = JSON.parse(message.content.toString());
    } catch {
      console.error(JSON.stringify({ event: "worker.message_malformed_json" }));
      settle(() => this.channel.reject(message, false));
      return;
    }

    const parsed = jobMessageSchema.safeParse(decoded);
    if (!parsed.success) {
      console.error(
        JSON.stringify({
          event: "worker.message_invalid",
          issueCount: parsed.error.issues.length,
        }),
      );
      settle(() => this.channel.reject(message, false));
      return;
    }

    try {
      const disposition = await this.executionService.execute(
        parsed.data,
        signal,
      );
      if (signal.aborted || disposition === "defer") {
        return;
      }
      if (disposition === "ack") {
        settle(() => this.channel.ack(message));
      }
    } catch (error: unknown) {
      console.error(
        JSON.stringify({
          event: "worker.processing_error",
          jobId: parsed.data.jobId,
          errorName: error instanceof Error ? error.name : "UnknownError",
        }),
      );
      if (signal.aborted) {
        return;
      }
      const retryAfterDelay = await abortableDelay(this.retryDelayMs, signal);
      if (!retryAfterDelay) {
        return;
      }
      settle(() => this.channel.nack(message, false, true));
    }
  }
}
