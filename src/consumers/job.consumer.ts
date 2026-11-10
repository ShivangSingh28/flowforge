import type { ConsumeMessage, ConfirmChannel } from "amqplib";
import { randomUUID } from "node:crypto";
import {
  DEAD_LETTER_EXCHANGE,
  DEAD_LETTER_ROUTING_KEY,
  DEFAULT_JOBS_QUEUE,
} from "../config/rabbitmq-topology";
import { jobMessageSchema } from "../types/job-message";
import type { JobExecutionService } from "../services/job-execution.service";
import { abortableDelay } from "../utils/abortable-delay";

export class JobConsumer {
  private consumerTag: string | undefined;
  private accepting = false;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly controllers = new Set<AbortController>();
  private channelClosed = false;
  private readonly onChannelClose = (): void => {
    this.channelClosed = true;
    this.accepting = false;
    this.controllers.forEach((controller) => controller.abort());
  };

  constructor(
    private readonly channel: ConfirmChannel,
    private readonly executionService: JobExecutionService,
    private readonly prefetchCount: number,
    private readonly retryDelayMs: number,
    private readonly queue = DEFAULT_JOBS_QUEUE,
    private readonly shutdownGraceMs = 15000,
  ) {}

  get isReady(): boolean {
    return this.accepting && this.consumerTag !== undefined;
  }

  async start(): Promise<void> {
    if (this.consumerTag) {
      return;
    }

    await this.channel.prefetch(this.prefetchCount);
    this.channel.on("close", this.onChannelClose);
    this.accepting = true;
    const result = await this.channel.consume(
      this.queue,
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
        queue: this.queue,
        prefetch: this.prefetchCount,
      }),
    );
  }

  async stop(graceMs = this.shutdownGraceMs): Promise<void> {
    this.accepting = false;
    const consumerTag = this.consumerTag;
    this.consumerTag = undefined;
    let cancelError: unknown;

    if (consumerTag && !this.channelClosed) {
      try {
        await this.channel.cancel(consumerTag);
      } catch (error: unknown) {
        cancelError = error;
      }
    }

    if (this.inFlight.size > 0 && graceMs > 0) {
      await new Promise<void>((resolve) => {
        let finished = false;
        const complete = (): void => {
          if (finished) {
            return;
          }
          finished = true;
          clearTimeout(timer);
          resolve();
        };
        const timer = setTimeout(complete, graceMs);
        void Promise.all([...this.inFlight]).then(complete);
      });
    }

    this.controllers.forEach((controller) => controller.abort());
    await Promise.all([...this.inFlight]);
    this.channel.removeListener("close", this.onChannelClose);

    if (cancelError !== undefined) {
      throw cancelError;
    }
  }

  private async deadLetter(
    message: ConsumeMessage,
    reason: string,
  ): Promise<void> {
    const messageId = message.properties.messageId ?? randomUUID();
    const content = Buffer.from(message.content);
    await new Promise<void>((resolve, reject) => {
      let confirmed = false;
      let returned = false;
      let settled = false;
      let drained = true;
      let needsDrain = false;
      const timeout = setTimeout(
        () => settle(new Error("Dead-letter confirmation timed out")),
        10000,
      );
      const cleanup = (): void => {
        clearTimeout(timeout);
        this.channel.removeListener("return", onReturn);
        this.channel.removeListener("drain", onDrain);
        this.channel.removeListener("error", onError);
        this.channel.removeListener("close", onClose);
      };
      const settle = (error?: Error): void => {
        if (settled) {
          return;
        }
        if (error) {
          settled = true;
          cleanup();
          reject(error);
          return;
        }
        if (confirmed && !returned && (!needsDrain || drained)) {
          settled = true;
          cleanup();
          resolve();
        }
      };
      const onReturn = (returnedMessage: {
        properties: { messageId?: string };
      }): void => {
        if (returnedMessage.properties.messageId === messageId) {
          returned = true;
          settle(new Error("Dead-letter message was unroutable"));
        }
      };
      const onDrain = (): void => {
        drained = true;
        settle();
      };
      const onError = (error: Error): void => settle(error);
      const onClose = (): void =>
        settle(new Error("RabbitMQ channel closed before dead-letter confirm"));
      this.channel.on("return", onReturn);
      this.channel.once("drain", onDrain);
      this.channel.once("error", onError);
      this.channel.once("close", onClose);

      try {
        const accepted = this.channel.publish(
          DEAD_LETTER_EXCHANGE,
          DEAD_LETTER_ROUTING_KEY,
          content,
          {
            persistent: true,
            mandatory: true,
            contentType: message.properties.contentType ?? "application/json",
            messageId,
            headers: {
              ...message.properties.headers,
              "x-flowforge-dead-letter-reason": reason,
              "x-flowforge-original-routing-key": message.fields.routingKey,
            },
          },
          (error: unknown) => {
            if (error) {
              settle(
                error instanceof Error
                  ? error
                  : new Error(
                      "Dead-letter message was negatively acknowledged",
                    ),
              );
              return;
            }
            confirmed = true;
            settle();
          },
        );
        needsDrain = !accepted;
        drained = accepted || drained;
        if (!needsDrain) {
          this.channel.removeListener("drain", onDrain);
        }
        settle();
      } catch (error: unknown) {
        settle(
          error instanceof Error
            ? error
            : new Error("Dead-letter publish failed"),
        );
      }
    });
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
      await this.deadLetter(message, "malformed_json")
        .then(() => settle(() => this.channel.ack(message)))
        .catch(async () => {
          if (await abortableDelay(this.retryDelayMs, signal)) {
            settle(() => this.channel.nack(message, false, true));
          }
        });
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
      await this.deadLetter(message, "invalid_message")
        .then(() => settle(() => this.channel.ack(message)))
        .catch(async () => {
          if (await abortableDelay(this.retryDelayMs, signal)) {
            settle(() => this.channel.nack(message, false, true));
          }
        });
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
