import type { ConfirmChannel, Options } from "amqplib";
import {
  DEFAULT_JOB_ROUTING_KEY,
  JOBS_EXCHANGE,
} from "../config/rabbitmq-topology";
import { jobMessageSchema, type JobMessage } from "../types/job-message";

type ChannelProvider = () => ConfirmChannel;

export class JobProducer {
  constructor(
    private readonly getChannel: ChannelProvider,
    private readonly routingKey = DEFAULT_JOB_ROUTING_KEY,
    private readonly confirmTimeoutMs = 30000,
  ) {}

  async publish(
    message: JobMessage,
    messageId: string = message.jobId,
  ): Promise<void> {
    const job = jobMessageSchema.parse(message);
    const content = Buffer.from(JSON.stringify(job));
    const options: Options.Publish = {
      persistent: true,
      contentType: "application/json",
      messageId,
      mandatory: true,
    };

    await this.publishWithConfirmation(
      this.getChannel(),
      this.routingKey,
      content,
      options,
    );
  }

  private publishWithConfirmation(
    channel: ConfirmChannel,
    routingKey: string,
    content: Buffer,
    options: Options.Publish,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let publishReturned = false;
      let drained = false;
      let confirmed = false;
      let returned = false;
      let settled = false;
      let needsDrain = false;

      const cleanup = (): void => {
        clearTimeout(timeout);
        channel.removeListener("drain", onDrain);
        channel.removeListener("error", onError);
        channel.removeListener("close", onClose);
        channel.removeListener("return", onReturn);
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

        if (publishReturned && confirmed && (!needsDrain || drained)) {
          settled = true;
          cleanup();
          resolve();
        }
      };
      const timeout = setTimeout(
        () => settle(new Error("RabbitMQ publish confirmation timed out")),
        this.confirmTimeoutMs,
      );

      const onDrain = (): void => {
        drained = true;
        settle();
      };
      const onError = (error: Error): void => settle(error);
      const onClose = (): void =>
        settle(new Error("RabbitMQ channel closed before publish completed"));
      const onReturn = (message: {
        properties: { messageId?: string };
      }): void => {
        if (message.properties.messageId !== options.messageId) {
          return;
        }
        returned = true;
        settle(new Error("RabbitMQ returned an unroutable message"));
      };

      channel.once("drain", onDrain);
      channel.once("error", onError);
      channel.once("close", onClose);
      channel.on("return", onReturn);

      try {
        const accepted = channel.publish(
          JOBS_EXCHANGE,
          routingKey,
          content,
          options,
          (error: unknown) => {
            if (error) {
              settle(
                error instanceof Error
                  ? error
                  : new Error("RabbitMQ negatively acknowledged the message"),
              );
              return;
            }

            confirmed = true;
            if (returned) {
              settle(new Error("RabbitMQ returned an unroutable message"));
              return;
            }
            settle();
          },
        );

        needsDrain = !accepted;
        drained = accepted || drained;
        publishReturned = true;

        if (!needsDrain) {
          channel.removeListener("drain", onDrain);
        }

        settle();
      } catch (error: unknown) {
        settle(
          error instanceof Error ? error : new Error("RabbitMQ publish failed"),
        );
      }
    });
  }
}
