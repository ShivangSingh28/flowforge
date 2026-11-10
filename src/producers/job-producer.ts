import type { ConfirmChannel, Options } from "amqplib";
import {
  DEFAULT_JOB_ROUTING_KEY,
  JOBS_EXCHANGE,
} from "../config/rabbitmq-topology";
import { jobMessageSchema, type JobMessage } from "../types/job-message";

type ChannelProvider = () => ConfirmChannel;

export class JobProducer {
  constructor(private readonly getChannel: ChannelProvider) {}

  async publish(message: JobMessage): Promise<void> {
    const job = jobMessageSchema.parse(message);
    const content = Buffer.from(JSON.stringify(job));
    const options: Options.Publish = {
      persistent: true,
      contentType: "application/json",
      messageId: job.jobId,
    };

    await this.publishWithConfirmation(this.getChannel(), content, options);
  }

  private publishWithConfirmation(
    channel: ConfirmChannel,
    content: Buffer,
    options: Options.Publish,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let publishReturned = false;
      let drained = false;
      let confirmed = false;
      let settled = false;
      let needsDrain = false;

      const cleanup = (): void => {
        channel.removeListener("drain", onDrain);
        channel.removeListener("error", onError);
        channel.removeListener("close", onClose);
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

      const onDrain = (): void => {
        drained = true;
        settle();
      };
      const onError = (error: Error): void => settle(error);
      const onClose = (): void =>
        settle(new Error("RabbitMQ channel closed before publish completed"));

      channel.once("drain", onDrain);
      channel.once("error", onError);
      channel.once("close", onClose);

      try {
        const accepted = channel.publish(
          JOBS_EXCHANGE,
          DEFAULT_JOB_ROUTING_KEY,
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
