import type { ConfirmChannel } from "amqplib";
import {
  DEFAULT_JOB_ROUTING_KEY,
  DEFAULT_JOBS_QUEUE,
  JOBS_EXCHANGE,
} from "../config/rabbitmq-topology";

const initializedChannels = new WeakMap<ConfirmChannel, Promise<void>>();

export const initializeRabbitMQTopology = (
  channel: ConfirmChannel,
): Promise<void> => {
  const existingInitialization = initializedChannels.get(channel);
  if (existingInitialization) {
    return existingInitialization;
  }

  const initialization = (async (): Promise<void> => {
    await channel.assertExchange(JOBS_EXCHANGE, "direct", { durable: true });
    await channel.assertQueue(DEFAULT_JOBS_QUEUE, { durable: true });
    await channel.bindQueue(
      DEFAULT_JOBS_QUEUE,
      JOBS_EXCHANGE,
      DEFAULT_JOB_ROUTING_KEY,
    );
  })();

  initializedChannels.set(channel, initialization);

  void initialization.catch(() => {
    if (initializedChannels.get(channel) === initialization) {
      initializedChannels.delete(channel);
    }
  });

  return initialization;
};
