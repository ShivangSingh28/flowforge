import type { ConfirmChannel } from "amqplib";
import {
  DEFAULT_JOB_ROUTING_KEY,
  DEFAULT_JOBS_QUEUE,
  DEAD_LETTER_EXCHANGE,
  DEAD_LETTER_QUEUE,
  DEAD_LETTER_ROUTING_KEY,
  JOBS_EXCHANGE,
} from "../config/rabbitmq-topology";

const initializedChannels = new WeakMap<
  ConfirmChannel,
  Map<string, Promise<void>>
>();

export const initializeRabbitMQTopology = (
  channel: ConfirmChannel,
  queue = DEFAULT_JOBS_QUEUE,
  routingKey = DEFAULT_JOB_ROUTING_KEY,
): Promise<void> => {
  let channelInitializations = initializedChannels.get(channel);
  if (!channelInitializations) {
    channelInitializations = new Map();
    initializedChannels.set(channel, channelInitializations);
  }
  const key = `${queue}\u0000${routingKey}`;
  const existingInitialization = channelInitializations.get(key);
  if (existingInitialization) {
    return existingInitialization;
  }

  const initialization = (async (): Promise<void> => {
    await channel.assertExchange(JOBS_EXCHANGE, "direct", { durable: true });
    await channel.assertQueue(queue, { durable: true });
    await channel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
    await channel.assertExchange(DEAD_LETTER_EXCHANGE, "direct", {
      durable: true,
    });
    await channel.assertQueue(DEAD_LETTER_QUEUE, { durable: true });
    await channel.bindQueue(
      DEAD_LETTER_QUEUE,
      DEAD_LETTER_EXCHANGE,
      DEAD_LETTER_ROUTING_KEY,
    );
  })();

  channelInitializations.set(key, initialization);

  void initialization.catch(() => {
    if (channelInitializations?.get(key) === initialization) {
      channelInitializations.delete(key);
    }
  });

  return initialization;
};
