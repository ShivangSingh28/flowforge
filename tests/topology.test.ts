import { EventEmitter } from "node:events";
import { describe, expect, it, jest } from "@jest/globals";
import type { ConfirmChannel } from "amqplib";
import {
  DEFAULT_JOB_ROUTING_KEY,
  DEFAULT_JOBS_QUEUE,
  JOBS_EXCHANGE,
} from "../src/config/rabbitmq-topology";
import { initializeRabbitMQTopology } from "../src/queues/topology";

const createChannel = () =>
  Object.assign(new EventEmitter(), {
    assertExchange: jest
      .fn<ConfirmChannel["assertExchange"]>()
      .mockResolvedValue({
        exchange: JOBS_EXCHANGE,
      }),
    assertQueue: jest.fn<ConfirmChannel["assertQueue"]>().mockResolvedValue({
      queue: DEFAULT_JOBS_QUEUE,
      messageCount: 0,
      consumerCount: 0,
    }),
    bindQueue: jest.fn<ConfirmChannel["bindQueue"]>().mockResolvedValue({}),
  }) as unknown as ConfirmChannel;

describe("RabbitMQ topology", () => {
  it("declares and binds the durable default jobs topology", async () => {
    const channel = createChannel();

    await initializeRabbitMQTopology(channel);

    expect(channel.assertExchange).toHaveBeenCalledWith(
      JOBS_EXCHANGE,
      "direct",
      {
        durable: true,
      },
    );
    expect(channel.assertQueue).toHaveBeenCalledWith(DEFAULT_JOBS_QUEUE, {
      durable: true,
    });
    expect(channel.bindQueue).toHaveBeenCalledWith(
      DEFAULT_JOBS_QUEUE,
      JOBS_EXCHANGE,
      DEFAULT_JOB_ROUTING_KEY,
    );
  });

  it("shares initialization when called repeatedly for the same channel", async () => {
    const channel = createChannel();

    await Promise.all([
      initializeRabbitMQTopology(channel),
      initializeRabbitMQTopology(channel),
    ]);

    expect(channel.assertExchange).toHaveBeenCalledTimes(1);
    expect(channel.assertQueue).toHaveBeenCalledTimes(1);
    expect(channel.bindQueue).toHaveBeenCalledTimes(1);
  });
});
