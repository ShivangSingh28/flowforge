import { randomUUID } from "node:crypto";
import amqp from "amqplib";
import type { ConfirmChannel, GetMessage } from "amqplib";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import { prisma } from "../../src/repositories/prisma";
import { RabbitMQConnectionManager } from "../../src/queues/rabbitmq-connection-manager";
import {
  DEAD_LETTER_EXCHANGE,
  DEAD_LETTER_ROUTING_KEY,
  DEFAULT_JOB_ROUTING_KEY,
  DEFAULT_JOBS_QUEUE,
  JOBS_EXCHANGE,
} from "../../src/config/rabbitmq-topology";
import { initializeRabbitMQTopology } from "../../src/queues/topology";
import { JobProducer } from "../../src/producers/job-producer";
import { JobConsumer } from "../../src/consumers/job.consumer";
import { app } from "../../src/app";
import type { JobExecutionService } from "../../src/services/job-execution.service";
import type { JobMessage } from "../../src/types/job-message";
import request from "supertest";

const rabbitMQUrl = process.env.RABBITMQ_URL;
if (!rabbitMQUrl) {
  throw new Error("Missing isolated integration RabbitMQ URL");
}

const connectionManager = new RabbitMQConnectionManager(rabbitMQUrl);
let channel: ConfirmChannel;
const createdJobIds: string[] = [];

const createMessage = (): JobMessage => ({
  jobId: randomUUID(),
  type: "integration-test",
  payload: { marker: "flowforge-integration" },
  priority: "MEDIUM",
  createdAt: new Date().toISOString(),
});

const waitFor = async (
  predicate: () => boolean,
  timeoutMs = 5000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error("Timed out waiting for RabbitMQ consumer settlement");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

const removeCreatedJobs = async (): Promise<void> => {
  if (createdJobIds.length === 0) {
    return;
  }
  await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
  createdJobIds.length = 0;
};

describe("FlowForge real-service integration", () => {
  beforeAll(async () => {
    channel = await connectionManager.initialize();
    await initializeRabbitMQTopology(channel);
  });

  afterEach(async () => {
    await removeCreatedJobs();
    jest.restoreAllMocks();
  });

  afterAll(async () => {
    try {
      await removeCreatedJobs();
    } finally {
      await Promise.all([connectionManager.close(), prisma.$disconnect()]);
    }
  });

  it("connects to the dedicated PostgreSQL database and persists a job", async () => {
    const databases = await prisma.$queryRaw<Array<{ database: string }>>`
      SELECT current_database() AS database
    `;
    expect(databases[0]?.database).toBe("flowforge_integration");

    const job = await prisma.job.create({
      data: {
        type: "integration-test",
        payload: { marker: "flowforge-integration" },
        idempotencyKey: `integration:${randomUUID()}`,
      },
    });
    createdJobIds.push(job.id);

    await expect(
      prisma.job.findUnique({ where: { id: job.id } }),
    ).resolves.toMatchObject({
      id: job.id,
      status: "PENDING",
      type: "integration-test",
    });
  });

  it("reports API operational metrics without exposing connection details", async () => {
    const response = await request(app).get("/health/operations");
    expect(response.status).toBe(200);
    expect(response.body).toEqual(
      expect.objectContaining({
        status: "ok",
        pendingOutboxEvents: expect.any(Number),
        failedJobs: expect.any(Number),
      }),
    );
    expect(response.body).toHaveProperty("oldestUnpublishedEventAgeMs");
    expect(JSON.stringify(response.body)).not.toContain("DATABASE_URL");
    expect(JSON.stringify(response.body)).not.toContain("RABBITMQ_URL");
  });

  it("connects to RabbitMQ and initializes the durable job topology", async () => {
    const queue = await channel.checkQueue(DEFAULT_JOBS_QUEUE);
    await expect(channel.checkExchange(JOBS_EXCHANGE)).resolves.toBeDefined();
    expect(queue.queue).toBe(DEFAULT_JOBS_QUEUE);
  });

  it("publishes a persistent message and waits for broker confirmation", async () => {
    const producer = new JobProducer(() => channel);
    const message = createMessage();
    const queue = `flowforge.integration.${randomUUID()}`;
    let queueCreated = false;
    let delivery: GetMessage | false = false;
    try {
      await channel.assertQueue(queue, { exclusive: true, autoDelete: true });
      queueCreated = true;
      await channel.bindQueue(queue, JOBS_EXCHANGE, DEFAULT_JOB_ROUTING_KEY);
      await producer.publish(message);
      delivery = await channel.get(queue, { noAck: false });
      if (delivery === false) {
        throw new Error(
          "Confirmed message was not available in its bound queue",
        );
      }

      expect(JSON.parse(delivery.content.toString())).toEqual(message);
      expect(delivery.properties.messageId).toBe(message.jobId);
      expect(delivery.properties.deliveryMode).toBe(2);
      channel.ack(delivery);
      delivery = false;
    } finally {
      if (delivery !== false) {
        channel.nack(delivery, false, false);
      }
      if (queueCreated) {
        await channel.deleteQueue(queue);
      }
    }
  });

  it("observes real consumer NACK/requeue followed by manual ACK", async () => {
    const connection = await amqp.connect(rabbitMQUrl);
    let consumerChannel: ConfirmChannel | undefined;
    let cleanupChannel:
      Awaited<ReturnType<typeof connection.createChannel>> | undefined;
    let consumer: JobConsumer | undefined;
    const queue = `flowforge.integration.consumer.${randomUUID()}`;
    const routingKey = `flowforge.integration.consumer.${randomUUID()}`;
    let queueCreated = false;
    try {
      consumerChannel = await connection.createConfirmChannel();
      cleanupChannel = await connection.createChannel();
      const message = createMessage();
      let executions = 0;
      const execute = jest.fn<JobExecutionService["execute"]>(async () => {
        executions += 1;
        if (executions === 1) {
          throw new Error("Simulated transient integration failure");
        }
        return "ack";
      });
      consumer = new JobConsumer(
        consumerChannel,
        { execute } as unknown as JobExecutionService,
        1,
        1,
        queue,
      );
      const nack = jest.spyOn(consumerChannel, "nack");
      const ack = jest.spyOn(consumerChannel, "ack");

      await cleanupChannel.assertQueue(queue, {
        exclusive: true,
        autoDelete: true,
      });
      queueCreated = true;
      await cleanupChannel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
      const routedProducer = new JobProducer(() => channel, routingKey);
      await consumer.start();
      await routedProducer.publish(message);
      await waitFor(() => ack.mock.calls.length === 1, 15000);

      expect(execute).toHaveBeenCalledTimes(2);
      expect(nack).toHaveBeenCalledTimes(1);
      expect(nack.mock.calls[0]?.[0].properties.messageId).toBe(message.jobId);
      expect(nack.mock.calls[0]?.[2]).toBe(true);
      expect(ack.mock.calls[0]?.[0].properties.messageId).toBe(message.jobId);
      expect(ack.mock.calls[0]?.[0].fields.redelivered).toBe(true);
    } finally {
      try {
        await consumer?.stop();
      } finally {
        try {
          if (queueCreated) {
            await cleanupChannel?.deleteQueue(queue);
          }
        } finally {
          try {
            await consumerChannel?.close();
          } finally {
            try {
              await cleanupChannel?.close();
            } finally {
              await connection.close();
            }
          }
        }
      }
    }
  });

  it("reconnects after a real RabbitMQ connection close and resubscribes", async () => {
    const manager = new RabbitMQConnectionManager(rabbitMQUrl);
    const queue = `flowforge.integration.reconnect.${randomUUID()}`;
    const routingKey = `flowforge.integration.reconnect.${randomUUID()}`;
    const execute = jest
      .fn<JobExecutionService["execute"]>()
      .mockResolvedValue("ack");
    let firstConsumer: JobConsumer | undefined;
    let secondConsumer: JobConsumer | undefined;
    let firstChannel: ConfirmChannel | undefined;
    let secondChannel: ConfirmChannel | undefined;
    let queueCreated = false;
    try {
      firstChannel = await manager.initialize();
      await initializeRabbitMQTopology(firstChannel);
      await channel.assertQueue(queue, { durable: true });
      queueCreated = true;
      await channel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
      firstConsumer = new JobConsumer(
        firstChannel,
        { execute } as unknown as JobExecutionService,
        1,
        1,
        queue,
      );
      await firstConsumer.start();

      const disconnected = manager.waitForDisconnect();
      await firstChannel.close();
      await disconnected;
      expect(manager.isConnected).toBe(false);
      expect(firstConsumer.isReady).toBe(false);
      await firstConsumer.stop(0).catch(() => {});

      secondChannel = await manager.initializeWithRetry(
        new AbortController().signal,
        10,
        50,
      );
      if (!secondChannel) {
        throw new Error("RabbitMQ reconnect was unexpectedly cancelled");
      }
      await initializeRabbitMQTopology(secondChannel);
      secondConsumer = new JobConsumer(
        secondChannel,
        { execute } as unknown as JobExecutionService,
        1,
        1,
        queue,
      );
      const prefetch = jest.spyOn(secondChannel, "prefetch");
      const ack = jest.spyOn(secondChannel, "ack");
      await secondConsumer.start();
      expect(prefetch).toHaveBeenCalledWith(1);

      await new JobProducer(() => channel, routingKey).publish(createMessage());
      await waitFor(() => ack.mock.calls.length === 1, 15000);
      expect(manager.isConnected).toBe(true);
      expect(secondConsumer.isReady).toBe(true);
      expect(execute).toHaveBeenCalledTimes(1);
    } finally {
      await secondConsumer?.stop(0);
      await firstConsumer?.stop(0).catch(() => {});
      await manager.close();
      if (queueCreated) {
        await channel.deleteQueue(queue);
      }
    }
  }, 30000);

  it("confirms malformed real deliveries into the durable dead-letter queue", async () => {
    const connection = await amqp.connect(rabbitMQUrl);
    const consumerChannel = await connection.createConfirmChannel();
    const setupChannel = await connection.createChannel();
    const queue = `flowforge.integration.dead.${randomUUID()}`;
    const deadLetterQueue = `flowforge.integration.dead.observer.${randomUUID()}`;
    const routingKey = `flowforge.integration.dead.${randomUUID()}`;
    const messageId = randomUUID();
    const consumer = new JobConsumer(
      consumerChannel,
      {
        execute: jest.fn<JobExecutionService["execute"]>(),
      } as unknown as JobExecutionService,
      1,
      1,
      queue,
    );
    const ack = jest.spyOn(consumerChannel, "ack");
    try {
      await setupChannel.assertQueue(queue, { durable: true });
      await setupChannel.assertQueue(deadLetterQueue, { durable: true });
      await setupChannel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
      await setupChannel.bindQueue(
        deadLetterQueue,
        DEAD_LETTER_EXCHANGE,
        DEAD_LETTER_ROUTING_KEY,
      );
      await consumer.start();
      await new Promise<void>((resolve, reject) => {
        consumerChannel.publish(
          JOBS_EXCHANGE,
          routingKey,
          Buffer.from("{"),
          { persistent: true, mandatory: true, messageId },
          (error: unknown) => (error ? reject(error) : resolve()),
        );
      });
      await waitFor(() => ack.mock.calls.length === 1);

      const deadLetter = await setupChannel.get(deadLetterQueue, {
        noAck: false,
      });
      if (deadLetter === false) {
        throw new Error("Malformed delivery was not routed to the DLQ");
      }
      expect(deadLetter.content.toString()).toBe("{");
      expect(deadLetter.properties.messageId).toBe(messageId);
      expect(deadLetter.properties.deliveryMode).toBe(2);
      expect(
        deadLetter.properties.headers?.["x-flowforge-dead-letter-reason"],
      ).toBe("malformed_json");
      setupChannel.ack(deadLetter);
    } finally {
      await consumer.stop(0);
      await setupChannel.deleteQueue(deadLetterQueue);
      await setupChannel.deleteQueue(queue);
      await Promise.all([consumerChannel.close(), setupChannel.close()]);
      await connection.close();
    }
  });
});
