import { randomUUID } from "node:crypto";
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
import { JobAttemptStatus, JobPriority, PrismaClient } from "@prisma/client";
import { JOBS_EXCHANGE } from "../../src/config/rabbitmq-topology";
import { OutboxPublisher } from "../../src/producers/outbox-publisher";
import { OutboxRepository } from "../../src/repositories/outbox.repository";
import { JobExecutionRepository } from "../../src/repositories/job-execution.repository";
import { JobRepository } from "../../src/repositories/job.repository";
import { prisma } from "../../src/repositories/prisma";
import { RabbitMQConnectionManager } from "../../src/queues/rabbitmq-connection-manager";
import { initializeRabbitMQTopology } from "../../src/queues/topology";
import { JobExecutionService } from "../../src/services/job-execution.service";
import { OutboxRelayService } from "../../src/services/outbox-relay.service";
import { JobSubmissionService } from "../../src/services/job-submission.service";
import { jobMessageSchema, type JobMessage } from "../../src/types/job-message";
import type { JobSubmissionInput } from "../../src/types/job-submission";

const databaseUrl = process.env.DATABASE_URL;
const rabbitMQUrl = process.env.RABBITMQ_URL;
if (!databaseUrl || !rabbitMQUrl) {
  throw new Error("Missing isolated integration service URLs");
}

const firstClient = new PrismaClient({
  datasources: { db: { url: databaseUrl } },
});
const secondClient = new PrismaClient({
  datasources: { db: { url: databaseUrl } },
});
const firstOutboxRepository = new OutboxRepository(firstClient);
const secondOutboxRepository = new OutboxRepository(secondClient);
const firstJobRepository = new JobRepository(firstClient);
const secondJobRepository = new JobRepository(secondClient);
const firstSubmissionService = new JobSubmissionService(firstJobRepository);
const secondSubmissionService = new JobSubmissionService(secondJobRepository);
const connectionManager = new RabbitMQConnectionManager(rabbitMQUrl);
const createdJobIds: string[] = [];
const createdQueues: string[] = [];
let channel: ConfirmChannel;

const createInput = (
  overrides: Partial<JobSubmissionInput> = {},
): JobSubmissionInput => ({
  type: "demo",
  payload: { marker: `outbox:${randomUUID()}` },
  priority: JobPriority.MEDIUM,
  maxAttempts: 3,
  idempotencyKey: `outbox:${randomUUID()}`,
  ...overrides,
});

const submitJob = async (
  service: JobSubmissionService = firstSubmissionService,
  input = createInput(),
): Promise<{ id: string; eventId: string; input: JobSubmissionInput }> => {
  const result = await service.submit(input);
  const jobId = result.job.id;
  createdJobIds.push(jobId);
  const event = await firstClient.outboxEvent.findFirstOrThrow({
    where: { jobId, eventType: "job.submitted" },
  });
  return { id: jobId, eventId: event.id, input };
};

const createBoundQueue = async (
  routingKey = `outbox.integration.${randomUUID()}`,
): Promise<{ queue: string; routingKey: string }> => {
  const queue = `flowforge.outbox.integration.${randomUUID()}`;
  await channel.assertQueue(queue, { autoDelete: true });
  createdQueues.push(queue);
  await channel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
  return { queue, routingKey };
};

const waitFor = async (
  predicate: () => Promise<boolean>,
  description: string,
  timeoutMs = 10000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

const readMessage = async (queue: string): Promise<GetMessage> => {
  const message = await channel.get(queue, { noAck: false });
  if (message === false) {
    throw new Error(`Expected a message in ${queue}`);
  }
  return message;
};

const cleanCreatedRecords = async (): Promise<void> => {
  if (createdJobIds.length > 0) {
    await firstClient.outboxEvent.deleteMany({
      where: { jobId: { in: createdJobIds } },
    });
    await firstClient.job.deleteMany({
      where: { id: { in: createdJobIds } },
    });
    createdJobIds.length = 0;
  }
  for (const queue of createdQueues.splice(0)) {
    await channel.deleteQueue(queue);
  }
};

const relayOptions = {
  batchSize: 10,
  leaseDurationMs: 20000,
  retryBaseDelayMs: 1000,
  retryMaxDelayMs: 3000,
  pollIntervalMs: 10,
};

describe("Transactional Outbox against real PostgreSQL and RabbitMQ", () => {
  beforeAll(async () => {
    const databases = await firstClient.$queryRaw<Array<{ database: string }>>`
      SELECT current_database() AS database
    `;
    if (databases[0]?.database !== "flowforge_integration") {
      throw new Error(
        "Outbox integration tests require the isolated flowforge_integration database",
      );
    }
    channel = await connectionManager.initialize();
    await initializeRabbitMQTopology(channel);
  });

  afterEach(async () => {
    await cleanCreatedRecords();
    jest.restoreAllMocks();
  });

  afterAll(async () => {
    try {
      await cleanCreatedRecords();
    } finally {
      await Promise.all([
        connectionManager.close(),
        firstClient.$disconnect(),
        secondClient.$disconnect(),
        prisma.$disconnect(),
      ]);
    }
  });

  it("commits a job and its immutable submission message in one transaction", async () => {
    const { id, eventId, input } = await submitJob();
    const [job, event] = await Promise.all([
      firstClient.job.findUniqueOrThrow({ where: { id } }),
      firstClient.outboxEvent.findUniqueOrThrow({ where: { id: eventId } }),
    ]);

    expect(job.status).toBe("PENDING");
    expect(event).toMatchObject({
      jobId: id,
      eventType: "job.submitted",
      attempts: 0,
      lockedUntil: null,
      lockToken: null,
      publishedAt: null,
    });
    expect(event.payload).toEqual({
      jobId: id,
      type: input.type,
      payload: input.payload,
      priority: input.priority,
      createdAt: job.createdAt.toISOString(),
    });
    await expect(
      firstClient.$executeRaw`
        UPDATE "OutboxEvent"
        SET "payload" = '{}'::jsonb
        WHERE "id" = ${eventId}::uuid
      `,
    ).rejects.toMatchObject({ code: "P2010" });
    await expect(
      firstClient.job.delete({ where: { id } }),
    ).rejects.toMatchObject({ code: "P2003" });
  });

  it("rolls back job insertion when the outbox insert fails in PostgreSQL", async () => {
    const idempotencyKey = `outbox-trigger:${randomUUID()}`;
    const suffix = randomUUID().replaceAll("-", "");
    const functionName = `integration_reject_outbox_insert_${suffix}`;
    const triggerName = `integration_reject_outbox_insert_${suffix}`;
    await firstClient.$executeRawUnsafe(`
      CREATE FUNCTION ${functionName}()
      RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'intentional outbox integration failure';
      END;
      $$;
    `);
    await firstClient.$executeRawUnsafe(`
      CREATE TRIGGER ${triggerName}
      BEFORE INSERT ON "OutboxEvent"
      FOR EACH ROW EXECUTE FUNCTION ${functionName}();
    `);

    try {
      await expect(
        firstSubmissionService.submit(createInput({ idempotencyKey })),
      ).rejects.toThrow();
    } finally {
      await firstClient.$executeRawUnsafe(
        `DROP TRIGGER ${triggerName} ON "OutboxEvent"`,
      );
      await firstClient.$executeRawUnsafe(`DROP FUNCTION ${functionName}()`);
    }

    expect(await firstClient.job.count({ where: { idempotencyKey } })).toBe(0);
    expect(
      await firstClient.outboxEvent.count({
        where: { job: { idempotencyKey } },
      }),
    ).toBe(0);
  });

  it("replays identical idempotent submissions without adding a job or event", async () => {
    const input = createInput();
    const first = await submitJob(firstSubmissionService, input);
    const second = await secondSubmissionService.submit(input);

    expect(second).toEqual({
      job: { id: first.id, status: "PENDING" },
      created: false,
    });
    expect(
      await firstClient.job.count({
        where: { idempotencyKey: input.idempotencyKey },
      }),
    ).toBe(1);
    expect(
      await firstClient.outboxEvent.count({
        where: { jobId: first.id, eventType: "job.submitted" },
      }),
    ).toBe(1);
  });

  it("rejects conflicting idempotent submissions", async () => {
    const input = createInput();
    const first = await submitJob(firstSubmissionService, input);

    await expect(
      secondSubmissionService.submit({
        ...input,
        payload: { different: true },
      }),
    ).rejects.toMatchObject({ statusCode: 409 });
    expect(await firstClient.job.count({ where: { id: first.id } })).toBe(1);
    expect(
      await firstClient.outboxEvent.count({ where: { jobId: first.id } }),
    ).toBe(1);
  });

  it("creates exactly one job and event for concurrent identical submissions", async () => {
    const input = createInput();
    const [first, second] = await Promise.all([
      firstSubmissionService.submit(input),
      secondSubmissionService.submit(input),
    ]);
    const jobId = first.job.id;
    createdJobIds.push(jobId);

    expect(first.job.id).toBe(second.job.id);
    expect([first.created, second.created].sort()).toEqual([false, true]);
    expect(
      await firstClient.job.count({
        where: { idempotencyKey: input.idempotencyKey },
      }),
    ).toBe(1);
    expect(
      await firstClient.outboxEvent.count({
        where: { jobId, eventType: "job.submitted" },
      }),
    ).toBe(1);
  });

  it("claims disjoint batches from two independent relay repositories", async () => {
    const submitted = await Promise.all(
      Array.from({ length: 4 }, () => submitJob()),
    );
    const [first, second] = await Promise.all([
      firstOutboxRepository.claimDue(2, 30000),
      secondOutboxRepository.claimDue(2, 30000),
    ]);
    const firstIds = first.map((event) => event.id);
    const secondIds = second.map((event) => event.id);

    expect(first).toHaveLength(2);
    expect(second).toHaveLength(2);
    expect(firstIds.filter((id) => secondIds.includes(id))).toHaveLength(0);
    expect(new Set([...firstIds, ...secondIds]).size).toBe(4);
    expect(new Set(submitted.map((event) => event.eventId))).toEqual(
      new Set([...firstIds, ...secondIds]),
    );
  });

  it("reclaims expired claims and fences a stale lock token", async () => {
    const { eventId } = await submitJob();
    const [oldClaim] = await firstOutboxRepository.claimDue(1, 30000);
    expect(oldClaim?.id).toBe(eventId);
    await firstClient.$executeRaw`
      UPDATE "OutboxEvent"
      SET "lockedUntil" = clock_timestamp() - INTERVAL '1 second'
      WHERE "id" = ${eventId}::uuid
    `;

    const [newClaim] = await secondOutboxRepository.claimDue(1, 30000);
    expect(newClaim?.id).toBe(eventId);
    expect(newClaim?.lockToken).not.toBe(oldClaim?.lockToken);
    expect(newClaim?.attempts).toBe(2);
    expect(
      await firstOutboxRepository.markPublished(eventId, oldClaim!.lockToken),
    ).toBe(false);
    expect(
      (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).publishedAt,
    ).toBeNull();
  });

  it("leaves publishedAt null while confirmation is pending, then marks after confirmation", async () => {
    const { eventId } = await submitJob();
    let releaseConfirmation = (): void => {};
    let signalPublisherCalled = (): void => {};
    const confirmationPending = new Promise<void>((resolve) => {
      releaseConfirmation = resolve;
    });
    const publisherCalled = new Promise<void>((resolve) => {
      signalPublisherCalled = resolve;
    });
    const publisher = {
      publish: async (): Promise<void> => {
        signalPublisherCalled();
        await confirmationPending;
      },
    };
    const relay = new OutboxRelayService(
      firstOutboxRepository,
      publisher,
      relayOptions,
    );

    const publishing = relay.publishBatch();
    await publisherCalled;
    expect(
      (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).publishedAt,
    ).toBeNull();
    releaseConfirmation();
    await publishing;
    expect(
      (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).publishedAt,
    ).not.toBeNull();
  });

  it("publishes persistent messages with stable event IDs and marks them after confirmation", async () => {
    const { eventId } = await submitJob();
    const { queue, routingKey } = await createBoundQueue();
    const relay = new OutboxRelayService(
      firstOutboxRepository,
      new OutboxPublisher(connectionManager, 30000, routingKey),
      relayOptions,
    );

    expect(await relay.publishBatch()).toBe(1);
    const delivery = await readMessage(queue);
    expect(delivery.properties.messageId).toBe(eventId);
    expect(delivery.properties.deliveryMode).toBe(2);
    expect(JSON.parse(delivery.content.toString())).toMatchObject({
      jobId: (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).jobId,
    });
    channel.ack(delivery);
    expect(
      (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).publishedAt,
    ).not.toBeNull();
  });

  it("resumes a pending event after the relay publisher connection is recreated", async () => {
    const { eventId } = await submitJob();
    const { queue, routingKey } = await createBoundQueue();
    const reconnectingManager = new RabbitMQConnectionManager(rabbitMQUrl);
    try {
      const firstChannel = await reconnectingManager.initialize();
      await initializeRabbitMQTopology(firstChannel);
      const disconnected = reconnectingManager.waitForDisconnect();
      await firstChannel.close();
      await disconnected;

      const replacementChannel = await reconnectingManager.initializeWithRetry(
        new AbortController().signal,
        10,
        50,
      );
      if (!replacementChannel) {
        throw new Error("Relay reconnect was unexpectedly cancelled");
      }
      expect(replacementChannel).not.toBe(firstChannel);
      await initializeRabbitMQTopology(replacementChannel);

      const relay = new OutboxRelayService(
        firstOutboxRepository,
        new OutboxPublisher(reconnectingManager, 30000, routingKey),
        { ...relayOptions, onlyEventId: eventId },
      );
      expect(await relay.publishBatch()).toBe(1);
      const delivery = await readMessage(queue);
      expect(delivery.properties.messageId).toBe(eventId);
      channel.ack(delivery);
      expect(
        (
          await firstClient.outboxEvent.findUniqueOrThrow({
            where: { id: eventId },
          })
        ).publishedAt,
      ).not.toBeNull();
    } finally {
      await reconnectingManager.close();
    }
  });

  it("retains unroutable events and successfully retries them", async () => {
    jest.spyOn(console, "error").mockImplementation(() => {});
    const { eventId } = await submitJob();
    const routingKey = `outbox.unroutable.${randomUUID()}`;
    const publisher = new OutboxPublisher(connectionManager, 30000, routingKey);
    const relay = new OutboxRelayService(
      firstOutboxRepository,
      publisher,
      relayOptions,
    );

    await relay.publishBatch();
    const failed = await firstClient.outboxEvent.findUniqueOrThrow({
      where: { id: eventId },
    });
    expect(failed.publishedAt).toBeNull();
    expect(failed.attempts).toBe(1);
    expect(failed.lastError).toBe("Error");
    expect(failed.lockToken).toBeNull();
    expect(failed.availableAt.getTime()).toBeGreaterThan(Date.now());

    const queue = `flowforge.outbox.retry.${randomUUID()}`;
    await channel.assertQueue(queue, { autoDelete: true });
    createdQueues.push(queue);
    await channel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
    await firstClient.$executeRaw`
      UPDATE "OutboxEvent"
      SET "availableAt" = clock_timestamp()
      WHERE "id" = ${eventId}::uuid
    `;

    await relay.publishBatch();
    const delivery = await readMessage(queue);
    expect(delivery.properties.messageId).toBe(eventId);
    channel.ack(delivery);
    const retried = await firstClient.outboxEvent.findUniqueOrThrow({
      where: { id: eventId },
    });
    expect(retried.publishedAt).not.toBeNull();
    expect(retried.lastError).toBeNull();
    expect(retried.attempts).toBe(2);
  });

  it("recovers a claim abandoned before publication after its lease expires", async () => {
    const { eventId } = await submitJob();
    const [abandoned] = await firstOutboxRepository.claimDue(1, 30000);
    expect(abandoned?.id).toBe(eventId);
    await firstClient.$executeRaw`
      UPDATE "OutboxEvent"
      SET "lockedUntil" = clock_timestamp() - INTERVAL '1 second'
      WHERE "id" = ${eventId}::uuid
    `;
    const { queue, routingKey } = await createBoundQueue();
    const relay = new OutboxRelayService(
      firstOutboxRepository,
      new OutboxPublisher(connectionManager, 30000, routingKey),
      relayOptions,
    );

    await relay.publishBatch();
    const delivery = await readMessage(queue);
    expect(delivery.properties.messageId).toBe(eventId);
    channel.ack(delivery);
    expect(
      (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).publishedAt,
    ).not.toBeNull();
  });

  it("safely republishes with the same event identity after simulated post-confirm crash", async () => {
    const { eventId } = await submitJob();
    const { queue, routingKey } = await createBoundQueue();
    const producer = new OutboxPublisher(connectionManager, 30000, routingKey);
    const [claim] = await firstOutboxRepository.claimDue(1, 30000);
    const event = await firstClient.outboxEvent.findUniqueOrThrow({
      where: { id: eventId },
    });

    await producer.publish(event.payload as JobMessage, event.id);
    await firstClient.$executeRaw`
      UPDATE "OutboxEvent"
      SET "lockedUntil" = clock_timestamp() - INTERVAL '1 second'
      WHERE "id" = ${eventId}::uuid
    `;
    const relay = new OutboxRelayService(
      secondOutboxRepository,
      producer,
      relayOptions,
    );
    await relay.publishBatch();

    const firstDelivery = await readMessage(queue);
    const secondDelivery = await readMessage(queue);
    expect(firstDelivery.properties.messageId).toBe(eventId);
    expect(secondDelivery.properties.messageId).toBe(eventId);
    expect(firstDelivery.properties.messageId).toBe(
      secondDelivery.properties.messageId,
    );
    channel.ack(firstDelivery);
    channel.ack(secondDelivery);
    expect(claim?.id).toBe(eventId);
    expect(
      (
        await firstClient.outboxEvent.findUniqueOrThrow({
          where: { id: eventId },
        })
      ).publishedAt,
    ).not.toBeNull();
  });

  it("keeps duplicate publication from creating parallel active worker attempts", async () => {
    const { id, eventId } = await submitJob();
    const event = await firstClient.outboxEvent.findUniqueOrThrow({
      where: { id: eventId },
    });
    const message = event.payload;
    const queue = `flowforge.outbox.worker.${randomUUID()}`;
    const routingKey = `outbox.worker.${randomUUID()}`;
    await channel.assertQueue(queue, { autoDelete: true });
    createdQueues.push(queue);
    await channel.bindQueue(queue, JOBS_EXCHANGE, routingKey);

    let releaseHandler = (): void => {};
    let signalHandlerStarted = (): void => {};
    const handlerGate = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    const handlerStarted = new Promise<void>((resolve) => {
      signalHandlerStarted = resolve;
    });
    let executions = 0;
    const executionRepository = new JobExecutionRepository(firstClient);
    const executionService = new JobExecutionService(
      executionRepository,
      new Map([
        [
          "demo",
          async () => {
            executions += 1;
            signalHandlerStarted();
            await handlerGate;
          },
        ],
      ]),
      3000,
      25,
      "test",
    );
    const producer = new OutboxPublisher(connectionManager, 30000, routingKey);
    await Promise.all([
      producer.publish(jobMessageSchema.parse(message), eventId),
      producer.publish(jobMessageSchema.parse(message), eventId),
    ]);
    const firstDelivery = await readMessage(queue);
    const secondDelivery = await readMessage(queue);
    const claim = jest.spyOn(executionRepository, "claimAvailable");
    const firstExecution = executionService.execute(
      jobMessageSchema.parse(JSON.parse(firstDelivery.content.toString())),
    );
    const secondExecution = executionService.execute(
      jobMessageSchema.parse(JSON.parse(secondDelivery.content.toString())),
    );
    try {
      await handlerStarted;
      await waitFor(
        async () => claim.mock.calls.length >= 2,
        "both duplicate messages to reach job claiming",
      );
      expect(
        await firstClient.jobAttempt.count({
          where: { jobId: id, status: JobAttemptStatus.PROCESSING },
        }),
      ).toBe(1);
    } finally {
      releaseHandler();
    }

    expect(await Promise.all([firstExecution, secondExecution])).toEqual([
      "ack",
      "ack",
    ]);
    channel.ack(firstDelivery);
    channel.ack(secondDelivery);
    expect(executions).toBe(1);
    expect(
      await firstClient.jobAttempt.count({
        where: { jobId: id, status: JobAttemptStatus.PROCESSING },
      }),
    ).toBe(0);
    expect(
      await firstClient.jobAttempt.count({
        where: { jobId: id, status: JobAttemptStatus.COMPLETED },
      }),
    ).toBe(1);
  }, 30000);
});
