import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";
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
import { JobPriority } from "@prisma/client";
import { JOBS_EXCHANGE } from "../../src/config/rabbitmq-topology";
import { JobProducer } from "../../src/producers/job-producer";
import { JobRepository } from "../../src/repositories/job.repository";
import { prisma } from "../../src/repositories/prisma";
import { RabbitMQConnectionManager } from "../../src/queues/rabbitmq-connection-manager";
import { initializeRabbitMQTopology } from "../../src/queues/topology";
import type { JobMessage } from "../../src/types/job-message";

const databaseUrl = process.env.DATABASE_URL;
const rabbitMQUrl = process.env.RABBITMQ_URL;
if (!databaseUrl || !rabbitMQUrl) {
  throw new Error("Missing isolated integration service URLs");
}

interface ChildRuntime {
  child: ChildProcess;
  output(): string;
  waitForOutput(marker: string, timeoutMs?: number): Promise<void>;
  waitForHealthPort(timeoutMs?: number): Promise<number>;
  waitForOutputCount(
    marker: string,
    count: number,
    timeoutMs?: number,
  ): Promise<void>;
  sendMessage(message: string): Promise<void>;
  disconnect(): void;
  waitForExit(timeoutMs?: number): Promise<void>;
  signal(name: NodeJS.Signals): Promise<void>;
  kill(): Promise<void>;
  stop(): Promise<void>;
}

const createdJobIds: string[] = [];
const createdQueues: string[] = [];
const integrationComposeArgs = [
  "compose",
  "--env-file",
  ".env.integration",
  "--project-name",
  "flowforge-integration",
  "-f",
  "docker-compose.integration.yml",
];
const broker = new RabbitMQConnectionManager(rabbitMQUrl);
let channel: ConfirmChannel;

const startChild = (
  sourceFile: string,
  overrides: Record<string, string>,
  withIpc = false,
): ChildRuntime => {
  const child = spawn(
    process.execPath,
    ["--import", "tsx", path.join(process.cwd(), sourceFile)],
    {
      cwd: process.cwd(),
      env: {
        ...process.env,
        NODE_ENV: "test",
        DATABASE_URL: databaseUrl,
        RABBITMQ_URL: rabbitMQUrl,
        ...overrides,
      },
      stdio: withIpc
        ? ["ignore", "pipe", "pipe", "ipc"]
        : ["ignore", "pipe", "pipe"],
      windowsHide: true,
    },
  );
  let logs = "";
  let spawnError: Error | undefined;
  let closed = false;
  if (!child.stdout || !child.stderr) {
    throw new Error("Child process output streams were not created");
  }
  const onSpawnError = (error: Error): void => {
    spawnError = error;
  };
  const onStdoutData = (chunk: Buffer): void => {
    logs += chunk.toString();
  };
  const onStderrData = (chunk: Buffer): void => {
    logs += chunk.toString();
  };
  child.on("error", onSpawnError);
  child.stdout.on("data", onStdoutData);
  child.stderr.on("data", onStderrData);
  child.once("close", () => {
    closed = true;
    child.stdout?.off("data", onStdoutData);
    child.stderr?.off("data", onStderrData);
    child.off("error", onSpawnError);
  });

  const waitForOutput = async (
    marker: string,
    timeoutMs = 15000,
  ): Promise<void> => {
    const deadline = Date.now() + timeoutMs;
    while (!logs.includes(marker)) {
      if (spawnError) {
        throw spawnError;
      }
      if (closed) {
        throw new Error(`Child exited before ${marker}: ${logs}`);
      }
      if (Date.now() >= deadline) {
        throw new Error(`Timed out waiting for ${marker}: ${logs}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };
  const waitForHealthPort = async (timeoutMs = 15000): Promise<number> => {
    const marker = '"event":"process.health_started"';
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (spawnError) {
        throw spawnError;
      }
      const line = logs.split(/\r?\n/).find((entry) => entry.includes(marker));
      if (line) {
        try {
          const event: unknown = JSON.parse(line);
          if (
            typeof event === "object" &&
            event !== null &&
            "port" in event &&
            typeof event.port === "number" &&
            Number.isInteger(event.port) &&
            event.port > 0 &&
            event.port <= 65535
          ) {
            return event.port;
          }
        } catch {
          // Wait for the complete JSON log line.
        }
      }
      if (closed) {
        throw new Error(
          `Child exited before reporting its health port: ${logs}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    throw new Error(`Timed out waiting for the child health port: ${logs}`);
  };
  const waitForExit = async (timeoutMs = 15000): Promise<void> => {
    if (closed) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const cleanup = (): void => {
        clearTimeout(timer);
        child.off("close", onClose);
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Child did not exit: ${logs}`));
      }, timeoutMs);
      const onClose = (): void => {
        cleanup();
        resolve();
      };
      child.once("close", onClose);
    });
  };
  const waitForOutputCount = async (
    marker: string,
    count: number,
    timeoutMs = 15000,
  ): Promise<void> => {
    const deadline = Date.now() + timeoutMs;
    const occurrences = (): number => logs.split(marker).length - 1;
    while (occurrences() < count) {
      if (spawnError) {
        throw spawnError;
      }
      if (closed) {
        throw new Error(
          `Child exited before ${marker} occurred ${count} times: ${logs}`,
        );
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `Timed out waiting for ${marker} to occur ${count} times: ${logs}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };

  return {
    child,
    output: () => logs,
    waitForOutput,
    waitForHealthPort,
    waitForOutputCount,
    async sendMessage(message): Promise<void> {
      if (!child.connected) {
        throw new Error("Child process IPC channel is not connected");
      }
      await new Promise<void>((resolve, reject) => {
        const cleanup = (): void => {
          clearTimeout(timer);
          child.off("close", onClose);
        };
        const timer = setTimeout(() => {
          cleanup();
          reject(new Error(`Child IPC send timed out: ${logs}`));
        }, 5000);
        const onClose = (): void => {
          cleanup();
          reject(new Error(`Child exited before IPC send completed: ${logs}`));
        };
        child.once("close", onClose);
        child.send(message, (error) => {
          cleanup();
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
    disconnect(): void {
      if (child.connected) {
        child.disconnect();
      }
    },
    waitForExit,
    async signal(name): Promise<void> {
      if (!closed) {
        child.kill(name);
        await waitForExit();
      }
    },
    async kill(): Promise<void> {
      await this.signal("SIGKILL");
    },
    async stop(): Promise<void> {
      if (closed) {
        return;
      }

      let gracefulError: unknown;
      try {
        child.kill("SIGTERM");
        await waitForExit(3000);
        return;
      } catch (error: unknown) {
        gracefulError = error;
      }

      try {
        if (!closed) {
          child.kill("SIGKILL");
        }
        await waitForExit(5000);
      } catch (forceKillError: unknown) {
        throw new AggregateError(
          [gracefulError, forceKillError],
          "Child process did not exit after graceful and forced termination",
        );
      }
    },
  };
};

const stopChildren = async (
  ...children: Array<ChildRuntime | undefined>
): Promise<void> => {
  const results = await Promise.allSettled(
    children
      .filter((child): child is ChildRuntime => child !== undefined)
      .map((child) => child.stop()),
  );
  const errors = results.flatMap((result) =>
    result.status === "rejected" ? [result.reason] : [],
  );
  if (errors.length > 0) {
    throw new AggregateError(errors, "Failed to clean up child processes");
  }
};

const waitFor = async (
  predicate: () => Promise<boolean>,
  description: string,
  timeoutMs = 15000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

const randomPort = (): string =>
  String(31000 + Math.floor(Math.random() * 20000));

const runIntegrationCompose = (args: string[]): void => {
  const result = spawnSync("docker", [...integrationComposeArgs, ...args], {
    cwd: process.cwd(),
    encoding: "utf8",
  });
  if (result.error || result.status !== 0) {
    throw new Error(
      `Dedicated integration Compose command failed: ${
        result.error?.message ?? result.stderr
      }`,
    );
  }
};

const getHealth = async (
  port: number | string,
  pathName: string,
): Promise<{ status: number; body: Record<string, unknown> }> => {
  const response = await fetch(`http://127.0.0.1:${port}${pathName}`);
  return {
    status: response.status,
    body: (await response.json()) as Record<string, unknown>,
  };
};

const createSubmission = async (
  type: string,
): Promise<{ jobId: string; eventId: string; message: JobMessage }> => {
  const payload = { marker: `process-recovery:${randomUUID()}` };
  const job = await new JobRepository().createSubmission({
    type,
    payload,
    priority: JobPriority.MEDIUM,
    maxAttempts: 3,
    idempotencyKey: `process-recovery:${randomUUID()}`,
  });
  createdJobIds.push(job.id);
  const event = await prisma.outboxEvent.findFirstOrThrow({
    where: { jobId: job.id, eventType: "job.submitted" },
  });
  return {
    jobId: job.id,
    eventId: event.id,
    message: {
      jobId: job.id,
      type: job.type,
      payload,
      priority: job.priority,
      createdAt: job.createdAt.toISOString(),
    },
  };
};

const createQueue = async (routingKey: string): Promise<string> => {
  const queue = `flowforge.process.integration.${randomUUID()}`;
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, JOBS_EXCHANGE, routingKey);
  createdQueues.push(queue);
  return queue;
};

const getMessage = async (
  queue: string,
  timeoutMs = 15000,
): Promise<GetMessage> => {
  let message: GetMessage | false = false;
  await waitFor(
    async () => {
      message = await channel.get(queue, { noAck: false });
      return message !== false;
    },
    `message in ${queue}`,
    timeoutMs,
  );
  if (message === false) {
    throw new Error(`Expected a message in ${queue}`);
  }
  return message;
};

const waitForLeaseExpiry = async (eventId: string): Promise<void> => {
  await waitFor(async () => {
    const event = await prisma.outboxEvent.findUniqueOrThrow({
      where: { id: eventId },
    });
    return event.lockedUntil === null || event.lockedUntil <= new Date();
  }, `outbox lease expiry for ${eventId}`);
};

const cleanCreatedRecords = async (): Promise<void> => {
  if (createdJobIds.length > 0) {
    await prisma.outboxEvent.deleteMany({
      where: { jobId: { in: createdJobIds } },
    });
    await prisma.job.deleteMany({ where: { id: { in: createdJobIds } } });
    createdJobIds.length = 0;
  }
  for (const queue of createdQueues.splice(0)) {
    await channel.deleteQueue(queue);
  }
};

const relayEnvironment = (
  eventId: string,
  routingKey: string,
  overrides: Record<string, string> = {},
): Record<string, string> => ({
  OUTBOX_HEALTH_PORT: "0",
  OUTBOX_ROUTING_KEY: routingKey,
  OUTBOX_LEASE_DURATION_MS: "3000",
  OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS: "500",
  OUTBOX_POLL_INTERVAL_MS: "10",
  OUTBOX_RETRY_BASE_DELAY_MS: "30",
  OUTBOX_RETRY_MAX_DELAY_MS: "1000",
  FLOWFORGE_TEST_RELAY_EVENT_ID: eventId,
  ...overrides,
});

describe("actual child-process recovery against isolated PostgreSQL and RabbitMQ", () => {
  jest.setTimeout(90000);

  beforeAll(async () => {
    const databases = await prisma.$queryRaw<Array<{ database: string }>>`
      SELECT current_database() AS database
    `;
    if (databases[0]?.database !== "flowforge_integration") {
      throw new Error(
        "Process tests require the isolated flowforge_integration database",
      );
    }
    channel = await broker.initialize();
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
      await Promise.all([broker.close(), prisma.$disconnect()]);
    }
  });

  it("starts the API without any RabbitMQ configuration", async () => {
    const api = startChild("src/server.ts", {
      HOST: "127.0.0.1",
      PORT: String(randomPort()),
      RABBITMQ_URL: "",
    });
    try {
      await api.waitForOutput("FlowForge API listening");
    } finally {
      await stopChildren(api);
    }
  });

  it("reconnects a worker after recreating only the isolated RabbitMQ service", async () => {
    const routingKey = `flowforge.process.broker-recovery.${randomUUID()}`;
    const queue = `flowforge.process.integration.${randomUUID()}`;
    createdQueues.push(queue);
    const submission = await createSubmission("crash-test");
    const worker = startChild("src/workers/worker.ts", {
      WORKER_HEALTH_PORT: "0",
      WORKER_QUEUE: queue,
      WORKER_ROUTING_KEY: routingKey,
      WORKER_LEASE_DURATION_MS: "4000",
      WORKER_HEARTBEAT_INTERVAL_MS: "500",
      WORKER_CRASH_TEST_DURATION_MS: "100",
      RABBITMQ_RECONNECT_BASE_DELAY_MS: "50",
      RABBITMQ_RECONNECT_MAX_DELAY_MS: "200",
    });

    try {
      await worker.waitForOutput('"event":"worker.ready"');

      runIntegrationCompose(["rm", "--stop", "--force", "rabbitmq"]);
      runIntegrationCompose([
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "120",
        "rabbitmq",
      ]);

      await worker.waitForOutput('"event":"worker.rabbitmq_disconnected"');
      await worker.waitForOutputCount('"event":"worker.ready"', 2, 30000);
      await waitFor(
        async () => !broker.isConnected,
        "integration publisher noticing RabbitMQ service loss",
      );
      channel = await broker.initialize();
      await initializeRabbitMQTopology(channel);
      await channel.checkExchange(JOBS_EXCHANGE);
      const restoredQueue = await channel.checkQueue(queue);
      expect(restoredQueue.queue).toBe(queue);

      const readiness = await getHealth(
        await worker.waitForHealthPort(),
        "/health/ready",
      );
      expect(readiness).toMatchObject({
        status: 200,
        body: { status: "ok", database: true, rabbitmq: true, consumer: true },
      });

      await new JobProducer(() => channel, routingKey).publish(
        submission.message,
        submission.eventId,
      );
      await waitFor(async () => {
        const job = await prisma.job.findUnique({
          where: { id: submission.jobId },
        });
        return job?.status === "COMPLETED";
      }, "job consumption after RabbitMQ service recreation");
      const recovered = await prisma.job.findUniqueOrThrow({
        where: { id: submission.jobId },
        include: { jobAttempts: true },
      });
      expect(recovered.jobAttempts).toHaveLength(1);
      expect(recovered.jobAttempts[0]?.status).toBe("COMPLETED");
    } finally {
      await stopChildren(worker);
    }
  });

  it("recovers a job after force-terminating the actual worker during execution", async () => {
    const routingKey = `flowforge.process.worker.${randomUUID()}`;
    const queue = await createQueue(routingKey);
    const { jobId, eventId, message } = await createSubmission("crash-test");
    const producer = new JobProducer(() => channel, routingKey);
    const options = {
      WORKER_HEALTH_PORT: "0",
      WORKER_QUEUE: queue,
      WORKER_ROUTING_KEY: routingKey,
      WORKER_LEASE_DURATION_MS: "2000",
      WORKER_HEARTBEAT_INTERVAL_MS: "300",
      WORKER_CRASH_TEST_DURATION_MS: "800",
      WORKER_SHUTDOWN_GRACE_MS: "1000",
      RABBITMQ_RECONNECT_BASE_DELAY_MS: "20",
      RABBITMQ_RECONNECT_MAX_DELAY_MS: "100",
    };
    let firstWorker: ChildRuntime | undefined;
    let recoveryWorker: ChildRuntime | undefined;
    try {
      firstWorker = startChild("src/workers/worker.ts", options);
      await firstWorker.waitForOutput('"event":"worker.ready"');
      const readiness = await getHealth(
        await firstWorker.waitForHealthPort(),
        "/health/ready",
      );
      expect(readiness).toMatchObject({
        status: 200,
        body: { status: "ok", database: true, rabbitmq: true, consumer: true },
      });
      await producer.publish(message, eventId);
      await firstWorker.waitForOutput('"event":"worker.crash_test_started"');
      await firstWorker.kill();

      recoveryWorker = startChild("src/workers/worker.ts", options);
      await recoveryWorker.waitForOutput('"event":"worker.ready"');
      await waitFor(
        async () => {
          const job = await prisma.job.findUnique({ where: { id: jobId } });
          return job?.status === "COMPLETED";
        },
        "job completion after actual worker termination",
        20000,
      );

      const job = await prisma.job.findUniqueOrThrow({
        where: { id: jobId },
        include: { jobAttempts: { orderBy: { attemptNumber: "asc" } } },
      });
      expect(job.attempts).toBe(2);
      expect(job.jobAttempts.map((attempt) => attempt.status)).toEqual([
        "FAILED",
        "COMPLETED",
      ]);
    } finally {
      await stopChildren(firstWorker, recoveryWorker);
    }
  });

  it("runs multiple worker processes concurrently without parallel attempts for one job", async () => {
    const routingKey = `flowforge.process.workers.${randomUUID()}`;
    const queue = await createQueue(routingKey);
    const first = await createSubmission("crash-test");
    const second = await createSubmission("crash-test");
    const options = {
      WORKER_HEALTH_PORT: "0",
      WORKER_QUEUE: queue,
      WORKER_ROUTING_KEY: routingKey,
      WORKER_LEASE_DURATION_MS: "2500",
      WORKER_HEARTBEAT_INTERVAL_MS: "300",
      WORKER_CRASH_TEST_DURATION_MS: "1000",
      WORKER_SHUTDOWN_GRACE_MS: "1500",
    };
    let firstWorker: ChildRuntime | undefined;
    let secondWorker: ChildRuntime | undefined;
    try {
      firstWorker = startChild("src/workers/worker.ts", options);
      secondWorker = startChild("src/workers/worker.ts", {
        ...options,
        WORKER_HEALTH_PORT: "0",
      });
      await Promise.all([
        firstWorker.waitForOutput('"event":"worker.ready"'),
        secondWorker.waitForOutput('"event":"worker.ready"'),
      ]);
      const producer = new JobProducer(() => channel, routingKey);
      await producer.publish(first.message, first.eventId);
      await producer.publish(second.message, second.eventId);

      await Promise.all([
        firstWorker.waitForOutput('"event":"worker.crash_test_started"'),
        secondWorker.waitForOutput('"event":"worker.crash_test_started"'),
      ]);
      await waitFor(async () => {
        const jobs = await prisma.job.findMany({
          where: { id: { in: [first.jobId, second.jobId] } },
          select: { status: true },
        });
        return (
          jobs.length === 2 && jobs.every((job) => job.status === "COMPLETED")
        );
      }, "both jobs completing across worker processes");

      for (const id of [first.jobId, second.jobId]) {
        const attempts = await prisma.jobAttempt.findMany({
          where: { jobId: id, status: "PROCESSING" },
        });
        expect(attempts).toHaveLength(0);
        expect(
          await prisma.jobAttempt.count({
            where: { jobId: id, status: "COMPLETED" },
          }),
        ).toBe(1);
      }
    } finally {
      await stopChildren(firstWorker, secondWorker);
    }
  });

  it("recovers unfinished work after actual worker interruption", async () => {
    const routingKey = `flowforge.process.shutdown.${randomUUID()}`;
    const queue = await createQueue(routingKey);
    const submission = await createSubmission("crash-test");
    const options = {
      WORKER_HEALTH_PORT: "0",
      WORKER_QUEUE: queue,
      WORKER_ROUTING_KEY: routingKey,
      WORKER_LEASE_DURATION_MS: "2000",
      WORKER_HEARTBEAT_INTERVAL_MS: "300",
      WORKER_CRASH_TEST_DURATION_MS: "10000",
      WORKER_SHUTDOWN_GRACE_MS: "500",
    };
    let stoppingWorker: ChildRuntime | undefined;
    let recoveryWorker: ChildRuntime | undefined;
    try {
      stoppingWorker = startChild("src/workers/worker.ts", options);
      await stoppingWorker.waitForOutput('"event":"worker.ready"');
      await new JobProducer(() => channel, routingKey).publish(
        submission.message,
        submission.eventId,
      );
      await stoppingWorker.waitForOutput('"event":"worker.crash_test_started"');
      await stoppingWorker.kill();

      const interrupted = await prisma.job.findUniqueOrThrow({
        where: { id: submission.jobId },
        include: { jobAttempts: true },
      });
      expect(interrupted.status).toBe("PROCESSING");
      expect(interrupted.jobAttempts[0]?.status).toBe("PROCESSING");

      recoveryWorker = startChild("src/workers/worker.ts", {
        ...options,
        WORKER_CRASH_TEST_DURATION_MS: "500",
        WORKER_HEALTH_PORT: "0",
      });
      await recoveryWorker.waitForOutput('"event":"worker.ready"');
      await waitFor(
        async () => {
          const job = await prisma.job.findUnique({
            where: { id: submission.jobId },
          });
          return job?.status === "COMPLETED";
        },
        "completion after worker interruption",
        15000,
      );
      const recovered = await prisma.job.findUniqueOrThrow({
        where: { id: submission.jobId },
        include: { jobAttempts: { orderBy: { attemptNumber: "asc" } } },
      });
      expect(recovered.attempts).toBe(2);
      expect(recovered.jobAttempts.map((attempt) => attempt.status)).toEqual([
        "FAILED",
        "COMPLETED",
      ]);
    } finally {
      await stopChildren(stoppingWorker, recoveryWorker);
    }
  });

  it("runs two relay processes concurrently on disjoint leased events", async () => {
    const routingKey = `flowforge.process.relays.${randomUUID()}`;
    const queue = await createQueue(routingKey);
    const first = await createSubmission("demo");
    const second = await createSubmission("demo");
    const pause = { FLOWFORGE_TEST_RELAY_PAUSE_BEFORE_PUBLISH_MS: "15000" };
    const firstOptions = relayEnvironment(first.eventId, routingKey, {
      OUTBOX_BATCH_SIZE: "1",
      OUTBOX_LEASE_DURATION_MS: "30000",
      ...pause,
    });
    const secondOptions = relayEnvironment(second.eventId, routingKey, {
      OUTBOX_BATCH_SIZE: "1",
      OUTBOX_LEASE_DURATION_MS: "30000",
      ...pause,
    });
    let firstRelay: ChildRuntime | undefined;
    let secondRelay: ChildRuntime | undefined;
    let firstMessage: GetMessage | undefined;
    let secondMessage: GetMessage | undefined;
    try {
      firstRelay = startChild("src/relay/relay.ts", firstOptions);
      await firstRelay.waitForOutput('"event":"outbox.test_before_publish"');
      secondRelay = startChild("src/relay/relay.ts", secondOptions);
      await secondRelay.waitForOutput('"event":"outbox.test_before_publish"');

      const [firstClaim, secondClaim] = await Promise.all([
        prisma.outboxEvent.findUniqueOrThrow({ where: { id: first.eventId } }),
        prisma.outboxEvent.findUniqueOrThrow({ where: { id: second.eventId } }),
      ]);
      expect(firstClaim.lockToken).not.toBeNull();
      expect(secondClaim.lockToken).not.toBeNull();
      expect(firstClaim.lockToken).not.toBe(secondClaim.lockToken);

      await waitFor(
        async () => {
          const events = await prisma.outboxEvent.findMany({
            where: { id: { in: [first.eventId, second.eventId] } },
            select: { publishedAt: true },
          });
          return (
            events.length === 2 && events.every((event) => event.publishedAt)
          );
        },
        "both relay processes publishing their events",
        30000,
      );

      firstMessage = await getMessage(queue);
      secondMessage = await getMessage(queue);
      expect(
        [
          firstMessage.properties.messageId,
          secondMessage.properties.messageId,
        ].sort(),
      ).toEqual([first.eventId, second.eventId].sort());
    } finally {
      if (firstMessage) {
        channel.ack(firstMessage);
      }
      if (secondMessage) {
        channel.ack(secondMessage);
      }
      await stopChildren(firstRelay, secondRelay);
    }
  });

  it("gracefully drains a confirmed in-flight publication before relay exit", async () => {
    const routingKey = `flowforge.process.relay.graceful.${randomUUID()}`;
    const queue = await createQueue(routingKey);
    const submission = await createSubmission("demo");
    const relay = startChild(
      "src/relay/relay.ts",
      relayEnvironment(submission.eventId, routingKey, {
        OUTBOX_LEASE_DURATION_MS: "10000",
        OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS: "2000",
        OUTBOX_SHUTDOWN_GRACE_MS: "5000",
        FLOWFORGE_TEST_RELAY_WAIT_AFTER_CONFIRM: "true",
      }),
      true,
    );
    let message: GetMessage | undefined;

    try {
      await relay.waitForOutput('"event":"outbox.test_after_confirm"');
      const eventAtConfirm = await prisma.outboxEvent.findUniqueOrThrow({
        where: { id: submission.eventId },
      });
      expect(eventAtConfirm.publishedAt).toBeNull();
      expect(eventAtConfirm.lockToken).not.toBeNull();

      message = await getMessage(queue);
      expect(message.properties.messageId).toBe(submission.eventId);

      await relay.sendMessage("FLOWFORGE_TEST_GRACEFUL_SHUTDOWN");
      await relay.waitForOutput(
        '"event":"outbox.test_graceful_shutdown_received"',
      );
      const eventDuringShutdown = await prisma.outboxEvent.findUniqueOrThrow({
        where: { id: submission.eventId },
      });
      expect(eventDuringShutdown.publishedAt).toBeNull();
      expect(eventDuringShutdown.lockToken).toBe(eventAtConfirm.lockToken);

      await relay.sendMessage("FLOWFORGE_TEST_RELEASE_PUBLICATION");
      await relay.waitForOutput('"event":"outbox.test_publication_released"');
      relay.disconnect();
      await relay.waitForOutput('"event":"outbox.relay_stopped"');
      await relay.waitForExit();

      const eventAfterShutdown = await prisma.outboxEvent.findUniqueOrThrow({
        where: { id: submission.eventId },
      });
      expect(eventAfterShutdown.publishedAt).not.toBeNull();
      expect(eventAfterShutdown.lockToken).toBeNull();
      expect(eventAfterShutdown.lockedUntil).toBeNull();
      expect(eventAfterShutdown.lastError).toBeNull();
      expect(await channel.get(queue, { noAck: false })).toBe(false);
    } finally {
      if (message) {
        channel.ack(message);
      }
      await stopChildren(relay);
    }
  });

  it("recovers after actual relay termination before publish and republishes after confirm-before-DB crash", async () => {
    const beforeKey = `flowforge.process.relay.before.${randomUUID()}`;
    const beforeQueue = await createQueue(beforeKey);
    const before = await createSubmission("demo");
    let killedBefore: ChildRuntime | undefined;
    let recoveredBefore: ChildRuntime | undefined;
    try {
      const beforeEnvironment = relayEnvironment(before.eventId, beforeKey, {
        FLOWFORGE_TEST_RELAY_PAUSE_BEFORE_PUBLISH_MS: "60000",
      });
      killedBefore = startChild("src/relay/relay.ts", beforeEnvironment);
      await killedBefore.waitForOutput('"event":"outbox.test_before_publish"');
      const relayReadiness = await getHealth(
        await killedBefore.waitForHealthPort(),
        "/health/ready",
      );
      expect(relayReadiness).toMatchObject({
        status: 200,
        body: { status: "ok", database: true, rabbitmq: true, publisher: true },
      });
      const operational = await getHealth(
        await killedBefore.waitForHealthPort(),
        "/health/operations",
      );
      expect(operational.body).toHaveProperty("pendingOutboxEvents");
      expect(operational.body).toHaveProperty("oldestUnpublishedEventAgeMs");
      expect(operational.body).toHaveProperty("failedJobs");
      const claimed = await prisma.outboxEvent.findUniqueOrThrow({
        where: { id: before.eventId },
      });
      expect(claimed.publishedAt).toBeNull();
      expect(claimed.lockToken).not.toBeNull();
      await killedBefore.kill();
      await waitForLeaseExpiry(before.eventId);

      recoveredBefore = startChild(
        "src/relay/relay.ts",
        relayEnvironment(before.eventId, beforeKey),
      );
      await recoveredBefore.waitForOutput('"event":"outbox.relay_ready"');
      await waitFor(async () => {
        const event = await prisma.outboxEvent.findUniqueOrThrow({
          where: { id: before.eventId },
        });
        return event.publishedAt !== null;
      }, "publication after actual pre-publish relay termination");
      const recoveredMessage = await getMessage(beforeQueue);
      expect(recoveredMessage.properties.messageId).toBe(before.eventId);
      channel.ack(recoveredMessage);
    } finally {
      await stopChildren(killedBefore, recoveredBefore);
    }

    const afterKey = `flowforge.process.relay.after.${randomUUID()}`;
    const afterQueue = await createQueue(afterKey);
    const after = await createSubmission("demo");
    let killedAfter: ChildRuntime | undefined;
    let recoveredAfter: ChildRuntime | undefined;
    let firstCopy: GetMessage | undefined;
    let secondCopy: GetMessage | undefined;
    try {
      const afterEnvironment = relayEnvironment(after.eventId, afterKey, {
        FLOWFORGE_TEST_RELAY_PAUSE_AFTER_CONFIRM_MS: "60000",
      });
      killedAfter = startChild("src/relay/relay.ts", afterEnvironment);
      await killedAfter.waitForOutput('"event":"outbox.test_after_confirm"');
      const eventBeforeCompletion = await prisma.outboxEvent.findUniqueOrThrow({
        where: { id: after.eventId },
      });
      expect(eventBeforeCompletion.publishedAt).toBeNull();
      firstCopy = await getMessage(afterQueue);
      expect(firstCopy.properties.messageId).toBe(after.eventId);
      await killedAfter.kill();
      await waitForLeaseExpiry(after.eventId);

      recoveredAfter = startChild(
        "src/relay/relay.ts",
        relayEnvironment(after.eventId, afterKey),
      );
      await recoveredAfter.waitForOutput('"event":"outbox.relay_ready"');
      await waitFor(async () => {
        const event = await prisma.outboxEvent.findUniqueOrThrow({
          where: { id: after.eventId },
        });
        return event.publishedAt !== null;
      }, "database completion after actual post-confirm relay termination");
      secondCopy = await getMessage(afterQueue);
      expect(secondCopy.properties.messageId).toBe(after.eventId);
      expect(secondCopy.properties.messageId).toBe(
        firstCopy.properties.messageId,
      );
    } finally {
      if (firstCopy) {
        channel.ack(firstCopy);
      }
      if (secondCopy) {
        channel.ack(secondCopy);
      }
      await stopChildren(killedAfter, recoveredAfter);
    }
  });
});
