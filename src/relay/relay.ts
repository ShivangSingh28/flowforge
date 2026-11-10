import { env } from "../config/env";
import { OutboxPublisher } from "../producers/outbox-publisher";
import { OperationsRepository } from "../repositories/operations.repository";
import { OutboxRepository } from "../repositories/outbox.repository";
import { prisma } from "../repositories/prisma";
import { rabbitMQ } from "../queues/rabbitmq";
import { initializeRabbitMQTopology } from "../queues/topology";
import { OutboxRelayService } from "../services/outbox-relay.service";
import type { JobMessage } from "../types/job-message";
import { abortableDelay } from "../utils/abortable-delay";
import { startProcessHealthServer } from "../utils/process-health";

const abortController = new AbortController();
let topologyReady = false;
let shutdownTimer: NodeJS.Timeout | undefined;

const readiness = async (): Promise<Record<string, boolean>> => {
  let database = false;
  try {
    await prisma.$queryRaw`SELECT 1`;
    database = true;
  } catch {
    database = false;
  }
  return {
    database,
    rabbitmq: rabbitMQ.isConnected,
    publisher: topologyReady && rabbitMQ.isConnected,
  };
};

const healthServer = startProcessHealthServer(
  env.OUTBOX_HEALTH_PORT,
  readiness,
  async () => ({ ...(await new OperationsRepository().getMetrics()) }),
);

const closeHealthServer = async (): Promise<void> =>
  new Promise((resolve, reject) => {
    healthServer.close((error) => (error ? reject(error) : resolve()));
  });

const waitBeforeReconnect = async (attempt: number): Promise<void> => {
  const maximum = Math.min(
    env.RABBITMQ_RECONNECT_MAX_DELAY_MS,
    env.RABBITMQ_RECONNECT_BASE_DELAY_MS * 2 ** Math.min(attempt, 30),
  );
  const delay = Math.floor(maximum * (0.5 + Math.random() * 0.5));
  console.warn(
    JSON.stringify({
      event: "outbox.reconnect_scheduled",
      attempt: attempt + 1,
      delayMs: delay,
    }),
  );
  await abortableDelay(delay, abortController.signal);
};

const createPublisher = (): {
  publish(message: JobMessage, eventId: string): Promise<void>;
} => {
  const publisher = new OutboxPublisher(
    rabbitMQ,
    env.OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS,
    env.OUTBOX_ROUTING_KEY,
  );
  const testPause = (name: string): number =>
    env.NODE_ENV === "test" ? Number(process.env[name] ?? 0) : 0;
  const pause = async (event: string, milliseconds: number): Promise<void> => {
    const waitForTestRelease =
      env.NODE_ENV === "test" &&
      event === "outbox.test_after_confirm" &&
      process.env.FLOWFORGE_TEST_RELAY_WAIT_AFTER_CONFIRM === "true";
    if (
      !waitForTestRelease &&
      (!Number.isFinite(milliseconds) || milliseconds <= 0)
    ) {
      return;
    }
    console.info(JSON.stringify({ event, delayMs: milliseconds }));
    if (waitForTestRelease) {
      await new Promise<void>((resolve) => {
        const onMessage = (message: unknown): void => {
          if (message === "FLOWFORGE_TEST_RELEASE_PUBLICATION") {
            process.off("message", onMessage);
            console.info(
              JSON.stringify({ event: "outbox.test_publication_released" }),
            );
            resolve();
            if (process.connected) {
              process.disconnect();
            }
          }
        };
        process.on("message", onMessage);
      });
    }
    if (
      waitForTestRelease ||
      !Number.isFinite(milliseconds) ||
      milliseconds <= 0
    ) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
  };
  return {
    async publish(message, eventId): Promise<void> {
      await pause(
        "outbox.test_before_publish",
        testPause("FLOWFORGE_TEST_RELAY_PAUSE_BEFORE_PUBLISH_MS"),
      );
      await publisher.publish(message, eventId);
      await pause(
        "outbox.test_after_confirm",
        testPause("FLOWFORGE_TEST_RELAY_PAUSE_AFTER_CONFIRM_MS"),
      );
    },
  };
};

const runRelay = async (): Promise<void> => {
  let disconnectCount = 0;
  while (!abortController.signal.aborted) {
    try {
      const channel = await rabbitMQ.initializeWithRetry(
        abortController.signal,
        env.RABBITMQ_RECONNECT_BASE_DELAY_MS,
        env.RABBITMQ_RECONNECT_MAX_DELAY_MS,
      );
      if (!channel || abortController.signal.aborted) {
        break;
      }

      topologyReady = false;
      await initializeRabbitMQTopology(
        channel,
        env.WORKER_QUEUE,
        env.OUTBOX_ROUTING_KEY,
      );
      topologyReady = true;
      const relay = new OutboxRelayService(
        new OutboxRepository(),
        createPublisher(),
        {
          batchSize: env.OUTBOX_BATCH_SIZE,
          leaseDurationMs: env.OUTBOX_LEASE_DURATION_MS,
          retryBaseDelayMs: env.OUTBOX_RETRY_BASE_DELAY_MS,
          retryMaxDelayMs: env.OUTBOX_RETRY_MAX_DELAY_MS,
          pollIntervalMs: env.OUTBOX_POLL_INTERVAL_MS,
          ...(env.NODE_ENV === "test" &&
          process.env.FLOWFORGE_TEST_RELAY_EVENT_ID
            ? { onlyEventId: process.env.FLOWFORGE_TEST_RELAY_EVENT_ID }
            : {}),
        },
      );
      console.info(JSON.stringify({ event: "outbox.relay_ready" }));
      const connectedAt = Date.now();
      const runController = new AbortController();
      const stopRun = (): void => runController.abort();
      abortController.signal.addEventListener("abort", stopRun, { once: true });
      const run = relay.run(runController.signal);
      await rabbitMQ.waitForDisconnect(abortController.signal);
      runController.abort();
      abortController.signal.removeEventListener("abort", stopRun);
      await run;
      if (abortController.signal.aborted) {
        break;
      }

      topologyReady = false;
      console.warn(JSON.stringify({ event: "outbox.rabbitmq_disconnected" }));
      if (Date.now() - connectedAt >= 60000) {
        disconnectCount = 0;
      }
      await rabbitMQ.close();
      await waitBeforeReconnect(disconnectCount++);
    } catch (error: unknown) {
      topologyReady = false;
      console.error(
        JSON.stringify({
          event: "outbox.relay_lifecycle_failed",
          errorName: error instanceof Error ? error.name : "UnknownError",
        }),
      );
      try {
        await rabbitMQ.close();
      } catch (closeError: unknown) {
        console.error(
          JSON.stringify({
            event: "outbox.connection_cleanup_failed",
            errorName:
              closeError instanceof Error ? closeError.name : "UnknownError",
          }),
        );
      }
      if (!abortController.signal.aborted) {
        await waitBeforeReconnect(disconnectCount++);
      }
    }
  }
};

const requestShutdown = (): void => {
  abortController.abort();
  shutdownTimer ??= setTimeout(() => {
    console.error(
      JSON.stringify({ event: "outbox.relay_shutdown_grace_expired" }),
    );
    process.exit(1);
  }, env.OUTBOX_SHUTDOWN_GRACE_MS + 5000);
};
if (env.NODE_ENV === "test" && process.send) {
  process.on("message", (message: unknown) => {
    if (message === "FLOWFORGE_TEST_GRACEFUL_SHUTDOWN") {
      console.info(
        JSON.stringify({ event: "outbox.test_graceful_shutdown_received" }),
      );
      requestShutdown();
    }
  });
}
process.once("SIGINT", requestShutdown);
process.once("SIGTERM", requestShutdown);

void runRelay().finally(async () => {
  if (shutdownTimer) {
    clearTimeout(shutdownTimer);
  }
  topologyReady = false;
  const results = await Promise.allSettled([
    rabbitMQ.close(),
    prisma.$disconnect(),
    closeHealthServer(),
  ]);
  results.forEach((result) => {
    if (result.status === "rejected") {
      console.error(
        JSON.stringify({
          event: "outbox.relay_shutdown_failed",
          errorName:
            result.reason instanceof Error
              ? result.reason.name
              : "UnknownError",
        }),
      );
      process.exitCode = 1;
    }
  });
  console.info(JSON.stringify({ event: "outbox.relay_stopped" }));
});
