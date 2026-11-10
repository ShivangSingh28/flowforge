import { env } from "../config/env";
import { JobConsumer } from "../consumers/job.consumer";
import { JobExecutionRepository } from "../repositories/job-execution.repository";
import { OperationsRepository } from "../repositories/operations.repository";
import { prisma } from "../repositories/prisma";
import { rabbitMQ } from "../queues/rabbitmq";
import { initializeRabbitMQTopology } from "../queues/topology";
import { JobExecutionService } from "../services/job-execution.service";
import { abortableDelay } from "../utils/abortable-delay";
import { startProcessHealthServer } from "../utils/process-health";

const abortController = new AbortController();
let consumer: JobConsumer | undefined;
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
    consumer: consumer?.isReady ?? false,
  };
};

const healthServer = startProcessHealthServer(
  env.WORKER_HEALTH_PORT,
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
      event: "worker.reconnect_scheduled",
      attempt: attempt + 1,
      delayMs: delay,
    }),
  );
  await abortableDelay(delay, abortController.signal);
};

const runWorker = async (): Promise<void> => {
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

      await initializeRabbitMQTopology(
        channel,
        env.WORKER_QUEUE,
        env.WORKER_ROUTING_KEY,
      );
      consumer = new JobConsumer(
        channel,
        new JobExecutionService(new JobExecutionRepository()),
        env.WORKER_PREFETCH,
        env.WORKER_HEARTBEAT_INTERVAL_MS,
        env.WORKER_QUEUE,
        env.WORKER_SHUTDOWN_GRACE_MS,
      );
      await consumer.start();
      const connectedAt = Date.now();
      console.info(JSON.stringify({ event: "worker.ready" }));
      await rabbitMQ.waitForDisconnect(abortController.signal);
      if (abortController.signal.aborted) {
        break;
      }

      console.warn(JSON.stringify({ event: "worker.rabbitmq_disconnected" }));
      if (Date.now() - connectedAt >= 60000) {
        disconnectCount = 0;
      }
      await consumer.stop(0);
      consumer = undefined;
      await rabbitMQ.close();
      await waitBeforeReconnect(disconnectCount++);
    } catch (error: unknown) {
      console.error(
        JSON.stringify({
          event: "worker.lifecycle_failed",
          errorName: error instanceof Error ? error.name : "UnknownError",
        }),
      );
      try {
        await consumer?.stop(0);
      } catch (cleanupError: unknown) {
        console.warn(
          JSON.stringify({
            event: "worker.consumer_recovery_cleanup_failed",
            errorName:
              cleanupError instanceof Error
                ? cleanupError.name
                : "UnknownError",
          }),
        );
      }
      consumer = undefined;
      try {
        await rabbitMQ.close();
      } catch (closeError: unknown) {
        console.error(
          JSON.stringify({
            event: "worker.connection_cleanup_failed",
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
    console.error(JSON.stringify({ event: "worker.shutdown_grace_expired" }));
    process.exit(1);
  }, env.WORKER_SHUTDOWN_GRACE_MS + 5000);
};
process.once("SIGINT", requestShutdown);
process.once("SIGTERM", requestShutdown);

void runWorker().finally(async () => {
  if (shutdownTimer) {
    clearTimeout(shutdownTimer);
  }
  try {
    await consumer?.stop(env.WORKER_SHUTDOWN_GRACE_MS);
  } catch (error: unknown) {
    console.error(
      JSON.stringify({
        event: "worker.consumer_shutdown_failed",
        errorName: error instanceof Error ? error.name : "UnknownError",
      }),
    );
    process.exitCode = 1;
  }
  const results = await Promise.allSettled([
    rabbitMQ.close(),
    prisma.$disconnect(),
    closeHealthServer(),
  ]);
  results.forEach((result) => {
    if (result.status === "rejected") {
      console.error(
        JSON.stringify({
          event: "worker.shutdown_resource_failed",
          errorName:
            result.reason instanceof Error
              ? result.reason.name
              : "UnknownError",
        }),
      );
      process.exitCode = 1;
    }
  });
  console.info(JSON.stringify({ event: "worker.shutdown_complete" }));
});
