import { env } from "../config/env";
import { JobConsumer } from "../consumers/job.consumer";
import { JobExecutionRepository } from "../repositories/job-execution.repository";
import { prisma } from "../repositories/prisma";
import { rabbitMQ } from "../queues/rabbitmq";
import { initializeRabbitMQTopology } from "../queues/topology";
import { JobExecutionService } from "../services/job-execution.service";

let consumer: JobConsumer | undefined;
let isShuttingDown = false;
let shutdownPromise: Promise<void> | undefined;

const startWorker = async (): Promise<void> => {
  try {
    const channel = await rabbitMQ.initialize();
    if (isShuttingDown) {
      return;
    }

    await initializeRabbitMQTopology(channel);
    const executionService = new JobExecutionService(
      new JobExecutionRepository(),
    );
    consumer = new JobConsumer(
      channel,
      executionService,
      env.WORKER_PREFETCH,
      env.WORKER_HEARTBEAT_INTERVAL_MS,
    );
    await consumer.start();

    if (isShuttingDown) {
      await consumer.stop();
    }
  } catch (error: unknown) {
    console.error(
      JSON.stringify({
        event: "worker.startup_failed",
        errorName: error instanceof Error ? error.name : "UnknownError",
      }),
    );
    process.exitCode = 1;
    await shutdown();
  }
};

const shutdown = (): Promise<void> => {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  isShuttingDown = true;
  shutdownPromise = (async (): Promise<void> => {
    try {
      await consumer?.stop();
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
    ]);

    results.forEach((result, index) => {
      if (result.status === "rejected") {
        const resource = index === 0 ? "rabbitmq" : "prisma";
        console.error(
          JSON.stringify({
            event: "worker.resource_shutdown_failed",
            resource,
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
  })();

  return shutdownPromise;
};

process.once("SIGINT", () => {
  void shutdown();
});
process.once("SIGTERM", () => {
  void shutdown();
});

void startWorker();
