import type { Server } from "node:http";
import { app } from "./app";
import { env } from "./config/env";
import { prisma } from "./repositories/prisma";
import { rabbitMQ } from "./queues/rabbitmq";
import { initializeRabbitMQTopology } from "./queues/topology";

let server: Server | undefined;
let isShuttingDown = false;

const start = async (): Promise<void> => {
  try {
    const channel = await rabbitMQ.initialize();
    await initializeRabbitMQTopology(channel);
  } catch {
    console.error(
      "Application startup aborted because RabbitMQ initialization failed",
    );
    const cleanupResults = await Promise.allSettled([
      rabbitMQ.close(),
      prisma.$disconnect(),
    ]);

    cleanupResults.forEach((result, index) => {
      if (result.status === "rejected") {
        const resource = index === 0 ? "RabbitMQ" : "Prisma";
        console.error(`Failed to clean up ${resource} after startup failure`);
      }
    });

    process.exitCode = 1;
    return;
  }

  if (isShuttingDown) {
    return;
  }

  server = app.listen(env.PORT, env.HOST, () => {
    console.info(`FlowForge API listening on http://${env.HOST}:${env.PORT}`);
  });
};

void start();

const shutdown = (signal: NodeJS.Signals): void => {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  console.info(`${signal} received; closing application resources`);

  const timeout = setTimeout(() => {
    console.error("Graceful shutdown timed out; closing HTTP connections");
    server?.closeAllConnections();
    process.exitCode = 1;
  }, 10_000);
  timeout.unref();

  const closeHttpServer = server
    ? new Promise<void>((resolve, reject) => {
        server?.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      })
    : Promise.resolve();

  const shutdownResources = async (): Promise<void> => {
    const [httpResult] = await Promise.allSettled([closeHttpServer]);
    const dependencyResults = await Promise.allSettled([
      rabbitMQ.close(),
      prisma.$disconnect(),
    ]);

    clearTimeout(timeout);

    const resourceNames = ["HTTP server", "RabbitMQ", "Prisma"];
    [httpResult, ...dependencyResults].forEach((result, index) => {
      if (result.status === "rejected") {
        console.error(
          `Failed to close ${resourceNames[index]} cleanly`,
          result.reason,
        );
        process.exitCode = 1;
      }
    });

    console.info("Application shutdown complete");
  };

  void shutdownResources();
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
