import { env } from "../config/env";
import { RabbitMQConnectionManager } from "./rabbitmq-connection-manager";

if (!env.RABBITMQ_URL) {
  throw new Error("RABBITMQ_URL is required by the worker and outbox relay");
}

export const rabbitMQ = new RabbitMQConnectionManager(env.RABBITMQ_URL);
