import { env } from "../config/env";
import { JobProducer } from "../producers/job-producer";
import { RabbitMQConnectionManager } from "./rabbitmq-connection-manager";

export const rabbitMQ = new RabbitMQConnectionManager(env.RABBITMQ_URL);

export const jobProducer = new JobProducer(() => rabbitMQ.getChannel());
