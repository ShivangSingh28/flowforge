import type { JobMessage } from "../types/job-message";
import { DEFAULT_JOB_ROUTING_KEY } from "../config/rabbitmq-topology";
import { JobProducer } from "./job-producer";
import type { RabbitMQConnectionManager } from "../queues/rabbitmq-connection-manager";
import { initializeRabbitMQTopology } from "../queues/topology";

export class OutboxPublisher {
  constructor(
    private readonly connectionManager: RabbitMQConnectionManager,
    private readonly confirmTimeoutMs: number,
    private readonly routingKey = DEFAULT_JOB_ROUTING_KEY,
  ) {}

  async publish(message: JobMessage, eventId: string): Promise<void> {
    try {
      const channel = await this.connectionManager.initialize();
      await initializeRabbitMQTopology(channel);
      await new JobProducer(
        () => channel,
        this.routingKey,
        this.confirmTimeoutMs,
      ).publish(message, eventId);
    } catch (error: unknown) {
      try {
        this.connectionManager.getChannel();
      } catch {
        await this.connectionManager.close();
      }
      throw error;
    }
  }
}
