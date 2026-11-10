import { EventEmitter } from "node:events";
import { describe, expect, it, jest } from "@jest/globals";
import type { ConfirmChannel } from "amqplib";
import {
  DEFAULT_JOB_ROUTING_KEY,
  JOBS_EXCHANGE,
} from "../src/config/rabbitmq-topology";
import { JobProducer } from "../src/producers/job-producer";
import type { JobMessage } from "../src/types/job-message";

const createChannel = (publish: ConfirmChannel["publish"]): ConfirmChannel =>
  Object.assign(new EventEmitter(), { publish }) as unknown as ConfirmChannel;

const message: JobMessage = {
  jobId: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
  type: "email.send",
  payload: { recipient: "test@example.com" },
  priority: "HIGH",
  createdAt: "2026-10-31T12:00:00.000Z",
};

describe("JobProducer", () => {
  it("publishes persistent JSON and resolves after broker confirmation", async () => {
    const publish = jest.fn<ConfirmChannel["publish"]>(
      (_exchange, _routingKey, _content, _options, confirm) => {
        confirm?.(null, {});
        return true;
      },
    );
    const channel = createChannel(publish);
    const producer = new JobProducer(() => channel);

    await expect(producer.publish(message)).resolves.toBeUndefined();

    expect(publish).toHaveBeenCalledTimes(1);
    const [exchange, routingKey, content, options] = publish.mock.calls[0];
    expect(exchange).toBe(JOBS_EXCHANGE);
    expect(routingKey).toBe(DEFAULT_JOB_ROUTING_KEY);
    expect(JSON.parse(content.toString())).toEqual(message);
    expect(options).toEqual({
      persistent: true,
      contentType: "application/json",
      messageId: message.jobId,
    });
  });

  it("rejects negative broker acknowledgements", async () => {
    const publish = jest.fn<ConfirmChannel["publish"]>(
      (_exchange, _routingKey, _content, _options, confirm) => {
        confirm?.(new Error("Message was negatively acknowledged"), {});
        return true;
      },
    );
    const producer = new JobProducer(() => createChannel(publish));

    await expect(producer.publish(message)).rejects.toThrow(
      "Message was negatively acknowledged",
    );
  });

  it("rejects synchronous publish failures", async () => {
    const publish = jest
      .fn<ConfirmChannel["publish"]>()
      .mockImplementation(() => {
        throw new Error("Publish failed");
      });
    const producer = new JobProducer(() => createChannel(publish));

    await expect(producer.publish(message)).rejects.toThrow("Publish failed");
  });

  it("waits for backpressure to drain as well as broker confirmation", async () => {
    let confirm: ((error: unknown, reply: object) => void) | undefined;
    const publish = jest.fn<ConfirmChannel["publish"]>(
      (_exchange, _routingKey, _content, _options, callback) => {
        confirm = callback as typeof confirm;
        return false;
      },
    );
    const channel = createChannel(publish);
    const producer = new JobProducer(() => channel);
    let settled = false;
    const publishing = producer.publish(message).then(() => {
      settled = true;
    });

    confirm?.(null, {});
    await Promise.resolve();
    expect(settled).toBe(false);

    channel.emit("drain");
    await publishing;
    expect(settled).toBe(true);
  });
});
