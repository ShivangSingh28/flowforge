import { EventEmitter } from "node:events";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import type { ConsumeMessage, ConfirmChannel } from "amqplib";
import { JobConsumer } from "../src/consumers/job.consumer";
import type { JobExecutionService } from "../src/services/job-execution.service";

const validMessage = {
  jobId: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
  type: "demo",
  payload: { value: 1 },
  priority: "MEDIUM",
  createdAt: "2026-10-31T12:00:00.000Z",
};

const createDelivery = (content: string): ConsumeMessage =>
  ({
    content: Buffer.from(content),
    fields: {},
    properties: {},
  }) as ConsumeMessage;

const createHarness = (
  execute: JobExecutionService["execute"] = jest
    .fn<JobExecutionService["execute"]>()
    .mockResolvedValue("ack"),
) => {
  let deliver: (message: ConsumeMessage | null) => void = () => {};
  const channel = Object.assign(new EventEmitter(), {
    prefetch: jest.fn<ConfirmChannel["prefetch"]>().mockResolvedValue({}),
    consume: jest
      .fn<ConfirmChannel["consume"]>()
      .mockImplementation(async (_queue, callback) => {
        deliver = callback;
        return { consumerTag: "worker-test" };
      }),
    cancel: jest.fn<ConfirmChannel["cancel"]>().mockResolvedValue({}),
    ack: jest.fn<ConfirmChannel["ack"]>(),
    nack: jest.fn<ConfirmChannel["nack"]>(),
    reject: jest.fn<ConfirmChannel["reject"]>(),
  }) as unknown as ConfirmChannel;
  const consumer = new JobConsumer(
    channel,
    { execute } as JobExecutionService,
    1,
    1,
  );

  return {
    channel,
    consumer,
    deliver: (message: ConsumeMessage) => deliver(message),
  };
};

describe("JobConsumer", () => {
  beforeEach(() => {
    jest.spyOn(console, "info").mockImplementation(() => {});
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("uses configured prefetch and manual acknowledgements", async () => {
    const { channel, consumer } = createHarness();

    await consumer.start();

    expect(channel.prefetch).toHaveBeenCalledWith(1);
    expect(channel.consume).toHaveBeenCalledWith(
      "flowforge.jobs.default",
      expect.any(Function),
      { noAck: false },
    );
  });

  it("ACKs only after execution and database completion finish", async () => {
    let finishExecution: ((value: "ack") => void) | undefined;
    const execute = jest.fn<JobExecutionService["execute"]>(
      () =>
        new Promise((resolve) => {
          finishExecution = resolve;
        }),
    );
    const { channel, consumer, deliver } = createHarness(execute);
    const delivery = createDelivery(JSON.stringify(validMessage));
    await consumer.start();

    deliver(delivery);
    await Promise.resolve();
    expect(channel.ack).not.toHaveBeenCalled();

    finishExecution?.("ack");
    await Promise.resolve();
    await consumer.stop();
    expect(channel.ack).toHaveBeenCalledTimes(1);
    expect(channel.nack).not.toHaveBeenCalled();
  });

  it("rejects malformed messages without requeue", async () => {
    const execute = jest.fn<JobExecutionService["execute"]>();
    const { channel, consumer, deliver } = createHarness(execute);
    const delivery = createDelivery("{");
    await consumer.start();

    deliver(delivery);
    await consumer.stop();

    expect(channel.reject).toHaveBeenCalledWith(delivery, false);
    expect(execute).not.toHaveBeenCalled();
  });

  it("requeues deliveries when database processing fails", async () => {
    const execute = jest
      .fn<JobExecutionService["execute"]>()
      .mockRejectedValue(new Error("Database unavailable"));
    const { channel, consumer, deliver } = createHarness(execute);
    const delivery = createDelivery(JSON.stringify(validMessage));
    await consumer.start();

    deliver(delivery);
    await new Promise((resolve) => setTimeout(resolve, 5));
    await consumer.stop();

    expect(channel.nack).toHaveBeenCalledWith(delivery, false, true);
    expect(channel.ack).not.toHaveBeenCalled();
  });

  it("waits for in-flight work after cancelling the consumer", async () => {
    let finishExecution: ((value: "ack") => void) | undefined;
    const execute = jest.fn<JobExecutionService["execute"]>(
      () =>
        new Promise((resolve) => {
          finishExecution = resolve;
        }),
    );
    const { channel, consumer, deliver } = createHarness(execute);
    await consumer.start();
    deliver(createDelivery(JSON.stringify(validMessage)));

    let stopped = false;
    const stopping = consumer.stop().then(() => {
      stopped = true;
    });
    await Promise.resolve();
    expect(channel.cancel).toHaveBeenCalledWith("worker-test");
    expect(stopped).toBe(false);

    finishExecution?.("ack");
    await stopping;
    expect(stopped).toBe(true);
    expect(channel.ack).not.toHaveBeenCalled();
  });

  it("leaves a deferred redelivery unacknowledged", async () => {
    const execute = jest
      .fn<JobExecutionService["execute"]>()
      .mockResolvedValue("defer");
    const { channel, consumer, deliver } = createHarness(execute);
    const delivery = createDelivery(JSON.stringify(validMessage));
    await consumer.start();

    deliver(delivery);
    await consumer.stop();

    expect(channel.ack).not.toHaveBeenCalled();
    expect(channel.nack).not.toHaveBeenCalled();
  });
});
