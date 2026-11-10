import { EventEmitter } from "node:events";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import type { ChannelModel, ConfirmChannel } from "amqplib";
import { RabbitMQConnectionManager } from "../src/queues/rabbitmq-connection-manager";

const createChannel = () =>
  Object.assign(new EventEmitter(), {
    close: jest.fn<() => Promise<void>>().mockResolvedValue(undefined),
  }) as unknown as ConfirmChannel;

const createConnection = (channel: ConfirmChannel) =>
  (() => {
    const createConfirmChannel = jest
      .fn<() => Promise<ConfirmChannel>>()
      .mockResolvedValue(channel);
    const close = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
    const connection = Object.assign(new EventEmitter(), {
      createConfirmChannel,
      close,
    }) as unknown as ChannelModel;

    return { connection, createConfirmChannel, close };
  })();

describe("RabbitMQConnectionManager", () => {
  beforeEach(() => {
    jest.spyOn(console, "info").mockImplementation(() => {});
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("connects and creates a channel", async () => {
    const channel = createChannel();
    const { connection, createConfirmChannel } = createConnection(channel);
    const connect = jest
      .fn<(url: string) => Promise<ChannelModel>>()
      .mockResolvedValue(connection);
    const manager = new RabbitMQConnectionManager(
      "amqp://test:test@localhost:5672",
      connect,
    );
    const info = jest.spyOn(console, "info").mockImplementation(() => {});

    await expect(manager.initialize()).resolves.toBe(channel);
    expect(connect).toHaveBeenCalledWith("amqp://test:test@localhost:5672");
    expect(createConfirmChannel).toHaveBeenCalledTimes(1);
    expect(manager.getChannel()).toBe(channel);
    expect(manager.getConnection()).toBe(connection.connection);
    expect(info).toHaveBeenCalledWith(
      JSON.stringify({
        event: "rabbitmq.initialized",
        brokerHost: "localhost:5672",
        connection: "established",
        channel: "created",
      }),
    );
  });

  it("surfaces connection failures", async () => {
    const error = new Error(
      "Failed to connect to amqp://test:password123@localhost:5672",
    );
    const connect = jest
      .fn<(url: string) => Promise<ChannelModel>>()
      .mockRejectedValue(error);
    const manager = new RabbitMQConnectionManager(
      "amqp://test:password123@localhost:5672",
      connect,
    );
    const log = jest.spyOn(console, "error").mockImplementation(() => {});

    await expect(manager.initialize()).rejects.toThrow(error);
    expect(() => manager.getChannel()).toThrow(
      "RabbitMQ connection is not initialized",
    );
    const diagnostic = log.mock.calls
      .map(([message]) => String(message))
      .join("\n");
    expect(diagnostic).toContain('"event":"rabbitmq.connection_failed"');
    expect(diagnostic).toContain("localhost:5672");
    expect(diagnostic).not.toContain("password123");
  });

  it("closes the connection if channel creation fails", async () => {
    const channel = createChannel();
    const { connection, createConfirmChannel, close } =
      createConnection(channel);
    createConfirmChannel.mockRejectedValue(
      new Error("Channel creation failed"),
    );
    const connect = jest
      .fn<(url: string) => Promise<ChannelModel>>()
      .mockResolvedValue(connection);
    const manager = new RabbitMQConnectionManager(
      "amqp://test:test@localhost:5672",
      connect,
    );

    await expect(manager.initialize()).rejects.toThrow(
      "Channel creation failed",
    );
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("gracefully closes the channel and connection", async () => {
    const channel = createChannel();
    const { connection, close } = createConnection(channel);
    const connect = jest
      .fn<(url: string) => Promise<ChannelModel>>()
      .mockResolvedValue(connection);
    const manager = new RabbitMQConnectionManager(
      "amqp://test:test@localhost:5672",
      connect,
    );
    await manager.initialize();

    await manager.close();

    expect(channel.close).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledTimes(1);
    expect(() => manager.getChannel()).toThrow(
      "RabbitMQ connection is not initialized",
    );
  });

  it("shares one in-flight connection initialization", async () => {
    const channel = createChannel();
    const { connection } = createConnection(channel);
    const connect = jest
      .fn<(url: string) => Promise<ChannelModel>>()
      .mockResolvedValue(connection);
    const manager = new RabbitMQConnectionManager(
      "amqp://test:test@localhost:5672",
      connect,
    );

    const firstInitialization = manager.initialize();
    const secondInitialization = manager.initialize();

    await expect(firstInitialization).resolves.toBe(channel);
    await expect(secondInitialization).resolves.toBe(channel);
    expect(connect).toHaveBeenCalledTimes(1);
  });
});
