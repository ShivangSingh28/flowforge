import amqp, {
  type ChannelModel,
  type ConfirmChannel,
  type Connection,
} from "amqplib";
import { EventEmitter } from "node:events";
import { abortableDelay } from "../utils/abortable-delay";

type Connect = (url: string) => Promise<ChannelModel>;
type ErrorDetailKey = "code" | "syscall" | "address" | "port";

const getErrorDetail = (
  error: unknown,
  key: ErrorDetailKey,
): string | number | undefined => {
  if (typeof error !== "object" || error === null || !(key in error)) {
    return undefined;
  }

  const details = error as Partial<Record<ErrorDetailKey, unknown>>;
  const value = details[key];
  return typeof value === "string" || typeof value === "number"
    ? value
    : undefined;
};

export class RabbitMQConnectionManager {
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private initialization: Promise<ConfirmChannel> | undefined;
  private closing: Promise<void> | undefined;
  private readonly events = new EventEmitter();

  private readonly brokerHost: string;

  constructor(
    private readonly url: string,
    private readonly connect: Connect = amqp.connect,
  ) {
    this.brokerHost = new URL(url).host;
  }

  initialize(): Promise<ConfirmChannel> {
    if (this.channel && this.connection) {
      return Promise.resolve(this.channel);
    }
    if (this.closing) {
      return Promise.reject(
        new Error("Cannot initialize RabbitMQ while it is closing"),
      );
    }

    if (this.initialization) {
      return this.initialization;
    }

    const initialization = this.open();
    this.initialization = initialization;

    return initialization.finally(() => {
      if (this.initialization === initialization) {
        this.initialization = undefined;
      }
    });
  }

  get isConnected(): boolean {
    return Boolean(this.connection && this.channel);
  }

  onDisconnected(listener: () => void): () => void {
    this.events.on("disconnected", listener);
    return () => this.events.off("disconnected", listener);
  }

  waitForDisconnect(signal?: AbortSignal): Promise<void> {
    if (!this.isConnected || signal?.aborted) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      const finish = (): void => {
        this.events.off("disconnected", finish);
        signal?.removeEventListener("abort", finish);
        resolve();
      };
      this.events.once("disconnected", finish);
      signal?.addEventListener("abort", finish, { once: true });
    });
  }

  async initializeWithRetry(
    signal: AbortSignal,
    baseDelayMs: number,
    maxDelayMs: number,
  ): Promise<ConfirmChannel | undefined> {
    let failures = 0;
    while (!signal.aborted) {
      try {
        return await this.initialize();
      } catch {
        if (signal.aborted) {
          return undefined;
        }
        const ceiling = Math.min(
          maxDelayMs,
          baseDelayMs * 2 ** Math.min(failures, 30),
        );
        const delay = Math.floor(ceiling * (0.5 + Math.random() * 0.5));
        failures += 1;
        console.warn(
          JSON.stringify({
            event: "rabbitmq.reconnect_scheduled",
            attempt: failures,
            delayMs: delay,
            brokerHost: this.brokerHost,
          }),
        );
        if (!(await abortableDelay(delay, signal))) {
          return undefined;
        }
      }
    }
    return undefined;
  }

  getConnection(): Connection {
    if (!this.connection || !this.channel) {
      throw new Error("RabbitMQ connection is not initialized");
    }

    return this.connection.connection;
  }

  getChannel(): ConfirmChannel {
    if (!this.channel || !this.connection) {
      throw new Error("RabbitMQ connection is not initialized");
    }

    return this.channel;
  }

  close(): Promise<void> {
    if (this.closing) {
      return this.closing;
    }

    const closing = this.closeResources();
    this.closing = closing;

    return closing.finally(() => {
      if (this.closing === closing) {
        this.closing = undefined;
      }
    });
  }

  private async open(): Promise<ConfirmChannel> {
    let connection: ChannelModel;

    try {
      connection = await this.connect(this.url);
    } catch (error: unknown) {
      this.logError("rabbitmq.connection_failed", error);
      throw error;
    }

    this.connection = connection;

    connection.on("error", (error: Error) => {
      this.logError("rabbitmq.connection_error", error);
      this.invalidate(connection);
    });

    connection.on("close", () => {
      this.invalidate(connection);
    });

    let channel: ConfirmChannel;

    try {
      channel = await connection.createConfirmChannel();
    } catch (error: unknown) {
      this.logError("rabbitmq.channel_creation_failed", error);

      if (this.connection === connection) {
        this.connection = undefined;
      }

      try {
        await connection.close();
      } catch (closeError: unknown) {
        this.logError("rabbitmq.cleanup_failed", closeError);
      }

      throw error;
    }

    if (this.connection !== connection) {
      await channel.close();
      throw new Error("RabbitMQ connection closed during channel creation");
    }

    this.channel = channel;

    channel.on("error", (error: Error) => {
      this.logError("rabbitmq.channel_error", error);
      this.invalidate(connection);
    });

    channel.on("close", () => {
      this.invalidate(connection);
    });

    console.info(
      JSON.stringify({
        event: "rabbitmq.initialized",
        brokerHost: this.brokerHost,
        connection: "established",
        channel: "created",
      }),
    );

    return channel;
  }

  private logError(event: string, error: unknown): void {
    const details: Record<string, string | number> = {
      event,
      brokerHost: this.brokerHost,
    };

    if (error instanceof Error) {
      details.errorName = error.name;
      details.message = this.redactCredentials(error.message);
    } else {
      details.message = this.redactCredentials(String(error));
    }

    for (const key of ["code", "syscall", "address", "port"] as const) {
      const value = getErrorDetail(error, key);
      if (value !== undefined) {
        details[key] = value;
      }
    }

    console.error(JSON.stringify(details));
  }

  private invalidate(connection: ChannelModel): void {
    if (this.connection !== connection) {
      return;
    }
    this.connection = undefined;
    this.channel = undefined;
    console.error(JSON.stringify({ event: "rabbitmq.connection_unavailable" }));
    this.events.emit("disconnected");
    void connection.close().catch((error: unknown) => {
      this.logError("rabbitmq.invalid_connection_close_failed", error);
    });
  }

  private redactCredentials(message: string): string {
    const endpoint = new URL(this.url);
    const secrets = [endpoint.username, endpoint.password]
      .filter((secret) => secret.length > 0)
      .flatMap((secret) => {
        try {
          return [secret, decodeURIComponent(secret)];
        } catch {
          return [secret];
        }
      });

    let safeMessage = message.replace(
      /(amqps?:\/\/)[^\s/@]+@/gi,
      "$1[REDACTED]@",
    );

    for (const secret of secrets) {
      safeMessage = safeMessage.replaceAll(secret, "[REDACTED]");
    }

    return safeMessage;
  }

  private async closeResources(): Promise<void> {
    const initialization = this.initialization;
    if (initialization) {
      try {
        await initialization;
      } catch (error: unknown) {
        console.error("RabbitMQ initialization failed during shutdown", error);
      }
    }

    const channel = this.channel;
    const connection = this.connection;
    this.channel = undefined;
    this.connection = undefined;

    const errors: unknown[] = [];

    if (channel) {
      try {
        await channel.close();
      } catch (error: unknown) {
        errors.push(error);
      }
    }

    if (connection) {
      try {
        await connection.close();
      } catch (error: unknown) {
        errors.push(error);
      }
    }

    if (errors.length > 0) {
      throw new AggregateError(errors, "Failed to close RabbitMQ cleanly");
    }
  }
}
