import "dotenv/config";
import { z } from "zod";

const envSchema = z
  .object({
    NODE_ENV: z
      .enum(["development", "test", "production"])
      .default("development"),
    HOST: z.string().min(1).default("0.0.0.0"),
    PORT: z.coerce.number().int().min(1).max(65535).default(3000),
    WORKER_PREFETCH: z.coerce.number().int().positive().default(1),
    WORKER_QUEUE: z.string().min(1).default("flowforge.jobs.default"),
    WORKER_ROUTING_KEY: z.string().min(1).default("job.default"),
    WORKER_LEASE_DURATION_MS: z.coerce.number().int().positive().default(30000),
    WORKER_HEARTBEAT_INTERVAL_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(10000),
    WORKER_CRASH_TEST_DURATION_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(60000),
    WORKER_SHUTDOWN_GRACE_MS: z.coerce.number().int().positive().default(15000),
    WORKER_HEALTH_PORT: z.coerce.number().int().min(0).max(65535).default(0),
    JOB_RETRY_BASE_DELAY_MS: z.coerce.number().int().positive().default(1000),
    JOB_RETRY_MAX_DELAY_MS: z.coerce.number().int().positive().default(300000),
    RABBITMQ_RECONNECT_BASE_DELAY_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(500),
    RABBITMQ_RECONNECT_MAX_DELAY_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(30000),
    OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(1000),
    OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().max(500).default(50),
    OUTBOX_ROUTING_KEY: z.string().min(1).default("job.default"),
    OUTBOX_LEASE_DURATION_MS: z.coerce.number().int().positive().default(30000),
    OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(10000),
    OUTBOX_RETRY_BASE_DELAY_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(1000),
    OUTBOX_RETRY_MAX_DELAY_MS: z.coerce
      .number()
      .int()
      .positive()
      .default(300000),
    OUTBOX_SHUTDOWN_GRACE_MS: z.coerce.number().int().positive().default(15000),
    OUTBOX_HEALTH_PORT: z.coerce.number().int().min(0).max(65535).default(0),
    DATABASE_URL: z.string().url(),
    RABBITMQ_URL: z.preprocess(
      (value) => (value === "" ? undefined : value),
      z
        .string()
        .url()
        .refine(
          (value) => ["amqp:", "amqps:"].includes(new URL(value).protocol),
          {
            message: "RABBITMQ_URL must use the amqp or amqps protocol",
          },
        )
        .optional(),
    ),
  })
  .superRefine((config, context) => {
    if (
      config.WORKER_HEARTBEAT_INTERVAL_MS * 2 >=
      config.WORKER_LEASE_DURATION_MS
    ) {
      context.addIssue({
        code: "custom",
        path: ["WORKER_HEARTBEAT_INTERVAL_MS"],
        message:
          "WORKER_HEARTBEAT_INTERVAL_MS must be less than half of WORKER_LEASE_DURATION_MS",
      });
    }
    if (config.OUTBOX_RETRY_MAX_DELAY_MS < config.OUTBOX_RETRY_BASE_DELAY_MS) {
      context.addIssue({
        code: "custom",
        path: ["OUTBOX_RETRY_MAX_DELAY_MS"],
        message:
          "OUTBOX_RETRY_MAX_DELAY_MS must be greater than or equal to OUTBOX_RETRY_BASE_DELAY_MS",
      });
    }
    if (config.JOB_RETRY_MAX_DELAY_MS < config.JOB_RETRY_BASE_DELAY_MS) {
      context.addIssue({
        code: "custom",
        path: ["JOB_RETRY_MAX_DELAY_MS"],
        message:
          "JOB_RETRY_MAX_DELAY_MS must be greater than or equal to JOB_RETRY_BASE_DELAY_MS",
      });
    }
    if (
      config.RABBITMQ_RECONNECT_MAX_DELAY_MS <
      config.RABBITMQ_RECONNECT_BASE_DELAY_MS
    ) {
      context.addIssue({
        code: "custom",
        path: ["RABBITMQ_RECONNECT_MAX_DELAY_MS"],
        message:
          "RABBITMQ_RECONNECT_MAX_DELAY_MS must be greater than or equal to RABBITMQ_RECONNECT_BASE_DELAY_MS",
      });
    }
    if (
      config.OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS >=
      config.OUTBOX_LEASE_DURATION_MS
    ) {
      context.addIssue({
        code: "custom",
        path: ["OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS"],
        message:
          "OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS must be less than OUTBOX_LEASE_DURATION_MS",
      });
    }
    if (
      config.OUTBOX_SHUTDOWN_GRACE_MS < config.OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS
    ) {
      context.addIssue({
        code: "custom",
        path: ["OUTBOX_SHUTDOWN_GRACE_MS"],
        message:
          "OUTBOX_SHUTDOWN_GRACE_MS must be at least OUTBOX_PUBLISH_CONFIRM_TIMEOUT_MS",
      });
    }
  });

export const env = envSchema.parse(process.env);
