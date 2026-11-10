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
    DATABASE_URL: z.string().url(),
    RABBITMQ_URL: z
      .string()
      .url()
      .refine(
        (value) => ["amqp:", "amqps:"].includes(new URL(value).protocol),
        {
          message: "RABBITMQ_URL must use the amqp or amqps protocol",
        },
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
  });

export const env = envSchema.parse(process.env);
