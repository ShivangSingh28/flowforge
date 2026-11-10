import { z } from "zod";

export const jobSubmissionSchema = z
  .object({
    type: z.string().trim().min(1).max(100),
    payload: z.record(z.string(), z.json()),
    priority: z.enum(["LOW", "MEDIUM", "HIGH"]).default("MEDIUM"),
    maxAttempts: z.number().int().positive().default(3),
    idempotencyKey: z.string().trim().min(1).max(255).optional(),
  })
  .strict();

export type JobSubmissionInput = z.infer<typeof jobSubmissionSchema>;
