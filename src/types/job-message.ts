import { z } from "zod";

export const jobMessageSchema = z.object({
  jobId: z.uuid(),
  type: z.string().min(1),
  payload: z.json(),
  priority: z.enum(["LOW", "MEDIUM", "HIGH"]),
  createdAt: z.iso.datetime(),
});

export type JobMessage = z.infer<typeof jobMessageSchema>;
