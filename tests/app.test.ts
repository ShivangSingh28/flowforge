import request from "supertest";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  jest,
} from "@jest/globals";
import {
  JobPriority,
  JobStatus,
  type Job,
  type OutboxEvent,
} from "@prisma/client";
import type { Prisma } from "@prisma/client";
import { app } from "../src/app";
import { prisma } from "../src/repositories/prisma";

describe("HTTP application", () => {
  beforeEach(() => {
    jest.spyOn(console, "info").mockImplementation(() => {});
    jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("returns a healthy status", async () => {
    const response = await request(app).get("/health");

    expect(response.status).toBe(200);
    expect(response.body).toEqual({ status: "ok" });
  });

  it("reports database readiness when the database is reachable", async () => {
    const query = jest
      .spyOn(prisma, "$queryRaw")
      .mockResolvedValue([{ "?column?": 1 }]);

    const response = await request(app).get("/health/ready");

    expect(query).toHaveBeenCalledTimes(1);
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ status: "ok", database: "reachable" });
  });

  it("reports database unavailability without exposing internal errors", async () => {
    jest
      .spyOn(prisma, "$queryRaw")
      .mockRejectedValue(new Error("postgres://user:secret@db/private"));

    const response = await request(app).get("/health/ready");

    expect(response.status).toBe(503);
    expect(response.body).toEqual({
      status: "unavailable",
      database: "unreachable",
    });
    expect(JSON.stringify(response.body)).not.toContain("secret");
  });

  it("returns a structured 404 for unknown routes", async () => {
    const response = await request(app).get("/missing");

    expect(response.status).toBe(404);
    expect(response.body).toEqual({
      error: { message: "Route GET /missing not found" },
    });
  });

  it("returns a client error for malformed JSON", async () => {
    const response = await request(app)
      .post("/health")
      .set("Content-Type", "application/json")
      .send("{");

    expect(response.status).toBe(400);
    expect(response.body).toEqual({ error: { message: "Invalid request" } });
  });

  it("rejects invalid job submissions with a validation response", async () => {
    const response = await request(app)
      .post("/api/jobs")
      .send({ type: " ", payload: ["not", "an", "object"] });

    expect(response.status).toBe(400);
    expect(response.body.error.message).toBe("Invalid request");
    expect(response.body.error.details).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ path: "type" }),
        expect.objectContaining({ path: "payload" }),
      ]),
    );
  });

  it("returns 201 after a new job and outbox event commit", async () => {
    const persistedJob = {
      id: "b23d9dd9-bd88-4fa2-81be-fb2fcf55fcb0",
      type: "email.send",
      payload: { recipient: "test@example.com" },
      status: JobStatus.PENDING,
      priority: JobPriority.MEDIUM,
      attempts: 0,
      maxAttempts: 3,
      idempotencyKey: "http-request-1",
      leaseToken: null,
      leaseExpiresAt: null,
      scheduledAt: null,
      startedAt: null,
      completedAt: null,
      failedAt: null,
      lastError: null,
      createdAt: new Date("2026-10-31T12:00:00.000Z"),
      updatedAt: new Date("2026-10-31T12:00:00.000Z"),
    } satisfies Job;
    const createJob = jest
      .fn<(args: Prisma.JobCreateArgs) => Promise<Job>>()
      .mockResolvedValue(persistedJob);
    const createOutboxEvent = jest
      .fn<(args: Prisma.OutboxEventCreateArgs) => Promise<OutboxEvent>>()
      .mockResolvedValue({} as OutboxEvent);
    jest.spyOn(prisma.job, "findUnique").mockResolvedValue(null);
    const transaction = {
      job: {
        create: createJob,
      },
      outboxEvent: {
        create: createOutboxEvent,
      },
    } as unknown as Prisma.TransactionClient;
    const transact = jest
      .spyOn(prisma, "$transaction")
      .mockImplementation(async (callback) => {
        if (typeof callback !== "function") {
          throw new Error("Expected an interactive transaction");
        }
        return callback(transaction);
      });

    const response = await request(app)
      .post("/api/jobs")
      .send({
        type: "email.send",
        payload: { recipient: "test@example.com" },
        idempotencyKey: "http-request-1",
      });

    expect(response.status).toBe(201);
    expect(response.body).toEqual({
      id: persistedJob.id,
      status: JobStatus.PENDING,
    });
    expect(transact).toHaveBeenCalledTimes(1);
    expect(createOutboxEvent).toHaveBeenCalledWith({
      data: expect.objectContaining({
        jobId: persistedJob.id,
        eventType: "job.submitted",
      }),
    });
  });
});
