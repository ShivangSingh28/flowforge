import type { RequestHandler } from "express";
import { OperationsRepository } from "../repositories/operations.repository";

const operations = new OperationsRepository();

export const getOperations: RequestHandler = async (_request, response) => {
  try {
    response.status(200).json({
      status: "ok",
      ...(await operations.getMetrics()),
    });
  } catch (error: unknown) {
    console.error(
      JSON.stringify({
        event: "operations.metrics_failed",
        errorName: error instanceof Error ? error.name : "UnknownError",
      }),
    );
    response.status(503).json({ status: "unavailable" });
  }
};
