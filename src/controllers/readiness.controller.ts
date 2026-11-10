import type { RequestHandler } from "express";
import { prisma } from "../repositories/prisma";

export const getReadiness: RequestHandler = async (_request, response) => {
  try {
    await prisma.$queryRaw`SELECT 1`;
    response.status(200).json({ status: "ok", database: "reachable" });
  } catch (error: unknown) {
    console.error("Database readiness check failed", error);
    response.status(503).json({
      status: "unavailable",
      database: "unreachable",
    });
  }
};
