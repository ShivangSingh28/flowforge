import { Router } from "express";
import { getHealth } from "../controllers/health.controller";
import { getReadiness } from "../controllers/readiness.controller";
import { getOperations } from "../controllers/operations.controller";

export const healthRouter = Router();

healthRouter.get("/health", getHealth);
healthRouter.get("/health/ready", getReadiness);
healthRouter.get("/health/operations", getOperations);
