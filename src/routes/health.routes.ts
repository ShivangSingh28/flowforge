import { Router } from "express";
import { getHealth } from "../controllers/health.controller";
import { getReadiness } from "../controllers/readiness.controller";

export const healthRouter = Router();

healthRouter.get("/health", getHealth);
healthRouter.get("/health/ready", getReadiness);
