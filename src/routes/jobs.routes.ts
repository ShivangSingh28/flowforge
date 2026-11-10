import { Router } from "express";
import { submitJob } from "../controllers/jobs.controller";

export const jobsRouter = Router();

jobsRouter.post("/api/jobs", submitJob);
