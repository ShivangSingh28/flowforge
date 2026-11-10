import express from "express";
import { errorHandler } from "./middleware/error-handler";
import { notFoundHandler } from "./middleware/not-found";
import { requestLogger } from "./middleware/request-logger";
import { healthRouter } from "./routes/health.routes";
import { jobsRouter } from "./routes/jobs.routes";

export const app = express();

app.disable("x-powered-by");
app.use(requestLogger);
app.use(express.json());
app.use(healthRouter);
app.use(jobsRouter);
app.use(notFoundHandler);
app.use(errorHandler);
