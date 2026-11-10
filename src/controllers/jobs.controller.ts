import type { RequestHandler } from "express";
import { JobRepository } from "../repositories/job.repository";
import { JobSubmissionService } from "../services/job-submission.service";
import { jobSubmissionSchema } from "../types/job-submission";

const jobSubmissionService = new JobSubmissionService(new JobRepository());

export const submitJob: RequestHandler = async (request, response) => {
  const input = jobSubmissionSchema.parse(request.body);
  const result = await jobSubmissionService.submit(input);

  response.status(result.created ? 201 : 200).json(result.job);
};
