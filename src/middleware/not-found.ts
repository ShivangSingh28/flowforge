import type { RequestHandler } from "express";
import { HttpError } from "../utils/http-error";

export const notFoundHandler: RequestHandler = (request, _response, next) => {
  next(new HttpError(404, `Route ${request.method} ${request.path} not found`));
};
