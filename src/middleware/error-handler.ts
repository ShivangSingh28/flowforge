import type { ErrorRequestHandler } from "express";
import { ZodError } from "zod";
import { HttpError } from "../utils/http-error";

export const errorHandler: ErrorRequestHandler = (
  error: unknown,
  _request,
  response,
  _next,
) => {
  if (error instanceof HttpError) {
    if (error.statusCode >= 500) {
      console.error(
        JSON.stringify({
          event: "http.request_failed",
          statusCode: error.statusCode,
          errorName: error.name,
        }),
      );
    }
    response.status(error.statusCode).json({
      error: { message: error.message },
    });
    return;
  }

  if (error instanceof ZodError) {
    response.status(400).json({
      error: {
        message: "Invalid request",
        details: error.issues.map((issue) => ({
          path: issue.path.join("."),
          message: issue.message,
        })),
      },
    });
    return;
  }

  const statusCode =
    typeof error === "object" &&
    error !== null &&
    "status" in error &&
    typeof error.status === "number" &&
    error.status >= 400 &&
    error.status < 500
      ? error.status
      : 500;

  if (statusCode === 500) {
    console.error(
      JSON.stringify({
        event: "http.unhandled_error",
        errorName: error instanceof Error ? error.name : "UnknownError",
      }),
    );
  }

  response.status(statusCode).json({
    error: {
      message: statusCode === 500 ? "Internal server error" : "Invalid request",
    },
  });
};
