import type { RequestHandler } from "express";

export const requestLogger: RequestHandler = (request, response, next) => {
  const startedAt = Date.now();

  response.on("finish", () => {
    console.info(
      JSON.stringify({
        method: request.method,
        path: request.path,
        statusCode: response.statusCode,
        durationMs: Date.now() - startedAt,
      }),
    );
  });

  next();
};
