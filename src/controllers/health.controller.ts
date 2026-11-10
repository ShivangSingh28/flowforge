import type { RequestHandler } from "express";

export const getHealth: RequestHandler = (_request, response) => {
  response.status(200).json({ status: "ok" });
};
