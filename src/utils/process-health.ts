import { createServer, type Server } from "node:http";

export type ReadinessCheck = () => Promise<Record<string, boolean>>;
export type MetricsCheck = () => Promise<Record<string, number | null>>;

export const startProcessHealthServer = (
  port: number,
  checkReadiness: ReadinessCheck,
  checkMetrics: MetricsCheck,
): Server => {
  const server = createServer((request, response) => {
    const path = new URL(request.url ?? "/", "http://localhost").pathname;
    if (path === "/health") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ status: "ok" }));
      return;
    }

    void (async () => {
      try {
        const checks = await checkReadiness();
        const ready = Object.values(checks).every(Boolean);
        const body: Record<string, unknown> = {
          status: ready ? "ok" : "unavailable",
          ...checks,
        };
        if (path === "/health/operations") {
          Object.assign(body, await checkMetrics());
        } else if (path !== "/health/ready") {
          response.writeHead(404);
          response.end();
          return;
        }
        response.writeHead(ready ? 200 : 503, {
          "content-type": "application/json",
        });
        response.end(JSON.stringify(body));
      } catch (error: unknown) {
        console.error(
          JSON.stringify({
            event: "process.health_check_failed",
            errorName: error instanceof Error ? error.name : "UnknownError",
          }),
        );
        response.writeHead(503, { "content-type": "application/json" });
        response.end(JSON.stringify({ status: "unavailable" }));
      }
    })();
  });
  server.listen(port, "0.0.0.0", () => {
    const address = server.address();
    console.info(
      JSON.stringify({
        event: "process.health_started",
        port: typeof address === "object" && address ? address.port : port,
      }),
    );
  });
  return server;
};
