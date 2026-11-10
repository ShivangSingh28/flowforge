const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const dotenv = require("dotenv");

const root = path.resolve(__dirname, "..");
const envFile = path.join(root, ".env.integration");
const integrationDatabase = "flowforge_integration";
const postgresPort = "55432";
const rabbitMQPort = "5673";

const fail = (message) => {
  throw new Error(message);
};

const required = (name) => {
  const value = process.env[name];
  if (!value) {
    fail(`Missing ${name} in .env.integration`);
  }
  return value;
};

const validateConfiguration = () => {
  if (!fs.existsSync(envFile)) {
    fail("Missing .env.integration; copy .env.integration.example first");
  }
  dotenv.config({ path: envFile, override: true });

  const database = new URL(required("FLOWFORGE_INTEGRATION_DATABASE_URL"));
  const postgresUser = required("FLOWFORGE_INTEGRATION_POSTGRES_USER");
  const postgresPassword = required("FLOWFORGE_INTEGRATION_POSTGRES_PASSWORD");
  const postgresDatabase = required("FLOWFORGE_INTEGRATION_POSTGRES_DB");
  if (
    !["postgres:", "postgresql:"].includes(database.protocol) ||
    !["127.0.0.1", "localhost"].includes(database.hostname) ||
    database.port !== postgresPort ||
    database.pathname !== `/${integrationDatabase}` ||
    postgresDatabase !== integrationDatabase ||
    decodeURIComponent(database.username) !== postgresUser ||
    decodeURIComponent(database.password) !== postgresPassword
  ) {
    fail(
      `Integration database must be ${integrationDatabase} on loopback port ${postgresPort} with matching dedicated credentials`,
    );
  }

  const rabbitMQ = new URL(required("FLOWFORGE_INTEGRATION_RABBITMQ_URL"));
  const rabbitMQUser = required("FLOWFORGE_INTEGRATION_RABBITMQ_USER");
  const rabbitMQPassword = required("FLOWFORGE_INTEGRATION_RABBITMQ_PASSWORD");
  if (
    rabbitMQ.protocol !== "amqp:" ||
    !["127.0.0.1", "localhost"].includes(rabbitMQ.hostname) ||
    rabbitMQ.port !== rabbitMQPort ||
    !["", "/"].includes(rabbitMQ.pathname) ||
    decodeURIComponent(rabbitMQ.username) !== rabbitMQUser ||
    decodeURIComponent(rabbitMQ.password) !== rabbitMQPassword
  ) {
    fail(
      `Integration RabbitMQ URL must use loopback port ${rabbitMQPort} with matching dedicated credentials`,
    );
  }

  process.env.NODE_ENV = "test";
  process.env.DATABASE_URL = database.toString();
  process.env.RABBITMQ_URL = rabbitMQ.toString();
};

const run = (command, args, extraEnvironment = {}) => {
  const result = spawnSync(command, args, {
    cwd: root,
    env: { ...process.env, ...extraEnvironment },
    stdio: "inherit",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exitCode = result.status ?? 1;
    return false;
  }
  return true;
};

const compose = (args) =>
  run("docker", [
    "compose",
    "--env-file",
    envFile,
    "--project-name",
    "flowforge-integration",
    "-f",
    "docker-compose.integration.yml",
    ...args,
  ]);

const runIntegrationTests = () => {
  const migrationCli = require.resolve("prisma/build/index.js");
  if (
    !run(process.execPath, [migrationCli, "migrate", "deploy"], {
      DATABASE_URL: process.env.FLOWFORGE_INTEGRATION_DATABASE_URL,
    })
  ) {
    return;
  }

  const jestCli = path.join(root, "node_modules", "jest", "bin", "jest.js");
  if (!fs.existsSync(jestCli)) {
    fail("Jest CLI was not found; install the declared project dependencies");
  }
  run(process.execPath, [
    jestCli,
    "--config",
    "jest.integration.config.cjs",
    "--runInBand",
  ]);
};

try {
  validateConfiguration();

  const action = process.argv[2];
  if (action === "down") {
    compose(["down"]);
  } else if (action === "up" || action === "test") {
    if (compose(["up", "--detach", "--wait", "--wait-timeout", "120"])) {
      if (action === "test") {
        runIntegrationTests();
      }
    }
  } else {
    fail("Usage: node scripts/integration-test.cjs <up|test|down>");
  }
} catch (error) {
  console.error(
    error instanceof Error ? error.message : "Integration setup failed",
  );
  process.exitCode = 1;
}
