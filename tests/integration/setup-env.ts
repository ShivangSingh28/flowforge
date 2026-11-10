const required = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing ${name}; run npm run integration:test`);
  }
  return value;
};

const database = new URL(required("FLOWFORGE_INTEGRATION_DATABASE_URL"));
const postgresUser = required("FLOWFORGE_INTEGRATION_POSTGRES_USER");
const postgresPassword = required("FLOWFORGE_INTEGRATION_POSTGRES_PASSWORD");
const postgresDatabase = required("FLOWFORGE_INTEGRATION_POSTGRES_DB");

if (
  !["postgres:", "postgresql:"].includes(database.protocol) ||
  !["127.0.0.1", "localhost"].includes(database.hostname) ||
  database.port !== "55432" ||
  database.pathname !== "/flowforge_integration" ||
  postgresDatabase !== "flowforge_integration" ||
  decodeURIComponent(database.username) !== postgresUser ||
  decodeURIComponent(database.password) !== postgresPassword
) {
  throw new Error(
    "Integration tests require the dedicated flowforge_integration database on loopback port 55432",
  );
}

const rabbitMQ = new URL(required("FLOWFORGE_INTEGRATION_RABBITMQ_URL"));
const rabbitMQUser = required("FLOWFORGE_INTEGRATION_RABBITMQ_USER");
const rabbitMQPassword = required("FLOWFORGE_INTEGRATION_RABBITMQ_PASSWORD");

if (
  rabbitMQ.protocol !== "amqp:" ||
  !["127.0.0.1", "localhost"].includes(rabbitMQ.hostname) ||
  rabbitMQ.port !== "5673" ||
  !["", "/"].includes(rabbitMQ.pathname) ||
  decodeURIComponent(rabbitMQ.username) !== rabbitMQUser ||
  decodeURIComponent(rabbitMQ.password) !== rabbitMQPassword
) {
  throw new Error(
    "Integration tests require dedicated RabbitMQ on loopback port 5673",
  );
}

process.env.NODE_ENV = "test";
process.env.DATABASE_URL = database.toString();
process.env.RABBITMQ_URL = rabbitMQ.toString();
