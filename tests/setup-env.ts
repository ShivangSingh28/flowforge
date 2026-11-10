process.env.DATABASE_URL ??=
  "postgresql://test:test@localhost:5432/flowforge_test?schema=public";
process.env.RABBITMQ_URL ??= "amqp://test:test@localhost:5672";
