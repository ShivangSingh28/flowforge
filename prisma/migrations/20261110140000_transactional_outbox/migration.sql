CREATE TABLE "OutboxEvent" (
    "id" UUID NOT NULL,
    "jobId" UUID NOT NULL,
    "eventType" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "createdAt" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "availableAt" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "lockedUntil" TIMESTAMPTZ(6),
    "lockToken" UUID,
    "publishedAt" TIMESTAMPTZ(6),
    "lastError" TEXT,

    CONSTRAINT "OutboxEvent_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "OutboxEvent_jobId_fkey" FOREIGN KEY ("jobId")
        REFERENCES "Job"("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE UNIQUE INDEX "OutboxEvent_jobId_eventType_key"
ON "OutboxEvent"("jobId", "eventType");

CREATE INDEX "OutboxEvent_due_idx"
ON "OutboxEvent"("publishedAt", "availableAt", "createdAt");
