ALTER TABLE "Job"
ADD COLUMN "leaseToken" UUID,
ADD COLUMN "leaseExpiresAt" TIMESTAMPTZ(6);

CREATE UNIQUE INDEX "Job_leaseToken_key" ON "Job"("leaseToken");

UPDATE "Job"
SET "leaseToken" = NULL,
    "leaseExpiresAt" = NULL
WHERE "status" IN ('COMPLETED', 'FAILED', 'CANCELLED');

UPDATE "Job"
SET "leaseExpiresAt" = CURRENT_TIMESTAMP
WHERE "status" = 'PROCESSING';

ALTER TABLE "Job"
ADD CONSTRAINT "Job_terminal_lease_cleared_check"
CHECK (
    "status" NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
    OR ("leaseToken" IS NULL AND "leaseExpiresAt" IS NULL)
);
