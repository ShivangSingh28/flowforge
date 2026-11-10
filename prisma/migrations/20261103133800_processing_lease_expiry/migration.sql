ALTER TABLE "Job"
ADD CONSTRAINT "Job_processing_lease_expiry_check"
CHECK (
    "status" <> 'PROCESSING'
    OR "leaseExpiresAt" IS NOT NULL
);
