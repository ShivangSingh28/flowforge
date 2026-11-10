CREATE FUNCTION prevent_outbox_event_immutable_fields_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW."id" IS DISTINCT FROM OLD."id"
       OR NEW."jobId" IS DISTINCT FROM OLD."jobId"
       OR NEW."eventType" IS DISTINCT FROM OLD."eventType"
       OR NEW."payload" IS DISTINCT FROM OLD."payload"
       OR NEW."createdAt" IS DISTINCT FROM OLD."createdAt" THEN
        RAISE EXCEPTION 'Outbox event identity and payload are immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER "OutboxEvent_immutable_fields_trigger"
BEFORE UPDATE ON "OutboxEvent"
FOR EACH ROW EXECUTE FUNCTION prevent_outbox_event_immutable_fields_update();
