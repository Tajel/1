BEGIN;

-- Instead of letting the todo title just be text, let's cap the length by
-- making it a varchar(512).  This will guarantee that the notifications sent on
-- todo changes will be well under the LISTEN/NOTIFY limit of 8000 bytes, so we
-- won't have to deal with that case at the application level.

ALTER TABLE todos ALTER COLUMN title TYPE VARCHAR(512);

-- add a new column
INSERT INTO migration_history (name) VALUES ('0004-title-max-length');
COMMIT;
