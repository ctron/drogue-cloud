-- add new metadata fields

CREATE EXTENSION pgcrypto;

ALTER TABLE applications
    ADD COLUMN CREATION_TIMESTAMP TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    ADD COLUMN RESOURCE_VERSION uuid NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN GENERATION BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN DELETION_TIMESTAMP TIMESTAMP WITH TIME ZONE,
    ADD COLUMN FINALIZERS VARCHAR(63)[] NOT NULL DEFAULT ARRAY[]::VARCHAR[],

    ADD COLUMN ANNOTATIONS JSONB
;

ALTER TABLE devices
    ADD COLUMN CREATION_TIMESTAMP TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    ADD COLUMN RESOURCE_VERSION uuid NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN GENERATION BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN DELETION_TIMESTAMP TIMESTAMP WITH TIME ZONE,
    ADD COLUMN FINALIZERS VARCHAR(63)[] NOT NULL DEFAULT ARRAY[]::VARCHAR[],

    ADD COLUMN ANNOTATIONS JSONB
;

-- drop defaults

ALTER TABLE applications
    ALTER COLUMN CREATION_TIMESTAMP DROP DEFAULT,
    ALTER COLUMN RESOURCE_VERSION DROP DEFAULT,
    ALTER COLUMN GENERATION DROP DEFAULT
;

ALTER TABLE devices
    ALTER COLUMN CREATION_TIMESTAMP DROP DEFAULT,
    ALTER COLUMN RESOURCE_VERSION DROP DEFAULT,
    ALTER COLUMN GENERATION DROP DEFAULT
;
