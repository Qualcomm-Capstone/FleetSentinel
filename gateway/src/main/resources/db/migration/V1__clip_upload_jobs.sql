CREATE TABLE clip_upload_jobs (
    id varchar(26) PRIMARY KEY,
    version bigint NOT NULL DEFAULT 0,
    vehicle_id varchar(64) NOT NULL,
    request_id varchar(160) NOT NULL,
    fingerprint varchar(64) NOT NULL,
    metadata text NOT NULL,
    bucket varchar(255) NOT NULL,
    object_key text NOT NULL UNIQUE,
    state varchar(24) NOT NULL CHECK (state IN ('CREATING','UPLOADING','VERIFYING','STORED','COMPLETED','ABORTING','ABORTED','FAILED')),
    upload_id text,
    complete_parts text,
    actual_sha256 varchar(64),
    actual_size_bytes bigint,
    lease_token varchar(36),
    lease_until timestamptz,
    next_attempt timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    error text,
    UNIQUE (vehicle_id, request_id)
);
CREATE INDEX clip_upload_jobs_work ON clip_upload_jobs (next_attempt, lease_until)
    WHERE state IN ('CREATING','UPLOADING','VERIFYING','STORED','ABORTING');

CREATE TABLE clip_upload_outbox (
    job_id varchar(26) PRIMARY KEY REFERENCES clip_upload_jobs(id),
    topic varchar(255) NOT NULL,
    record_key varchar(255) NOT NULL,
    headers text NOT NULL,
    payload bytea NOT NULL,
    created_at timestamptz NOT NULL,
    published_at timestamptz
);
