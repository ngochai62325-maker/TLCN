CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.ingestion_runs (
  run_id VARCHAR(255) PRIMARY KEY,
  source_id VARCHAR(255) NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  status VARCHAR(50) NOT NULL DEFAULT 'IN_PROGRESS',
  load_type VARCHAR(50) NOT NULL,
  records_extracted INTEGER DEFAULT 0,
  records_quarantined INTEGER DEFAULT 0,
  chunks_processed INTEGER DEFAULT 0,
  chunks_total INTEGER DEFAULT 0,
  artifact_uri TEXT,
  manifest_uri TEXT,
  checksum VARCHAR(128),
  error_message TEXT,
  error_type VARCHAR(50),
  source_metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source_id ON ingestion.ingestion_runs(source_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status ON ingestion.ingestion_runs(status);

CREATE TABLE IF NOT EXISTS ingestion.source_snapshots (
  id SERIAL PRIMARY KEY,
  source_id VARCHAR(255) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  size_bytes BIGINT,
  retrieved_at TIMESTAMPTZ NOT NULL,
  run_id VARCHAR(255) REFERENCES ingestion.ingestion_runs(run_id),
  status VARCHAR(50) NOT NULL,
  source_uri TEXT,
  etag VARCHAR(255),
  last_modified VARCHAR(255),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_source_snapshots_source_id ON ingestion.source_snapshots(source_id);

CREATE TABLE IF NOT EXISTS ingestion.source_watermarks (
  source_id VARCHAR(255) PRIMARY KEY,
  watermark_value TEXT,
  last_run_id VARCHAR(255),
  last_batch_id VARCHAR(255),
  status VARCHAR(50),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion.ingestion_checkpoints (
  id SERIAL PRIMARY KEY,
  source_id VARCHAR(255) NOT NULL,
  run_id VARCHAR(255) NOT NULL,
  batch_id VARCHAR(255) NOT NULL,
  chunk_id INTEGER NOT NULL,
  chunk_key VARCHAR(255),
  row_start BIGINT,
  row_end BIGINT,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  checksum VARCHAR(128),
  error_message TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(source_id, run_id, chunk_id)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_checkpoints_source_run ON ingestion.ingestion_checkpoints(source_id, run_id);

CREATE TABLE IF NOT EXISTS ingestion.dead_letter_records (
  id SERIAL PRIMARY KEY,
  run_id VARCHAR(255) NOT NULL,
  batch_id VARCHAR(255),
  source_id VARCHAR(255) NOT NULL,
  chunk_id INTEGER,
  error_type VARCHAR(50) NOT NULL,
  error_code VARCHAR(100),
  error_message TEXT,
  stage VARCHAR(100),
  record_data JSONB,
  quarantine_uri TEXT,
  retryable BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
