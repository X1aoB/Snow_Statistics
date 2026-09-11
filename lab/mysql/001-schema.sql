USE snow_ops;
CREATE TABLE contents (
  id VARCHAR(64) PRIMARY KEY, title VARCHAR(128) NOT NULL, category VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL, version BIGINT NOT NULL, updated_at DATETIME(3) NOT NULL
);
CREATE TABLE campaigns (
  id VARCHAR(64) PRIMARY KEY, name VARCHAR(128) NOT NULL, status VARCHAR(32) NOT NULL,
  version BIGINT NOT NULL, updated_at DATETIME(3) NOT NULL
);
CREATE TABLE tickets (
  id VARCHAR(64) PRIMARY KEY, status VARCHAR(32) NOT NULL, category VARCHAR(64) NOT NULL,
  version BIGINT NOT NULL, updated_at DATETIME(3) NOT NULL
);
CREATE TABLE applied_transactions (
  run_id VARCHAR(64) NOT NULL, version BIGINT NOT NULL, PRIMARY KEY(run_id,version)
);
-- Create a separate CDC principal at runtime with REPLICATION SLAVE,
-- REPLICATION CLIENT, SELECT, RELOAD and SHOW DATABASES; credentials are not committed.
