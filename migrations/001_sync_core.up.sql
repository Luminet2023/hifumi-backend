CREATE TABLE IF NOT EXISTS schema_migrations (
  version BIGINT UNSIGNED NOT NULL,
  dirty BOOLEAN NOT NULL DEFAULT FALSE,
  applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (version)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sync_owners (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  created_at_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS user_profiles (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  linuxdo_subject VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  username VARCHAR(128) NOT NULL,
  display_name VARCHAR(256) NOT NULL,
  avatar_url VARCHAR(2048) NOT NULL,
  email VARCHAR(320) NULL,
  created_at_ms BIGINT UNSIGNED NOT NULL,
  updated_at_ms BIGINT UNSIGNED NOT NULL,
  last_login_at_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (owner_key),
  UNIQUE KEY uq_user_profiles_subject (linuxdo_subject),
  CONSTRAINT fk_user_profiles_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sync_heads (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `cursor` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NULL,
  version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  updated_at_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  progress_day VARCHAR(64) NOT NULL DEFAULT '2026-07-13',
  PRIMARY KEY (owner_key),
  CONSTRAINT fk_sync_heads_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sync_records (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  entity_key VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `cursor` BIGINT UNSIGNED NOT NULL,
  value_json LONGBLOB NOT NULL,
  deleted BOOLEAN NOT NULL,
  device_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  client_time_ms BIGINT UNSIGNED NOT NULL,
  op_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  PRIMARY KEY (owner_key, entity_key),
  CONSTRAINT fk_sync_records_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sync_operations (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `cursor` BIGINT UNSIGNED NOT NULL,
  op_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  entity_key VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  value_json LONGBLOB NOT NULL,
  deleted BOOLEAN NOT NULL,
  device_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  client_time_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (owner_key, `cursor`),
  UNIQUE KEY uq_sync_operations_op (owner_key, op_id),
  CONSTRAINT fk_sync_operations_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sync_receipts (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  op_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  server_cursor BIGINT UNSIGNED NOT NULL,
  conflict BOOLEAN NOT NULL,
  applied BOOLEAN NOT NULL,
  PRIMARY KEY (owner_key, op_id),
  CONSTRAINT fk_sync_receipts_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS baseline_resolutions (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  request_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  local_baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  expected_server_baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  expected_server_version BIGINT UNSIGNED NOT NULL,
  choice TINYINT UNSIGNED NOT NULL,
  result_baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  result_cursor BIGINT UNSIGNED NOT NULL,
  result_version BIGINT UNSIGNED NOT NULL,
  result_updated_at_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (owner_key, request_id),
  CONSTRAINT fk_baseline_resolutions_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

-- 当前基线可以被 USE_LOCAL 重置；该表保存跨基线不可变归档。
CREATE TABLE IF NOT EXISTS sync_archive_changes (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `cursor` BIGINT UNSIGNED NOT NULL,
  op_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  entity_key VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  value_json LONGBLOB NOT NULL,
  deleted BOOLEAN NOT NULL,
  device_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  client_time_ms BIGINT UNSIGNED NOT NULL,
  server_version BIGINT UNSIGNED NOT NULL,
  archived_at_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (owner_key, baseline_id, `cursor`),
  CONSTRAINT fk_sync_archive_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS sync_archive_heads (
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `cursor` BIGINT UNSIGNED NOT NULL,
  server_version BIGINT UNSIGNED NOT NULL,
  updated_at_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (owner_key, baseline_id),
  CONSTRAINT fk_sync_archive_head_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;

-- 发布进程读取未发布事件并转发到 Redis Pub/Sub；业务提交与事件入队同事务。
CREATE TABLE IF NOT EXISTS realtime_outbox (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  owner_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  event_type VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  baseline_id CHAR(41) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  server_cursor BIGINT UNSIGNED NOT NULL,
  server_version BIGINT UNSIGNED NOT NULL,
  origin_connection_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
  created_at_ms BIGINT UNSIGNED NOT NULL,
  published_at_ms BIGINT UNSIGNED NULL,
  publish_attempts INT UNSIGNED NOT NULL DEFAULT 0,
  last_error VARCHAR(1024) NULL,
  PRIMARY KEY (id),
  KEY idx_realtime_outbox_pending (published_at_ms, id),
  KEY idx_realtime_outbox_owner (owner_key, id),
  CONSTRAINT fk_realtime_outbox_owner FOREIGN KEY (owner_key) REFERENCES sync_owners (owner_key)
) ENGINE = InnoDB;
