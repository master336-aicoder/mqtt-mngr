CREATE TABLE IF NOT EXISTS server_config (
  id            VARCHAR(64) PRIMARY KEY,
  name          VARCHAR(128) NOT NULL,
  protocol      VARCHAR(16)  NOT NULL,
  enabled       BOOLEAN      NOT NULL DEFAULT TRUE,
  conn_mode     VARCHAR(16)  NOT NULL DEFAULT 'LAZY',
  conn_json     CLOB         NOT NULL,
  cred_enc      VARBINARY(1024),
  mgmt_json     CLOB,
  tags          VARCHAR(512),
  created_at    TIMESTAMP    NOT NULL,
  updated_at    TIMESTAMP    NOT NULL
);
CREATE TABLE IF NOT EXISTS topic_config (
  id            VARCHAR(64) PRIMARY KEY,
  server_id     VARCHAR(64)  NOT NULL,
  name_pattern  VARCHAR(512) NOT NULL,
  direction     VARCHAR(8)   NOT NULL,
  format        VARCHAR(16)  NOT NULL DEFAULT 'json',
  qos           TINYINT,
  key_strategy  VARCHAR(32),
  template_id   VARCHAR(64),
  auto_match    BOOLEAN      NOT NULL DEFAULT TRUE,
  tags          VARCHAR(512),
  owner         VARCHAR(64),
  remark        VARCHAR(512),
  deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
  updated_at    TIMESTAMP    NOT NULL
);
CREATE TABLE IF NOT EXISTS msg_template (
  id            VARCHAR(64),
  version       INT,
  name          VARCHAR(128) NOT NULL,
  description   VARCHAR(512),
  status        VARCHAR(16)  NOT NULL DEFAULT 'ENABLED',
  fields_json   CLOB         NOT NULL,
  source        VARCHAR(16)  NOT NULL,
  owner         VARCHAR(64),
  tags          VARCHAR(512),
  deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMP,
  updated_at    TIMESTAMP,
  PRIMARY KEY (id, version)
);
CREATE TABLE IF NOT EXISTS subscription (
  id            VARCHAR(64) PRIMARY KEY,
  server_id     VARCHAR(64) NOT NULL,
  topic_id      VARCHAR(64),
  topic         VARCHAR(512) NOT NULL,
  grp           VARCHAR(128),
  state         VARCHAR(16)  NOT NULL,
  options_json  CLOB,
  created_at    TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS dlq_message (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  server_id VARCHAR(64),
  topic VARCHAR(512),
  subscription_id VARCHAR(64),
  template_id VARCHAR(64),
  template_version INT,
  headers_json CLOB,
  payload BLOB,
  err_code INT,
  err_msg VARCHAR(1024),
  origin VARCHAR(16),
  created_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dlq_time ON dlq_message(created_at);
CREATE TABLE IF NOT EXISTS audit_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  actor VARCHAR(64),
  action VARCHAR(32),
  obj_type VARCHAR(16),
  obj_id VARCHAR(64),
  before_json CLOB,
  after_json CLOB,
  created_at TIMESTAMP NOT NULL
);
