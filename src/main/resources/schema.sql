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

-- ========== RBAC ==========
CREATE TABLE IF NOT EXISTS sys_user (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(64) NOT NULL,
  password VARCHAR(256) NOT NULL,
  nickname VARCHAR(64),
  email VARCHAR(128),
  phone VARCHAR(32),
  avatar VARCHAR(256),
  status TINYINT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_sys_user_username ON sys_user(username);

CREATE TABLE IF NOT EXISTS sys_role (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  role_code VARCHAR(64) NOT NULL,
  role_name VARCHAR(128) NOT NULL,
  description VARCHAR(512),
  status TINYINT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_sys_role_code ON sys_role(role_code);

CREATE TABLE IF NOT EXISTS sys_menu (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  parent_id BIGINT NOT NULL DEFAULT 0,
  menu_name VARCHAR(128) NOT NULL,
  menu_type TINYINT NOT NULL DEFAULT 1,
  path VARCHAR(256),
  icon VARCHAR(64),
  perm_code VARCHAR(128),
  sort_order INT NOT NULL DEFAULT 0,
  status TINYINT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sys_user_role (
  user_id BIGINT NOT NULL,
  role_id BIGINT NOT NULL,
  PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS sys_role_menu (
  role_id BIGINT NOT NULL,
  menu_id BIGINT NOT NULL,
  PRIMARY KEY (role_id, menu_id)
);

-- ========== Seed data ==========
INSERT INTO sys_user (username, password, nickname, status)
SELECT 'admin', '$2a$10$vXU7uUSkn8b3VplyaUeooO0rfYCfCq2VXFAL7d9NX0D0vn9i5Utsy', '系统管理员', 1
WHERE NOT EXISTS (SELECT 1 FROM sys_user WHERE username = 'admin');

INSERT INTO sys_role (role_code, role_name, description, status)
SELECT 'super_admin', '超级管理员', '拥有全部权限', 1
WHERE NOT EXISTS (SELECT 1 FROM sys_role WHERE role_code = 'super_admin');

INSERT INTO sys_user_role (user_id, role_id)
SELECT u.id, r.id FROM sys_user u, sys_role r
WHERE u.username = 'admin' AND r.role_code = 'super_admin'
AND NOT EXISTS (SELECT 1 FROM sys_user_role ur WHERE ur.user_id = u.id AND ur.role_id = r.id);

INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 1, 0, '监控总览', 1, 'dashboard', 'grid', 'dashboard', 1
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 1);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 2, 0, '连接管理', 0, NULL, 'server', NULL, 2
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 2);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 3, 2, '服务连接', 1, 'servers', 'server', 'server:view', 1
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 3);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 4, 2, 'Topic 管理', 1, 'topics', 'hash', 'topic:view', 2
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 4);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 5, 0, '消息模板', 1, 'templates', 'file-text', 'template:view', 3
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 5);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 6, 0, '收发调试', 1, 'messaging', 'send', 'messaging:view', 4
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 6);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 7, 0, '订阅管理', 1, 'subscriptions', 'inbox', 'subscription:view', 5
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 7);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 8, 0, '死信 DLQ', 1, 'dlq', 'warn', 'dlq:view', 6
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 8);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 9, 0, '集群管理', 1, 'cluster', 'network', 'cluster:view', 7
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 9);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 10, 0, '系统管理', 0, NULL, 'settings', NULL, 90
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 10);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 11, 10, '用户管理', 1, 'users', 'avatar', 'sys:user:view', 1
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 11);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 12, 10, '角色管理', 1, 'roles', 'layers', 'sys:role:view', 2
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 12);
INSERT INTO sys_menu (id, parent_id, menu_name, menu_type, path, icon, perm_code, sort_order)
SELECT 13, 10, '菜单管理', 1, 'menus', 'file-text', 'sys:menu:view', 3
WHERE NOT EXISTS (SELECT 1 FROM sys_menu WHERE id = 13);

INSERT INTO sys_role_menu (role_id, menu_id)
SELECT r.id, m.id FROM sys_role r, sys_menu m
WHERE r.role_code = 'super_admin'
AND NOT EXISTS (SELECT 1 FROM sys_role_menu rm WHERE rm.role_id = r.id AND rm.menu_id = m.id);
