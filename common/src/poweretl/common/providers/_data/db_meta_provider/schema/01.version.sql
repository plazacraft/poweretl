-- VERSION: 1.0.0
-- COMMAND
CREATE TABLE {table_version} (version STRING, step INT, updated_at TIMESTAMP);
INSERT INTO {table_version} (version, step, updated_at) VALUES ('', 0, current_timestamp());




