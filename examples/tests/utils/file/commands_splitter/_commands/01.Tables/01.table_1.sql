-- VERSION: 1.0
-- COMMAND
CREATE TABLE {catalog}.{schema}.t1 (id INT);

-- COMMAND
SELECT * FROM {catalog}.{schema}.t1;

-- VERSION: 1.1
-- COMMAND
ALTER TABLE {catalog}.{schema}.t1 ADD COLUMN(name STRING)

