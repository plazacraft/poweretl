-- VERSION: 1.0
-- COMMAND
CREATE TABLE {catalog}.{schema}.t2 (id INT);

-- COMMAND
SELECT * FROM {catalog}.{schema}.t2;

-- VERSION: 1.2
-- COMMAND
ALTER TABLE {catalog}.{schema}.t2 ADD COLUMN(name STRING)

