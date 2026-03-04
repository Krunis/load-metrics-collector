CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS aggregated_metrics(
    id PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_name TEXT,
    metric_name TEXT,
    bucket BIGINT,
    count INT, 
    min REAL,
    max REAL,
    avg REAL,
    p95 REAL
)