CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS aggregated_metrics(
    id PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_title TEXT,
    metric_name TEXT,
    bucket TIMESTAMPTZ,
    count INT,
    avg INT,
    p95 INT,
    max INT
)