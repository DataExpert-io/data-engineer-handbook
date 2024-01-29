CREATE TABLE Jaswanthv.actors_history_scd (
actor VARCHAR,
quality_class VARCHAR,
is_active BOOLEAN,
start_date TIMESTAMP,
end_date TIMESTAMP
)
WITH
(
FORMAT = 'PARQUET'
)
