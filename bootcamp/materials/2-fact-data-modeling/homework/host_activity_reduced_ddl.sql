-- DROP TABLE host_activity_reduced

CREATE TABLE host_activity_reduced (
    host TEXT PRIMARY KEY,
    month DATE,
    hit_array BIGINT[],
    unique_visitors_array BIGINT[]
)
