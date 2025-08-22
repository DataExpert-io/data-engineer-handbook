CREATE TABLE host_activity_reduced (
    month_start DATE,
    host TEXT,
    hit_array INTEGER[],
    unique_visitors_array INTEGER[],
    PRIMARY KEY(month_start, host)
);
