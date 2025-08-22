CREATE TABLE hosts_cumulated (
    user_id NUMERIC,
    host TEXT,
    host_activity_datelist DATE[],
    record_date DATE,
    PRIMARY KEY(user_id, host, record_date)
);  