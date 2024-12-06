-- A DDL for hosts_cumulated table
--
-- a host_activity_datelist which logs to see which dates each host is experiencing any activity

DROP TABLE hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
        -- Current date for the user
    date DATE,
    PRIMARY KEY (host, date)
)