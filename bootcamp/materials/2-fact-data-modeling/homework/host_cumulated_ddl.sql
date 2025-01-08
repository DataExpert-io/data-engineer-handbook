
-- select * from events

-- DROP TABLE hosts_cumulated

CREATE TABLE hosts_cumulated (
    host TEXT PRIMARY KEY,
    host_activity_datelist DATE[]
)
