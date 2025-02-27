-- SELECT ARRAY(
--         SELECT 0::REAL
--         FROM generate_series(1, ('2023-01-02'::DATE - '2023-01-01'::DATE))
--     );
-- drop table array_metrics;
-- CREATE TABLE array_metrics (
--     user_id NUMERIC,
--     month_start DATE,
--     metric_name TEXT,
--     metric_array REAL [],
--     PRIMARY KEY(user_id, month_start, metric_name)
-- )
-- SELECT DATE(event_time),
--     count(*)
-- from events
-- where user_id = '10060569187331700000'
-- GROUP BY DATE(event_time);
-- SELECT *
-- from array_metrics;
-- SELECT cardinality(metric_array),
--     count(1)
-- from array_metrics
-- GROUP BY 1;
SELECT user_id,
    device_id,
    COUNT(1) as event_counts,
    COLLECT_LIST(DISTINCT host) as host_array
FROM events
GROUP BY 1,
    2;