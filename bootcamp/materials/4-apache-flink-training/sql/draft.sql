-- SELECT *
-- from processed_events;
-- SELECT MIN(event_timestamp) AS oldest_time,
--     MAX(event_timestamp) AS newest_time
-- FROM processed_events;
select *
from processed_events_aggregated;
select *
from processed_events_aggregated_source;
-- SELECT geodata::json->>'country',
--     COUNT(1)
-- from processed_events
-- GROUP BY 1;
-- SELECT MIN(event_timestamp) AS oldest_time,
--     MAX(event_timestamp) AS newest_time
-- FROM processed_events;
-- # ip VARCHAR,
-- # event_time VARCHAR,
-- # referrer VARCHAR,
-- # host VARCHAR,
-- # url VARCHAR,
-- # geodata VARCHAR,
-- # window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
-- # WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND