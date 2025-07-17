-- Create processed_events_session_source table
CREATE TABLE IF NOT EXISTS processed_events_session_source (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    ip VARCHAR,
    num_hits BIGINT
);

-- Calculate average number of web events of a session from a user on Tech Creator
SELECT AVG(num_hits) FROM processed_events_session_source
WHERE host LIKE '%techcreator%'

-- Compare results between different hosts
SELECT host, AVG(num_hits) FROM processed_events_session_source
WHERE host LIKE '%techcreator%'
GROUP BY host