-- Create processed_events_aggregated_ip_host_tumble table
CREATE TABLE IF NOT EXISTS processed_events_aggregated_ip_host_tumble (
    event_hour TIMESTAMP(3),
    ip VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);

-- Create processed_events_aggregated_ip_host_session table
CREATE TABLE IF NOT EXISTS processed_events_aggregated_ip_host_session (
    event_hour TIMESTAMP(3),
    ip VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);
