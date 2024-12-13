
WITH today AS (
    SELECT
        e.host, -- Aggregation only at the host level since we want to knwo if ANY activiy was recorded
        DATE_TRUNC('day', e.event_time::TIMESTAMP) AS today_date
    FROM events e
    WHERE DATE_TRUNC('day', e.event_time::TIMESTAMP) = DATE('2023-01-19')
    GROUP BY e.host, DATE_TRUNC('day', e.event_time::TIMESTAMP)
    )
INSERT INTO hosts_cumulated (host, host_activity_datelist)
SELECT
    COALESCE(t.host, hc.host) AS host,
    -- Update or initialize host_activity_datelist with today's date appended
    COALESCE(hc.host_activity_datelist, ARRAY[]::DATE[])
     || CASE WHEN t.host IS NOT NULL
            THEN ARRAY[t.today_date::DATE]
            ELSE ARRAY[]::DATE[]
        END
        AS host_activity_datelist
FROM hosts_cumulated hc
    FULL OUTER JOIN
today t ON hc.host = t.host
-- Handle conflicts by updating existing rows
ON CONFLICT (host) DO UPDATE
SET
    host = EXCLUDED.host,
    host_activity_datelist = EXCLUDED.host_activity_datelist;

SELECT * FROM hosts_cumulated