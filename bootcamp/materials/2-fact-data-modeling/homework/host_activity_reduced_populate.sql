
WITH today AS (
    SELECT
        -- e.user_id,
        e.host,
        DATE_TRUNC('month', e.event_time::DATE) as month,
        COUNT(*) AS hit,  -- Count the total events per host per month
        COUNT(DISTINCT user_id) as unique_visitors-- Count distinct user IDs (includes nulls for default users)
                                -- For default users
    FROM events e
        WHERE DATE_TRUNC('day', e.event_time::DATE) = DATE('2023-01-11')
    GROUP BY e.host, DATE_TRUNC('month', e.event_time::DATE)
    )
-- Insert or update aggregated data into the host_activity_reduced table
INSERT INTO host_activity_reduced (host, month, hit_array, unique_visitors_array)
SELECT
    COALESCE(t.host, har.host) AS host,
    COALESCE(t.month, har.month) AS month,
    -- Update or initialize hit_array with today's data appended
    COALESCE(har.hit_array, ARRAY[]::BIGINT[])
     || CASE WHEN t.host IS NOT NULL
            THEN ARRAY[t.hit]
            ELSE ARRAY[]::BIGINT[]
        END
    AS hit_array,
    -- Update or initialize unique_visitors_array with today's data appended
    COALESCE(har.unique_visitors_array, ARRAY[]::BIGINT[])
     || CASE WHEN t.host IS NOT NULL
            THEN ARRAY[t.unique_visitors]
            ELSE ARRAY[]::BIGINT[]
        END
    AS unique_visitors_array
FROM host_activity_reduced har
    FULL OUTER JOIN
today t ON har.host = t.host
-- Handle conflicts by updating existing rows
ON CONFLICT (host) DO UPDATE
SET
    host = EXCLUDED.host,
    month = EXCLUDED.month,
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array

-- SELECT * FROM host_activity_reduced