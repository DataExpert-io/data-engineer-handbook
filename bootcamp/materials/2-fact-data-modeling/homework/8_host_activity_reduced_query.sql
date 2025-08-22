-- SELECT cardinality(hit_array), COUNT(1)
-- FROM host_activity_reduced GROUP BY 1;

INSERT INTO host_activity_reduced
WITH yesterday AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-01')
),
events_ranked AS (
    SELECT *,
        CAST(event_time AS DATE) AS event_date,
        ROW_NUMBER() OVER
            (PARTITION BY user_id, host, CAST(event_time AS DATE) ORDER BY event_time)
            AS row_num
    FROM events
    WHERE user_id IS NOT NULL
),
today AS (
    SELECT
        host,
        event_date,
        CAST(DATE_TRUNC('MONTH', event_date) AS DATE) AS event_month,
        COUNT(1) AS num_hits,
        COUNT(DISTINCT user_id) AS num_unique_visitors
    FROM events_ranked
    WHERE event_date = DATE('2023-01-08') AND row_num = 1
    GROUP BY host, event_date
)
SELECT
    COALESCE(y.month_start, t.event_month) AS month_start,
    COALESCE(y.host, t.host) AS host,
    CASE
        WHEN t.host IS NULL THEN y.hit_array || ARRAY[0]
        ELSE y.hit_array || ARRAY[COALESCE(t.num_hits, 0)]
    END AS hit_array,
    CASE
        WHEN t.host IS NULL THEN y.unique_visitors_array || ARRAY[0]
        ELSE y.unique_visitors_array || ARRAY[COALESCE(t.num_unique_visitors, 0)]
    END AS unique_visitors_array
FROM today t
FULL OUTER JOIN yesterday y ON y.host = t.host
ON CONFLICT (month_start, host) DO
    UPDATE SET
        hit_array = EXCLUDED.hit_array,
        unique_visitors_array = EXCLUDED.unique_visitors_array;