-- An incremental query that loads host_activity_reduced
--
-- day-by-day

INSERT INTO host_activity_reduced
WITH yesterday_array AS (
    SELECT * FROM host_activity_reduced
    WHERE month = DATE_PART('month', DATE(:'2022-12-31'))
), daily_aggregate AS (
    select
        host,
        DATE(event_time) AS date,
        COUNT(1) as num_site_hits,
        COUNT(DISTINCT(user_id)) as num_unique_visitors
    FROM events
    WHERE DATE(event_time) = DATE(:'2023-01-01')
    GROUP BY host, DATE(event_time)
)
SELECT
    COALESCE(da.host, ya.host) AS host,
    COALESCE(ya.month, DATE_PART('month', da.date)) AS month,
    CASE
        WHEN ya.hit_array IS NOT NULL
            THEN ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.hit_array IS NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(date_trunc('month', date)), 0)])
                     || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS hit_array,
    CASE
        WHEN ya.unique_visitors IS NOT NULL
            THEN ya.unique_visitors || ARRAY[COALESCE(da.num_unique_visitors, 0)]
        WHEN ya.unique_visitors IS NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(date_trunc('month', date)), 0)])
                     || ARRAY[COALESCE(da.num_unique_visitors, 0)]
    END AS unique_visitors
FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
        ON da.host = ya.host
ON CONFLICT(host, month)
DO UPDATE SET hit_array = EXCLUDED.hit_array, unique_visitors = EXCLUDED.unique_visitors

SELECT * FROM host_activity_reduced