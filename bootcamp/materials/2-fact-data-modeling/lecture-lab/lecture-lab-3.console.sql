
DROP TABLE array_metrics;
CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array INTEGER[],

    UNIQUE (user_id, month_start, metric_name),
    PRIMARY KEY (user_id, month_start, metric_name)
);

INSERT INTO array_metrics
WITH yesterday_array AS (
    SELECT * FROM array_metrics
    WHERE month_start = DATE(:'2022-12-31')
), daily_aggregate AS (
    select
        user_id,
        DATE(event_time) AS date,
        COUNT(1) as num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE(:'2023-01-01')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
)
SELECT
    COALESCE(da.user_id, ya.user_id) AS user_id,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    'site_hits' AS metric_name,
    CASE
        WHEN ya.metric_array IS NOT NULL
            THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array IS NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(date_trunc('month', date)), 0)])
                     || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
        ON da.user_id = ya.user_id
ON CONFLICT(user_id, month_start, metric_name)
DO UPDATE SET metric_array = EXCLUDED.metric_array

SELECT * FROM array_metrics