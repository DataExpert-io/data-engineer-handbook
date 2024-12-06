-- Insert aggregated metrics into the array_metrics table
INSERT INTO array_metrics
WITH daily_aggregate AS (
    -- Aggregate daily site hits per user
    SELECT 
        user_id,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM array_metrics 
    WHERE month_start = DATE('2023-01-01')
)
SELECT 
    -- Select user_id from either daily_aggregate or yesterday_array
    COALESCE( da.user_id, ya.user_id) AS user_id,
    -- Determine month_start date
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    -- Set metric name to 'site_hits'
    'site_hits' AS metric_name,
    -- Update metric_array based on existing data and new daily aggregates
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN 
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)] 
        WHEN ya.metric_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya 
ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO 
    UPDATE SET metric_array = EXCLUDED.metric_array;

-- Uncomment and run the following query to verify the cardinality of metric_array
-- SELECT cardinality(metric_array), COUNT(1)
-- FROM array_metrics
-- GROUP BY 1;

-- Aggregate metrics by summing specific elements in the metric_array
WITH agg AS (
    SELECT metric_name, month_start, ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
-- Select and display the metric_name, date (adjusted by index), and summed value
SELECT 
    metric_name, 
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);
