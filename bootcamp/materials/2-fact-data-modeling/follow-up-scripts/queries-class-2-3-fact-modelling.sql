
CREATE TABLE array_metrics (
  user_id NUMERIC,
  month_start DATE,
  metric_name TEXT,
  metric_array REAL[],
  PRIMARY KEY (user_id, month_start, metric_name)
);

WITH daily_aggregate AS (
  SELECT user_id, COUNT(*) AS num_site_hits
  FROM events
  WHERE event_time::DATE = '2023-01-01'::DATE AND user_id IS NOT NULL
  GROUP BY user_id
)
select * from daily_aggregate;

WITH daily_aggregate AS (
  SELECT user_id, COUNT(*) AS num_site_hits, event_time::DATE AS curr_date
  FROM events
  WHERE event_time::DATE = '2023-01-01'::DATE AND user_id IS NOT NULL
  GROUP BY user_id, event_time::DATE
), yesterday_array AS (
  SELECT * FROM array_metrics WHERE month_start = '2023-01-01'::DATE
)
SELECT
  COALESCE(da.user_id, ya.user_id) AS user_id,
  COALESCE(ya.month_start, DATE_TRUNC('month', da.curr_date)) AS month_start,
  'site_hits' AS metrics_name,
  CASE WHEN ya.metric_array IS NOT NULL
    THEN ya.metric_array || ARRAY [COALESCE(da.num_site_hits, 0)]
    WHEN ya.metric_array IS NULL THEN ARRAY [COALESCE(da.num_site_hits, 0)]
  END AS metric_array
FROM daily_aggregate da
FULL JOIN yesterday_array ya ON ya.user_id = da.user_id;


INSERT INTO array_metrics
WITH daily_aggregate AS (
  SELECT user_id, COUNT(*) AS num_site_hits, event_time::DATE AS curr_date
  FROM events
  WHERE event_time::DATE = '2023-01-02'::DATE AND user_id IS NOT NULL
  GROUP BY user_id, event_time::DATE
), yesterday_array AS (
  SELECT * FROM array_metrics WHERE month_start = '2023-01-01'::DATE
)
SELECT
  COALESCE(da.user_id, ya.user_id) AS user_id,
  COALESCE(ya.month_start, DATE_TRUNC('month', da.curr_date)) AS month_start,
  'site_hits' AS metrics_name,
  CASE WHEN ya.metric_array IS NOT NULL
         THEN ya.metric_array || ARRAY [COALESCE(da.num_site_hits, 0)]
       WHEN ya.metric_array IS NULL THEN ARRAY [COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM daily_aggregate da
       FULL JOIN yesterday_array ya ON ya.user_id = da.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO
  UPDATE SET metric_array = EXCLUDED.metric_array;

SELECT * FROM array_metrics;


-- with array fill

DELETE FROM array_metrics WHERE month_start = '2023-01-01';

INSERT INTO array_metrics
WITH daily_aggregate AS (
  SELECT user_id, COUNT(*) AS num_site_hits, event_time::DATE AS curr_date
  FROM events
  WHERE user_id IS NOT NULL AND event_time::DATE = '2023-01-03'::DATE
  GROUP BY user_id, event_time::DATE
), yesterday_array AS (
  SELECT * FROM array_metrics WHERE month_start = '2023-01-01'::DATE
)
SELECT
  COALESCE(da.user_id, ya.user_id) AS user_id,
  COALESCE(ya.month_start, DATE_TRUNC('month', da.curr_date)) AS month_start,
  'site_hits' AS metrics_name,
  CASE WHEN ya.metric_array IS NOT NULL
         THEN ya.metric_array || ARRAY [COALESCE(da.num_site_hits, 0)]
       WHEN ya.metric_array IS NULL
         THEN ARRAY_FILL(0, ARRAY [COALESCE(da.curr_date - DATE_TRUNC('month', da.curr_date)::DATE, 0)])
                || ARRAY [COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM daily_aggregate da
       FULL JOIN yesterday_array ya ON ya.user_id = da.user_id
ON CONFLICT (user_id, month_start, metric_name)
  DO UPDATE SET metric_array = EXCLUDED.metric_array;

SELECT * FROM array_metrics;



-- AGGREGATION ANALYSIS


SELECT metric_name, month_start,
       ARRAY [SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
FROM array_metrics
GROUP BY metric_name, month_start ;


WITH agg AS (
SELECT metric_name, month_start,
       ARRAY [SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
FROM array_metrics
GROUP BY metric_name, month_start
)
SELECT metric_name,
       month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) month_day,
       elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index)
;