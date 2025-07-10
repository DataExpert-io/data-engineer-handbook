
-- 1. A query to deduplicate game_details from Day 1 so there's no duplicates

WITH game_details_deduped AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS rn
  FROM game_details
)
SELECT * FROM game_details_deduped WHERE rn = 1;

-- 2. A DDL for an user_devices_cumulated table that has:
-- 2.1. a `device_activity_datelist` which tracks a users active days by `browser_type`
-- 2.2. data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--   - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

select * from events limit 100;
select * from devices limit 100;


SELECT
  e.user_id,
  d.browser_type,
  CAST(e.event_time AS DATE) AS device_activity_datelist
FROM devices d JOIN events e ON d.device_id = e.device_id
WHERE e.user_id IS NOT NULL
GROUP BY 1,2,3;

SELECT
  e.user_id,
  ROW(d.browser_type, CAST(e.event_time AS DATE)) AS device_activity_datelist
FROM devices d JOIN events e ON d.device_id = e.device_id
WHERE e.user_id IS NOT NULL
GROUP BY 1,2;

CREATE TYPE browser_dates AS (
  browser TEXT,
  dates DATE[]
);

DROP TABLE IF EXISTS user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
user_id NUMERIC,
device_activity_datelist browser_dates,
curr_date DATE,
PRIMARY KEY (user_id, device_activity_datelist, curr_date)
);

-- 3. A cumulative query to generate `device_activity_datelist` from `events`
-- maybe it should be user_devices_cumulated

WITH user_grouped AS (
  SELECT e.user_id,
    ROW(d.browser_type, ARRAY[CAST(e.event_time AS DATE)])::browser_dates AS device_activity_datelist
  FROM devices d JOIN events e ON d.device_id = e.device_id
  WHERE e.user_id IS NOT NULL
  GROUP BY 1,2
) SELECT (device_activity_datelist).browser, '2023-01-01'::DATE || (device_activity_datelist).dates AS dates, *
  FROM user_grouped;

SELECT e.user_id, count(DISTINCT browser_type), count(DISTINCT event_time)
FROM devices d JOIN events e ON d.device_id = e.device_id
WHERE e.user_id IS NOT NULL
GROUP BY 1 having count(DISTINCT browser_type) > 1 and count(DISTINCT event_time) > 1;

WITH user_grouped AS (
  SELECT e.user_id,
         ROW(d.browser_type, ARRAY[CAST(e.event_time AS DATE)])::browser_dates AS device_activity_datelist
  FROM devices d JOIN events e ON d.device_id = e.device_id
  WHERE e.user_id IS NOT NULL
  GROUP BY 1,2
) SELECT (device_activity_datelist).browser, '2023-01-01'::DATE || (device_activity_datelist).dates AS dates, *
FROM user_grouped where user_id = 146099182543984160;

WITH yesterday AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-01'
), today AS (
  SELECT e.user_id, d.browser_type, e.event_time::DATE AS event_date
  FROM events e JOIN devices d ON d.device_id = e.device_id
  WHERE e.user_id IS NOT NULL AND e.event_time::DATE = '2023-01-01'
  GROUP BY e.user_id, d.browser_type, e.event_time::DATE
)
SELECT COALESCE(t.user_id, y.user_id) AS user_id,
       CASE
         WHEN y.device_activity_datelist IS NULL
           THEN ROW(t.browser_type, ARRAY[t.event_date])::browser_dates
         WHEN t.event_date IS NULL THEN y.device_activity_datelist
         ELSE ROW(t.browser_type, ARRAY[t.event_date]
           || (y.device_activity_datelist).dates)::browser_dates
       END AS device_activity_datelist,
--        ROW(t.browser_type, ARRAY[t.event_date])::browser_dates AS device_activity_datelist,
       COALESCE(t.event_date, y.curr_date + 1) AS curr_date
FROM today t FULL JOIN yesterday y ON y.user_id = t.user_id
 AND (y.device_activity_datelist).browser = t.browser_type;


INSERT INTO user_devices_cumulated
WITH yesterday AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-02'
), today AS (
  SELECT e.user_id, d.browser_type, e.event_time::DATE AS event_date
  FROM events e JOIN devices d ON d.device_id = e.device_id
  WHERE e.user_id IS NOT NULL AND e.event_time::DATE = '2023-01-03'
  GROUP BY e.user_id, d.browser_type, e.event_time::DATE
)
SELECT COALESCE(t.user_id, y.user_id) AS user_id, CASE
  WHEN y.device_activity_datelist IS NULL
    THEN ROW(t.browser_type, ARRAY[t.event_date])::browser_dates
  WHEN t.event_date IS NULL THEN y.device_activity_datelist
  ELSE ROW(t.browser_type, ARRAY[t.event_date]
    || (y.device_activity_datelist).dates)::browser_dates END AS device_activity_datelist,
  COALESCE(t.event_date, y.curr_date + 1) AS curr_date
FROM today t FULL JOIN yesterday y ON y.user_id = t.user_id
  AND (y.device_activity_datelist).browser = t.browser_type;

SELECT * FROM user_devices_cumulated order by curr_date desc;


DO $$
  DECLARE
    min_date DATE := '2023-01-03';
    max_date DATE := '2023-01-30';
    cd DATE;
  BEGIN
    FOR cd IN SELECT generate_series(min_date, max_date, INTERVAL '1 day')  LOOP
      INSERT INTO user_devices_cumulated
      WITH yesterday AS (
      SELECT * FROM user_devices_cumulated WHERE curr_date = cd
      ), today AS (
      SELECT e.user_id, d.browser_type, e.event_time::DATE AS event_date
      FROM events e JOIN devices d ON d.device_id = e.device_id
      WHERE e.user_id IS NOT NULL AND e.event_time::DATE = cd + INTERVAL '1 day'
      GROUP BY e.user_id, d.browser_type, e.event_time::DATE
      )
      SELECT COALESCE(t.user_id, y.user_id) AS user_id, CASE
      WHEN y.device_activity_datelist IS NULL
      THEN ROW(t.browser_type, ARRAY[t.event_date])::browser_dates
      WHEN t.event_date IS NULL THEN y.device_activity_datelist
      ELSE ROW(t.browser_type, ARRAY[t.event_date]
      || (y.device_activity_datelist).dates)::browser_dates END AS device_activity_datelist,
      COALESCE(t.event_date, y.curr_date + 1) AS curr_date
      FROM today t FULL JOIN yesterday y ON y.user_id = t.user_id
      AND (y.device_activity_datelist).browser = t.browser_type;
      END LOOP;
  END $$

SELECT * FROM user_devices_cumulated order by curr_date desc;
SELECT * FROM user_devices_cumulated
WHERE CARDINALITY((device_activity_datelist).dates) > 10
ORDER BY curr_date desc;

-- 4. A datelist_int generation query.
-- Convert the device_activity_datelist column into a datelist_int column

WITH users AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-31'::DATE
), series AS (
  SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
)
SELECT user_id, device_activity_datelist, curr_date,
       series_date::DATE,
       curr_date - series_date::DATE AS date_diff,
       (device_activity_datelist).dates @> ARRAY[series_date::DATE] is_active
FROM users CROSS JOIN series WHERE user_id = 406876712821807740;


WITH users AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-31'::DATE
), series AS (
  SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
)
SELECT user_id, device_activity_datelist, curr_date,
       CASE WHEN (device_activity_datelist).dates @> ARRAY [series_date::DATE]
        THEN POW(2, 32 - (curr_date - series_date::DATE)) ELSE 0 END int_value
FROM users CROSS JOIN series WHERE user_id = 406876712821807740;


WITH users AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-31'::DATE
), series AS (
  SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
)
SELECT user_id, device_activity_datelist, curr_date,
       CASE WHEN (device_activity_datelist).dates @> ARRAY [series_date::DATE]
              THEN POW(2, 32 - (curr_date - series_date::DATE)) ELSE 0 END int_value
FROM users CROSS JOIN series WHERE user_id = 406876712821807740;


WITH users AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-31'::DATE
), series AS (
  SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
), placeholder_ints AS (
  SELECT user_id, device_activity_datelist, curr_date,
    CAST(CASE WHEN (device_activity_datelist).dates @> ARRAY [series_date::DATE]
           THEN CAST(POW(2, 32 - (curr_date - series_date::DATE)) AS BIGINT)
         ELSE 0 END AS BIT(32)) bit_value, *
  FROM series CROSS JOIN users WHERE user_id = '439578290726747300'
)
SELECT * FROM placeholder_ints;

SELECT '2023-01-31'::DATE - '2023-01-31'::DATE A, '2023-01-31'::DATE - '2023-01-01'::DATE B

WITH users AS (
  SELECT * FROM user_devices_cumulated WHERE curr_date = '2023-01-31'::DATE
), series AS (
  SELECT * FROM generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS series_date
), placeholder_ints AS (
  SELECT user_id, device_activity_datelist, curr_date,
         CASE WHEN (device_activity_datelist).dates @> ARRAY [series_date::DATE]
           THEN CAST(POW(2, 32 - (curr_date - series_date::DATE) - 2) AS BIGINT)
         ELSE 0 END bit_value
  FROM series CROSS JOIN users -- WHERE user_id = '439578290726747300'
)
SELECT user_id, device_activity_datelist, curr_date,
  CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(31)) as bit_summed,
  BIT_COUNT(CAST(CAST(SUM(bit_value) AS BIGINT) AS BIT(31))) as days_active
  FROM placeholder_ints
GROUP BY user_id, device_activity_datelist, curr_date;


-- 5. A DDL for `hosts_cumulated` table
-- 5.1. a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
SELECT * from events LIMIT 400;
SELECT host, count(*) from events GROUP BY host;
SELECT user_id, host, count(*) from events GROUP BY user_id, host;
SELECT user_id, count(DISTINCT host) dist_host from events GROUP BY user_id ORDER BY dist_host DESC;
16178357822690200000

DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (
 host TEXT,
-- the list of dates where the host experienced any activity
 host_activity_datelist DATE[],
-- the current date for the host analysis
 curr_date DATE,
 PRIMARY KEY (host, curr_date)
);
COMMENT ON COLUMN hosts_cumulated.host_activity_datelist IS 'logs to see which dates each host is experiencing any activity';

-- 6. The incremental query to generate `host_activity_datelist`

SELECT * FROM events WHERE host IS NULL;

WITH host_grouped_today AS (
  SELECT host, ARRAY[event_time::DATE] AS host_activity_datelist, '2023-01-01'::DATE AS curr_date
  FROM events
  WHERE event_time::DATE = '2023-01-01'::DATE
  GROUP BY host, event_time::DATE
), host_grouped_yesterday AS (
  SELECT * FROM hosts_cumulated WHERE curr_date = '2023-01-01'::DATE
)
SELECT *
FROM host_grouped_today hgt
FULL JOIN host_grouped_yesterday hgy ON hgy.host = hgt.host;

TRUNCATE TABLE hosts_cumulated;

INSERT INTO hosts_cumulated
WITH host_grouped_today AS (
  SELECT host, ARRAY[event_time::DATE] AS host_activity_datelist, x.dt AS curr_date
  FROM events, (SELECT '2023-01-04'::DATE AS dt) x
  WHERE event_time::DATE = x.dt
  GROUP BY host, event_time::DATE, x.dt
), host_grouped_yesterday AS (
  SELECT * FROM hosts_cumulated WHERE curr_date = '2023-01-03'::DATE
)
SELECT COALESCE(hgt.host, hgy.host) AS host,
       CASE
         WHEN hgy.host_activity_datelist IS NULL THEN hgt.host_activity_datelist
         WHEN hgt.host_activity_datelist IS NULL THEN hgy.host_activity_datelist
       ELSE hgt.host_activity_datelist || hgy.host_activity_datelist END AS host_activity_datelist,
  COALESCE(hgt.curr_date, hgy.curr_date + 1) AS curr_date
FROM host_grouped_today hgt
FULL JOIN host_grouped_yesterday hgy ON hgy.host = hgt.host;

SELECT * FROM hosts_cumulated;

-- 7. A monthly, reduced fact table DDL `host_activity_reduced`
--    - month
--    - host
--    - hit_array - think COUNT(1)
--    - unique_visitors array -  think COUNT(DISTINCT user_id)

DROP TABLE IF EXISTS host_activity_reduced;
CREATE TABLE host_activity_reduced (
  host TEXT,
  month_start DATE,
  metric_name TEXT,
  metric_array REAL[],
  PRIMARY KEY (host, month_start, metric_name)
);
COMMENT ON COLUMN host_activity_reduced.metric_name IS 'column to store all metrics names';
COMMENT ON COLUMN host_activity_reduced.metric_array IS 'column to store the metric name all values by table granularity';


-- 8. An incremental query that loads `host_activity_reduced`
--   - day-by-day
-- Metrics:
--    - hit_array - think COUNT(1)
--    - unique_visitors array -  think COUNT(DISTINCT user_id)

WITH host_grouped_today AS (
  SELECT host, ARRAY[event_time::DATE] AS host_activity_datelist, event_time::DATE AS curr_date
  FROM events
  WHERE event_time::DATE = '2023-01-04'::DATE
  GROUP BY host, event_time::DATE
), host_grouped_yesterday AS (
  SELECT * FROM hosts_cumulated WHERE curr_date = '2023-01-03'::DATE
), metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
)
SELECT *
FROM host_grouped_yesterday hgy
CROSS JOIN metrics_name;


-- v2


WITH metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
)
SELECT e.host, DATE_TRUNC('month', e.event_time::DATE)::DATE AS month_start, mn.metric_name, CASE
 WHEN mn.metric_name = 'hit_array' THEN COUNT(*)
 WHEN mn.metric_name = 'unique_visitors' THEN COUNT(DISTINCT e.user_id)
  END AS metric_value
FROM events e CROSS JOIN metrics_name mn
WHERE e.event_time::DATE = '2023-01-01'::DATE
GROUP BY host, DATE_TRUNC('month', e.event_time::DATE)::DATE, metric_name;

WITH metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
), daily_event_metrics AS (
  SELECT e.host, DATE_TRUNC('month', e.event_time::DATE)::DATE AS month_start, mn.metric_name, CASE
     WHEN mn.metric_name = 'hit_array' THEN COUNT(*)
     WHEN mn.metric_name = 'unique_visitors' THEN COUNT(DISTINCT e.user_id)
    END AS metric_value, e.event_time::DATE AS curr_date
  FROM events e CROSS JOIN metrics_name mn
  WHERE e.event_time::DATE = '2023-01-02'::DATE
  GROUP BY host, e.event_time::DATE, metric_name
) SELECT *, curr_date - month_start dd FROM daily_event_metrics;



WITH metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
), daily_event_metrics AS (
SELECT e.host, DATE_TRUNC('month', e.event_time::DATE)::DATE AS month_start, mn.metric_name, CASE
WHEN mn.metric_name = 'hit_array' THEN COUNT(*)
WHEN mn.metric_name = 'unique_visitors' THEN COUNT(DISTINCT e.user_id)
END AS metric_value,
e.event_time::DATE AS curr_date
FROM events e CROSS JOIN metrics_name mn
WHERE event_time::DATE = '2023-01-01'::DATE
GROUP BY host, e.event_time::DATE, metric_name
), yesterday_event_metrics AS (
  SELECT * FROM host_activity_reduced WHERE month_start = '2023-01-01'::DATE
)
SELECT COALESCE(dem.host, yem.host) AS host,
       COALESCE(dem.month_start, yem.month_start) AS month_start,
       COALESCE(dem.metric_name, yem.metric_name) AS metric_name,
       CASE WHEN yem.metric_array IS NOT NULL THEN yem.metric_array || ARRAY[COALESCE(dem.metric_value, 0)]
       ELSE ARRAY_FILL(0, ARRAY[COALESCE(dem.curr_date - dem.month_start, 0)])
              || ARRAY[COALESCE(dem.metric_value, 0)]
       END AS metric_array
FROM daily_event_metrics dem
FULL JOIN yesterday_event_metrics yem ON yem.host = dem.host
      AND yem.month_start = dem.month_start AND yem.metric_name = dem.metric_name;



TRUNCATE TABLE host_activity_reduced;
INSERT INTO host_activity_reduced
WITH metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
), daily_event_metrics AS (
  SELECT e.host, DATE_TRUNC('month', e.event_time::DATE)::DATE AS month_start, mn.metric_name,
         CASE WHEN mn.metric_name = 'hit_array' THEN COUNT(*)
           WHEN mn.metric_name = 'unique_visitors' THEN COUNT(DISTINCT e.user_id)
           END AS metric_value,
         e.event_time::DATE AS curr_date
  FROM events e CROSS JOIN metrics_name mn
  WHERE event_time::DATE = '2023-01-03'::DATE
  GROUP BY host, e.event_time::DATE, metric_name
), yesterday_event_metrics AS (
  SELECT * FROM host_activity_reduced WHERE month_start = '2023-01-01'::DATE
)
SELECT COALESCE(dem.host, yem.host) AS host,
       COALESCE(dem.month_start, yem.month_start) AS month_start,
       COALESCE(dem.metric_name, yem.metric_name) AS metric_name,
       CASE WHEN yem.metric_array IS NOT NULL THEN yem.metric_array || ARRAY[COALESCE(dem.metric_value, 0)]
            ELSE ARRAY_FILL(0, ARRAY[COALESCE(dem.curr_date - dem.month_start, 0)])
              || ARRAY[COALESCE(dem.metric_value, 0)]
         END AS metric_array
FROM daily_event_metrics dem
       FULL JOIN yesterday_event_metrics yem ON yem.host = dem.host
  AND yem.month_start = dem.month_start AND yem.metric_name = dem.metric_name
ON CONFLICT (host, month_start, metric_name)
  DO UPDATE SET metric_array = EXCLUDED.metric_array;


select * from host_activity_reduced;

WITH metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
), daily_event_metrics AS (
  SELECT e.host, DATE_TRUNC('month', e.event_time::DATE)::DATE AS month_start, mn.metric_name,
         CASE
           WHEN mn.metric_name = 'hit_array' THEN COUNT(*)
           WHEN mn.metric_name = 'unique_visitors' THEN COUNT(DISTINCT e.user_id)
    END AS metric_value, e.event_time::DATE AS curr_date
  FROM events e CROSS JOIN metrics_name mn
  WHERE e.event_time::DATE = '2023-01-02'::DATE
  GROUP BY host, e.event_time::DATE, metric_name
) SELECT *, curr_date - month_start dd FROM daily_event_metrics;

WITH metrics_name AS (
  SELECT 'hit_array' AS metric_name UNION SELECT 'unique_visitors' AS metric_name
), daily_event_metrics AS (
  SELECT e.host, DATE_TRUNC('month', e.event_time::DATE)::DATE AS month_start, mn.metric_name,
         CASE
           WHEN mn.metric_name = 'hit_array' THEN COUNT(*)
           WHEN mn.metric_name = 'unique_visitors' THEN COUNT(DISTINCT e.user_id)
           END AS metric_value, e.event_time::DATE AS curr_date
  FROM events e CROSS JOIN metrics_name mn
  WHERE e.event_time::DATE = '2023-01-03'::DATE
  GROUP BY host, e.event_time::DATE, metric_name
) SELECT *, curr_date - month_start dd FROM daily_event_metrics;