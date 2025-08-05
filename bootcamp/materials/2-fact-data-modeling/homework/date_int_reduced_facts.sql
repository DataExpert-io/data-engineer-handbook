--A query to deduplicate `game_details` from Day 1 so there's no duplicates
with deduped as 
	(SELECT *,
			-- Assign row numbers partitioned by game_id, team_id, and player_id for deduplication
			row_number() over (partition by game_id, team_id, player_id order by team_id desc, player_id desc) as rnk
	FROM game_details)

-- Select only unique entries
select *
from deduped
where rnk = 1;

-- DDL for user_devices_cumulated
CREATE TABLE user_devices_cumulated (
    user_id numeric,
    browser_type TEXT,
    device_activity_datelist DATE[], -- Tracks active days per browser type
    date DATE, -- Date partition for cumulative tracking
    PRIMARY KEY (user_id, browser_type, date)
);

-- Cumulative query to generate `device_activity_datelist` from `events`
WITH yesterday AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT user_id,
           url as browser_type,
           DATE_TRUNC('day', event_time::timestamp) AS today_date
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-31')
    AND user_id IS NOT NULL
    GROUP BY user_id, url, DATE_TRUNC('day', event_time::timestamp)
)

-- Merge today's and yesterday's data
SELECT COALESCE(t.user_id, y.user_id) AS user_id,
       COALESCE(t.browser_type, y.browser_type) AS browser_type,
       -- Concatenate today's activity to cumulative array; handle NULL appropriately
       COALESCE(y.device_activity_datelist, ARRAY[]::DATE[]) ||
                CASE WHEN t.user_id IS NOT NULL
                     THEN ARRAY[t.today_date]
                     ELSE ARRAY[]::DATE[] END AS device_activity_datelist,
       -- If no activity today, increment the date by one day
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t 
  ON y.user_id = t.user_id AND y.browser_type = t.browser_type;

--  `datelist_int` generation query from `device_activity_datelist`
WITH starter AS (
    SELECT user_id,
           browser_type,
           valid_date,
           EXTRACT(DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           -- Check if the valid_date exists in device_activity_datelist
           device_activity_datelist @> ARRAY [DATE(d.valid_date)] AS is_active
    FROM user_devices_cumulated
	CROSS JOIN
         (SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = date('2023-01-31')
),
bits AS (
    SELECT user_id,
           browser_type,
           -- Bitwise representation of active dates
           SUM(POW(2, 31 - days_since))::bigint::bit(32) AS datelist_int,
           DATE('2023-01-31') AS date
    FROM starter
    GROUP BY user_id, browser_type
)
SELECT * FROM bits;

--  DDL for `hosts_cumulated`
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[], -- Logs dates with activity
    date DATE, -- Cumulative date tracking
    PRIMARY KEY (host, date)
);

-- Incremental query to generate `host_activity_datelist`
WITH yesterday AS (
    SELECT * FROM hosts_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT host,
           DATE_TRUNC('day', event_time::timestamp) AS today_date
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-31')
    GROUP BY host, DATE_TRUNC('day', event_time::timestamp)
)

-- Merge today's data with cumulative array
SELECT COALESCE(t.host, y.host) AS host,
       COALESCE(y.host_activity_datelist, ARRAY[]::DATE[]) ||
                CASE WHEN t.host IS NOT NULL
                     THEN ARRAY[t.today_date]
                     ELSE ARRAY[]::DATE[] END AS host_activity_datelist,
       COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t 
  ON y.host = t.host;

-- Monthly, reduced fact table DDL `host_activity_reduced`
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[], -- Monthly hits array
    unique_visitors INTEGER[], -- Monthly unique visitors array
    PRIMARY KEY (month, host)
);

-- Incremental query to load `host_activity_reduced` with detailed comments
WITH yesterday AS (
    SELECT * FROM host_activity_reduced
    WHERE month = DATE('2023-03-01')
),
today AS (
    SELECT host,
           DATE_TRUNC('day', event_time) AS today_date,
           COUNT(1) AS num_hits,
           COUNT(DISTINCT user_id) AS num_unique_visitors
    FROM events
    WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-03')
    GROUP BY host, DATE_TRUNC('day', event_time)
)
INSERT INTO host_activity_reduced
SELECT DATE('2023-03-01') AS month,
       COALESCE(y.host, t.host) AS host,
       -- Initialize arrays if no prior data exists
       COALESCE(y.hit_array, array_fill(NULL::INTEGER, ARRAY[DATE('2023-03-03') - DATE('2023-03-01')])) || ARRAY[t.num_hits] AS hit_array,
       COALESCE(y.unique_visitors, array_fill(NULL::INTEGER, ARRAY[DATE('2023-03-03') - DATE('2023-03-01')])) || ARRAY[t.num_unique_visitors] AS unique_visitors
FROM yesterday y
FULL OUTER JOIN today t
ON y.host = t.host;
