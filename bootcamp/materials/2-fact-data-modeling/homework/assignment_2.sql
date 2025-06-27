-- Query 1
WITH deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
	FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1

-- Query 2
CREATE TABLE user_devices_cumulated (
	user_id NUMERIC,
	device_activity_datelist DATE[],
	browser_type NUMERIC,
	date DATE,
	PRIMARY KEY(user_id, date, browser_type)
);

-- Query 3
INSERT INTO user_devices_cumulated(
	-- historical data
	WITH yesterday AS(
		SELECT
			user_id,
			device_activity_datelist,
			browser_type,
			date
		FROM user_devices_cumulated
		WHERE date = DATE('2022-12-31')
	),
	-- daily data to be loaded, change date as required
	today AS (
		SELECT
			user_id,
			DATE(CAST(event_time AS TIMESTAMP)) AS device_activity_date,
			device_id AS browser_type
		FROM events
		WHERE user_id IS NOT NULL AND device_id IS NOT NULL AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
		GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP)), browser_type
	)
	SELECT
		COALESCE(t.user_id, y.user_id) AS user_id,
		CASE
			-- if no prior records exists, start a new list
			WHEN y.device_activity_datelist IS NULL
			THEN ARRAY[t.device_activity_date]
			-- if no new record, retain previous
			WHEN t.device_activity_date IS NULL
			THEN y.device_activity_datelist
			-- if both exists, append new record to the front of the list
			ELSE ARRAY[t.device_activity_date] || y.device_activity_datelist
		END AS device_activity_datelist,
		COALESCE(t.browser_type, y.browser_type) AS browser_type,
		-- if no new activity date, use previous date + 1 day
		COALESCE(t.device_activity_date, DATE(y.date + INTERVAL '1 day')) AS date
	FROM today t
	-- join on user id and browser type to keep device data
	FULL OUTER JOIN yesterday y ON t.user_id = y.user_id AND t.browser_type = y.browser_type
	ORDER BY user_id;
)

-- Query 4
WITH users AS (
	SELECT * FROM user_devices_cumulated
	WHERE date = '2023-01-10'
),
-- generate series of dates to perform days since calculations
series AS (
	SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
placeholder_ints AS (
	SELECT 
		CASE
			-- check activity dates if exists in date series
			WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
			THEN POW(2, 32 - (date - DATE(series_date)))
			ELSE 0
		END AS placeholder_dec,
		*
	FROM users
	CROSS JOIN series
)
SELECT
	CAST(CAST(SUM(placeholder_dec) AS BIGINT) AS BIT(32)),
	user_id,
	browser_type,
	-- cast sum of values to 32 bits to account for 30 days, 2 bits are extra
	-- left-most bit denotes the current day, every bit to the right is the day prior
	BIT_COUNT(CAST(CAST(SUM(placeholder_dec) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
	-- using bitwise '&' gate filters bits to only the first 7 for weekly precision
	BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_dec) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active,
	-- using bitwise '&' gate filters bits to only the first bit for daily precision
	BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_dec) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM placeholder_ints
GROUP BY user_id, browser_type

-- Query 5
CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	user_id NUMERIC,
	date DATE,
	PRIMARY KEY(host, date)
);

-- Query 6
INSERT INTO hosts_cumulated(
	WITH yesterday AS (
		SELECT
			*
		FROM hosts_cumulated
		WHERE date = DATE('2022-12-31')
	),
	today AS (
		SELECT
			host,
			user_id,
			DATE(CAST(event_time AS TIMESTAMP)) AS host_activity_date
		FROM events
		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
	)
	SELECT
		COALESCE(t.host, y.host) AS host,
		CASE
			-- if no prior records exists, start a new list
			WHEN y.host_activity_datelist IS NULL
			THEN ARRAY[t.host_activity_date]
			-- if no new record, retain previous
			WHEN t.host_activity_date IS NULL
			THEN y.host_activity_datelist
			-- if both exists, append new record to the front of the list
			ELSE ARRAY[t.host_activity_date] || y.host_activity_datelist
		END AS host_activity_datelist,
		COALESCE(t.user_id, y.user_id) AS user_id,
		-- if no new activity date, use previous date + 1 day
		COALESCE(t.host_activity_date, DATE(y.date + INTERVAL '1 day')) AS date
	FROM today t FULL OUTER JOIN yesterday y ON t.host = y.host AND t.user_id = y.user_id;
)

-- Query 7
CREATE TABLE host_activity_reduced (
	month DATE,
	host TEXT,
	hit_array INTEGER[],
	unique_visitors INTEGER[],
	PRIMARY KEY(month, host)
);

-- Query 8
INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
    -- Aggregate daily site hits per user
    SELECT 
        user_id,
		host,
        DATE(event_time) AS date,
        COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
    AND user_id IS NOT NULL
    GROUP BY user_id, host, DATE(event_time)
),
yesterday_array AS (
    -- Retrieve existing metrics for the month starting from '2023-01-01'
    SELECT *
    FROM host_activity_reduced 
    WHERE month = DATE('2023-01-01')
),
daily_hits_aggregate AS (
	-- Aggregate daily site hits
	SELECT 
		-- Determine month
		DATE_TRUNC('month', date) AS month,
		-- Hostname of hit sites
		host,
		date,
		-- Total of site hits
		SUM(num_site_hits) AS daily_hits
	FROM daily_aggregate
	GROUP BY host, date
),
daily_unique_hits_aggregate AS (
	-- Aggregate daily unique site hits
	SELECT 
		DATE_TRUNC('month', date) AS month,
		host,
		date,
		-- Each row is total hits from a user in a day
		COUNT(1) AS daily_unique_visitors
	FROM daily_aggregate
	GROUP BY host, date
)
SELECT 
	-- Determine month
	COALESCE(ya.month, DATE_TRUNC('month', dha.date)) AS month,
	-- Hostname of hit sites
	COALESCE(dha.host, ya.host) AS host,
	-- Update hit_array based on existing data and new daily aggregates
	CASE 
		WHEN ya.hit_array IS NOT NULL THEN 
			ya.hit_array || ARRAY[COALESCE(dha.daily_hits,0)] 
		WHEN ya.hit_array IS NULL THEN
			ARRAY_FILL(0, ARRAY[COALESCE (dha.date - DATE(DATE_TRUNC('month', dha.date)), 0)]) 
				|| ARRAY[COALESCE(dha.daily_hits,0)]
	END AS hit_array,
	CASE 
		WHEN ya.unique_visitors IS NOT NULL THEN 
			ya.unique_visitors || ARRAY[COALESCE(duha.daily_unique_visitors,0)] 
		WHEN ya.unique_visitors IS NULL THEN
			ARRAY_FILL(0, ARRAY[COALESCE (duha.date - DATE(DATE_TRUNC('month', duha.date)), 0)]) 
				|| ARRAY[COALESCE(duha.daily_unique_visitors,0)]
	END AS unique_visitors
FROM daily_hits_aggregate dha
FULL OUTER JOIN daily_unique_hits_aggregate duha
ON dha.host = duha.host AND dha.month = duha.month
FULL OUTER JOIN yesterday_array ya 
ON dha.host = ya.host AND dha.month = ya.month
-- for onward query runs, update to add new data without overwriting previous records
ON CONFLICT (host, month)
DO 
    UPDATE SET hit_array = EXCLUDED.hit_array, unique_visitors = EXCLUDED.unique_visitors;