
/*---------------------
Task 1 - A query to deduplicate game_details 
 from Day 1 so there's no duplicates
 ---------------------*/

WITH deduped AS(

SELECT 
	g.game_date_est,
	g.season,
	g.home_team_id,
	gd.*,
	ROW_NUMBER () over(PARTITION BY gd.game_id, gd.team_id, gd.player_id) AS row_num
FROM game_details gd 
INNER JOIN games g ON gd.game_id =g.game_id
)
SELECT 
	game_date_est,
	season,
	team_id,
	team_id = home_team_id AS dim_ist_playing_at_home,
	player_id,
	player_name,
	start_position ,
	COALESCE (POSITION('DNP' IN comment),0) > 0 AS dim_did_not_play,
	COALESCE (POSITION('DND' IN comment),0) > 0 AS dim_did_not_dress,
	COALESCE (POSITION('NWT' IN comment),0) > 0 AS dim_not_with_team,
	CASE 
        WHEN min IS NOT NULL THEN SPLIT_PART(min, ':', 1)::REAL + SPLIT_PART(min, ':', 2)::REAL / 60
        ELSE 0 
    END AS minutes,
	fgm,
	fga,
	fgm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	"TO" AS turnovers,
	pf,
	pts,
	plus_minus 
FROM deduped
WHERE row_num = 1



/*---------------------
Task 2 - DDL for an user_devices_cumulated table
 ---------------------*/

CREATE TABLE user_devices_cumulated(

user_id NUMERIC,
device_id NUMERIC,
browser_type varchar,
device_type varchar,
os_type varchar,
date date,
device_activity_datelist date[],
PRIMARY KEY (user_id, device_id, browser_type,date )
);


/*---------------------
Task 3 - A cumulative query to generate device_activity_datelist from events
 ---------------------*/

INSERT INTO user_devices_cumulated (
    user_id,
    device_id,
    browser_type,
    device_type,
    os_type,
    date,
    device_activity_datelist
)
WITH yesterday AS (
    SELECT 
        user_id,
        device_id,
        browser_type,
        device_type,
        os_type,
        device_activity_datelist,
        date
    FROM user_devices_cumulated
    WHERE date = '2023-01-04'
),
 today AS (
    SELECT 
        e.user_id::NUMERIC ,
        e.device_id::NUMERIC ,
        d.browser_type,
        d.device_type,
        d.os_type,
        DATE_TRUNC('day', e.event_time::timestamp) AS today_date
    FROM events e
    INNER JOIN devices d
        ON e.device_id = d.device_id
    WHERE DATE_TRUNC('day', e.event_time::timestamp) = DATE('2023-01-05')
    AND e.user_id IS NOT NULL 
    GROUP BY 
        e.user_id, e.device_id, d.browser_type, d.device_type, d.os_type, DATE_TRUNC('day', e.event_time::timestamp)
)
SELECT
    COALESCE(t.user_id, y.user_id ) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    COALESCE(t.device_type, y.device_type ) AS device_type,
    COALESCE(t.os_type, y.os_type) AS os_type,
    COALESCE (t.today_date, y.date + INTERVAL '1 DAY') AS date,
    CASE 
       WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.today_date]
        WHEN t.today_date IS NULL THEN y.device_activity_datelist
        ELSE y.device_activity_datelist || ARRAY[t.today_date]
    END AS device_activity_datelist
FROM yesterday y
FULL OUTER JOIN today t
    ON y.user_id = t.user_id
    AND y.device_id = t.device_id
    AND y.browser_type = t.browser_type;
   
   
   /*---------------------
Task 4 - A datelist_int generation query
 ---------------------*/

WITH starter AS (
    SELECT 
        uc.device_activity_datelist @> ARRAY [DATE(d.valid_date)] AS is_active,
        EXTRACT(DAY FROM DATE('2023-01-05') - d.valid_date) AS days_since,
        uc.user_id,
        uc.device_id,
        uc.browser_type,
        uc.device_type,
        uc.os_type
    FROM user_devices_cumulated uc
    CROSS JOIN (
        SELECT generate_series('2023-01-01', '2023-01-05', INTERVAL '1 day') AS valid_date
    ) AS d
    WHERE uc.date = DATE('2023-01-05')
),
bits AS (
    SELECT 
        user_id,
        device_id,
        browser_type,
        device_type,
        os_type,
        SUM(
            CASE
                WHEN is_active THEN POW(2, 32 - days_since)
                ELSE 0
            END
        )::BIGINT::BIT(32) AS datelist_int
    FROM starter
    GROUP BY user_id, device_id, browser_type, device_type, os_type
)
SELECT
    user_id,
    device_id,
    browser_type,
    device_type,
    os_type,
    datelist_int,
    BIT_COUNT(datelist_int) > 0 AS monthly_active,
    BIT_COUNT(datelist_int) AS l32,
    BIT_COUNT(
        datelist_int & CAST('11111110000000000000000000000000' AS BIT(32))
    ) > 0 AS weekly_active,
    BIT_COUNT(
        datelist_int & CAST('11111110000000000000000000000000' AS BIT(32))
    ) AS l7,
    BIT_COUNT(
        datelist_int & CAST('00000001111111000000000000000000' AS BIT(32))
    ) > 0 AS weekly_active_previous_week
FROM bits;

  /*---------------------
Task 5 - A DDL for hosts_cumulated table
 ---------------------*/

CREATE TABLE hosts_cumulated (
    host VARCHAR, -- The host column from the events table
    host_activity_datelist DATE[], -- Array of dates representing activity
    month_start DATE, -- When the aggregation was last updated
    PRIMARY KEY (host, month_start) -- Unique per host
);


  /*---------------------
Task 6 - The incremental query to generate host_activity_datelist
 ---------------------*/
 
WITH daily_aggregate AS (
    SELECT 
        host,
        event_time::date AS activity_date
    FROM events
    WHERE DATE(event_time) = '2023-01-02'::date -- Adjust the date as needed
    GROUP BY host, DATE(event_time)
),
yesterday_array AS (
    SELECT 
        host,
        host_activity_datelist,
        month_start
    FROM hosts_cumulated
    WHERE month_start = '2023-01-01'::date -- Adjust to the previous day's date
),
updated_data AS (
    SELECT
        COALESCE(da.host, ya.host) AS host,
        CASE 
            -- Append today's activity date to the existing list
            WHEN ya.host_activity_datelist IS NOT NULL THEN 
                ARRAY(SELECT DISTINCT UNNEST(ya.host_activity_datelist || ARRAY[da.activity_date]))
            -- Initialize the list with today's activity date
            WHEN ya.host_activity_datelist IS NULL THEN 
                ARRAY[da.activity_date]
        END AS host_activity_datelist,
        COALESCE(da.activity_date, ya.month_start) AS month_start -- Update the last activity date
    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
        ON da.host = ya.host
)
INSERT INTO hosts_cumulated (host, host_activity_datelist, month_start)
SELECT
    host,
    host_activity_datelist,
    month_start 
FROM updated_data
ON CONFLICT (host,month_start) DO UPDATE
SET 
    host_activity_datelist = EXCLUDED.host_activity_datelist,
    month_start = EXCLUDED.month_start;
    
   /*---------------------
Task 7 - A monthly, reduced fact table DDL host_activity_reduced
 ---------------------*/
    
    CREATE TABLE host_activity_reduced (
    month DATE, -- The start of the month
    host VARCHAR, -- The host being tracked
    hit_array BIGINT[], -- Array of daily hit counts (COUNT(1))
    unique_visitors_array BIGINT[], -- Array of unique visitors per day (COUNT(DISTINCT user_id))
    PRIMARY KEY (month, host) -- Ensures each host has one entry per month
);
   

   /*---------------------
Task 8 - An incremental query that loads host_activity_reduced
 ---------------------*/

WITH daily_aggregate AS (
    SELECT 
        DATE_TRUNC('month', event_time::DATE) AS month,
        host,
        event_time::DATE AS activity_date,
        COUNT(1) AS daily_hits,
        COUNT(DISTINCT user_id) AS daily_unique_visitors
    FROM events
    WHERE event_time::DATE = '2023-01-02'::DATE -- Replace with the current date being processed
    GROUP BY DATE_TRUNC('month', event_time::DATE), host, event_time::DATE
),
yesterday_array AS (
    SELECT 
        month,
        host,
        hit_array,
        unique_visitors_array
    FROM host_activity_reduced
    WHERE month = '2023-01-01'::DATE 
),
updated_data AS (
    SELECT
        COALESCE(da.host, ya.host) AS host,
        COALESCE(da.month, ya.month) AS month,
        -- Update hit_array
        CASE 
            WHEN ya.hit_array IS NOT NULL THEN 
                ya.hit_array || ARRAY[COALESCE(da.daily_hits, 0)]
            WHEN ya.hit_array IS NULL THEN 
                ARRAY_FILL(0, ARRAY[COALESCE((da.activity_date - ya.month)::INT, 0)]) || ARRAY[COALESCE(da.daily_hits, 0)]
        END AS hit_array,
        -- Update unique_visitors_array
        CASE 
            WHEN ya.unique_visitors_array IS NOT NULL THEN 
                ya.unique_visitors_array || ARRAY[COALESCE(da.daily_unique_visitors, 0)]
            WHEN ya.unique_visitors_array IS NULL THEN 
                ARRAY_FILL(0, ARRAY[COALESCE((da.activity_date - ya.month)::INT, 0)]) || ARRAY[COALESCE(da.daily_unique_visitors, 0)]
        END AS unique_visitors_array
    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
        ON da.host = ya.host AND da.month = ya.month
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT
    month,
    host,
    hit_array,
    unique_visitors_array
FROM updated_data
ON CONFLICT (month, host) DO UPDATE
SET 
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;
   
  