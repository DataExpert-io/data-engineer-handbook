-- -- 1. Query to duplicate game_details from Day 1 so there are no duplicates
SELECT *
FROM devices;
--
SELECT *
FROM events;

-- These two tables share the device_id

-- WITH duplicates AS (SELECT *
--         , COUNT(*) OVER (PARTITION BY (gd.*)::text
--         ) AS count
--         FROM game_details gd)
-- SELECT *
-- FROM duplicates
-- WHERE count > 1;

-- Note, there are a lot of duplicates!
-- WITH deduped AS (SELECT gd.*
--                          , g.game_date_est
--                          , g.season
--                          , g.home_team_id
--                          , ROW_NUMBER()
--                            OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
--                     FROM game_details gd
--                              JOIN games g
--                                   ON gd.game_id = g.game_id)
-- , duplicates AS (SELECT *
--         , COUNT(*) OVER (PARTITION BY (deduped.*)::text
--         ) AS count
--         FROM deduped)
-- SELECT *
-- FROM duplicates
-- WHERE count > 1;

-- This checks out with no duplicates
--
-- WITH deduper AS (
--     SELECT
--         gd.*
--         , g.game_date_est
--         , g.season
--         , g.home_team_id
--         , ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
--     FROM game_details gd
--     JOIN games g
--     ON gd.game_id = g.game_id
-- ),
-- deduped AS (
--     SELECT *
--     , COUNT(*) OVER (PARTITION BY (deduper.*)::text) AS count
-- FROM deduper
-- WHERE row_num = 1
-- )
-- SELECT DISTINCT(count)
-- FROM deduped
-- ;
-- This verifies that the only count is 1, so there are no longer dupes


WITH deduper AS (
    SELECT
        gd.*
        , g.game_date_est
        , g.season
        , g.home_team_id
        , ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g
    ON gd.game_id = g.game_id
),
deduped AS (
    SELECT *
FROM deduper
WHERE row_num = 1
)
SELECT *
FROM deduped;
--
--
-- ---------------------
-- -- 2. A DDL for a user_devices_cumulated table with device_activity_datelist that tracks users active days by browser_type
-- -- To get a user_devices_cumulated, we'll need to create a table with
--
-- DROP TABLE user_devices_cumulated;
--
-- -- CREATE TABLE user_devices_cumulated (
-- --     user_id text
-- --     , browser_type text
-- --     , device_activity_datelist DATE[]
-- --     , curr_date DATE
-- --     , PRIMARY KEY (user_id, browser_type, curr_date)
-- -- );
--
CREATE TABLE user_devices_cumulated (
    user_id TEXT
    , device_id NUMERIC
    , browser_type TEXT
    , curr_date DATE
    , device_activity_datelist DATE[]
    , PRIMARY KEY (user_id, device_id, browser_type, curr_date)  -- the date is important here
);
--
-- -----------------
-- -- 3. Cumulative query to generate device_activity_datelist
    -- Check the limits of the dates for the dataset
-- -- SELECT MIN(CAST (event_time AS TIMESTAMP)::date) AS first_date_active
-- --     , MAX(CAST (event_time AS TIMESTAMP)::date) AS last_date_active
-- -- FROM events;
--
-- -- So all of the dates go from 2023-01-01 to 2023-01-31
-- -- WITH date_series AS (
-- --     SELECT generate_series(
-- --            '2023-01-01'::date
-- --            , '2023-01-31'::date
-- --            , '1 day'::interval
-- --            ) AS date_active
-- -- )
-- -- SELECT
-- --     COALESCE(e.device_id, d.device_id) AS device_id
-- --     , CAST (e.user_id AS TEXT) AS user_id
-- --     , CAST (e.event_time AS TIMESTAMP)::date AS date_active
-- -- FROM events e
-- --     FULL OUTER JOIN devices d
-- --         ON e.device_id = d.device_id
-- -- WHERE CAST(e.event_time AS TIMESTAMP)::date = '2023-01-01'    -- IN (SELECT date_active FROM date_series)
-- --     AND e.user_id IS NOT NULL
-- -- GROUP BY
-- --         CAST (e.user_id AS TEXT) AS user_id
-- --         , COALESCE(e.device_id, d.device_id) AS device_id
-- --         , CAST(event_time AS TIMESTAMP)::date
-- -- ;
-- --
-- -- SELECT e.*
-- --     , d.browser_type
-- -- FROM events e
-- --     FULL OUTER JOIN devices d
-- --         ON e.device_id = d.device_id;
-- --
-- -- INSERT INTO user_devices_cumulated
-- -- WITH today AS (SELECT CAST (e.user_id AS TEXT) AS user_id
-- --                     , d.browser_type
-- --                     , CAST(e.event_time AS TIMESTAMP)::date AS date_active
-- --                FROM events e
-- --                         JOIN devices d ON e.device_id = d.device_id
-- --                WHERE CAST(e.event_time AS TIMESTAMP)::date = DATE('2023-01-01') + 31
-- --                  AND e.user_id IS NOT NULL
-- --                GROUP BY CAST (e.user_id AS TEXT)
-- --                       , d.browser_type
-- --                       , CAST(e.event_time AS TIMESTAMP)::date
-- --
-- -- ),
-- --     yesterday AS (
-- --         SELECT *
-- --         FROM user_devices_cumulated
-- --         WHERE curr_date = DATE('2022-12-31') + 31
-- -- )
-- --
-- -- SELECT
-- --     COALESCE(t.user_id, y.user_id) AS user_id
-- --     , COALESCE(t.browser_type, y.browser_type) AS browser_type
-- --     , CASE
-- --         WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
-- --         WHEN t.date_active IS NULL THEN y.device_activity_datelist
-- --         ELSE ARRAY[t.date_active] || y.device_activity_datelist
-- --     END AS device_activity_datelist
-- --     , DATE(COALESCE(t.date_active, y.curr_date + INTERVAL '1 day')) AS curr_date
-- -- FROM today t
-- --     FULL OUTER JOIN yesterday y
-- --         ON t.user_id = y.user_id
-- --         AND t.browser_type = y.browser_type;
-- --
-- -- SELECT *
-- -- FROM user_devices_cumulated;
--
INSERT INTO user_devices_cumulated
WITH today AS (
    SELECT
        CAST(e.user_id AS TEXT)
        , DATE(e.event_time) AS curr_date
        , e.device_id
        , d.browser_type
        , ROW_NUMBER() OVER (PARTITION BY e.user_id, e.device_id, d.browser_type) AS row_num -- any number that exceeds 1 is a duplicate
    FROM events e
        LEFT JOIN devices d ON d.device_id=e.device_id
    WHERE DATE(e.event_time) = DATE('2023-01-01') + 31
      -- instead of changing the date 30 times, I increased this number from 0 and reprocessed
        AND e.user_id IS NOT NULL
        AND e.device_id IS NOT NULL
), deduped_today AS (
    SELECT *
    FROM today
    WHERE row_num = 1
), yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE curr_date = DATE('2022-12-31') + 31
    -- AS above, this date was offset back by one, but increased numerically instead of changing the dates
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id
    , COALESCE(t.device_id, y.device_id) AS device_id
    , COALESCE(t.browser_type, y.browser_type) AS browser_type
    , COALESCE(t.curr_date, y.curr_date + 1) AS curr_date -- curr_date from yesterday is 1 day behind
    , CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.curr_date]
        WHEN t.curr_date IS NULL THEN y.device_activity_datelist
        ELSE ARRAY[t.curr_date] || y.device_activity_datelist
    END AS device_activity_datelist
FROM deduped_today t
    FULL OUTER JOIN yesterday y
        ON t.user_id = y.user_id
        AND t.device_id = y.device_id
        AND t.browser_type = y.browser_type;

-- -------------------------------------
-- --- Generate a datelist_int generation query

WITH user_devices AS(
    SELECT *
    FROM user_devices_cumulated
    WHERE curr_date=DATE('2023-01-31')
), series AS (
    SELECT *
    FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
), placeholder_ints AS (
    SELECT
        CASE
            WHEN device_activity_datelist @> ARRAY[DATE(s.series_date)] -- this checks that prior is within the following array
                THEN CAST(POW(2, 31 - (curr_date - DATE(s.series_date))) AS BIGINT) -- 31 days minus 1 gives a 30 day rotation
            ELSE 0
            END AS placeholder_int_value
        , *
    FROM user_devices ud
        CROSS JOIN series s
)
SELECT
    user_id
    , device_id
    , browser_type
    , device_activity_datelist
    , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist -- this converts to a binary sequence showing a digit per day
FROM placeholder_ints AS p
GROUP BY user_id, device_id, browser_type, device_activity_datelist;


--
-- ----------------------------
-- -- Create a DDL for hosts_cumulated, and a host_activity_datelist which logs to see which dates the host is experiencing any activity
--
CREATE TABLE hosts_cumulated (
    host TEXT
    , month_start_date DATE
    , host_activity_datelist DATE[]
    , PRIMARY KEY (host, month_start_date)
);
--
-- -------------------------------------------
-- -- Generate the host_activity_datelist


INSERT INTO hosts_cumulated
WITH today AS (
    SELECT
        host
        , DATE(event_time) AS curr_date
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01') + 14 -- numerical manual iteration to increase dates starting at 0, ended at 14 this time, but it can go to 31
        AND host IS NOT NULL
    GROUP BY
        host
        , DATE(event_time)
), yesterday AS(
    SELECT
        *
    FROM hosts_cumulated
    WHERE month_start_date = DATE('2023-01-01') -- the month start date is in January, not december, hence yesterday not being yesterday's date
)
SELECT
    COALESCE(t.host, y.host) AS host
    , COALESCE(DATE_TRUNC('month', t.curr_date), y.month_start_date) AS month_start_date
    , CASE
        WHEN y.host_activity_datelist IS NULL
            THEN ARRAY[t.curr_date]
        WHEN t.curr_date IS NULL
            THEN y.host_activity_datelist
        ELSE y.host_activity_datelist || ARRAY[t.curr_date]
    END AS host_activity_datelist
FROM today t
    FULL OUTER JOIN yesterday y
        ON t.host = y.host
ON CONFLICT(host, month_start_date)
DO
    UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist;
--
-- ----------------------------------
-- -- Incremental query to generate, I added numbers to the date up to 14 and inserted each row; this can increase up to 30

-- this step was done in tandem with the previous query, but I iterated through 14 days again numerically instead of adding the dates

SELECT *
FROM hosts_cumulated;

----------------------------------
-- Host Activity Reduced DDL

CREATE TABLE host_activity_reduced (
    host TEXT
    , month_start_date DATE
    , hit_array BIGINT[]
    , unique_visitors BIGINT[]
    , PRIMARY KEY (host, month_start_date)
);


-- TRUNCATE TABLE host_activity_reduced;

INSERT INTO host_activity_reduced
WITH today AS (
    SELECT
        host
        , DATE(event_time) AS curr_date
        , COUNT(DISTINCT (user_id)) AS unique_visitors
        , COUNT(1) AS hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01') + 14 -- numerical date iterator
        AND host IS NOT NULL
    GROUP BY
        host
        , DATE(event_time)
), yesterday AS(
    SELECT
        *
    FROM host_activity_reduced
    WHERE month_start_date = DATE('2023-01-01')
)
SELECT
    COALESCE(t.host, y.host) AS host
    , COALESCE(DATE_TRUNC('month', t.curr_date), y.month_start_date) AS month_start_date -- truncates the current date for the month, then coalesce with month
    , CASE WHEN y.unique_visitors IS NOT NULL
                THEN y.unique_visitors || ARRAY[COALESCE(t.unique_visitors, 0)]
            WHEN y.unique_visitors IS NULL
                THEN ARRAY_FILL(0, ARRAY[COALESCE(curr_date - DATE(DATE_TRUNC('month', curr_date)), 0)]) || ARRAY[COALESCE(t.unique_visitors, 0)]
        END AS unique_visitors
    , CASE WHEN y.hit_array IS NOT NULL
                THEN y.hit_array || ARRAY[COALESCE(t.hits, 0)]
            WHEN y.hit_array IS NULL
                THEN ARRAY_FILL(0, ARRAY[COALESCE(curr_date - DATE(DATE_TRUNC('month', curr_date)), 0)]) || ARRAY[COALESCE(t.hits, 0)]
        END AS hit_array
FROM today t
    FULL OUTER JOIN yesterday y
        ON t.host = y.host
ON CONFLICT (host, month_start_date)
    DO
    UPDATE SET unique_visitors = EXCLUDED.unique_visitors
    , hit_array = EXCLUDED.hit_array;


SELECT *
FROM host_activity_reduced;

-- AGAIN, I incrementally changed the + number up to 14, but there are 31 total days