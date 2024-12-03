-- INSERT INTO fct_game_details
-- WITH deduped AS (
--     SELECT
--         gd.*
--         , g.game_date_est
--         , g.season
--         , g.home_team_id
--         , ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
--     FROM game_details gd
--         JOIN games g ON gd.game_id = g.game_id
-- )
-- SELECT
--     game_date_est AS dim_game_date
--     , season AS dim_season
--     , team_id AS dim_team_id
--
--     , player_id AS dim_player_id
--     , player_name AS dim_player_name
--     , start_position AS dim_start_position
--     , team_id = home_team_id AS dim_is_playing_at_home
--     , COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play
--     , COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress
--     , COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team
--     , CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS m_minutes -- allows us to calcualte rates from other columns
--     , fgm AS m_fgm
--     , fga AS m_fga
--     , fg3m AS m_fg3m
--     , fg3a AS m_fg3a
--     , ftm AS m_ftm
--     , fta AS m_fta
--     , oreb AS m_oreb
--     , dreb AS m_dreb
--     , reb AS m_reb
--     , ast AS m_ast
--     , stl AS m_stl
--     , blk AS m_blk
--     , "TO" AS m_turnovers
--     , pf AS m_pf
--     , pts AS m_pts
--     , plus_minus AS m_plus_minus
-- FROM deduped
-- WHERE row_num = 1;
--
--
-- CREATE TABLE fct_game_details (
--     dim_game_date DATE -- labeling dim as dimension (group by  dimensions)
--     , dim_season INTEGER
--     , dim_team_id INTEGER
--     , dim_player_id INTEGER
--     , dim_player_name TEXT
--     , dim_start_position TEXT
--     , dim_is_playing_at_home BOOLEAN
--     , dim_did_not_play BOOLEAN
--     , dim_did_not_dress BOOLEAN
--     , dim_not_with_team BOOLEAN
--     , m_minutes REAL -- m is for measure (aggregate and calculate)
--     , m_fgm INTEGER
--     , m_fga INTEGER
--     , m_fg3m INTEGER
--     , m_fg3a INTEGER
--     , m_ftm INTEGER
--     , m_fta INTEGER
--     , m_oreb INTEGER
--     , m_dreb INTEGER
--     , m_reb INTEGER
--     , m_ast INTEGER
--     , m_stl INTEGER
--     , m_blk INTEGER
--     , m_turnovers INTEGER
--     , m_pf INTEGER
--     , m_pts INTEGER
--     , m_plus_minus INTEGER
--     , PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)  -- note, team_id isn't necessary
-- );
--
-- --- TEST
--
-- SELECT t.*, gd.*
-- FROM fct_game_details gd JOIN teams t
-- ON t.team_id = gd.dim_team_id;
--
-- SELECT dim_player_name
--      , COUNT(1) AS num_games
--      , COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num
--     , CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL)/COUNT(1) AS bailed_pct
-- FROM fct_game_details
-- GROUP BY 1
-- ORDER BY 4 DESC



-------------
-- Day 2 Lab --
--
-- DROP TABLE users_cumulated;
-- CREATE TABLE users_cumulated (
--     user_id TEXT
--     , dates_active DATE[]  -- list of dates in past where user active
--     , curr_date DATE -- Current date for user
--     , PRIMARY KEY (user_id, curr_date)
-- );
--
-- INSERT INTO users_cumulated
-- WITH
--     yesterday AS (
--         SELECT *
--         FROM users_cumulated
--         WHERE curr_date = DATE('2023-01-01') +30
--     ),
--     today AS (
--         SELECT
--             CAST(user_id AS TEXT) AS user_id
--             , CAST (event_time AS TIMESTAMP)::date AS date_active
--         FROM events
--         WHERE CAST (event_time AS TIMESTAMP)::date = DATE('2023-01-02') +30
--             AND user_id IS NOT NULL
--         GROUP BY
--                 user_id
--                , CAST (event_time AS TIMESTAMP)::date
--     )
--
-- SELECT
--     COALESCE(t.user_id, y.user_id) AS user_id
--     , CASE
--         WHEN y.dates_active IS NULL THEN ARRAY [t.date_active]
--         WHEN t.date_active IS NULL THEN y.dates_active
--         ELSE ARRAY[t.date_active] || y.dates_active
--     END AS dates_active
--     , DATE(
--         COALESCE(t.date_active, y.curr_date + INTERVAL '1 day')
--       )AS curr_date
-- FROM today t
--     FULL OUTER JOIN yesterday y
--     ON t.user_id=y.user_id;
--
--
-- SELECT *
-- FROM users_cumulated
-- WHERE curr_date = '2023-01-31'
--

---  Want to generate a series of dates
--
-- SELECT *
-- FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')

--
-- WITH users AS (
--     SELECT *
--     FROM users_cumulated
--     WHERE curr_date=DATE('2023-01-31')
-- ),
--     series AS(
--         SELECT *
--         FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
--     )
-- SELECT dates_active @> ARRAY [DATE(series_date)], *
-- FROM users
-- CROSS JOIN series
-- WHERE user_id = '2945778911874778600'


------ Making some additional changes -----
--
-- WITH users AS (
--     SELECT *
--     FROM users_cumulated
--     WHERE curr_date=DATE('2023-01-31')
-- ),
--     series AS(
--         SELECT *
--         FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
-- ),
--     placeholder_ints AS (
--         SELECT
--             CASE WHEN dates_active @> ARRAY [DATE(series_date)]
--                 THEN POW(2, 32 - (curr_date-DATE(series_date)))
--                 ELSE 0
--                 END AS placeholder_int_value
--                 , *
--         FROM users
--         CROSS JOIN series
-- )
-- SELECT
--     user_id
--     , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
--     , BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active
--     , BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &  -- the ampersand is a bitwise AND
--         CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active
-- -- This casts the first 7 values as 1, or True, and then passes the activity as true
--     , BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) &
--             CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
-- FROM placeholder_ints
-- GROUP BY user_id;


------------------------------------------
-- Lab Day 3 Week 2
-- DROP TABLE array_metrics;
-- CREATE TABLE array_metrics (
--     user_id NUMERIC
--     , month_start DATE
--     , metric_name TEXT
--     , metric_array REAL[]
--     , PRIMARY KEY(user_id, month_start, metric_name)
-- );

INSERT INTO array_metrics
WITH daily_aggregate AS (
    SELECT
        user_id
        , DATE(event_time) AS curr_date
        , COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-03')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
),
    yesterday_array AS (
        SELECT *
        FROM array_metrics
        WHERE month_start = DATE('2023-01-01')
    )
SELECT
    COALESCE(da.user_id, ya.user_id) as user_id
    , COALESCE(ya.month_start, DATE_TRUNC('month', da.curr_date)) AS month_start
    , 'site_hits' AS metric_name
    , CASE WHEN ya.metric_array IS NOT NULL
            THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.month_start IS NULL THEN ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array IS NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(curr_date - DATE(DATE_TRUNC('month', curr_date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
        END AS metric_array
FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
        ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO
    UPDATE SET metric_array = EXCLUDED.metric_array;




SELECT cardinality(metric_array), COUNT(1)
FROM array_metrics
GROUP BY 1;


SELECT *
FROM array_metrics;

WITH agg AS(
    SELECT
        metric_name
        , month_start
        , ARRAY[SUM(metric_array[1])
                , SUM(metric_array[2])
                , SUM(metric_array[3])
                , SUM(metric_array[4])
            ] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
SELECT metric_name
    , month_start + CAST(CAST(index-1 AS TEXT) || 'day' AS INTERVAL)
    , elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array)
    WITH ORDINALITY AS a(elem, index)