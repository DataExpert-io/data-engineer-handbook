-- DAY 1, Struct and Array
-------------------
-- Create a struct (this will be using CREATE TYPE)                        )

-- SELECT * FROM player_seasons;
-- CREATE TYPE season_stats AS
-- (
--     season INTEGER,
--     gp     INTEGER,
--     pts    REAL,
--     reb    REAL,
--     ast    REAL
-- );
--------------------------
-- Create a quality enumeration
-- CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');
--
--
--
--
-- -- DROP TABLE players;
-- CREATE TABLE players (
--     player_name TEXT
--     , height TEXT
--     , college TEXT
--     , country TEXT
--     , draft_year TEXT
--     , draft_round TEXT
--     , draft_number TEXT
--     , season_stats season_stats[]
--     , scoring_class scoring_class
--     , years_since_last_season INTEGER
--     , current_season INTEGER
--     , is_active BOOLEAN
--     , PRIMARY KEY(player_name, current_season)
-- );


-- THIS is a Seed Query, Since the first year will be null
WITH yesterday AS (
    SELECT *
    FROM players
    WHERE current_season=1995
),
today AS (
    SELECT *
    FROM player_seasons
    WHERE season = 1996
)

-- here, we want to coalesce all of the non-changing values
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name
    , COALESCE(t.height, y.height) AS height
    , COALESCE(t.college, y.college) AS college
    , COALESCE(t.country, y.country) AS country
    , COALESCE(t.draft_year, y.draft_year) AS draft_year
    , COALESCE(t.draft_round, y.draft_round) AS draft_round
    , COALESCE(t.draft_number, y.draft_number) AS draft_number
    , CASE WHEN y.season_stats IS NULL
        THEN ARRAY[ROW(
            t.season
            , t.gp
            , t.pts
            , t.reb
            , t.ast
            )::season_stats]
        WHEN t.season IS NOT NULL THEN  y.season_stats || ARRAY[ROW(  -- the || concatenates
            t.season
            , t.gp
            , t.pts
            , t.reb
            , t.ast
            )::season_stats]
        ELSE y.season_stats
        END as season_stats
        , CASE
            WHEN t.season IS NOT NULL THEN
                CASE
                    WHEN t.pts > 20 THEN 'star'
                    WHEN t.pts > 15 THEN 'good'
                    WHEN t.pts > 10 THEN 'average'
                    ELSE 'bad'
                END::scoring_class
            ELSE y.scoring_class
        END AS scoring_class
        , CASE WHEN t.season IS NOT NULL THEN 0
            ELSE y.years_since_last_season + 1
        END AS years_since_last_season
--     , CASE WHEN t.season IS NOT NULL THEN t.season
--         ELSE y.current_season + 1
--         END
        , COALESCE(t.season, y.current_season + 1) as current_season -- just a more elegant version of the above Case
FROM today t
FULL OUTER JOIN yesterday y
    ON t.player_name = y.player_name;



WITH unnested AS (
    SELECT player_name
        , UNNEST(season_stats)::season_stats AS season_stats
    FROM players
    WHERE current_season = 2001
    AND player_name = 'Michael Jordan'
)

SELECT player_name
    , (season_stats::season_stats).*  -- this syntax explodes the array
FROM unnested





----------------------------------------
    -- This was the implemented code from the repo, NOT the lecture. Lecture code is above.

--
--
-- INSERT INTO players
-- WITH years AS (
--     SELECT *
--     FROM GENERATE_SERIES(1996, 2022) AS season
-- ), p AS (
--     SELECT
--         player_name,
--         MIN(season) AS first_season
--     FROM player_seasons
--     GROUP BY player_name
-- ), players_and_seasons AS (
--     SELECT *
--     FROM p
--     JOIN years y
--         ON p.first_season <= y.season
-- ), windowed AS (
--     SELECT
--         pas.player_name,
--         pas.season,
--         ARRAY_REMOVE(
--             ARRAY_AGG(
--                 CASE
--                     WHEN ps.season IS NOT NULL
--                         THEN ROW(
--                             ps.season,
--                             ps.gp,
--                             ps.pts,
--                             ps.reb,
--                             ps.ast
--                         )::season_stats
--                 END)
--             OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
--             NULL
--         ) AS seasons
--     FROM players_and_seasons pas
--     LEFT JOIN player_seasons ps
--         ON pas.player_name = ps.player_name
--         AND pas.season = ps.season
--     ORDER BY pas.player_name, pas.season
-- ), static AS (
--     SELECT
--         player_name,
--         MAX(height) AS height,
--         MAX(college) AS college,
--         MAX(country) AS country,
--         MAX(draft_year) AS draft_year,
--         MAX(draft_round) AS draft_round,
--         MAX(draft_number) AS draft_number
--     FROM player_seasons
--     GROUP BY player_name
-- )
-- SELECT
--     w.player_name,
--     s.height,
--     s.college,
--     s.country,
--     s.draft_year,
--     s.draft_round,
--     s.draft_number,
--     seasons AS season_stats,
--     CASE
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
--         ELSE 'bad'
--     END::scoring_class AS scoring_class,
--     w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
--     w.season,
--     (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
-- FROM windowed w
-- JOIN static s
--     ON w.player_name = s.player_name;



---------------------------------------------------------------------------

--  Lesson 1 Queries
--
-- SELECT
--     player_name
--     , scoring_class
--     , (season_stats[1]::season_stats).pts AS first_season
--     , (season_stats[CARDINALITY(season_stats)]::season_stats).pts AS latest_season
--     , (season_stats[CARDINALITY(season_stats)]::season_stats).pts/
--       CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END AS improvement
-- FROM players
-- WHERE current_season = 2001 AND scoring_class = 'star'
-- ORDER BY improvement DESC;
--
-- WITH unnested AS (
--     SELECT player_name
--         , UNNEST(season_stats)::season_stats AS season_stats
--     FROM players
--     WHERE current_season=2001
-- --     AND player_name = 'Michael Jordan'
-- )
--
-- SELECT player_name,
--        (season_stats::season_stats).*
-- FROM unnested

----------------------------------------------
Lesson 2 Queries
DROP TABLE players_scd;

Creating an SCD Table
CREATE TABLE players_scd(
    player_name TEXT
    , scoring_class scoring_class
    , is_active BOOLEAN
    , start_season INTEGER
    , end_season INTEGER
    , current_season INTEGER
    , PRIMARY KEY(player_name, start_season)
);




INSERT INTO players_scd
WITH with_previous AS (SELECT player_name
                            , current_season
                            , scoring_class
                            , is_active
                            , LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class
                            , LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
                       FROM players
                       WHERE current_season <= 2021
),
with_indicators AS (
    SELECT *
        , CASE
            WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS
    (SELECT *
          , SUM(change_indicator)
            OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
SELECT
    player_name
    , scoring_class
    , is_active
    , MIN(current_season) AS start_season
    , MAX(current_season) AS end_season
    , 2021 AS current_season
FROM with_streaks
GROUP BY player_name
    , streak_identifier
    , is_active
    , scoring_class
ORDER BY player_name
    , streak_identifier


SELECT * FROM players_scd;
-- This is prone to skew and memory issues.

--------------
-- Alternative Query Type: Incremental

CREATE TYPE scd_type AS (
    scoring_class scoring_class
    , is_active BOOLEAN
    , start_season INTEGER
    , end_season INTEGER
                        );

WITH last_season_scd AS (
    SELECT *
    FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
    historical_scd AS (
        SELECT player_name
            , scoring_class
            , is_active
            , start_season
            , end_season
        FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
    ),
    this_season_data AS (
        SELECT *
        FROM players
        WHERE current_season = 2022
    ),
    unchanged_records AS (
        SELECT ts.player_name
            , ts.scoring_class
            , ts.is_active
            , ls.start_season
            , ts.current_season AS end_season
        FROM this_season_data ts
            JOIN last_season_scd ls
            ON ls.player_name = ts.player_name
        WHERE ts.scoring_class = ls.scoring_class
        AND ts.is_active = ls.is_active
    ),
    changed_records AS (
        SELECT ts.player_name
            , UNNEST(ARRAY[
                ROW(
                    ls.scoring_class
                    , ls.is_active
                    , ls.start_season
                    , ls.end_season
                    )::scd_type
                , ROW(
                    ts.scoring_class
                    , ts.is_active
                    , ts.current_season
                    , ts.current_season
                    )::scd_type
                ]) records
        FROM this_season_data ts
            LEFT JOIN last_season_scd ls
            ON ls.player_name = ts.player_name
        WHERE (ts.scoring_class <> ls.scoring_class
            OR ts.is_active <> ls.is_active)
    ),
    unnested_changed_records AS (
        SELECT player_name
            , (records::scd_type).scoring_class
            , (records::scd_type).is_active
            , (records::scd_type).start_season
            , (records::scd_type).end_season
        FROM changed_records
    ),
    new_records AS (
        SELECT
            ts.player_name
            , ts.scoring_class
            , ts.is_active
            , ts.current_season AS start_season
            , ts.current_season AS end_season
        FROM this_season_data ts
    )

SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL

SELECT * FROM new_records


-----------------
-- Lab 3: Graph Modeling

-- CREATE TYPE vertex_type
--     AS ENUM('player', 'team', 'game');
--
-- CREATE TABLE vertices (
--         identifier TEXT
--       , type vertex_type
--       , properties json
--       , PRIMARY KEY (identifier, type)
-- );

-- CREATE TYPE edge_type AS
--     ENUM ('plays_against'
--         , 'shares_team'
--         , 'plays_in'
--         , 'plays_on'
--         );

-- CREATE TABLE edges (
--      subject_identifier TEXT
--    , subject_type vertex_type
--    , object_identifier TEXT
--    , object_type vertex_type
--    , edge_type edge_type
--    , properties JSON
--    , PRIMARY KEY (subject_identifier
--        , subject_type
--        , object_identifier
--        , object_type
--        , edge_type)
-- );


-- INSERT INTO vertices
-- SELECT
--     game_id as identifier
--     , 'game'::vertex_type AS type
--     , json_build_object(
--       'pts_home', pts_home
--       , 'pts_away', pts_away
--       , 'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
--   ) as properties
-- FROM games;

--
-- INSERT INTO vertices
-- WITH player_agg AS (
-- SELECT
--     player_id AS identifier
--     , MAX(player_name) AS player_name
--     , COUNT(1) as number_of_games
--     , SUM(pts) as total_points
--     , ARRAY_AGG(DISTINCT team_id) AS teams
--
-- FROM game_details
-- GROUP BY player_id
-- )
-- SELECT identifier
--     , 'player'::vertex_type
--     , json_build_object('player_name', player_name
--         , 'number_of_games', number_of_games
--         , 'total_points', total_points
--         , 'teams', teams
--       )
-- FROM player_agg;
--
-- INSERT INTO vertices
-- WITH teams_deduped AS (
--     SELECT *
--         , ROW_NUMBER() OVER(PARTITION BY  team_id) AS row_num
--     FROM teams
-- )
-- SELECT
--     team_id AS identifier
--     , 'team'::vertex_type AS type
--     , json_build_object(
--       'abbreviation', abbreviation
--       , 'nickname', nickname
--       , 'city', city
--       , 'arena', arena
--       , 'year_founded', yearfounded
--       )
-- FROM teams_deduped
-- WHERE row_num = 1;
--

-----------Player Game Edges
-- INSERT INTO edges
-- WITH deduped AS (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
--     FROM game_details
-- )
-- SELECT
--     player_id AS subject_identifier
-- , 'player'::vertex_type AS subject_type
-- , game_id AS object_identifier
-- , 'game'::vertex_type AS object_type
-- , 'plays_in'::edge_type AS edge_type
-- , json_build_object(
--   'start_position', start_position
--   , 'pts', pts
--   , 'team_id', team_id
--   , 'team_abbreviation', team_abbreviation
--   ) AS properties
-- FROM deduped
-- WHERE row_num = 1;


-- SELECT type, COUNT(1)
-- FROM vertices
-- GROUP BY 1;
--
-- SELECT
--     v.properties->>'player_name'
--     , MAX(CAST(e.properties->>'pts' AS INTEGER))
-- FROM vertices v
--     JOIN edges e
--     ON e.subject_identifier = v.identifier
--     AND e.subject_type = v.type
-- WHERE CAST(e.properties->>'pts' AS INTEGER) IS NOT NULL
-- GROUP BY 1
-- ORDER BY 2 DESC;


---------------Player Player Edges
--
-- INSERT INTO edges
-- WITH deduped AS (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
--     FROM game_details
-- ),
-- filtered AS (
--     SELECT * FROM deduped
--     WHERE row_num=1
-- ),
-- aggregated AS (
--     SELECT
--         f1.player_id AS subject_player_id
--         , f2.player_id AS object_player_id
--         , CASE WHEN (f1.team_abbreviation = f2.team_abbreviation)
--             THEN 'shares_team'::edge_type
--             ELSE 'plays_against'::edge_type
--             END AS edge_type
--         , MAX(f2.player_name) AS object_player_name
--         , MAX(f1.player_name) AS subject_player_name
--         , COUNT(1) AS num_games
--         , SUM(f1.pts) AS subject_points
--         , SUM(f2.pts) AS object_points
--     FROM filtered f1
--         JOIN filtered f2
--             ON f1.game_id = f2.game_id
--             AND f1.player_id <> f2.player_id
--     WHERE f1.player_id > f2.player_id
--     GROUP BY f1.player_id
--         , f2.player_id
--         , CASE WHEN (f1.team_abbreviation = f2.team_abbreviation)
--             THEN 'shares_team'::edge_type
--             ELSE 'plays_against'::edge_type
--             END
-- )
-- SELECT
--     subject_player_id AS subject_identifier
--     , 'player'::vertex_type AS subject_type
--     , object_player_id AS object_identifier
--     , 'player'::vertex_type AS object_type
--     , edge_type AS edge_type
--     , json_build_object(
--       'num_games', num_games
--       , 'subject_points', subject_points
--       , 'object_points', object_points
--       )
-- FROM aggregated;
--
-- SELECT v.properties->>'player_name'
--     , e.object_identifier
--     , CASE WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1
--         ELSE  CAST(v.properties->>'total_points' AS REAL)
--         END / CAST(v.properties->>'number_of_games' AS REAL)
--     , e.properties->>'subject_points'
--     , e.properties->>'num_games'
-- FROM vertices v
--     JOIN edges e
--         ON v.identifier = e.subject_identifier
--         AND v.type = e.subject_type
-- WHERE e.object_type = 'player'::vertex_type







