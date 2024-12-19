/* STEP 0:
 So far so good. Now, we drop the players table and create
 it again only adding two more attributes this time:
 scoring_class and years_since_last_season */
-- drop table players; 
/* ---------------------------------------------------------------------------------------*/
/* STEP 1: Create the players table.
 Note that the season_stats struct is still defined. */
-- CREATE TYPE scoring_class AS ENUM ('bad', 'average', 'good', 'star');
-- CREATE TABLE players (
--     player_name TEXT,
--     height TEXT,
--     college TEXT,
--     country TEXT,
--     draft_year TEXT,
--     draft_round TEXT,
--     draft_number TEXT,
--     seasons season_stats [],
--     scoring_class scoring_class,
--     years_since_last_season Integer,
--     current_season INTEGER,
--     PRIMARY KEY (player_name, current_season)
-- );
/* ---------------------------------------------------------------------------------------*/
/* STEP 2: Populate the players table using player_seasons 1996 data */
-- INSERT into players WITH yesterday AS (
--         SELECT *
--         FROM players
--         WHERE current_season = 1995
--     ),
--     today AS (
--         SELECT *
--         FROM player_seasons
--         WHERE season = 1996
--     )
-- SELECT -- we don't need to repeat the constant info twice
--     COALESCE (t.player_name, y.player_name) AS player_name,
--     COALESCE (t.height, y.height) AS height,
--     COALESCE (t.college, y.college) AS college,
--     COALESCE (t.country, y.country) AS country,
--     COALESCE(t.draft_year, y.draft_year) AS draft_year,
--     COALESCE(t.draft_round, y.draft_round) AS draft_round,
--     COALESCE(t.draft_number, y.draft_number) AS draft_number,
--     CASE
--         /* create a season_stats column in the final query result using CASE
--          and y.seasons and t.season values.*/
--         WHEN y.seasons IS NULL THEN ARRAY [ROW(
--             t.season,
--             t.gp,
--             t.pts,
--             t.reb,
--             t.ast,
--             t.weight
--         )::season_stats]
--         WHEN t.season IS NOT NULL THEN y.seasons || ARRAY [ROW(
--         t.season,
--         t.gp,
--         t.pts,
--         t.reb,
--         t.ast,
--         t.weight
--     )::season_stats]
--         ELSE y.seasons
--     END AS season_stats,
--     CASE
--         WHEN t.season IS NOT NULL THEN CASE
--             WHEN t.pts > 20 then 'star'
--             when t.pts > 15 then 'good'
--             when t.pts > 10 then 'average'
--             else 'bad'
--         END::scoring_class
--         ELSE y.scoring_class
--     END AS scoring_class,
--     CASE
--         WHEN t.season IS NOT NULL then 0
--         ELSE y.years_since_last_season + 1
--     END AS years_since_last_season,
--     COALESCE(t.season, y.current_season + 1) as current_season
-- FROM today t
--     FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;
/* note that years_since_last_season is 0 for all players */
/* -----------------------------------------------------------------------*/
/* STEP3: repeat the same query for: yesterday 1996 and today 1997
 until yesterday 2000 and today 2001 */
-- INSERT into players WITH yesterday AS (
--         SELECT *
--         FROM players
--         WHERE current_season = 2000
--     ),
--     today AS (
--         SELECT *
--         FROM player_seasons
--         WHERE season = 2001
--     )
-- SELECT -- we don't need to repeat the constant info twice
--     COALESCE (t.player_name, y.player_name) AS player_name,
--     COALESCE (t.height, y.height) AS height,
--     COALESCE (t.college, y.college) AS college,
--     COALESCE (t.country, y.country) AS country,
--     COALESCE(t.draft_year, y.draft_year) AS draft_year,
--     COALESCE(t.draft_round, y.draft_round) AS draft_round,
--     COALESCE(t.draft_number, y.draft_number) AS draft_number,
--     CASE
--         /* create a season_stats column in the final query result using CASE
--          and y.seasons and t.season values.*/
--         WHEN y.seasons IS NULL THEN ARRAY [ROW(
--             t.season,
--             t.gp,
--             t.pts,
--             t.reb,
--             t.ast,
--             t.weight
--         )::season_stats]
--         WHEN t.season IS NOT NULL THEN y.seasons || ARRAY [ROW(
--         t.season,
--         t.gp,
--         t.pts,
--         t.reb,
--         t.ast,
--         t.weight
--     )::season_stats]
--         ELSE y.seasons
--     END AS season_stats,
--     CASE
--         WHEN t.season IS NOT NULL THEN CASE
--             WHEN t.pts > 20 then 'star'
--             when t.pts > 15 then 'good'
--             when t.pts > 10 then 'average'
--             else 'bad'
--         END::scoring_class
--         ELSE y.scoring_class
--     END AS scoring_class,
--     CASE
--         WHEN t.season IS NOT NULL then 0
--         ELSE y.years_since_last_season + 1
--     END AS years_since_last_season,
--     COALESCE(t.season, y.current_season + 1) as current_season
-- FROM today t
--     FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;
/* ----------------------------------------------------------------------*/
/* STEP4: let us have a look at the result */
select *
from players
where current_season = 2001;
/* --------------------------------------------------------------------------*/
/* STEP5: Example of specific player */
select *
from players
where player_name = 'Michael Jordan';
/* --------------------------------------------------------------------------*/