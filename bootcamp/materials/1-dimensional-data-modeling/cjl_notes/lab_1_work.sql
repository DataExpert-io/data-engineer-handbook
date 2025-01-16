-----------------------------------------------------------------------------------------------------------
----- Create 1 row per player (with an array for all their seasons). This removes temporal component! -----
-----------------------------------------------------------------------------------------------------------

/* Create STRUCT */
-- CREATE TYPE season_stats AS (
--     season INTEGER
--   , gp INTEGER
--   , pts REAL
--   , reb REAL
--   , ast REAL
-- )

-- CREATE TABLE players (
--     player_name TEXT
--   , height TEXT
--   , college TEXT
--   , country TEXT
--   , draft_year TEXT
--   , draft_round TEXT
--   , draft_number TEXT
--   , season_stats season_stats[]
--   , current_season INTEGER
--   , PRIMARY KEY(player_name, current_season)
-- )

INSERT INTO players
WITH yesterday AS (
  SELECT *
  FROM players
  WHERE 1=1
        -- AND current_season = 1995 -- SEED QUERY
		-- AND current_season = 1996
		-- AND current_season = 1997
		-- AND current_season = 1998
		-- AND current_season = 1999
		AND current_season = 2000
)
, today AS (
  SELECT *
  FROM player_seasons
  WHERE 1=1
        -- AND season = 1996 -- SEED QUERY
		-- AND season = 1997
		-- AND season = 1998
		-- AND season = 1999
		-- AND season = 2000
		AND season = 2001
)

SELECT 
  COALESCE(t.player_name, y.player_name) AS player_name
, COALESCE(t.height, y.height) AS height
, COALESCE(t.college, y.college) AS college
, COALESCE(t.country, y.country) AS country
, COALESCE(t.draft_year, y.draft_year) AS draft_year
, COALESCE(t.draft_round, y.draft_round) AS draft_round
, COALESCE(t.draft_number, y.draft_number) AS draft_number
, CASE 
	-- if new player with stats
    WHEN y.season_stats IS NULL THEN ARRAY[ROW(
		  t.season
		, t.gp
		, t.pts
		, t.reb
		, t.ast
		)
		::season_stats] -- CASTS this as the actual struct type we defined earlier
	-- if new stats for player exist
	WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
				  t.season
				, t.gp
				, t.pts
				, t.reb
				, t.ast
				)
			::season_stats] 
	-- else, carry through existing stats
	ELSE y.season_stats
  END AS season_stats

  
, CASE 
    WHEN t.season IS NOT NULL THEN 
	  CASE WHEN t.pts > 20 THEN 'star'
	 	   WHEN t.pts > 15 THEN 'good'
		   WHEN t.pts >10 THEN 'average'
		   ELSE 'bad'
	  END::scoring_class
	-- else, just pull in their most recent scoring_class
	ELSE y.scoring_class::scoring_class
  END 
, CASE 
    WHEN t.season IS NOT NULL THEN 0 
	ELSE y.years_since_last_season + 1
  END AS years_since_last_season

  
, COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y 
  ON t.player_name = y.player_name


-- SELECT * FROM players 
-- WHERE 1=1 
--       -- AND player_name = 'Aaron McKie'
-- 	  -- AND current_season = 2001
-- 	  AND player_name = 'Michael Jordan'

-- /* You can UNNEST it, too, to get the original format back! */
-- WITH unnested_data AS (
--   SELECT 
--     player_name
--   , UNNEST(season_stats)::season_stats AS season_stats
--   FROM players
--   WHERE 1=1
-- 	    AND current_season = 2001
-- 	    AND player_name = 'Michael Jordan'
-- )

-- SELECT 
--   player_name
-- -- , (season_stats::season_stats).season
-- -- , (season_stats::season_stats).gp
-- -- , (season_stats::season_stats).pts
-- -- , (season_stats::season_stats).reb
-- -- , (season_stats::season_stats).ast
-- , (season_stats::season_stats).*
-- FROM unnested_data






/* Re-creating players to do _____*/
-- DROP TABLE players 

-- CREATE TYPE scoring_class AS ENUM('star', 'good', 'average', 'bad');

-- CREATE TABLE players (
--     player_name TEXT
--   , height TEXT
--   , college TEXT
--   , country TEXT
--   , draft_year TEXT
--   , draft_round TEXT
--   , draft_number TEXT
--   , season_stats season_stats[]
--   , scoring_class scoring_class
--   , years_since_last_season INTEGER
--   , current_season INTEGER
--   , PRIMARY KEY(player_name, current_season)
-- );

-- WENT BACK UP TO CTEs & ADDED STUFF!

-- SELECT *
-- FROM players
-- WHERE 1=1
-- 	  -- AND current_season = 2001
-- 	  AND player_name = 'Michael Jordan'


/* which player had biggest improvement from their first season to their most recent? */
SELECT
  player_name
, (season_stats[1]::season_stats).pts AS first_season_pts
, (season_stats[CARDINALITY(season_stats)]::season_stats).pts AS latest_season_pts
, CASE
    WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
	ELSE (season_stats[CARDINALITY(season_stats)]::season_stats).pts / (season_stats[1]::season_stats).pts
  END AS pts_improvement_pct
FROM players
WHERE 1=1
	  AND current_season = 2001
	  AND scoring_class = 'star'
ORDER BY 4 DESC



