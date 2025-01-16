---------------------------------------------------
----- LAB 2: SLOWLY CHANGING DIMENSIONS (SCD) -----
---------------------------------------------------

/* SETUP */

-- DROP TABLE players;
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
--   , is_active BOOLEAN
--   , PRIMARY KEY(player_name, current_season)
-- );

-- INSERT INTO players
-- WITH years AS (
--   SELECT *
--   FROM GENERATE_SERIES(1996, 2022) AS season
-- )

-- , p AS (
--   SELECT 
--     player_name
--   , MIN(season) AS first_season
--   FROM player_seasons
--   GROUP BY 1
-- )

-- , players_and_seasons AS (
--   SELECT 
--     *
--   FROM p 
--   INNER JOIN years y
--     ON p.first_season <= y.season
-- )

-- , windowed AS (
--   SELECT 
--     pas.player_name
--   , pas.season
--   , ARRAY_REMOVE(
--   	  ARRAY_AGG(
-- 	    CASE 
-- 		  WHEN ps.season IS NOT NULL THEN ROW(
--             ps.season
-- 		  , ps.gp
-- 		  , ps.pts
-- 		  , ps.reb
-- 		  , ps.ast
-- 		  )::season_stats
-- 		END
--   	  )
-- 	 OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
-- 	 NULL
-- 	) AS seasons
--   FROM players_and_seasons pas 
--   LEFT JOIN player_seasons ps
--     ON pas.player_name = ps.player_name AND pas.season = ps.season
--   ORDER BY pas.player_name, pas.season
-- )

-- , static AS (
--   SELECT 
--     player_name
--   , MAX(height) AS height
--   , MAX(college) AS college
--   , MAX(country) AS country
--   , MAX(draft_year) AS draft_year
--   , MAX(draft_round) AS draft_round
--   , MAX(draft_number) AS draft_number
--   FROM player_seasons
--   GROUP BY 1
-- )

-- SELECT 
--   w.player_name
-- , s.height
-- , s.college
-- , s.country
-- , s.draft_year
-- , s.draft_round
-- , s.draft_number
-- , seasons AS season_stats
-- , CASE 
--     WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
-- 	WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
-- 	WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
-- 	ELSE 'bad'
--   END::scoring_class AS scoring_class
-- , w.season - (seasons[CARDINALITY(seasons)]::season_stats).season AS years_since_last_active
-- , w.season
-- , (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
-- FROM windowed w 
-- INNER JOIN static s 
-- 	ON w.player_name = s.player_name;


-- SELECT * FROM players LIMIT 100



/* SCD PREP */
-- CREATE TABLE players_scd (
--   player_name TEXT,
--   scoring_class scoring_class,
--   is_active BOOLEAN,
--   start_season INTEGER,
--   end_season INTEGER,
--   current_season INTEGER,
--   PRIMARY KEY(player_name, start_season, end_season)
-- );



/* CREATE SCD TABLE IN 1 QUERY */
-- INSERT INTO players_scd
-- WITH with_previous AS (
--   SELECT 
--     player_name
--   , current_season
--   , scoring_class
--   , is_active
--   , LAG(scoring_class) OVER(PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class
--   , LAG(is_active) OVER(PARTITION BY player_name ORDER BY current_season) AS previous_is_active
--   FROM players
-- )

-- , with_indicators AS (
--   SELECT 
--     *
--   , CASE 
--       WHEN scoring_class <> previous_scoring_class THEN 1 
--    	  WHEN is_active <> previous_is_active THEN 1 
-- 	  ELSE 0 
-- 	END AS change_indicator
--   FROM with_previous
--   WHERE 1=1
--   		AND current_season <= 2021
-- )

-- , with_streaks AS (
--   SELECT 
--     *
--   , SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
--   FROM with_indicators
--   WHERE 1=1
-- )

-- -- SELECT * FROM with_streaks WHERE player_name = 'Aaron Brooks'

-- , final_data AS (
--   SELECT 
--     player_name
--   , scoring_class
--   , is_active
--   , streak_identifier
--   , MIN(current_season) AS start_season
--   , MAX(current_season) AS end_season
--   , 2021 AS current_season
--   FROM with_streaks
--   GROUP BY 1,2,3,4
--   ORDER BY player_name, start_season
-- )

-- -- SELECT * FROM final_data WHERE player_name = 'Aaron Brooks';

-- SELECT 
--   player_name
-- , scoring_class
-- , is_active
-- , start_season
-- , end_season
-- , current_season
-- FROM final_data



-- SELECT * 
-- FROM players_scd 
-- WHERE player_name = 'Aaron Brooks';
/* 
	This version works great, and will work in production at scale. But there may be a better way
	to do this to avoid things such as unpredictable cardinality (i.e. one player's changing a ton, others not as much...)
*/


-- CREATE TYPE scd_type AS (
--   scoring_class scoring_class
-- , is_active BOOLEAN
-- , start_season INTEGER
-- , end_season INTEGER
-- );


WITH historical_scd AS (
  SELECT 
    player_name
  , scoring_class
  , is_active
  , start_season
  , end_season
  FROM players_scd
  WHERE 1=1
        AND current_season = 2021
		AND end_season < 2021 -- records that are done, will not change
)

, last_season_scd AS (
  SELECT *
  FROM players_scd
  WHERE 1=1
        AND current_season = 2021
		AND end_season = 2021
)

, this_season_data AS ( 
    SELECT *
	FROM players
	WHERE 1=1
		  AND current_season = 2022
)

, unchanged_records AS (
  SELECT 
    ts.player_name
  , ts.scoring_class
  , ts.is_active
  , ls.start_season 
  , ts.current_season AS end_season
  FROM this_season_data ts
  INNER JOIN last_season_scd ls
    ON ts.player_name = ls.player_name
  WHERE 1=1
        AND ts.scoring_class = ls.scoring_class
		AND ts.is_active = ls.is_active
)

-- SELECT * FROM unchanged_records WHERE player_name = 'Aaron Brooks'

/* all players that changed in 'current_year' (i.e. 2022) */
, changed_records AS (
  SELECT 
    ts.player_name
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
  ]) AS records
  FROM this_season_data ts
  LEFT JOIN last_season_scd ls
    ON ts.player_name = ls.player_name
  WHERE 1=1
        AND (
		     ts.scoring_class <> ls.scoring_class
		  OR ts.is_active <> ls.is_active
		)
)

, unnested_changed_records AS (
  SELECT 
    player_name
  , (records::scd_type).scoring_class
  , (records::scd_type).is_active
  , (records::scd_type).start_season
  , (records::scd_type).end_season
  FROM changed_records
)

-- SELECT * FROM unnested_changed_records WHERE player_name = 'Aaron Brooks'

, new_records AS (
  SELECT 
    ts.player_name
  , ts.scoring_class
  , ts.is_active
  , ts.current_season AS start_season
  , ts.current_season AS end_season
  FROM this_season_data ts 
  LEFT JOIN last_season_scd ls
    ON ts.player_name = ls.player_name
  WHERE 1=1
  		AND ls.player_name IS NULL
)

-- SELECT * FROM new_records

SELECT * FROM historical_scd

UNION ALL 

SELECT * FROM unchanged_records 

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL 

SELECT * FROM new_records


