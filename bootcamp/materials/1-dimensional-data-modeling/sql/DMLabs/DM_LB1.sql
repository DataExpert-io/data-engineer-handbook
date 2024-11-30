
-- CREATE TYPE seasons_stats AS (
--     seasons INTEGER,
--     gp INTEGER,
--     pts REAL,
--     reb REAL,
--     ast REAL,
--     weight INTEGER
-- );

--- Add 2 More class 
-- CREATE TYPE scoring_class as ENUM ('star','good','avg','bad');

-- DROPing DATA TYPE
-- DROP TYPE IF EXISTS scoring_class CASCADE;
-- DROP TYPE IF EXISTS season_stats CASCADE;

-- DROP TABLE players
-- - Creating Player Table:

-- CREATE TABLE players (
--     player_name TEXT,
--     height TEXT,
--     college TEXT,
--     country TEXT,
--     draft_year TEXT,
--     draft_round TEXT,
--     draft_number TEXT,
--     seasons_stats seasons_stats[], -- Using the defined composite type as an array
--     current_season INTEGER,
--     scoring_class scoring_class,
--     years_since_last_season  INTEGER,
--     is_active BOOLEAN,
--     PRIMARY KEY (player_name, current_season)
    

-- );

--Check values in players table 
-- SELECT * FROM players

--- Data Pipe line Query Where we append data in nested form form 1995 t0 2001 
-- we have used 2 table 1 as today which content latest data
-- Other is yesterday which contain data till last update

-- INSERT INTO players (
--     player_name,
--     height,
--     college,
--     country,
--     draft_year,
--     draft_round,
--     seasons_stats,
--     scoring_class,
--     years_since_last_season,
--     is_active,
--     current_season
-- )

-- WITH 
-- yesterday AS (
--     SELECT * FROM players 
--     WHERE current_season = 2020
-- ),
-- today AS (
--     SELECT * FROM player_seasons
--     WHERE season = 2021
-- )
-- SELECT 
--     COALESCE(t.player_name, y.player_name) AS player_name,
--     COALESCE(t.height, y.height) AS height,
--     COALESCE(t.college, y.college) AS college,
--     COALESCE(t.country, y.country) AS country,
--     COALESCE(t.draft_year, y.draft_year) AS draft_year,
--     COALESCE(t.draft_round, y.draft_round) AS draft_round,
--     CASE 
--     -- 1st case when season year is null then we create initial value 
--         WHEN y.seasons_stats IS NULL THEN 
--             ARRAY[ROW(
--                 t.season,
--                 t.gp,
--                 t.pts,
--                 t.reb,
--                 t.ast,
--                 t.weight
--             )::seasons_stats]
--      -- if its not null then then appending the new data with last years data 
--         WHEN t.season IS NOT NULL THEN 
--             y.seasons_stats || ARRAY[ROW(
--                 t.season,
--                 t.gp,
--                 t.pts,
--                 t.reb,
--                 t.ast,
--                 t.weight
--             )::seasons_stats]
--     -- if last years data exist then holding up that value  
--         ELSE 
--             y.seasons_stats
--     END AS seasons_stats,
--     --- adding condition for scoring class
--     CASE
--              WHEN t.season IS NOT NULL THEN
--                  (CASE WHEN t.pts > 20 THEN 'star'
--                     WHEN t.pts > 15 THEN 'good'
--                     WHEN t.pts > 10 THEN 'avg'
--                     ELSE 'bad' END)::scoring_class
--              ELSE y.scoring_class
--          END as scoring_class, 

--     CASE WHEN t.season IS NOT NULL  THEN 0 
--     ELSE  y.years_since_last_season + 1 END as years_since_last_season,
--     t.season IS NOT NULL as is_active,

--     COALESCE(t.season, y.current_season + 1) AS current_season
-- FROM today t
-- FULL OUTER JOIN yesterday y
-- ON t.player_name = y.player_name;



---- EXtracting All the NESTED Data Form season_data Array 
-- SELECT * FROM players WHERE current_season = 2021
-- AND player_name = 'Michael Jordan';

-- SELECT 
--     player_name,
--     season_data.*    -- Extracting the season from the composite type
--     -- season_data.gp,         -- Extracting games played (gp)
--     -- season_data.pts,        -- Extracting points (pts)
--     -- season_data.reb,        -- Extracting rebounds (reb)
--     -- season_data.ast,        -- Extracting assists (ast)
--     -- season_data.weight      -- Extracting weight
-- FROM players,
--      UNNEST(seasons_stats) AS season_data   -- UNNEST the seasons_stats array
-- WHERE current_season = 2001
--  -- AND player_name = 'Michael Jordan';

-- DROP Table
--DROP TABLE players;


--- Check the player improvement 
-- SELECT  
--     player_name,
--     (seasons_stats[CARDINALITY(seasons_stats)]::seasons_stats).pts / 
--     CASE 
--         WHEN (seasons_stats[1]::seasons_stats).pts = 0 THEN 1 
--         ELSE (seasons_stats[1]::seasons_stats).pts 
--     END AS pts_ratio
-- FROM players
-- WHERE current_season=2001 AND scoring_class='star'
-- ORDER BY 2 DESC;






